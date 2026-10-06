package workflow

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// scriptedGateActivity 按调用序号返回脚本化结果：
//   - block 命中的那次调用先发出 started 信号，再阻塞到 release 关闭（模拟执行中崩溃被遗弃）；
//   - transient 命中的调用返回瞬时错误（业务真实失败，有闭环）；
//   - 其余调用成功。
type scriptedGateActivity struct {
	mu        sync.Mutex
	calls     int
	block     map[int]bool // 命中的调用先上报 started 再阻塞到 release 关闭
	transient map[int]bool
	started   chan int // 有调用进入阻塞时上报其调用序号
	release   chan struct{}
	se        *testSideEffect
}

func (g *scriptedGateActivity) fn() ActivityFunc {
	return func(ctx ActivityContext, in ActivityInput) (string, error) {
		g.mu.Lock()
		g.calls++
		n := g.calls
		g.mu.Unlock()
		if g.block[n] {
			g.started <- n
			<-g.release
		}
		if g.transient[n] {
			return "", Transientf("boom at attempt %d", n)
		}
		g.se.apply(in.IdempotencyToken)
		return "ok@" + in.IdempotencyToken, nil
	}
}

// crashAndReopen 模拟“杀掉旧进程并换人接管”：关闭当前存储、以新 worker 重新打开，
// 再把假时钟推进到旧租约过期之后。
func crashAndReopen(t *testing.T, env *testEnv, def WorkflowDef, act *scriptedGateActivity, worker string) *Engine {
	t.Helper()
	if err := env.store.Close(); err != nil {
		t.Fatalf("close store (crash): %v", err)
	}
	store, err := NewFileStore(env.dir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	env.store = store
	e := NewEngine(store, Options{WorkerID: worker, LeaseDuration: 10 * time.Second, Clock: env.clock, Logger: env.log})
	mustDef(e, def)
	e.RegisterActivity("gate", act.fn())
	env.clock.Advance(11 * time.Second) // 旧租约过期，允许接管
	return e
}

func singleStepGateDef(maxAttempts int) WorkflowDef {
	return WorkflowDef{Name: "audit-interrupt", Steps: []StepDef{
		{Name: "only", Activity: "gate", Retry: RetryPolicy{MaxAttempts: maxAttempts}},
	}}
}

// reopenReadOnly 模拟“再起一个进程只读复盘”：不动 env.store（Close 为空操作，
// 同目录可安全并存多个 FileStore），返回一个独立引擎，测试只用它读盘做 Audit。
func (env *testEnv) reopenReadOnly(t *testing.T, worker string) *Engine {
	t.Helper()
	store, err := NewFileStore(env.dir)
	if err != nil {
		t.Fatalf("open read-only store: %v", err)
	}
	return NewEngine(store, Options{WorkerID: worker, LeaseDuration: 10 * time.Second, Clock: env.clock, Logger: env.log})
}

// 场景 1：崩溃打断的正好是允许的最后一次尝试，实例因预算耗尽判失败。
// 复盘必须把最后一次记为 interrupted（被中断、没跑出业务结果），
// 与前两次真实的瞬时失败区分；重启后复盘逐字段一致。
func TestAuditInterruptedLastBudgetExhausted(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e1 := env.newEngine(t, "old-worker", 10*time.Second)
	def := singleStepGateDef(3)
	mustDef(e1, def)
	gate := &scriptedGateActivity{
		block:     map[int]bool{3: true}, // 第 3 次（最后一次预算）执行中崩溃
		transient: map[int]bool{1: true, 2: true},
		started:   make(chan int, 4), release: make(chan struct{}), se: env.se,
	}
	e1.RegisterActivity("gate", gate.fn())
	t.Cleanup(func() { close(gate.release) }) // 放行被遗弃的旧协程，避免泄漏

	if _, err := e1.Start(ctx, "wf-i1", "audit-interrupt", "x"); err != nil {
		t.Fatal(err)
	}
	// 前两次是真实的瞬时失败（零退避），每次 Tick 只推进一步。
	if _, err := e1.Tick(ctx); err != nil {
		t.Fatalf("tick attempt1: %v", err)
	}
	if _, err := e1.Tick(ctx); err != nil {
		t.Fatalf("tick attempt2: %v", err)
	}
	st, _ := e1.Get(ctx, "wf-i1")
	if st.Steps[0].Attempt != 2 || st.Steps[0].Status != StepWaiting {
		t.Fatalf("precondition: attempt=2 waiting, got %+v", st.Steps[0])
	}
	// 第三次尝试执行中“崩溃”。
	tickDone := make(chan error, 1)
	go func() { _, err := e1.Tick(ctx); tickDone <- err }()
	<-gate.started

	e2 := crashAndReopen(t, env, def, gate, "new-worker")
	// 接管者发现 Attempt=3 已耗尽预算：不再发起活动调用，直接落账预算耗尽失败。
	if _, err := e2.Tick(ctx); err != nil {
		t.Fatalf("takeover tick: %v", err)
	}
	st, _ = e2.Get(ctx, "wf-i1")
	if st.Status != WFFailed || st.Steps[0].Status != StepFailed || st.Steps[0].Attempt != 3 {
		t.Fatalf("expected failed with attempt=3, got %+v step=%+v", st, st.Steps[0])
	}
	if !strings.Contains(st.Steps[0].FinalReason, "interrupted") {
		t.Fatalf("final reason must say interrupted, got %q", st.Steps[0].FinalReason)
	}
	if gate.calls != 3 { // 接管后不得多发起一次活动调用
		t.Fatalf("no new activity call allowed after budget exhausted, calls=%d", gate.calls)
	}

	rep, err := e2.Audit(ctx, "wf-i1")
	if err != nil {
		t.Fatalf("Audit: %v", err)
	}
	if len(rep.Attempts) != 3 {
		t.Fatalf("expected 3 attempt records, got %+v", rep.Attempts)
	}
	want := []struct {
		attempt   int
		outcome   AttemptOutcome
		class     FailureClass
		remaining int
	}{
		{1, OutcomeFailed, FailureTransient, 2}, // 业务真实瞬时失败
		{2, OutcomeFailed, FailureTransient, 1}, // 业务真实瞬时失败
		{3, OutcomeInterrupted, FailureNone, 0}, // 被中断用光最后一次预算
	}
	for i, w := range want {
		got := rep.Attempts[i]
		if got.Attempt != w.attempt || got.Outcome != w.outcome ||
			got.Class != w.class || got.RemainingBudget != w.remaining {
			t.Errorf("attempt[%d]=%+v, want attempt=%d outcome=%s class=%q remaining=%d",
				i, got, w.attempt, w.outcome, w.class, w.remaining)
		}
		if got.EndedAt == nil {
			t.Errorf("attempt[%d] must be closed: %+v", i, got)
		}
		t.Logf("scenario=interrupted-budget-exhausted attempt=%d outcome=%s class=%q remaining=%d ended=%v",
			got.Attempt, got.Outcome, got.Class, got.RemainingBudget, got.EndedAt != nil)
	}
	if rep.Status != WFFailed {
		t.Fatalf("report status=%s, want failed", rep.Status)
	}

	// 进程重启后重新复盘：结论必须逐字段一致。
	e3 := env.reopen(t, "third-worker")
	mustDef(e3, def)
	rep2, err := e3.Audit(ctx, "wf-i1")
	if err != nil {
		t.Fatalf("Audit after restart: %v", err)
	}
	if !reflect.DeepEqual(rep, rep2) {
		t.Fatalf("audit changed across restart:\n before=%+v\n after=%+v", rep, rep2)
	}
	t.Logf("scenario=interrupted-budget-exhausted result=failed_status=interrupted_last stable_after_restart=true")
}

// 场景 2：崩溃打断一次尝试但预算仍有剩余：接管者继续重试，旧尝试记为 interrupted，
// 后续真实失败/成功各自正常闭环，最终成功；重启后复盘一致。
func TestAuditInterruptedButBudgetRemainsRetrySucceeds(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e1 := env.newEngine(t, "old-worker", 10*time.Second)
	def := singleStepGateDef(3)
	mustDef(e1, def)
	gate := &scriptedGateActivity{
		block:     map[int]bool{1: true}, // 第 1 次执行中崩溃
		transient: map[int]bool{2: true},
		started:   make(chan int, 4), release: make(chan struct{}), se: env.se,
	}
	e1.RegisterActivity("gate", gate.fn())
	t.Cleanup(func() { close(gate.release) })

	if _, err := e1.Start(ctx, "wf-i2", "audit-interrupt", "x"); err != nil {
		t.Fatal(err)
	}
	tickDone := make(chan error, 1)
	go func() { _, err := e1.Tick(ctx); tickDone <- err }()
	<-gate.started

	e2 := crashAndReopen(t, env, def, gate, "new-worker")
	// 预算还剩 2 次：接管后第 2 次真实瞬时失败、第 3 次成功。
	if _, err := e2.Tick(ctx); err != nil {
		t.Fatalf("takeover tick (attempt2): %v", err)
	}
	st, _ := e2.Get(ctx, "wf-i2")
	if st.Steps[0].Attempt != 2 || st.Steps[0].Status != StepWaiting {
		t.Fatalf("precondition after retry-fail: %+v", st.Steps[0])
	}
	if _, err := e2.Tick(ctx); err != nil {
		t.Fatalf("tick (attempt3): %v", err)
	}
	st, _ = e2.Get(ctx, "wf-i2")
	if st.Status != WFCompleted || st.Steps[0].Status != StepSucceeded || st.Steps[0].Attempt != 3 {
		t.Fatalf("expected completed at attempt3, got %+v step=%+v", st, st.Steps[0])
	}
	if gate.calls != 3 {
		t.Fatalf("expected 3 activity calls, got %d", gate.calls)
	}

	rep, err := e2.Audit(ctx, "wf-i2")
	if err != nil {
		t.Fatalf("Audit: %v", err)
	}
	if len(rep.Attempts) != 3 {
		t.Fatalf("expected 3 attempt records, got %+v", rep.Attempts)
	}
	want := []struct {
		attempt   int
		outcome   AttemptOutcome
		class     FailureClass
		remaining int
	}{
		{1, OutcomeInterrupted, FailureNone, 2}, // 崩溃被遗弃，无闭环、无结束时间
		{2, OutcomeFailed, FailureTransient, 1}, // 业务真实瞬时失败
		{3, OutcomeSucceeded, FailureNone, 0},   // 成功收尾
	}
	var inProgress int
	for i, w := range want {
		got := rep.Attempts[i]
		if got.Outcome == OutcomeInProgress {
			inProgress++
		}
		if got.Attempt != w.attempt || got.Outcome != w.outcome ||
			got.Class != w.class || got.RemainingBudget != w.remaining {
			t.Errorf("attempt[%d]=%+v, want attempt=%d outcome=%s class=%q remaining=%d",
				i, got, w.attempt, w.outcome, w.class, w.remaining)
		}
		if w.outcome == OutcomeInterrupted && got.EndedAt != nil {
			t.Errorf("abandoned attempt %d must have no end time, got %+v", w.attempt, got)
		}
		t.Logf("scenario=interrupted-retry-succeeds attempt=%d outcome=%s class=%q remaining=%d",
			got.Attempt, got.Outcome, got.Class, got.RemainingBudget)
	}
	if inProgress != 0 {
		t.Fatalf("completed workflow must have no in_progress rows, got %d", inProgress)
	}

	e3 := env.reopen(t, "third-worker")
	mustDef(e3, def)
	rep2, err := e3.Audit(ctx, "wf-i2")
	if err != nil {
		t.Fatalf("Audit after restart: %v", err)
	}
	if !reflect.DeepEqual(rep, rep2) {
		t.Fatalf("audit changed across restart:\n before=%+v\n after=%+v", rep, rep2)
	}
	t.Logf("scenario=interrupted-retry-succeeds result=completed interrupted=1 failed=1 succeeded=1 stable_after_restart=true")
}

// 场景 3：接管之后，旧的那次尝试被打断、新的尝试正在执行时去查复盘：
// 同一步骤同一时刻只能有一条 in_progress（新尝试），旧尝试必须按真实结局显示为
// interrupted。等新尝试跑完、以及重启后再查，结论分别与当时状态一致。
func TestAuditSingleInProgressAfterTakeover(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e1 := env.newEngine(t, "old-worker", 10*time.Second)
	def := singleStepGateDef(3)
	mustDef(e1, def)
	gate := &scriptedGateActivity{
		block:     map[int]bool{1: true, 2: true}, // 旧尝试崩溃被遗弃；新尝试执行中
		transient: map[int]bool{},
		started:   make(chan int, 4), release: make(chan struct{}), se: env.se,
	}
	e1.RegisterActivity("gate", gate.fn())

	if _, err := e1.Start(ctx, "wf-i3", "audit-interrupt", "x"); err != nil {
		t.Fatal(err)
	}
	tick1 := make(chan error, 1)
	go func() { _, err := e1.Tick(ctx); tick1 <- err }()
	if n := <-gate.started; n != 1 {
		t.Fatalf("expected attempt call 1 to block, got %d", n)
	}

	e2 := crashAndReopen(t, env, def, gate, "new-worker")
	// 接管后第 2 次尝试在执行中（block 命中），此时做一次“在线复盘”。
	tick2 := make(chan error, 1)
	go func() { _, err := e2.Tick(ctx); tick2 <- err }()
	if n := <-gate.started; n != 2 {
		t.Fatalf("expected attempt call 2 to block, got %d", n)
	}

	rep, err := e2.Audit(ctx, "wf-i3")
	if err != nil {
		t.Fatalf("Audit while new attempt in flight: %v", err)
	}
	if len(rep.Attempts) != 2 {
		t.Fatalf("expected 2 attempt records, got %+v", rep.Attempts)
	}
	old, cur := rep.Attempts[0], rep.Attempts[1]
	if old.Attempt != 1 || cur.Attempt != 2 {
		t.Fatalf("attempt numbering wrong: %+v %+v", old, cur)
	}
	if old.Outcome != OutcomeInterrupted || old.EndedAt != nil || old.RemainingBudget != 2 {
		t.Fatalf("old abandoned attempt must be interrupted (no end, remaining=2), got %+v", old)
	}
	if cur.Outcome != OutcomeInProgress || cur.EndedAt != nil || cur.RemainingBudget != 1 {
		t.Fatalf("current attempt must be in_progress (no end, remaining=1), got %+v", cur)
	}
	st, _ := e2.Get(ctx, "wf-i3")
	if st.Lease == nil || cur.LeaseID != st.Lease.Token || old.LeaseID == cur.LeaseID {
		t.Fatalf("in_progress must match the active takeover lease: old=%s cur=%s active=%v",
			old.LeaseID, cur.LeaseID, st.Lease)
	}
	t.Logf("scenario=takeover-single-in-progress attempt1=interrupted attempt2=in_progress in_progress_rows=1")

	// 新尝试跑完成功后再查：旧的仍是 interrupted，新的闭环为 succeeded，没有 in_progress。
	close(gate.release)
	select {
	case err := <-tick2:
		if err != nil {
			t.Fatalf("tick2: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("new attempt did not finish after release")
	}
	st, _ = e2.Get(ctx, "wf-i3")
	if st.Status != WFCompleted {
		t.Fatalf("expected completed, got %s", st.Status)
	}
	rep2, err := e2.Audit(ctx, "wf-i3")
	if err != nil {
		t.Fatalf("Audit after completion: %v", err)
	}
	if len(rep2.Attempts) != 2 {
		t.Fatalf("expected 2 records after completion, got %+v", rep2.Attempts)
	}
	if rep2.Attempts[0].Outcome != OutcomeInterrupted || rep2.Attempts[1].Outcome != OutcomeSucceeded {
		t.Fatalf("post-completion trail wrong: %+v", rep2.Attempts)
	}
	for _, a := range rep2.Attempts {
		if a.Outcome == OutcomeInProgress {
			t.Fatalf("completed workflow must have no in_progress row, got %+v", a)
		}
		// 被遗弃的中断尝试没有闭环事件，本来就没有结束时间；成功/失败才有。
		if a.Outcome != OutcomeInterrupted && a.EndedAt == nil {
			t.Fatalf("closed attempt must have an end time, got %+v", a)
		}
	}
	t.Logf("scenario=takeover-single-in-progress post_completion attempt1=interrupted attempt2=succeeded")

	// 重启后复盘与完成后的复盘逐字段一致（只依赖落盘数据）。
	e3 := env.reopen(t, "third-worker")
	mustDef(e3, def)
	rep3, err := e3.Audit(ctx, "wf-i3")
	if err != nil {
		t.Fatalf("Audit after restart: %v", err)
	}
	if !reflect.DeepEqual(rep2, rep3) {
		t.Fatalf("audit changed across restart:\n before=%+v\n after=%+v", rep2, rep3)
	}
	t.Logf("scenario=takeover-single-in-progress result=stable_after_restart=true")
}

// 场景 4：同一时刻的落盘状态，换一个进程重新打开来复盘，结论必须完全一致——
// 包括“接管后新尝试仍在执行中”这种在途视角；等实例到终态后再查也稳定。
// 该用例只通过独立引擎读盘复盘，结论只能来自已落盘记录。
func TestAuditStableAcrossRestartAtEveryStage(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e1 := env.newEngine(t, "old-worker", 10*time.Second)
	def := singleStepGateDef(3)
	mustDef(e1, def)
	gate := &scriptedGateActivity{
		block:     map[int]bool{1: true, 2: true},
		transient: map[int]bool{},
		started:   make(chan int, 4), release: make(chan struct{}), se: env.se,
	}
	e1.RegisterActivity("gate", gate.fn())

	if _, err := e1.Start(ctx, "wf-i4", "audit-interrupt", "x"); err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = e1.Tick(ctx) }()
	<-gate.started // 第 1 次尝试在途

	// 崩溃换人接管，且接管后的第 2 次尝试也在执行中。
	e2 := crashAndReopen(t, env, def, gate, "new-worker")
	go func() { _, _ = e2.Tick(ctx) }()
	<-gate.started

	// 在 e2 上查一次，再用一个全新进程 e3（只读盘、不推进）查同一时刻状态。
	live, err := e2.Audit(ctx, "wf-i4")
	if err != nil {
		t.Fatalf("Audit live: %v", err)
	}
	e3 := env.reopenReadOnly(t, "reader")
	mustDef(e3, def)
	liveAfterRestart, err := e3.Audit(ctx, "wf-i4")
	if err != nil {
		t.Fatalf("Audit live from fresh process: %v", err)
	}
	if !reflect.DeepEqual(live, liveAfterRestart) {
		t.Fatalf("live audit differs across restart:\n e2=%+v\n e3=%+v", live, liveAfterRestart)
	}
	if n := countOutcomes(live, OutcomeInProgress); n != 1 {
		t.Fatalf("expected exactly 1 in_progress, got %d in %+v", n, live.Attempts)
	}
	if n := countOutcomes(live, OutcomeInterrupted); n != 1 {
		t.Fatalf("expected exactly 1 interrupted, got %d in %+v", n, live.Attempts)
	}
	t.Logf("scenario=restart-consistency stage=in-flight interrupted=1 in_progress=1 stable=true")

	// 放行后由 e2 跑完；e3 不推进。再用全新进程 e4 复盘终态并与 e2 视角比对。
	close(gate.release)
	deadline := time.Now().Add(2 * time.Second)
	for {
		st, _ := e2.Get(ctx, "wf-i4")
		if st.Status.Terminal() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("workflow did not finish after release")
		}
		time.Sleep(5 * time.Millisecond)
	}
	final2, err := e2.Audit(ctx, "wf-i4")
	if err != nil {
		t.Fatalf("Audit final on e2: %v", err)
	}
	e4 := env.reopenReadOnly(t, "reader2")
	mustDef(e4, def)
	final4, err := e4.Audit(ctx, "wf-i4")
	if err != nil {
		t.Fatalf("Audit final from fresh process: %v", err)
	}
	if !reflect.DeepEqual(final2, final4) {
		t.Fatalf("final audit differs across restart:\n e2=%+v\n e4=%+v", final2, final4)
	}
	if final4.Status != WFCompleted ||
		countOutcomes(final4, OutcomeInProgress) != 0 ||
		countOutcomes(final4, OutcomeSucceeded) != 1 ||
		countOutcomes(final4, OutcomeInterrupted) != 1 {
		t.Fatalf("final trail not self-consistent: %+v", final4.Attempts)
	}
	// 相互印证：预算 3，轨迹里 1 次被中断（已消耗）+ 1 次成功；中断那条 remaining=2、成功那条 remaining=0。
	for _, a := range final4.Attempts {
		switch a.Attempt {
		case 1:
			if a.Outcome != OutcomeInterrupted || a.RemainingBudget != 2 {
				t.Fatalf("attempt1 self-contradiction: %+v", a)
			}
		case 2:
			if a.Outcome != OutcomeSucceeded || a.RemainingBudget != 1 {
				t.Fatalf("attempt2 self-contradiction: %+v", a)
			} // max=3：本次成功后还剩 1 次未用（成功无需再用）
		}
	}
	t.Logf("scenario=restart-consistency stage=final interrupted=1 succeeded=1 in_progress=0 stable=true")
}

func countOutcomes(rep *AuditReport, want AttemptOutcome) int {
	n := 0
	for _, a := range rep.Attempts {
		if a.Outcome == want {
			n++
		}
	}
	return n
}
