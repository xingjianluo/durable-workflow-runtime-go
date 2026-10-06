package workflow

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestAuditCrashOnLastAttemptMarkedInterrupted 场景：进程崩溃打断的那次尝试
// 正好把允许的尝试次数用光，接管时实例直接判失败。复盘必须把这次尝试标成
// interrupted（执行被打断、没跑出结果），而不是普通 failed（业务真的报错）；
// 工作流终态失败原因与该结论相互印证，且重启后复盘结论一致。
func TestAuditCrashOnLastAttemptMarkedInterrupted(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	const ttl = 10 * time.Second
	def := WorkflowDef{Name: "audit-crash-exhaust", Steps: []StepDef{
		{Name: "only", Activity: "gate", Retry: RetryPolicy{MaxAttempts: 1}},
	}}
	started := make(chan struct{})
	gate := make(chan struct{})
	var mu sync.Mutex
	calls := 0
	act := func(ctx ActivityContext, in ActivityInput) (string, error) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n == 1 {
			close(started)
			<-gate
		}
		return "", Transientf("should never be observed: call #%d", n)
	}

	// 旧执行者：尝试 1 在途（MaxAttempts=1，唯一一次预算已入账）时进程崩溃。
	e1 := env.newEngine(t, "old-worker", ttl)
	mustDef(e1, def)
	e1.RegisterActivity("gate", act)
	if _, err := e1.Start(ctx, "wf-c1", "audit-crash-exhaust", "x"); err != nil {
		t.Fatal(err)
	}
	oldDone := make(chan error, 1)
	go func() { _, err := e1.Tick(ctx); oldDone <- err }()
	<-started
	if err := env.store.Close(); err != nil { // 模拟进程崩溃
		t.Fatal(err)
	}

	// 新执行者用同一目录重开（= 进程重启后的接管），旧租约过期后接管。
	store2, err := NewFileStore(env.dir)
	if err != nil {
		t.Fatal(err)
	}
	env.store = store2
	e2 := NewEngine(store2, Options{WorkerID: "new-worker", LeaseDuration: ttl, Clock: env.clock, Logger: env.log})
	mustDef(e2, def)
	e2.RegisterActivity("gate", act)
	env.clock.Advance(ttl + time.Second)
	if _, err := e2.Tick(ctx); err != nil {
		t.Fatalf("takeover tick: %v", err)
	}
	st, err := e2.Get(ctx, "wf-c1")
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != WFFailed {
		t.Fatalf("precondition: workflow should be failed, got %s", st.Status)
	}

	rep, err := e2.Audit(ctx, "wf-c1")
	if err != nil {
		t.Fatalf("Audit: %v", err)
	}
	if len(rep.Attempts) != 1 {
		t.Fatalf("expected exactly 1 attempt record, got %+v", rep.Attempts)
	}
	a := rep.Attempts[0]
	if a.Outcome != OutcomeInterrupted {
		t.Fatalf("crash-exhausted attempt must be %s, got %s: %+v",
			OutcomeInterrupted, a.Outcome, a)
	}
	if a.Attempt != 1 || a.RemainingBudget != 0 {
		t.Fatalf("interrupted attempt accounting wrong (want attempt=1 remaining=0): %+v", a)
	}
	if a.EndedAt != nil {
		t.Fatalf("interrupted attempt must carry no end time: %+v", a)
	}
	if st.Failure == nil || !strings.Contains(st.Failure.Message, "last attempt interrupted before completion") {
		t.Fatalf("final failure reason must explain interruption, got %+v", st.Failure)
	}
	// 失败历史也必须能与业务真实错误区分：显式的 interrupted 标志。
	if fs, _ := e2.Failures(ctx, "wf-c1", 0); len(fs) != 1 || !fs[0].Interrupted {
		t.Fatalf("step failure history must record an explicitly interrupted failure, got %+v", fs)
	}
	// 工作流终态失败同样指向“被中断”，与轨迹结论一致。
	if st.Failure == nil || !st.Failure.Interrupted {
		t.Fatalf("workflow final failure must be flagged interrupted, got %+v", st.Failure)
	}

	// 重启后复盘逐字段一致：结论只依赖落盘记录。
	if err := store2.Close(); err != nil {
		t.Fatal(err)
	}
	store3, err := NewFileStore(env.dir)
	if err != nil {
		t.Fatal(err)
	}
	env.store = store3
	e3 := NewEngine(store3, Options{WorkerID: "inspector", LeaseDuration: ttl, Clock: env.clock, Logger: env.log})
	mustDef(e3, def)
	rep2, err := e3.Audit(ctx, "wf-c1")
	if err != nil {
		t.Fatalf("Audit after restart: %v", err)
	}
	if !reflect.DeepEqual(rep, rep2) {
		t.Fatalf("audit not stable across restart:\n before=%+v\n after=%+v", rep, rep2)
	}

	close(gate)
	select {
	case <-oldDone:
	case <-time.After(2 * time.Second):
		t.Fatal("abandoned old tick did not return")
	}
	t.Logf("scenario=crash-exhausts-budget result=attempt1=%s remaining=%d workflow=%s final_reason=%q stable_after_restart=true",
		a.Outcome, a.RemainingBudget, rep.Status, st.Failure.Message)
}

// TestAuditCrashWithBudgetLeftRetriesAndInterruptedIsSeparated 场景：崩溃打断
// 发生时预算仍有剩余，接管方以连续编号继续重试并成功。复盘里被打断的旧尝试
// 是 interrupted（无失败记录），接管后的尝试按真实结果（失败/成功）显示，
// 剩余预算逐次递减且与事件账相互印证。
func TestAuditCrashWithBudgetLeftRetriesAndInterruptedIsSeparated(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	const ttl = 10 * time.Second
	def := WorkflowDef{Name: "audit-crash-retry", Steps: []StepDef{
		{Name: "only", Activity: "gate", Retry: RetryPolicy{MaxAttempts: 3}},
	}}
	started := make(chan struct{})
	gate := make(chan struct{})
	var mu sync.Mutex
	calls := 0
	act := func(ctx ActivityContext, in ActivityInput) (string, error) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		switch n {
		case 1: // 崩溃前的在途尝试：永远等不到结果（进程死掉）。
			close(started)
			<-gate
			return "", Transientf("abandoned")
		case 2: // 接管后的第一次尝试：业务真实返回瞬时错误。
			return "", Transientf("real transient boom #2")
		default: // 第 3 次成功。
			return "ok-after-takeover", nil
		}
	}

	e1 := env.newEngine(t, "old-worker", ttl)
	mustDef(e1, def)
	e1.RegisterActivity("gate", act)
	if _, err := e1.Start(ctx, "wf-c2", "audit-crash-retry", "x"); err != nil {
		t.Fatal(err)
	}
	oldDone := make(chan error, 1)
	go func() { _, err := e1.Tick(ctx); oldDone <- err }()
	<-started
	if err := env.store.Close(); err != nil {
		t.Fatal(err)
	}

	store2, err := NewFileStore(env.dir)
	if err != nil {
		t.Fatal(err)
	}
	env.store = store2
	e2 := NewEngine(store2, Options{WorkerID: "new-worker", LeaseDuration: ttl, Clock: env.clock, Logger: env.log})
	mustDef(e2, def)
	e2.RegisterActivity("gate", act)

	// 接管后第 2 次尝试（业务瞬时失败，预算 3 仍够重试）。
	env.clock.Advance(ttl + time.Second)
	if _, err := e2.Tick(ctx); err != nil {
		t.Fatalf("takeover tick: %v", err)
	}
	st, _ := e2.Get(ctx, "wf-c2")
	if st.Steps[0].Attempt != 2 || st.Steps[0].Status != StepWaiting {
		t.Fatalf("precondition: attempt 2 failed transiently -> waiting, got %+v", st.Steps[0])
	}
	rep, err := e2.Audit(ctx, "wf-c2")
	if err != nil {
		t.Fatalf("Audit mid-retry: %v", err)
	}
	if len(rep.Attempts) != 2 {
		t.Fatalf("expected 2 attempt records, got %+v", rep.Attempts)
	}
	if rep.Attempts[0].Outcome != OutcomeInterrupted || rep.Attempts[0].RemainingBudget != 2 {
		t.Fatalf("attempt 1 must be interrupted remaining=2, got %+v", rep.Attempts[0])
	}
	if rep.Attempts[1].Outcome != OutcomeFailed || rep.Attempts[1].Class != FailureTransient ||
		rep.Attempts[1].RemainingBudget != 1 {
		t.Fatalf("attempt 2 must be failed/transient remaining=1, got %+v", rep.Attempts[1])
	}
	// 只有 attempt 2 有业务失败记录；被中断的 attempt 1 不产生 Failure。
	fs, _ := e2.Failures(ctx, "wf-c2", 0)
	if len(fs) != 1 || fs[0].Attempt != 2 || fs[0].Interrupted {
		t.Fatalf("only the real business failure of attempt 2 should be recorded, got %+v", fs)
	}

	// 退避到期后第 3 次尝试成功，实例完成。
	env.clock.Advance(time.Hour)
	if _, err := e2.Tick(ctx); err != nil {
		t.Fatalf("retry tick: %v", err)
	}
	st, _ = e2.Get(ctx, "wf-c2")
	if st.Status != WFCompleted {
		t.Fatalf("expected completed, got %s", st.Status)
	}
	rep, err = e2.Audit(ctx, "wf-c2")
	if err != nil {
		t.Fatalf("Audit final: %v", err)
	}
	want := []struct {
		attempt   int
		outcome   AttemptOutcome
		remaining int
	}{
		{1, OutcomeInterrupted, 2},
		{2, OutcomeFailed, 1},
		{3, OutcomeSucceeded, 0},
	}
	if len(rep.Attempts) != len(want) {
		t.Fatalf("expected %d attempt records, got %+v", len(want), rep.Attempts)
	}
	for i, w := range want {
		got := rep.Attempts[i]
		if got.Attempt != w.attempt || got.Outcome != w.outcome || got.RemainingBudget != w.remaining {
			t.Errorf("attempt[%d] = %+v, want attempt=%d outcome=%s remaining=%d",
				i, got, w.attempt, w.outcome, w.remaining)
		}
	}
	// 重启后复盘逐字段一致。
	if err := store2.Close(); err != nil {
		t.Fatal(err)
	}
	store3, err := NewFileStore(env.dir)
	if err != nil {
		t.Fatal(err)
	}
	env.store = store3
	e3 := NewEngine(store3, Options{WorkerID: "inspector", LeaseDuration: ttl, Clock: env.clock, Logger: env.log})
	mustDef(e3, def)
	rep2, err := e3.Audit(ctx, "wf-c2")
	if err != nil {
		t.Fatalf("Audit after restart: %v", err)
	}
	if !reflect.DeepEqual(rep, rep2) {
		t.Fatalf("audit not stable across restart:\n before=%+v\n after=%+v", rep, rep2)
	}

	close(gate)
	select {
	case <-oldDone:
	case <-time.After(2 * time.Second):
		t.Fatal("abandoned old tick did not return")
	}
	t.Logf("scenario=crash-budget-left-continues result=attempts=[%s(rem=%d) %s(rem=%d) %s(rem=%d)] stable_after_restart=true",
		rep.Attempts[0].Outcome, rep.Attempts[0].RemainingBudget,
		rep.Attempts[1].Outcome, rep.Attempts[1].RemainingBudget,
		rep.Attempts[2].Outcome, rep.Attempts[2].RemainingBudget)
}

// TestAuditTakeoverLeavesOnlyOneInProgress 场景：同一步骤连续两次被崩溃打断，
// 第三次执行者的新尝试还在执行时查复盘：同一时刻只能有一条 in_progress（最新尝试、
// 租约活跃），被打断的历史尝试全部按真实结局显示为 interrupted，绝不能出现
// 两条“进行中”。新尝试跑完后结论闭环，重启后复盘一致。
func TestAuditTakeoverLeavesOnlyOneInProgress(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	const ttl = 10 * time.Second
	def := WorkflowDef{Name: "audit-takeover-live", Steps: []StepDef{
		{Name: "only", Activity: "gate", Retry: RetryPolicy{MaxAttempts: 4}},
	}}
	started1 := make(chan struct{})
	started2 := make(chan struct{})
	started3 := make(chan struct{})
	gate1 := make(chan struct{})
	gate2 := make(chan struct{})
	gate3 := make(chan struct{})
	var mu sync.Mutex
	calls := 0
	act := func(ctx ActivityContext, in ActivityInput) (string, error) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		switch n {
		case 1:
			close(started1)
			<-gate1
			return "", Transientf("abandoned 1")
		case 2:
			close(started2)
			<-gate2
			return "", Transientf("abandoned 2")
		case 3:
			close(started3)
			<-gate3
			return "", Transientf("real boom 3")
		default:
			return "ok", nil
		}
	}

	// 第一次崩溃：尝试 1 在途。
	e1 := env.newEngine(t, "worker-a", ttl)
	mustDef(e1, def)
	e1.RegisterActivity("gate", act)
	if _, err := e1.Start(ctx, "wf-c3", "audit-takeover-live", "x"); err != nil {
		t.Fatal(err)
	}
	done1 := make(chan error, 1)
	go func() { _, err := e1.Tick(ctx); done1 <- err }()
	<-started1
	if err := env.store.Close(); err != nil {
		t.Fatal(err)
	}

	// 第一次接管：尝试 2 在途时再次崩溃。
	store2, err := NewFileStore(env.dir)
	if err != nil {
		t.Fatal(err)
	}
	env.store = store2
	e2 := NewEngine(store2, Options{WorkerID: "worker-b", LeaseDuration: ttl, Clock: env.clock, Logger: env.log})
	mustDef(e2, def)
	e2.RegisterActivity("gate", act)
	env.clock.Advance(ttl + time.Second)
	done2 := make(chan error, 1)
	go func() { _, err := e2.Tick(ctx); done2 <- err }()
	<-started2
	if err := store2.Close(); err != nil {
		t.Fatal(err)
	}

	// 第二次接管：尝试 3 正在执行，此刻做复盘。
	store3, err := NewFileStore(env.dir)
	if err != nil {
		t.Fatal(err)
	}
	env.store = store3
	e3 := NewEngine(store3, Options{WorkerID: "worker-c", LeaseDuration: ttl, Clock: env.clock, Logger: env.log})
	mustDef(e3, def)
	e3.RegisterActivity("gate", act)
	env.clock.Advance(ttl + time.Second)
	done3 := make(chan error, 1)
	go func() { _, err := e3.Tick(ctx); done3 <- err }()
	<-started3

	rep, err := e3.Audit(ctx, "wf-c3")
	if err != nil {
		t.Fatalf("Audit while attempt 3 live: %v", err)
	}
	if len(rep.Attempts) != 3 {
		t.Fatalf("expected 3 attempt records, got %+v", rep.Attempts)
	}
	inProgress := 0
	for _, a := range rep.Attempts {
		if a.Outcome == OutcomeInProgress {
			inProgress++
		}
	}
	if inProgress != 1 {
		t.Fatalf("exactly one in_progress required, got %d: %+v", inProgress, rep.Attempts)
	}
	if rep.Attempts[0].Outcome != OutcomeInterrupted || rep.Attempts[1].Outcome != OutcomeInterrupted {
		t.Fatalf("attempts 1,2 must be interrupted, got %+v %+v", rep.Attempts[0], rep.Attempts[1])
	}
	live := rep.Attempts[2]
	st, _ := e3.Get(ctx, "wf-c3")
	if live.Outcome != OutcomeInProgress || live.Attempt != 3 ||
		live.LeaseID != st.Lease.Token || live.RemainingBudget != 1 {
		t.Fatalf("attempt 3 must be the single in_progress under current lease, got %+v lease=%+v",
			live, st.Lease)
	}

	// 尝试 3 业务真实瞬时失败 → 退避；然后尝试 4 成功，实例完成。
	close(gate3)
	select {
	case <-done3:
	case <-time.After(2 * time.Second):
		t.Fatal("takeover tick did not return")
	}
	env.clock.Advance(time.Hour)
	if _, err := e3.Tick(ctx); err != nil {
		t.Fatalf("final tick: %v", err)
	}
	st, _ = e3.Get(ctx, "wf-c3")
	if st.Status != WFCompleted {
		t.Fatalf("expected completed, got %s", st.Status)
	}
	rep, err = e3.Audit(ctx, "wf-c3")
	if err != nil {
		t.Fatalf("Audit final: %v", err)
	}
	want := []struct {
		attempt   int
		outcome   AttemptOutcome
		remaining int
	}{
		{1, OutcomeInterrupted, 3},
		{2, OutcomeInterrupted, 2},
		{3, OutcomeFailed, 1},
		{4, OutcomeSucceeded, 0},
	}
	if len(rep.Attempts) != len(want) {
		t.Fatalf("expected %d records, got %+v", len(want), rep.Attempts)
	}
	for i, w := range want {
		got := rep.Attempts[i]
		if got.Attempt != w.attempt || got.Outcome != w.outcome || got.RemainingBudget != w.remaining {
			t.Errorf("attempt[%d]=%+v, want attempt=%d outcome=%s remaining=%d",
				i, got, w.attempt, w.outcome, w.remaining)
		}
	}

	// 重启后复盘逐字段一致（终态下不应再有任何 in_progress）。
	if err := store3.Close(); err != nil {
		t.Fatal(err)
	}
	store4, err := NewFileStore(env.dir)
	if err != nil {
		t.Fatal(err)
	}
	env.store = store4
	e4 := NewEngine(store4, Options{WorkerID: "inspector", LeaseDuration: ttl, Clock: env.clock, Logger: env.log})
	mustDef(e4, def)
	rep2, err := e4.Audit(ctx, "wf-c3")
	if err != nil {
		t.Fatalf("Audit after restart: %v", err)
	}
	if !reflect.DeepEqual(rep, rep2) {
		t.Fatalf("audit not stable across restart:\n before=%+v\n after=%+v", rep, rep2)
	}

	close(gate1)
	close(gate2)
	waitOrFatal := func(ch chan error) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatal("abandoned old tick did not return")
		}
	}
	waitOrFatal(done1)
	waitOrFatal(done2)
	t.Logf("scenario=takeover-single-in-progress result=live_audit=[%s,%s,%s(in_progress=1)] final=[interrupted,interrupted,failed,succeeded] stable_after_restart=true",
		rep2.Attempts[0].Outcome, rep2.Attempts[1].Outcome, rep2.Attempts[2].Outcome)
}

// TestAuditStableAcrossRestartsAtEveryStage 场景：复盘结论只能依赖落盘记录。
// 同一个实例在“在途执行 → 崩溃后立即重开（旧租约尚未过期）→ 旧租约已过期
// 但尚无人接管 → 接管后重试 → 终态”每个阶段，用全新引擎重开目录复查，
// 结论都必须与当时观察一致：
//   - 活跃租约在途：恰好一条 in_progress；
//   - 崩溃后旧租约未过期：磁盘上仍是旧租约，结论仍是旧尝试 in_progress
//     （这正是崩溃瞬间的真实落盘状态，重启不改写）；
//   - 旧租约过期后、接管前：无人在执行，旧尝试必须转为 interrupted，
//     绝不能继续显示 in_progress；
//   - 接管重试至终态后：闭环结论逐字段稳定。
func TestAuditStableAcrossRestartsAtEveryStage(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	const ttl = 10 * time.Second
	def := WorkflowDef{Name: "audit-restart-stable", Steps: []StepDef{
		{Name: "only", Activity: "gate", Retry: RetryPolicy{MaxAttempts: 1}},
	}}
	started := make(chan struct{})
	gate := make(chan struct{})
	act := func(ctx ActivityContext, in ActivityInput) (string, error) {
		close(started)
		<-gate
		return "unreachable", nil
	}

	e1 := env.newEngine(t, "worker-a", ttl)
	mustDef(e1, def)
	e1.RegisterActivity("gate", act)
	if _, err := e1.Start(ctx, "wf-c4", "audit-restart-stable", "x"); err != nil {
		t.Fatal(err)
	}
	done1 := make(chan error, 1)
	go func() { _, err := e1.Tick(ctx); done1 <- err }()
	<-started

	// snapshotAudit：用一个只读引擎重开目录（不注册活动、不推进），重建复盘。
	snapshotAudit := func(stage string, status WorkflowStatus, wantOutcome AttemptOutcome) AuditReport {
		t.Helper()
		if err := env.store.Close(); err != nil {
			t.Fatalf("close before %s: %v", stage, err)
		}
		st, err := NewFileStore(env.dir)
		if err != nil {
			t.Fatalf("reopen at %s: %v", stage, err)
		}
		env.store = st
		inspector := NewEngine(st, Options{WorkerID: "inspector-" + stage, LeaseDuration: ttl,
			Clock: env.clock, Logger: env.log})
		mustDef(inspector, def)
		rep, err := inspector.Audit(ctx, "wf-c4")
		if err != nil {
			t.Fatalf("Audit at %s: %v", stage, err)
		}
		if rep.Status != status {
			t.Fatalf("stage %s: status=%s want %s", stage, rep.Status, status)
		}
		if len(rep.Attempts) != 1 || rep.Attempts[0].Outcome != wantOutcome ||
			rep.Attempts[0].Attempt != 1 || rep.Attempts[0].RemainingBudget != 0 {
			t.Fatalf("stage %s: unexpected report %+v", stage, rep.Attempts)
		}
		// 同阶段再重开一次：逐字段一致。
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		st2, err := NewFileStore(env.dir)
		if err != nil {
			t.Fatal(err)
		}
		env.store = st2
		in2 := NewEngine(st2, Options{WorkerID: "inspector2-" + stage, LeaseDuration: ttl,
			Clock: env.clock, Logger: env.log})
		mustDef(in2, def)
		rep2, err := in2.Audit(ctx, "wf-c4")
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(rep, rep2) {
			t.Fatalf("stage %s: audit not reproducible:\n %+v\n %+v", stage, rep, rep2)
		}
		return *rep
	}

	// 阶段 1：活跃租约，尝试在途 → in_progress。
	live := snapshotAudit("live", WFRunning, OutcomeInProgress)

	// 阶段 2：崩溃后立刻重开，时钟未动，磁盘上旧租约仍“活跃” → 仍是 in_progress，
	// 与崩溃前逐字段一致（重启本身不改写任何状态）。
	if err := env.store.Close(); err != nil {
		t.Fatal(err)
	}
	storeAfterCrash, err := NewFileStore(env.dir)
	if err != nil {
		t.Fatal(err)
	}
	env.store = storeAfterCrash
	afterCrash := snapshotAudit("after-crash", WFRunning, OutcomeInProgress)
	if !reflect.DeepEqual(live.Attempts, afterCrash.Attempts) {
		t.Fatalf("audit must not change merely due to restart:\n %+v\n %+v", live.Attempts, afterCrash.Attempts)
	}

	// 阶段 3：时钟走过租约过期点，但尚无任何人接管。此时没有执行者在跑，
	// 旧尝试必须显示 interrupted——不能让一条已经死掉的尝试永远挂在“进行中”。
	env.clock.Advance(ttl + time.Second)
	expired := snapshotAudit("lease-expired", WFRunning, OutcomeInterrupted)
	if expired.Attempts[0].EndedAt != nil {
		t.Fatalf("dead-but-not-taken-over attempt must stay without end time: %+v", expired.Attempts[0])
	}

	// 阶段 4：新执行者接管。唯一一次预算已被在途尝试用掉，直接落账失败（interrupted）。
	taker := NewEngine(env.store, Options{WorkerID: "worker-b", LeaseDuration: ttl,
		Clock: env.clock, Logger: env.log})
	mustDef(taker, def)
	taker.RegisterActivity("gate", act)
	if _, err := taker.Tick(ctx); err != nil {
		t.Fatalf("takeover tick: %v", err)
	}
	final := snapshotAudit("terminal", WFFailed, OutcomeInterrupted)
	st, _ := taker.Get(ctx, "wf-c4")
	if st.Failure == nil || !st.Failure.Interrupted {
		t.Fatalf("terminal failure must explain interruption: %+v", st.Failure)
	}
	if final.Attempts[0].EndedAt != nil {
		t.Fatalf("interrupted attempt must have no end time even at terminal: %+v", final.Attempts[0])
	}

	close(gate)
	select {
	case <-done1:
	case <-time.After(2 * time.Second):
		t.Fatal("abandoned old tick did not return")
	}
	t.Logf("scenario=audit-stable-across-restarts result=stages=[live:in_progress after-crash:in_progress lease-expired:interrupted terminal:interrupted]")
}
