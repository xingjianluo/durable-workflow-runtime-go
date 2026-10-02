package workflow

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// crashActivity 模拟“执行到一半进程崩溃”的活动：
//   - 第 1 次调用瞬时失败；
//   - 第 2 次调用永久阻塞（模拟崩溃遗留的在途尝试，测试结束时放行）；
//   - 第 3 次起成功并登记副作用。
type crashActivity struct {
	mu      sync.Mutex
	calls   int
	se      *testSideEffect
	started chan struct{}
	gate    chan struct{}
}

func newCrashActivity(se *testSideEffect) *crashActivity {
	return &crashActivity{se: se, started: make(chan struct{}), gate: make(chan struct{})}
}

func (g *crashActivity) fn() ActivityFunc {
	return func(ctx ActivityContext, in ActivityInput) (string, error) {
		g.mu.Lock()
		g.calls++
		n := g.calls
		g.mu.Unlock()
		switch n {
		case 1:
			return "", Transientf("transient boom #1")
		case 2:
			select {
			case <-g.started:
			default:
				close(g.started)
			}
			<-g.gate // 崩溃模拟：永不返回，直到测试放行
			return "", Transientf("late failure from abandoned attempt")
		default:
			g.se.apply(in.IdempotencyToken)
			return "ok:" + in.Input, nil
		}
	}
}

func (g *crashActivity) callCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls
}

// TestInterruptedAttemptAccountingAtBudgetLimit 崩溃留下的在途尝试必须计为已消耗：
// MaxAttempts=2 时第 2 次尝试在途中崩溃，接管者发现预算已尽，
// 不得再发起新的活动调用，直接以“预算耗尽”终局失败；
// 之后显式续跑追加 1 次预算，第 3 次尝试成功完成。
func TestInterruptedAttemptAccountingAtBudgetLimit(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e1 := env.newEngine(t, "old-worker", 10*time.Second)
	def := WorkflowDef{Name: "crash-wf", Steps: []StepDef{
		{Name: "only", Activity: "crash", Retry: RetryPolicy{
			MaxAttempts: 2, InitialBackoff: 10 * time.Millisecond, Multiplier: 1}},
	}}
	mustDef(e1, def)
	gate := newCrashActivity(env.se)
	e1.RegisterActivity("crash", gate.fn())
	if _, err := e1.Start(ctx, "wf-crash", "crash-wf", "x"); err != nil {
		t.Fatal(err)
	}

	// 第 1 次尝试：瞬时失败进入退避。
	if _, err := e1.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	env.clock.Advance(20 * time.Millisecond)

	// 第 2 次尝试：step_started 落盘后活动阻塞——模拟进程崩溃。
	l := mustAcquire(t, e1, ctx, "wf-crash")
	advDone := make(chan error, 1)
	go func() {
		_, err := e1.Advance(ctx, "wf-crash", l.Token, l.Fence)
		advDone <- err
	}()
	<-gate.started
	if err := env.store.Close(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		close(gate.gate)
		select {
		case <-advDone:
		case <-time.After(2 * time.Second):
			t.Error("abandoned attempt did not return after gate release")
		}
	})

	// 新进程接管：租约过期后 Tick。
	store, err := NewFileStore(env.dir)
	if err != nil {
		t.Fatal(err)
	}
	env.store = store
	e2 := NewEngine(store, Options{WorkerID: "new-worker", LeaseDuration: 10 * time.Second, Clock: env.clock, Logger: env.log})
	mustDef(e2, def)
	e2.RegisterActivity("crash", gate.fn())

	env.clock.Advance(11 * time.Second)
	if _, err := e2.Tick(ctx); err != nil {
		t.Fatalf("takeover tick: %v", err)
	}
	st, _ := e2.Get(ctx, "wf-crash")
	if st.Status != WFFailed {
		t.Fatalf("interrupted attempt at budget limit must fail workflow, got %s", st.Status)
	}
	if gate.callCount() != 2 {
		t.Fatalf("budget exhausted: no new activity call allowed, calls=%d", gate.callCount())
	}
	if st.Steps[0].Attempt != 2 {
		t.Fatalf("interrupted attempt must be accounted as consumed: attempt=%d", st.Steps[0].Attempt)
	}
	failures, _ := e2.Failures(ctx, "wf-crash", 0)
	if len(failures) != 2 || !failures[1].Interrupted || failures[1].Attempt != 2 {
		t.Fatalf("interrupted attempt not accounted: %+v", failures)
	}
	if st.Failure == nil || !strings.Contains(st.Failure.Message, "retry budget exhausted") {
		t.Fatalf("final reason must explain budget exhaustion: %+v", st.Failure)
	}

	// 显式续跑追加 1 次预算：第 3 次尝试成功。
	if _, err := e2.Resume(ctx, "wf-crash", 1); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	st = drainToCompletion(t, e2, ctx, "wf-crash", 10)
	if st.Status != WFCompleted || st.Steps[0].Attempt != 3 {
		t.Fatalf("expected completed at attempt 3, got %s attempt=%d", st.Status, st.Steps[0].Attempt)
	}
	if gate.callCount() != 3 {
		t.Fatalf("expected exactly 3 activity calls, got %d", gate.callCount())
	}
	if env.se.count() != 1 {
		t.Fatalf("expected exactly 1 side effect, got %d", env.se.count())
	}
}

// TestAuditTrailMatchesFinalState 审计轨迹完整覆盖「重试 → 崩溃中断 → 接管判负 →
// 续跑 → 再重试 → 成功」的全生命周期：每一轮尝试的编号、结局、剩余预算与每次续跑
// 追加的预算都可查，且进程重启后重建结果稳定一致、与最终状态对得上。
func TestAuditTrailMatchesFinalState(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e1 := env.newEngine(t, "old-worker", 10*time.Second)
	def := WorkflowDef{Name: "audit-wf", Steps: []StepDef{
		{Name: "only", Activity: "crash", Retry: RetryPolicy{
			MaxAttempts: 2, InitialBackoff: 10 * time.Millisecond, Multiplier: 1}},
	}}
	mustDef(e1, def)
	gate := newCrashActivity(env.se)
	e1.RegisterActivity("crash", gate.fn())
	if _, err := e1.Start(ctx, "wf-audit", "audit-wf", "x"); err != nil {
		t.Fatal(err)
	}

	// 尝试 1：瞬时失败；尝试 2：在途崩溃。
	if _, err := e1.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	env.clock.Advance(20 * time.Millisecond)
	l := mustAcquire(t, e1, ctx, "wf-audit")
	advDone := make(chan error, 1)
	go func() {
		_, err := e1.Advance(ctx, "wf-audit", l.Token, l.Fence)
		advDone <- err
	}()
	<-gate.started
	if err := env.store.Close(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		close(gate.gate)
		select {
		case <-advDone:
		case <-time.After(2 * time.Second):
			t.Error("abandoned attempt did not return after gate release")
		}
	})

	// 接管：预算耗尽判负 → 续跑 +2 → 尝试 3 失败（crashActivity 第 3 次成功，
	// 这里换成前 3 次中第 3 次仍失败的行为由 crashActivity 决定：第 3 次成功，
	// 因此续跑后一次即可完成）。
	store, err := NewFileStore(env.dir)
	if err != nil {
		t.Fatal(err)
	}
	env.store = store
	e2 := NewEngine(store, Options{WorkerID: "new-worker", LeaseDuration: 10 * time.Second, Clock: env.clock, Logger: env.log})
	mustDef(e2, def)
	e2.RegisterActivity("crash", gate.fn())
	env.clock.Advance(11 * time.Second)
	if _, err := e2.Tick(ctx); err != nil {
		t.Fatalf("takeover tick: %v", err)
	}
	if _, err := e2.Resume(ctx, "wf-audit", 2); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	st := drainToCompletion(t, e2, ctx, "wf-audit", 10)
	if st.Status != WFCompleted {
		t.Fatalf("expected completed, got %s", st.Status)
	}

	// 重启后再查：审计结果必须只来自持久化数据且稳定一致。
	e3 := env.reopen(t, "auditor")
	mustDef(e3, def)
	rep, err := e3.Audit(ctx, "wf-audit")
	if err != nil {
		t.Fatalf("Audit: %v", err)
	}
	if rep.WorkflowID != "wf-audit" || rep.Status != WFCompleted {
		t.Fatalf("report header wrong: %+v", rep)
	}
	if len(rep.Attempts) != 3 {
		t.Fatalf("expected 3 attempt records, got %d: %+v", len(rep.Attempts), rep.Attempts)
	}
	want := []struct {
		attempt         int
		outcome         AttemptOutcome
		class           FailureClass
		budgetRemaining int
	}{
		{1, OutcomeFailed, FailureTransient, 1},      // 当时预算 2，用后剩 1
		{2, OutcomeInterrupted, FailureTransient, 0}, // 崩溃中断，预算耗尽
		{3, OutcomeSucceeded, FailureNone, 1},        // 续跑 +2 后预算 4，用后剩 1
	}
	for i, w := range want {
		rec := rep.Attempts[i]
		if rec.Attempt != w.attempt || rec.Outcome != w.outcome || rec.Class != w.class {
			t.Fatalf("attempt record %d wrong: %+v", i, rec)
		}
		if rec.BudgetRemaining != w.budgetRemaining {
			t.Fatalf("attempt %d budget remaining=%d, want %d", i, rec.BudgetRemaining, w.budgetRemaining)
		}
		if rec.StartedAt.IsZero() || rec.EndedAt == nil {
			t.Fatalf("attempt %d missing timestamps: %+v", i, rec)
		}
	}
	if len(rep.Resumes) != 1 || rep.Resumes[0].ExtraAttempts != 2 || rep.Resumes[0].Seq != 1 {
		t.Fatalf("resume history wrong: %+v", rep.Resumes)
	}
	if len(rep.EffectiveMaxAttempts) != 1 || rep.EffectiveMaxAttempts[0] != 4 {
		t.Fatalf("effective max attempts wrong: %v", rep.EffectiveMaxAttempts)
	}
	// 与最终状态对得上：审计的尝试总数 == 快照的 Attempt；最后一条是成功。
	if st.Steps[0].Attempt != len(rep.Attempts) {
		t.Fatalf("audit attempts %d != state attempt %d", len(rep.Attempts), st.Steps[0].Attempt)
	}
	if rep.Attempts[len(rep.Attempts)-1].Outcome != OutcomeSucceeded {
		t.Fatalf("final attempt outcome mismatch: %+v", rep.Attempts[len(rep.Attempts)-1])
	}

	// 再次重建（同一路径第二次查询）结果一致：不依赖进程内存。
	rep2, err := e3.Audit(ctx, "wf-audit")
	if err != nil {
		t.Fatal(err)
	}
	if len(rep2.Attempts) != len(rep.Attempts) || len(rep2.Resumes) != len(rep.Resumes) {
		t.Fatalf("audit not reproducible: %+v vs %+v", rep, rep2)
	}
	for i := range rep.Attempts {
		if !reflect.DeepEqual(rep2.Attempts[i], rep.Attempts[i]) {
			t.Fatalf("attempt %d not reproducible: %+v vs %+v", i, rep.Attempts[i], rep2.Attempts[i])
		}
	}
}
