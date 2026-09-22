package workflow

import (
	"context"
	"sync"
	"testing"
	"time"
)

// flakyRegistry 记录每个幂等令牌的调用次数：前 failFirst-1 次返回瞬时错误，
// 之后成功。注意：瞬时失败的尝试本身也登记副作用，用于验证“失败尝试不重复产生副作用”的断言。
type flakyRegistry struct {
	mu        sync.Mutex
	calls     map[string]int
	se        *testSideEffect
	failFirst int // 前 failFirst 次调用失败（第 failFirst 次成功）
}

func (r *flakyRegistry) activity() ActivityFunc {
	return func(ctx ActivityContext, in ActivityInput) (string, error) {
		r.mu.Lock()
		r.calls[in.IdempotencyToken]++
		n := r.calls[in.IdempotencyToken]
		r.mu.Unlock()
		if n < r.failFirst {
			return "", Transientf("flaky transient failure #%d", n)
		}
		r.se.apply(in.IdempotencyToken) // 只有成功时才产生业务副作用
		return "ok@" + in.IdempotencyToken, nil
	}
}

func (r *flakyRegistry) callsOf(token string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls[token]
}

func retryDef() WorkflowDef {
	return WorkflowDef{Name: "retry-wf", Steps: []StepDef{
		{Name: "flaky", Activity: "flaky", Retry: RetryPolicy{
			MaxAttempts: 5, InitialBackoff: 50 * time.Millisecond, MaxBackoff: time.Second,
			Multiplier: 2,
		}},
	}}
}

func TestTransientRetryThenSuccess(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w-retry", 10*time.Second)
	if err := e.RegisterDef(retryDef()); err != nil {
		t.Fatal(err)
	}
	reg := &flakyRegistry{calls: map[string]int{}, se: env.se, failFirst: 3}
	e.RegisterActivity("flaky", reg.activity())

	if _, err := e.Start(ctx, "wf-r", "retry-wf", "in"); err != nil {
		t.Fatal(err)
	}
	token := idempotencyToken("wf-r", 0)

	// attempt 1：瞬时失败 → 进入 waiting。
	l := mustAcquire(t, e, ctx, "wf-r")
	if _, err := e.Advance(ctx, "wf-r", l.Token, l.Fence); err != nil {
		t.Fatal(err)
	}
	st, _ := e.Get(ctx, "wf-r")
	if st.Steps[0].Status != StepWaiting || st.Steps[0].Attempt != 1 {
		t.Fatalf("expected waiting attempt=1, got %s attempt=%d", st.Steps[0].Status, st.Steps[0].Attempt)
	}
	if st.Steps[0].NextAttemptAt == nil || !st.Steps[0].NextAttemptAt.After(env.clock.Now()) {
		t.Fatal("expected NextAttemptAt in the future")
	}
	// 退避未到期：重复推进被拒绝。
	l = mustAcquire(t, e, ctx, "wf-r")
	_, err := e.Advance(ctx, "wf-r", l.Token, l.Fence)
	if ck := conflictKind(err); ck != ConflictNoRunnable {
		t.Fatalf("expected ConflictNoRunnable during backoff, got %v", err)
	}

	// 推进时钟经过退避；attempt 2 再次瞬时失败。
	env.clock.Advance(time.Second)
	_, err = e.Tick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	st, _ = e.Get(ctx, "wf-r")
	if st.Steps[0].Attempt != 2 || st.Steps[0].Status != StepWaiting {
		t.Fatalf("expected waiting attempt=2, got %s attempt=%d", st.Steps[0].Status, st.Steps[0].Attempt)
	}

	// 再经过退避；attempt 3 成功。
	env.clock.Advance(time.Second)
	if _, err := e.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	st, _ = e.Get(ctx, "wf-r")
	if st.Status != WFCompleted || st.Steps[0].Status != StepSucceeded {
		t.Fatalf("expected completed/succeeded, got %s/%s", st.Status, st.Steps[0].Status)
	}
	if reg.callsOf(token) != 3 {
		t.Fatalf("expected 3 activity calls, got %d", reg.callsOf(token))
	}
	// 三次尝试共享同一幂等令牌，业务副作用只在最终成功时发生一次。
	if env.se.count() != 1 {
		t.Fatalf("expected 1 side effect, got %d", env.se.count())
	}

	// 失败历史可查询：2 次瞬时失败，分类与尝试序号完整。
	failures, err := e.Failures(ctx, "wf-r", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(failures) != 2 {
		t.Fatalf("expected 2 failure records, got %d: %+v", len(failures), failures)
	}
	for i, f := range failures {
		if f.Attempt != i+1 || f.Class != FailureTransient {
			t.Fatalf("failure %d bad: %+v", i, f)
		}
	}
	// 事件历史中能看到 started*3 / failed*2 / retry_wait*2 / succeeded / completed。
	events, _ := e.History(ctx, "wf-r")
	counts := map[EventType]int{}
	for _, ev := range events {
		counts[ev.Type]++
	}
	if counts[EvStepStarted] != 3 || counts[EvStepFailed] != 2 || counts[EvStepRetryWait] != 2 ||
		counts[EvStepSucceeded] != 1 || counts[EvWorkflowCompleted] != 1 {
		t.Fatalf("unexpected event counts: %+v", counts)
	}
}

func TestPermanentFailureStopsImmediately(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w-perm", time.Minute)
	mustDef(e, WorkflowDef{Name: "perm-wf", Steps: []StepDef{
		{Name: "p", Activity: "perm", Retry: RetryPolicy{
			MaxAttempts: 5, InitialBackoff: time.Millisecond, Multiplier: 2}},
	}})
	calls := 0
	e.RegisterActivity("perm", func(ctx ActivityContext, in ActivityInput) (string, error) {
		calls++
		env.se.apply(in.IdempotencyToken)
		return "", Permanentf("business rule violated for input %q", in.Input)
	})
	if _, err := e.Start(ctx, "wf-p", "perm-wf", "bad"); err != nil {
		t.Fatal(err)
	}
	l := mustAcquire(t, e, ctx, "wf-p")
	st, err := e.Advance(ctx, "wf-p", l.Token, l.Fence)
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if st.Status != WFFailed || st.Steps[0].Status != StepFailed {
		t.Fatalf("expected failed, got %s/%s", st.Status, st.Steps[0].Status)
	}
	if calls != 1 {
		t.Fatalf("permanent error must not retry, got %d calls", calls)
	}
	if st.Steps[0].FinalClass != FailurePermanent {
		t.Fatalf("expected final class permanent, got %q", st.Steps[0].FinalClass)
	}
	// 时钟推进 + 重复 Tick 也不会复活工作流。
	env.clock.Advance(time.Hour)
	for i := 0; i < 3; i++ {
		if _, err := e.Tick(ctx); err != nil {
			t.Fatal(err)
		}
	}
	st, _ = e.Get(ctx, "wf-p")
	if st.Status != WFFailed || calls != 1 {
		t.Fatalf("terminal workflow resurrected: status=%s calls=%d", st.Status, calls)
	}
	wfFailures, err := e.Failures(ctx, "wf-p", -1)
	if err != nil || len(wfFailures) != 1 || wfFailures[0].Class != FailurePermanent {
		t.Fatalf("workflow failure history bad: %+v err=%v", wfFailures, err)
	}
}

func TestRetryBudgetExhaustion(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w-budget", time.Minute)
	mustDef(e, WorkflowDef{Name: "budget-wf", Steps: []StepDef{
		{Name: "b", Activity: "always-transient", Retry: RetryPolicy{
			MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond, Multiplier: 2}},
	}})
	calls := 0
	e.RegisterActivity("always-transient", func(ctx ActivityContext, in ActivityInput) (string, error) {
		calls++
		return "", Transientf("nope %d", calls)
	})
	if _, err := e.Start(ctx, "wf-b", "budget-wf", "x"); err != nil {
		t.Fatal(err)
	}
	st := drainToCompletion(t, e, ctx, "wf-b", 20)
	if st.Status != WFFailed {
		t.Fatalf("expected failed, got %s", st.Status)
	}
	if calls != 3 {
		t.Fatalf("expected exactly MaxAttempts=3 calls, got %d", calls)
	}
	if st.Steps[0].FinalClass != FailureTransient || st.Failure == nil {
		t.Fatalf("expected final transient with budget-exhausted reason, got %q failure=%v", st.Steps[0].FinalClass, st.Failure)
	}
	failures, _ := e.Failures(ctx, "wf-b", 0)
	if len(failures) != 3 {
		t.Fatalf("expected 3 failure records, got %d", len(failures))
	}
}

// TestDeterministicFailureConclusion 同一输入的失败结论可复现：
// 永久失败活动在两个独立实例上得到相同的分类与最终状态。
func TestDeterministicFailureConclusion(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w-det", time.Minute)
	mustDef(e, WorkflowDef{Name: "det-wf", Steps: []StepDef{
		{Name: "d", Activity: "det-perm", Retry: RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond}},
	}})
	e.RegisterActivity("det-perm", func(ctx ActivityContext, in ActivityInput) (string, error) {
		return "", Permanentf("deterministic rejection: %s", in.Input)
	})
	for _, id := range []string{"i1", "i2"} {
		if _, err := e.Start(ctx, id, "det-wf", "same-input"); err != nil {
			t.Fatal(err)
		}
		drainToCompletion(t, e, ctx, id, 10)
	}
	s1, _ := e.Get(ctx, "i1")
	s2, _ := e.Get(ctx, "i2")
	if s1.Failure == nil || s2.Failure == nil || s1.Failure.Message != s2.Failure.Message ||
		s1.Failure.Class != FailurePermanent || s2.Failure.Class != FailurePermanent {
		t.Fatalf("non-reproducible conclusions: %+v vs %+v", s1.Failure, s2.Failure)
	}
}
