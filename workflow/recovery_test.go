package workflow

import (
	"context"
	"sync"
	"testing"
	"time"
)

// reopenStore 模拟进程重启：关闭旧 Store（释放 flock）后以新 FileStore/新 Engine 重新打开。
func (env *testEnv) reopen(t *testing.T, workerID string) *Engine {
	t.Helper()
	if err := env.store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	store, err := NewFileStore(env.dir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	env.store = store
	e := NewEngine(store, Options{WorkerID: workerID, LeaseDuration: 10 * time.Second, Clock: env.clock, Logger: env.log})
	registerTestDef(e)
	return e
}

// TestRecoverAfterCompletedStep 步骤 a 成功提交后“重启”：新引擎从 b 续跑，a 不重复执行。
func TestRecoverAfterCompletedStep(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e1 := env.newEngine(t, "old", 10*time.Second)
	if err := e1.RegisterDef(twoStepEchoDef()); err != nil {
		t.Fatal(err)
	}
	e1.RegisterActivity("echo", echoActivity(env.se))
	if _, err := e1.Start(ctx, "wf-rec1", "two", "data"); err != nil {
		t.Fatal(err)
	}
	if _, err := e1.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	st, _ := e1.Get(ctx, "wf-rec1")
	if st.Cursor != 1 || st.Steps[0].Status != StepSucceeded {
		t.Fatalf("precondition: step a not done: cursor=%d", st.Cursor)
	}

	// 重启：全新 Store/Engine，状态全部来自磁盘。
	e2 := env.reopen(t, "new")
	if err := e2.RegisterDef(twoStepEchoDef()); err != nil {
		t.Fatal(err)
	}
	e2.RegisterActivity("echo", echoActivity(env.se))
	st, err := e2.Get(ctx, "wf-rec1")
	if err != nil {
		t.Fatalf("load after restart: %v", err)
	}
	if st.Cursor != 1 || st.Steps[0].Status != StepSucceeded || st.Steps[0].Result != "echo:data" {
		t.Fatalf("recovered state wrong: %+v", st)
	}
	if _, err := e2.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	st, _ = e2.Get(ctx, "wf-rec1")
	if st.Status != WFCompleted || st.Result != "echo:echo:data" {
		t.Fatalf("workflow did not resume correctly: %+v", st)
	}
	if env.se.count() != 2 {
		t.Fatalf("expected 2 side effects after resume, got %d", env.se.count())
	}
}

// gateActivity 第一次调用阻塞在 releaseGate 上（模拟执行中进程崩溃、协程被遗弃），
// 第二次调用（接管者的新尝试）立即成功。两次调用共享同一幂等令牌。
type gateActivity struct {
	mu      sync.Mutex
	calls   int
	se      *testSideEffect
	started chan struct{}
	gate    chan struct{}
}

func (g *gateActivity) fn() ActivityFunc {
	return func(ctx ActivityContext, in ActivityInput) (string, error) {
		g.mu.Lock()
		g.calls++
		n := g.calls
		g.mu.Unlock()
		if n == 1 {
			select {
			case <-g.started:
			default:
				close(g.started)
			}
			<-g.gate // 崩溃模拟：调用永远不返回，直到测试放行
		}
		g.se.apply(in.IdempotencyToken)
		return "recovered@" + in.IdempotencyToken, nil
	}
}

// TestRecoverAfterCrashMidAttempt 步骤首次尝试已把 running 状态落盘，随后“进程崩溃”：
// 新引擎必须等旧租约过期后接管、以同一幂等令牌重试，旧协程晚到的结果必须被丢弃。
func TestRecoverAfterCrashMidAttempt(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e1 := env.newEngine(t, "old-worker", 10*time.Second)
	mustDef(e1, WorkflowDef{Name: "recover-wf", Steps: []StepDef{
		{Name: "only", Activity: "gate", Retry: RetryPolicy{MaxAttempts: 3}},
	}})
	gate := &gateActivity{se: env.se, started: make(chan struct{}), gate: make(chan struct{})}
	e1.RegisterActivity("gate", gate.fn())
	if _, err := e1.Start(ctx, "wf-rec2", "recover-wf", "x"); err != nil {
		t.Fatal(err)
	}
	l := mustAcquire(t, e1, ctx, "wf-rec2")
	advDone := make(chan error, 1)
	go func() {
		_, err := e1.Advance(ctx, "wf-rec2", l.Token, l.Fence)
		advDone <- err
	}()
	<-gate.started
	// 此时 step_started 已落盘，活动仍在执行——直接“杀掉进程”（关闭存储、遗弃引擎与协程）。
	if err := env.store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := NewFileStore(env.dir)
	if err != nil {
		t.Fatal(err)
	}
	env.store = store
	e2 := NewEngine(store, Options{WorkerID: "new-worker", LeaseDuration: 10 * time.Second, Clock: env.clock, Logger: env.log})
	mustDef(e2, WorkflowDef{Name: "recover-wf", Steps: []StepDef{
		{Name: "only", Activity: "gate", Retry: RetryPolicy{MaxAttempts: 3}},
	}})
	e2.RegisterActivity("gate", gate.fn())

	st, err := e2.Get(ctx, "wf-rec2")
	if err != nil {
		t.Fatal(err)
	}
	if st.Steps[0].Status != StepRunning || st.Steps[0].Attempt != 1 {
		t.Fatalf("recovered mid-attempt state wrong: %+v", st.Steps[0])
	}
	// 旧租约未过期：不能接管。
	if _, _, err := e2.AcquireLease(ctx, "wf-rec2"); conflictKind(err) != ConflictLeaseHeldByOther {
		t.Fatalf("expected early takeover rejected, got %v", err)
	}

	// 租约过期，Tick 接管并完成步骤。
	env.clock.Advance(11 * time.Second)
	if _, err := e2.Tick(ctx); err != nil {
		t.Fatalf("tick after takeover: %v", err)
	}
	st, _ = e2.Get(ctx, "wf-rec2")
	if st.Status != WFCompleted || st.Steps[0].Status != StepSucceeded {
		t.Fatalf("expected recovered completion, got %s/%s", st.Status, st.Steps[0].Status)
	}
	if st.Steps[0].Attempt != 2 {
		t.Fatalf("expected takeover retry attempt=2, got %d", st.Steps[0].Attempt)
	}
	if st.Result != "recovered@"+idempotencyToken("wf-rec2", 0) {
		t.Fatalf("unexpected result %q", st.Result)
	}

	// 放行被遗弃的旧协程：它晚到的成功结果必须因租约已失而被丢弃，不产生覆盖提交。
	close(gate.gate)
	select {
	case err := <-advDone:
		if err == nil {
			t.Fatal("stale attempt result must be rejected, got nil")
		}
		if k := conflictKind(err); k != ConflictLeaseNotHeld && k != ConflictLeaseHeldByOther &&
			k != ConflictLeaseVersionStale && k != ConflictLeaseExpired {
			t.Fatalf("stale attempt rejected with unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("abandoned attempt did not return after gate release")
	}
	// 最终状态仍是接管者提交的完成态，版本未回退。
	st, _ = e2.Get(ctx, "wf-rec2")
	if st.Status != WFCompleted {
		t.Fatalf("stale result overwrote state: %s", st.Status)
	}
	events, _ := e2.History(ctx, "wf-rec2")
	var tookOver bool
	for _, ev := range events {
		if ev.Type == EvLeaseTakenOver && ev.Detail["previous_holder"] == "old-worker" {
			tookOver = true
		}
	}
	if !tookOver {
		t.Fatal("missing takeover event from old-worker")
	}
}

// TestRecoverDuringBackoff 瞬时失败后等待退避时重启：退避时间点持久化，
// 新引擎在到期前不会提前重试，到期后正常重试成功。
func TestRecoverDuringBackoff(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e1 := env.newEngine(t, "old", 10*time.Second)
	mustDef(e1, WorkflowDef{Name: "backoff-wf", Steps: []StepDef{
		{Name: "f", Activity: "flaky2", Retry: RetryPolicy{
			MaxAttempts: 3, InitialBackoff: 500 * time.Millisecond, Multiplier: 2}},
	}})
	calls := 0
	var mu sync.Mutex
	e1.RegisterActivity("flaky2", func(ctx ActivityContext, in ActivityInput) (string, error) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n == 1 {
			return "", Transientf("boom at first attempt")
		}
		env.se.apply(in.IdempotencyToken)
		return "ok", nil
	})
	if _, err := e1.Start(ctx, "wf-rec3", "backoff-wf", "x"); err != nil {
		t.Fatal(err)
	}
	if _, err := e1.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	st, _ := e1.Get(ctx, "wf-rec3")
	if st.Steps[0].Status != StepWaiting {
		t.Fatalf("precondition waiting, got %s", st.Steps[0].Status)
	}

	e2 := env.reopen(t, "new")
	mustDef(e2, WorkflowDef{Name: "backoff-wf", Steps: []StepDef{
		{Name: "f", Activity: "flaky2", Retry: RetryPolicy{
			MaxAttempts: 3, InitialBackoff: 500 * time.Millisecond, Multiplier: 2}},
	}})
	e2.RegisterActivity("flaky2", func(ctx ActivityContext, in ActivityInput) (string, error) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n == 1 {
			return "", Transientf("boom at first attempt")
		}
		env.se.apply(in.IdempotencyToken)
		return "ok", nil
	})
	// 退避未到期：Tick 跳过，不产生新尝试。
	if advanced, err := e2.Tick(ctx); err != nil || len(advanced) != 0 {
		t.Fatalf("expected no advance during backoff, advanced=%v err=%v", advanced, err)
	}
	st, _ = e2.Get(ctx, "wf-rec3")
	if st.Steps[0].Attempt != 1 {
		t.Fatalf("premature retry: attempt=%d", st.Steps[0].Attempt)
	}
	// 到期后续跑成功。
	env.clock.Advance(600 * time.Millisecond)
	if _, err := e2.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	st, _ = e2.Get(ctx, "wf-rec3")
	if st.Status != WFCompleted || calls != 2 {
		t.Fatalf("expected completion after backoff resume: status=%s calls=%d", st.Status, calls)
	}
}
