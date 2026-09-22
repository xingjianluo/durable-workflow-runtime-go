package workflow

import (
	"context"
	"testing"
	"time"
)

func timeoutDef() WorkflowDef {
	return WorkflowDef{Name: "timeout-wf", Steps: []StepDef{
		{Name: "slow", Activity: "slow", Retry: RetryPolicy{
			MaxAttempts: 3, AttemptTimeout: 100 * time.Millisecond,
			InitialBackoff: 50 * time.Millisecond, Multiplier: 2}},
	}}
}

// slowActivity 阻塞直到 ctx 完成，模拟超时活动。started 在活动真正开始时关闭，
// 测试据此确认超时计时器已注册后再推进假时钟，避免计时器注册与推进之间的竞态。
func slowActivity(started chan struct{}) ActivityFunc {
	return func(ctx ActivityContext, in ActivityInput) (string, error) {
		if started != nil {
			select {
			case <-started:
			default:
				close(started)
			}
		}
		<-ctx.Done()
		return "", ctx.Err()
	}
}

func TestAttemptTimeoutRetriedAsTransient(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w-timeout", 30*time.Second)
	if err := e.RegisterDef(timeoutDef()); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	e.RegisterActivity("slow", slowActivity(started))
	if _, err := e.Start(ctx, "wf-to", "timeout-wf", "x"); err != nil {
		t.Fatal(err)
	}

	// 第一次尝试：假时钟推进超过 AttemptTimeout，应被分类为 timeout 并进入退避。
	l := mustAcquire(t, e, ctx, "wf-to")
	done := make(chan struct{})
	go func() {
		_, _ = e.Advance(ctx, "wf-to", l.Token, l.Fence)
		close(done)
	}()
	<-started // 确认活动已开始、超时计时器已注册
	env.clock.Advance(150 * time.Millisecond)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Advance did not return after fake-clock timeout")
	}

	st, _ := e.Get(ctx, "wf-to")
	if st.Steps[0].Status != StepWaiting {
		t.Fatalf("expected waiting after timeout, got %s", st.Steps[0].Status)
	}
	failures, _ := e.Failures(ctx, "wf-to", 0)
	if len(failures) != 1 || failures[0].Class != FailureTimeout || !failures[0].TimedOut {
		t.Fatalf("expected one timeout failure, got %+v", failures)
	}
	if st.Steps[0].Attempt != 1 {
		t.Fatalf("expected attempt=1 recorded, got %d", st.Steps[0].Attempt)
	}
}

func TestLeaseMutualExclusionAndExpiryTakeover(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e1 := env.newEngine(t, "worker-A", 10*time.Second)
	e2 := env.newEngine(t, "worker-B", 10*time.Second)
	if _, err := e1.Start(ctx, "wf-l", "echo", "x"); err != nil {
		t.Fatal(err)
	}

	st, leaseA, err := e1.AcquireLease(ctx, "wf-l")
	if err != nil {
		t.Fatal(err)
	}
	// worker-A 重复领取：幂等返回同一租约。
	_, leaseAgain, err := e1.AcquireLease(ctx, "wf-l")
	if err != nil || leaseAgain.Token != leaseA.Token || leaseAgain.Fence != leaseA.Fence {
		t.Fatalf("idempotent acquire broken: %v %+v", err, leaseAgain)
	}
	// worker-B 在租约有效期内领取被拒绝，且有明确原因。
	_, _, err = e2.AcquireLease(ctx, "wf-l")
	if ck := conflictKind(err); ck != ConflictLeaseHeldByOther {
		t.Fatalf("expected lease-held-by-other, got %v (kind=%q)", err, ck)
	}
	// B 用 A 的 token/fence 推进也必须被拒绝。
	_, err = e2.Advance(ctx, "wf-l", leaseA.Token, leaseA.Fence)
	if ck := conflictKind(err); ck != ConflictLeaseHeldByOther {
		t.Fatalf("expected B advance rejected, got %v (kind=%q)", err, ck)
	}

	// 租约未过期时，A 的过期 token / 错误 fence 被拒绝。
	_, err = e1.RenewLease(ctx, "wf-l", "wrong-token", leaseA.Fence)
	if ck := conflictKind(err); ck != ConflictLeaseVersionStale {
		t.Fatalf("expected stale fence/token conflict, got %v", err)
	}

	// 租约过期后 B 接管：fence 递增，并记录 takeover。
	env.clock.Advance(11 * time.Second)
	st, leaseB, err := e2.AcquireLease(ctx, "wf-l")
	if err != nil {
		t.Fatalf("takeover after expiry failed: %v", err)
	}
	if leaseB.Fence != leaseA.Fence+1 || leaseB.Holder != "worker-B" {
		t.Fatalf("bad takeover lease: %+v (prev fence=%d)", leaseB, leaseA.Fence)
	}
	// A 手里的旧租约立刻失效：续租/释放/推进都被拒绝（持有者已变更），不能回退 B 的状态。
	rejected := func(err error) bool {
		k := conflictKind(err)
		return k == ConflictLeaseVersionStale || k == ConflictLeaseHeldByOther || k == ConflictLeaseExpired
	}
	if _, err := e1.RenewLease(ctx, "wf-l", leaseA.Token, leaseA.Fence); !rejected(err) {
		t.Fatalf("expected renew after takeover rejected, got %v", err)
	}
	if err := e1.ReleaseLease(ctx, "wf-l", leaseA.Token, leaseA.Fence); !rejected(err) {
		t.Fatalf("expected release after takeover rejected, got %v", err)
	}
	_, err = e1.Advance(ctx, "wf-l", leaseA.Token, leaseA.Fence)
	if !rejected(err) {
		t.Fatalf("expected advance after takeover rejected, got %v", err)
	}
	// B 的租约有效，可正常推进。
	e2.RegisterActivity("echo", echoActivity(env.se))
	st, err = e2.Advance(ctx, "wf-l", leaseB.Token, leaseB.Fence)
	if err != nil || st.Status != WFCompleted {
		t.Fatalf("B advance failed: %v status=%s", err, st.Status)
	}
	events, _ := e2.History(ctx, "wf-l")
	var sawTakeover bool
	for _, ev := range events {
		if ev.Type == EvLeaseTakenOver {
			sawTakeover = true
			if ev.Detail["previous_holder"] != "worker-A" {
				t.Fatalf("takeover detail bad: %+v", ev.Detail)
			}
		}
	}
	if !sawTakeover {
		t.Fatal("missing lease_taken_over event")
	}
	if env.se.count() != 1 {
		t.Fatalf("expected 1 side effect, got %d", env.se.count())
	}
}

// TestConcurrentTickersOnlyOneWins 两个引擎并发 Tick 同一实例：
// 必须恰好一个执行者成功，另一个收到版本/租约冲突，步骤只执行一次。
func TestConcurrentTickersOnlyOneWins(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e1 := env.newEngine(t, "worker-A", time.Minute)
	e2 := env.newEngine(t, "worker-B", time.Minute)
	if _, err := e1.Start(ctx, "wf-c", "echo", "z"); err != nil {
		t.Fatal(err)
	}
	e1.RegisterActivity("echo", echoActivity(env.se))
	e2.RegisterActivity("echo", echoActivity(env.se))

	start := make(chan struct{})
	errs := make(chan error, 2)
	tick := func(e *Engine) {
		<-start
		_, err := e.Tick(ctx)
		errs <- err
	}
	go tick(e1)
	go tick(e2)
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("tick: %v", err)
		}
	}
	st, _ := e1.Get(ctx, "wf-c")
	if st.Status != WFCompleted {
		t.Fatalf("expected completed, got %s", st.Status)
	}
	if st.Steps[0].Attempt != 1 {
		t.Fatalf("step executed more than once: attempt=%d", st.Steps[0].Attempt)
	}
	if env.se.count() != 1 {
		t.Fatalf("expected exactly 1 side effect, got %d (%v)", env.se.count(), env.se.tokens())
	}
}
