package workflow

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTerminate(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w", time.Minute)
	if _, err := e.Start(ctx, "wf-t1", "echo", "x"); err != nil {
		t.Fatal(err)
	}
	st, err := e.Terminate(ctx, "wf-t1", "operator abort")
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != WFFailed || st.Steps[0].Status != StepFailed || st.Steps[0].FinalClass != FailurePermanent {
		t.Fatalf("bad terminated state: %+v", st)
	}
	if st.Failure == nil || st.Failure.Message != "terminated: operator abort" {
		t.Fatalf("bad terminal reason: %+v", st.Failure)
	}
	// 重复终止幂等，不产生新提交。
	v := st.Version
	st2, err := e.Terminate(ctx, "wf-t1", "again")
	if err != nil || st2.Version != v || st2.Status != WFFailed {
		t.Fatalf("idempotent terminate broken: %v %+v", err, st2)
	}
	// 终态后 Tick 不会复活它，历史中只有一次 workflow_failed。
	env.clock.Advance(time.Hour)
	if _, err := e.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	events, _ := e.History(ctx, "wf-t1")
	n := 0
	for _, ev := range events {
		if ev.Type == EvWorkflowFailed {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("expected exactly 1 workflow_failed event, got %d", n)
	}
}

func TestUnknownActivityIsPermanent(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w", time.Minute)
	if _, err := e.Start(ctx, "wf-u", "echo", "x"); err != nil {
		t.Fatal(err)
	}
	l := mustAcquire(t, e, ctx, "wf-u")
	st, err := e.Advance(ctx, "wf-u", l.Token, l.Fence)
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != WFFailed || st.Steps[0].FinalClass != FailurePermanent {
		t.Fatalf("missing activity must be permanent failure, got status=%s class=%q",
			st.Status, st.Steps[0].FinalClass)
	}
}

func TestLeaseRenewalAndAdvanceWithWrongLease(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w", 10*time.Second)
	if _, err := e.Start(ctx, "wf-ren", "echo", "x"); err != nil {
		t.Fatal(err)
	}
	// 无租约直接推进被拒绝。
	if _, err := e.Advance(ctx, "wf-ren", "nope", 99); conflictKind(err) != ConflictLeaseNotHeld {
		t.Fatalf("expected no-lease conflict, got %v", err)
	}
	l := mustAcquire(t, e, ctx, "wf-ren")
	// 续租延长过期时间。
	env.clock.Advance(9 * time.Second)
	rl, err := e.RenewLease(ctx, "wf-ren", l.Token, l.Fence)
	if err != nil {
		t.Fatal(err)
	}
	if !rl.ExpiresAt.After(l.ExpiresAt) {
		t.Fatal("renewal did not extend expiry")
	}
	// 再推进 9s（累计 18s）：若无续租本该过期，但续租后仍有效，错误 fence 被拒。
	env.clock.Advance(9 * time.Second)
	if _, err := e.Advance(ctx, "wf-ren", l.Token, l.Fence+1); conflictKind(err) != ConflictLeaseVersionStale {
		t.Fatalf("expected stale fence conflict, got %v", err)
	}
	e.RegisterActivity("echo", echoActivity(env.se))
	st, err := e.Advance(ctx, "wf-ren", l.Token, l.Fence)
	if err != nil || st.Status != WFCompleted {
		t.Fatalf("advance after renew failed: %v %+v", err, st)
	}
}

func TestTickAdvancesMultipleInstances(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w", time.Minute)
	e.RegisterActivity("echo", echoActivity(env.se))
	for _, id := range []string{"m1", "m2", "m3"} {
		if _, err := e.Start(ctx, id, "echo", id); err != nil {
			t.Fatal(err)
		}
	}
	got, err := e.Tick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 advanced, got %v", got)
	}
	list, _ := e.List(ctx)
	if len(list) != 3 {
		t.Fatalf("expected 3 listed, got %d", len(list))
	}
	for _, st := range list {
		if st.Status != WFCompleted {
			t.Fatalf("instance %s not completed: %s", st.ID, st.Status)
		}
	}
}

func TestNotFound(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w", time.Minute)
	for _, fn := range []func() error{
		func() error { _, err := e.Get(ctx, "missing"); return err },
		func() error { _, err := e.History(ctx, "missing"); return err },
		func() error { _, _, err := e.Result(ctx, "missing"); return err },
		func() error { _, err := e.Failures(ctx, "missing", 0); return err },
		func() error { _, _, err := e.AcquireLease(ctx, "missing"); return err },
	} {
		if err := fn(); !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
	}
}
