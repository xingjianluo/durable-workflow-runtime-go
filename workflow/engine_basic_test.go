package workflow

import (
	"context"
	"errors"
	"testing"
	"time"
)

// echoActivity 成功活动；同时以幂等令牌登记副作用，验证只生效一次。
func echoActivity(se *testSideEffect) ActivityFunc {
	return func(ctx ActivityContext, in ActivityInput) (string, error) {
		se.apply(in.IdempotencyToken)
		return "echo:" + in.Input, nil
	}
}

func twoStepEchoDef() WorkflowDef {
	return WorkflowDef{Name: "two", Steps: []StepDef{
		{Name: "a", Activity: "echo", Retry: RetryPolicy{MaxAttempts: 1}},
		{Name: "b", Activity: "echo", Retry: RetryPolicy{MaxAttempts: 1}},
	}}
}

func TestStartAndCompleteHappyPath(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w1", time.Minute)
	if err := e.RegisterDef(twoStepEchoDef()); err != nil {
		t.Fatal(err)
	}
	e.RegisterActivity("echo", echoActivity(env.se))

	st, err := e.Start(ctx, "wf-1", "two", "hello")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if st.Status != WFRunning || st.Version != 1 {
		t.Fatalf("unexpected initial state: status=%s version=%d", st.Status, st.Version)
	}

	// 手动租约 + 推进，确定性走完两个步骤。
	l := mustAcquire(t, e, ctx, "wf-1")
	st, err = e.Advance(ctx, "wf-1", l.Token, l.Fence)
	if err != nil {
		t.Fatalf("Advance a: %v", err)
	}
	if st.Steps[0].Status != StepSucceeded || st.Steps[0].Result != "echo:hello" {
		t.Fatalf("step a bad state: %+v", st.Steps[0])
	}
	if st.Status != WFRunning || st.Cursor != 1 {
		t.Fatalf("expected running cursor=1, got %s cursor=%d", st.Status, st.Cursor)
	}
	// 成功一步后租约已释放，推进下一步需要重新获取。
	l2 := mustAcquire(t, e, ctx, "wf-1")
	st, err = e.Advance(ctx, "wf-1", l2.Token, l2.Fence)
	if err != nil {
		t.Fatalf("Advance b: %v", err)
	}
	if st.Status != WFCompleted {
		t.Fatalf("expected completed, got %s", st.Status)
	}
	if st.Result != "echo:echo:hello" {
		t.Fatalf("unexpected final result %q", st.Result)
	}
	if env.se.count() != 2 {
		t.Fatalf("expected exactly 2 side effects, got %d (%v)", env.se.count(), env.se.tokens())
	}

	// 终态不可再推进。
	_, _, err = e.AcquireLease(ctx, "wf-1")
	if conflictKind(err) != ConflictTerminal {
		t.Fatalf("expected terminal conflict, got %v", err)
	}

	result, gst, err := e.Result(ctx, "wf-1")
	if err != nil || result != "echo:echo:hello" {
		t.Fatalf("Result: %v %q", err, result)
	}
	if gst.Status != WFCompleted {
		t.Fatalf("result snapshot status=%s", gst.Status)
	}

	// 事件历史完整、序号连续。
	events, err := e.History(ctx, "wf-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) < 6 {
		t.Fatalf("expected >=6 events, got %d: %+v", len(events), events)
	}
	for i, ev := range events {
		if ev.Seq != int64(i+1) {
			t.Fatalf("event seq gap at %d: %d", i, ev.Seq)
		}
	}
}

func TestDuplicateStartRejected(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w1", time.Minute)
	if _, err := e.Start(ctx, "dup", "echo", "x"); err != nil {
		t.Fatal(err)
	}
	_, err := e.Start(ctx, "dup", "echo", "y")
	if ck := conflictKind(err); ck != ConflictStartAlreadyExists {
		t.Fatalf("expected ConflictStartAlreadyExists, got %v (kind=%q)", err, ck)
	}
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("expected errors.Is ErrAlreadyExists, got %v", err)
	}
	// 原实例输入不被第二次启动覆盖。
	st, err := e.Get(ctx, "dup")
	if err != nil || st.Input != "x" {
		t.Fatalf("original start overwritten: %+v err=%v", st, err)
	}
}

func TestDuplicateSchedulingIsIdempotent(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w1", time.Minute)
	if err := e.RegisterDef(twoStepEchoDef()); err != nil {
		t.Fatal(err)
	}
	e.RegisterActivity("echo", echoActivity(env.se))
	if _, err := e.Start(ctx, "wf-d", "two", "in"); err != nil {
		t.Fatal(err)
	}
	// 重复 Tick：第一次推进步骤 a，之后无租约可获取/无可推进时不得重复执行 a。
	for i := 0; i < 5; i++ {
		if _, err := e.Tick(ctx); err != nil {
			t.Fatal(err)
		}
	}
	st, err := e.Get(ctx, "wf-d")
	if err != nil {
		t.Fatal(err)
	}
	// 同一 worker 在首个循环里即可顺序领取并执行 a、b；重复 Tick 不得重跑任何已完成步骤。
	if st.Cursor != 2 || !st.Steps[0].Status.TerminalStep() || !st.Steps[1].Status.TerminalStep() {
		t.Fatalf("expected both steps done, cursor=%d states=%s,%s", st.Cursor, st.Steps[0].Status, st.Steps[1].Status)
	}
	if st.Steps[0].Attempt != 1 || st.Steps[1].Attempt != 1 {
		t.Fatalf("steps re-executed: attempts=%d,%d", st.Steps[0].Attempt, st.Steps[1].Attempt)
	}
	// 再推进时钟并继续重复调度：终态工作流不会再产生副作用。
	env.clock.Advance(2 * time.Minute)
	for i := 0; i < 5; i++ {
		if _, err := e.Tick(ctx); err != nil {
			t.Fatal(err)
		}
	}
	st, _ = e.Get(ctx, "wf-d")
	if st.Status != WFCompleted {
		t.Fatalf("expected completed, got %s", st.Status)
	}
	if env.se.count() != 2 {
		t.Fatalf("expected exactly 2 side effects, got %d (%v)", env.se.count(), env.se.tokens())
	}
}

func TestNotTerminalResultRejected(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w1", time.Minute)
	if _, err := e.Start(ctx, "wf-n", "echo", "x"); err != nil {
		t.Fatal(err)
	}
	_, st, err := e.Result(ctx, "wf-n")
	if conflictKind(err) != ConflictNotTerminal || st == nil {
		t.Fatalf("expected not-terminal conflict with snapshot, got %v st=%v", err, st)
	}
}
