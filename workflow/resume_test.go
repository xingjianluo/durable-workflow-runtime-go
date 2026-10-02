package workflow

import (
	"context"
	"sync"
	"testing"
	"time"
)

// flakyN 返回一个活动：前 failFirst 次调用瞬时失败，之后成功并登记副作用。
// calls 返回当前调用次数（跨引擎共享，模拟同一业务方）。
func flakyN(se *testSideEffect, failFirst int) (ActivityFunc, func() int) {
	var mu sync.Mutex
	calls := 0
	fn := func(ctx ActivityContext, in ActivityInput) (string, error) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n <= failFirst {
			return "", Transientf("transient boom #%d", n)
		}
		se.apply(in.IdempotencyToken)
		return "ok:" + in.Input, nil
	}
	return fn, func() int {
		mu.Lock()
		defer mu.Unlock()
		return calls
	}
}

// TestResumeContinuesFromFailedStep 第二步预算耗尽失败后显式续跑：
// 从卡住的第二步继续、尝试编号累进（2 而不是从 1 重来）、第一步不重跑，
// 实例 ID 与历史保持不变。
func TestResumeContinuesFromFailedStep(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w1", time.Minute)
	mustDef(e, WorkflowDef{Name: "two-resume", Steps: []StepDef{
		{Name: "a", Activity: "echo", Retry: RetryPolicy{MaxAttempts: 1}},
		{Name: "b", Activity: "flakyB", Retry: RetryPolicy{MaxAttempts: 1}},
	}})
	e.RegisterActivity("echo", echoActivity(env.se))
	flaky, calls := flakyN(env.se, 1)
	e.RegisterActivity("flakyB", flaky)

	if _, err := e.Start(ctx, "wf-rs1", "two-resume", "in"); err != nil {
		t.Fatal(err)
	}
	st := drainToCompletion(t, e, ctx, "wf-rs1", 10)
	if st.Status != WFFailed || st.Cursor != 1 || st.Steps[1].Attempt != 1 {
		t.Fatalf("precondition failed state wrong: status=%s cursor=%d attempt=%d",
			st.Status, st.Cursor, st.Steps[1].Attempt)
	}
	if env.se.count() != 1 {
		t.Fatalf("precondition: only step a side effect expected, got %d", env.se.count())
	}

	// 显式续跑：追加 1 次预算。
	st, err := e.Resume(ctx, "wf-rs1", 1)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if st.ID != "wf-rs1" || st.Status != WFRunning || st.Cursor != 1 {
		t.Fatalf("resume must keep identity and cursor: %+v", st)
	}
	if len(st.Resumes) != 1 || st.Resumes[0].ExtraAttempts != 1 || st.Resumes[0].Seq != 1 {
		t.Fatalf("resume record missing: %+v", st.Resumes)
	}

	st = drainToCompletion(t, e, ctx, "wf-rs1", 10)
	if st.Status != WFCompleted || st.Result != "ok:echo:in" {
		t.Fatalf("expected completed after resume, got %s result=%q", st.Status, st.Result)
	}
	if st.Steps[0].Attempt != 1 {
		t.Fatalf("step a must not re-run, attempt=%d", st.Steps[0].Attempt)
	}
	if st.Steps[1].Attempt != 2 {
		t.Fatalf("step b must continue at attempt 2, got %d", st.Steps[1].Attempt)
	}
	if calls() != 2 {
		t.Fatalf("flakyB activity calls=%d, want 2", calls())
	}
	if env.se.count() != 2 {
		t.Fatalf("expected 2 side effects total, got %d", env.se.count())
	}
	// 历史一条不丢：失败记录与续跑事件都在。
	failures, _ := e.Failures(ctx, "wf-rs1", 1)
	if len(failures) != 1 || failures[0].Attempt != 1 || failures[0].Class != FailureTransient {
		t.Fatalf("failure history lost after resume: %+v", failures)
	}
	events, _ := e.History(ctx, "wf-rs1")
	var resumed bool
	for _, ev := range events {
		if ev.Type == EvWorkflowResumed && ev.StepIndex == 1 {
			resumed = true
		}
	}
	if !resumed {
		t.Fatal("missing workflow_resumed event")
	}
}

// TestResumeRequiresExplicitBudget 不指定或给无效预算一律拒绝，且实例状态不变。
func TestResumeRequiresExplicitBudget(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w1", time.Minute)
	mustDef(e, WorkflowDef{Name: "one-shot", Steps: []StepDef{
		{Name: "only", Activity: "perm", Retry: RetryPolicy{MaxAttempts: 1}},
	}})
	e.RegisterActivity("perm", func(ctx ActivityContext, in ActivityInput) (string, error) {
		return "", Transientf("always fails")
	})
	if _, err := e.Start(ctx, "wf-rs2", "one-shot", "x"); err != nil {
		t.Fatal(err)
	}
	st := drainToCompletion(t, e, ctx, "wf-rs2", 5)
	if st.Status != WFFailed {
		t.Fatalf("precondition failed, got %s", st.Status)
	}
	ver := st.Version

	for _, budget := range []int{0, -1, -100} {
		if _, err := e.Resume(ctx, "wf-rs2", budget); conflictKind(err) != ConflictResumeInvalidBudget {
			t.Fatalf("budget %d: expected resume_invalid_budget, got %v", budget, err)
		}
	}
	st, _ = e.Get(ctx, "wf-rs2")
	if st.Status != WFFailed || st.Version != ver {
		t.Fatalf("rejected resume must not change state: status=%s version=%d->%d", st.Status, ver, st.Version)
	}
	if _, err := e.Resume(ctx, "missing", 1); err != ErrNotFound {
		t.Fatalf("resume missing id: got %v", err)
	}
}

// TestResumeCompletedAndRunningRejected 已成功完成的实例续跑被明确拒绝（可区分原因），
// 仍在运行的实例同样拒绝。
func TestResumeCompletedAndRunningRejected(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w1", time.Minute)
	e.RegisterActivity("echo", echoActivity(env.se))

	if _, err := e.Start(ctx, "wf-done", "echo", "x"); err != nil {
		t.Fatal(err)
	}
	st := drainToCompletion(t, e, ctx, "wf-done", 5)
	if st.Status != WFCompleted {
		t.Fatalf("precondition completed, got %s", st.Status)
	}
	if _, err := e.Resume(ctx, "wf-done", 3); conflictKind(err) != ConflictResumeCompleted {
		t.Fatalf("expected resume_completed_workflow, got %v", err)
	}

	if _, err := e.Start(ctx, "wf-running", "echo", "y"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Resume(ctx, "wf-running", 3); conflictKind(err) != ConflictResumeNotFailed {
		t.Fatalf("expected resume_not_failed, got %v", err)
	}
}

// TestResumeAfterTerminate 人工终止的实例可显式续跑：终止本身不消耗尝试预算，
// 续跑后尝试编号从 1 开始，最终正常完成。
func TestResumeAfterTerminate(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w1", time.Minute)
	e.RegisterActivity("echo", echoActivity(env.se))
	if _, err := e.Start(ctx, "wf-term", "echo", "t"); err != nil {
		t.Fatal(err)
	}
	st, err := e.Terminate(ctx, "wf-term", "operator stop")
	if err != nil {
		t.Fatalf("Terminate: %v", err)
	}
	if st.Status != WFFailed || st.Steps[0].Attempt != 0 {
		t.Fatalf("precondition terminated: status=%s attempt=%d", st.Status, st.Steps[0].Attempt)
	}
	if _, err := e.Resume(ctx, "wf-term", 2); err != nil {
		t.Fatalf("Resume after terminate: %v", err)
	}
	st = drainToCompletion(t, e, ctx, "wf-term", 5)
	if st.Status != WFCompleted || st.Steps[0].Attempt != 1 {
		t.Fatalf("expected completed with attempt=1, got %s attempt=%d", st.Status, st.Steps[0].Attempt)
	}
	if len(st.Resumes) != 1 {
		t.Fatalf("resume record missing: %+v", st.Resumes)
	}
}

// TestResumeMatchesStraightRunConclusion 同一活动逻辑：实例 A 预算充足一路跑完，
// 实例 B 预算耗尽后续跑追加预算跑完。两者最终状态、尝试次数、失败记录与结论必须一致。
func TestResumeMatchesStraightRunConclusion(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w1", time.Minute)
	mustDef(e, WorkflowDef{Name: "straight", Steps: []StepDef{
		{Name: "only", Activity: "flakyA", Retry: RetryPolicy{MaxAttempts: 3}},
	}})
	mustDef(e, WorkflowDef{Name: "resumable", Steps: []StepDef{
		{Name: "only", Activity: "flakyB", Retry: RetryPolicy{MaxAttempts: 1}},
	}})
	flakyA, _ := flakyN(env.se, 2)
	flakyB, _ := flakyN(env.se, 2)
	e.RegisterActivity("flakyA", flakyA)
	e.RegisterActivity("flakyB", flakyB)

	if _, err := e.Start(ctx, "wf-straight", "straight", "in"); err != nil {
		t.Fatal(err)
	}
	stA := drainToCompletion(t, e, ctx, "wf-straight", 10)

	if _, err := e.Start(ctx, "wf-resumed", "resumable", "in"); err != nil {
		t.Fatal(err)
	}
	stB := drainToCompletion(t, e, ctx, "wf-resumed", 10)
	if stB.Status != WFFailed {
		t.Fatalf("precondition: B must fail first, got %s", stB.Status)
	}
	if _, err := e.Resume(ctx, "wf-resumed", 2); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	stB = drainToCompletion(t, e, ctx, "wf-resumed", 10)

	if stA.Status != WFCompleted || stB.Status != WFCompleted {
		t.Fatalf("both must complete: A=%s B=%s", stA.Status, stB.Status)
	}
	if stA.Steps[0].Attempt != 3 || stB.Steps[0].Attempt != 3 {
		t.Fatalf("attempt counts must match: A=%d B=%d", stA.Steps[0].Attempt, stB.Steps[0].Attempt)
	}
	if stA.Result != stB.Result {
		t.Fatalf("results must match: A=%q B=%q", stA.Result, stB.Result)
	}
	fA, _ := e.Failures(ctx, "wf-straight", 0)
	fB, _ := e.Failures(ctx, "wf-resumed", 0)
	if len(fA) != 2 || len(fB) != 2 {
		t.Fatalf("failure records must match: A=%d B=%d", len(fA), len(fB))
	}
	for i := range fA {
		if fA[i].Attempt != fB[i].Attempt || fA[i].Class != fB[i].Class {
			t.Fatalf("failure %d mismatch: A=%+v B=%+v", i, fA[i], fB[i])
		}
	}
	if stB.Steps[0].FinalClass != FailureNone || stB.Failure != nil {
		t.Fatalf("resumed success must clear final failure: %+v", stB.Steps[0])
	}
}

// TestResumeConcurrentTakeoverSingleAdvancer 续跑与接管并发：
// 两个执行者同时推进同一实例的同一步骤，恰好一方成功，业务副作用不重复。
func TestResumeConcurrentTakeoverSingleAdvancer(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e1 := env.newEngine(t, "worker-A", time.Minute)
	e2 := env.newEngine(t, "worker-B", time.Minute)
	mustDef(e2, WorkflowDef{Name: "echo", Steps: []StepDef{
		{Name: "only", Activity: "echo", Retry: RetryPolicy{MaxAttempts: 1}},
	}})
	flaky, _ := flakyN(env.se, 1) // 首次调用失败，之后成功
	e1.RegisterActivity("echo", flaky)
	e2.RegisterActivity("echo", flaky)

	if _, err := e1.Start(ctx, "wf-rc", "echo", "z"); err != nil {
		t.Fatal(err)
	}
	st := drainToCompletion(t, e1, ctx, "wf-rc", 5)
	if st.Status != WFFailed {
		t.Fatalf("precondition failed, got %s", st.Status)
	}
	if _, err := e1.Resume(ctx, "wf-rc", 1); err != nil {
		t.Fatalf("Resume: %v", err)
	}

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
	st, _ = e1.Get(ctx, "wf-rc")
	if st.Status != WFCompleted {
		t.Fatalf("expected completed, got %s", st.Status)
	}
	if st.Steps[0].Attempt != 2 {
		t.Fatalf("resumed step must advance exactly once: attempt=%d", st.Steps[0].Attempt)
	}
	if env.se.count() != 1 {
		t.Fatalf("expected exactly 1 side effect, got %d (%v)", env.se.count(), env.se.tokens())
	}
}
