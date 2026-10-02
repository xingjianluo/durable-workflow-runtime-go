package workflow

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// flakyNActivity 前 failFirst 次瞬时失败，之后成功；成功时登记幂等令牌副作用。
// calls 返回活动被调用的总次数（含失败的尝试）。
func flakyNActivity(se *testSideEffect, failFirst int, calls *int, mu *sync.Mutex) ActivityFunc {
	return func(ctx ActivityContext, in ActivityInput) (string, error) {
		mu.Lock()
		*calls++
		n := *calls
		mu.Unlock()
		if n <= failFirst {
			return "", Transientf("transient boom #%d", n)
		}
		se.apply(in.IdempotencyToken)
		return "ok:" + in.Input, nil
	}
}

func resumeTestDef() WorkflowDef {
	return WorkflowDef{Name: "resume-wf", Steps: []StepDef{
		{Name: "a", Activity: "echo", Retry: RetryPolicy{MaxAttempts: 1}},
		{Name: "b", Activity: "flakyb", Retry: RetryPolicy{MaxAttempts: 2, InitialBackoff: 10 * time.Millisecond, Multiplier: 1}},
	}}
}

// TestResumeContinuesFromStuckStep 预算耗尽失败的实例被显式续跑：
// 从卡住的步骤继续、尝试编号连续、已成功步骤不重跑、历史一条不丢。
func TestResumeContinuesFromStuckStep(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w1", time.Minute)
	mustDef(e, resumeTestDef())
	var mu sync.Mutex
	calls := 0
	e.RegisterActivity("echo", echoActivity(env.se))
	e.RegisterActivity("flakyb", flakyNActivity(env.se, 3, &calls, &mu)) // 前 3 次失败，第 4 次成功

	if _, err := e.Start(ctx, "wf-r1", "resume-wf", "in"); err != nil {
		t.Fatal(err)
	}
	st := drainToCompletion(t, e, ctx, "wf-r1", 20)
	if st.Status != WFFailed {
		t.Fatalf("precondition: expected failed, got %s", st.Status)
	}
	if st.Cursor != 1 || st.Steps[1].Attempt != 2 || st.Steps[1].Status != StepFailed {
		t.Fatalf("precondition: stuck step wrong: cursor=%d step=%+v", st.Cursor, st.Steps[1])
	}
	if !strings.Contains(st.Failure.Message, "retry budget exhausted") {
		t.Fatalf("precondition: unexpected failure reason %q", st.Failure.Message)
	}
	if env.se.count() != 1 { // 只有步骤 a 产生过副作用
		t.Fatalf("precondition: side effects=%d, want 1", env.se.count())
	}
	eventsBefore, _ := e.History(ctx, "wf-r1")

	// 显式追加 2 次预算续跑。
	st, err := e.Resume(ctx, "wf-r1", 2)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if st.Status != WFRunning || st.Cursor != 1 {
		t.Fatalf("after resume: status=%s cursor=%d", st.Status, st.Cursor)
	}
	if st.Steps[1].Attempt != 2 || st.Steps[1].ExtraAttempts != 2 {
		t.Fatalf("attempt numbering must continue, got attempt=%d extra=%d", st.Steps[1].Attempt, st.Steps[1].ExtraAttempts)
	}
	if len(st.Steps[1].Failures) != 2 {
		t.Fatalf("failure history lost: %+v", st.Steps[1].Failures)
	}
	if len(st.Resumes) != 1 || st.Resumes[0].AddedAttempts != 2 || st.Resumes[0].Seq != 1 {
		t.Fatalf("resume record wrong: %+v", st.Resumes)
	}

	st = drainToCompletion(t, e, ctx, "wf-r1", 20)
	if st.Status != WFCompleted {
		t.Fatalf("expected completed after resume, got %s", st.Status)
	}
	if st.Steps[1].Attempt != 4 { // 2（续跑前）+ 2（追加预算）
		t.Fatalf("expected attempt=4, got %d", st.Steps[1].Attempt)
	}
	if st.Result != "ok:echo:in" {
		t.Fatalf("unexpected result %q", st.Result)
	}
	if env.se.count() != 2 { // 步骤 a 未重跑，步骤 b 只成功一次
		t.Fatalf("side effects=%d, want 2 (step a not re-run)", env.se.count())
	}

	// 历史完整：续跑前的事件全在，且依次有 failed → resumed → completed。
	eventsAfter, _ := e.History(ctx, "wf-r1")
	if len(eventsAfter) <= len(eventsBefore) {
		t.Fatalf("history not append-only: before=%d after=%d", len(eventsBefore), len(eventsAfter))
	}
	for i, ev := range eventsBefore {
		if eventsAfter[i].Seq != ev.Seq || eventsAfter[i].Type != ev.Type {
			t.Fatalf("history rewritten at %d: %v -> %v", i, ev.Type, eventsAfter[i].Type)
		}
	}
	var failedEv, resumedEv, completedEv int
	for _, ev := range eventsAfter {
		switch ev.Type {
		case EvWorkflowFailed:
			failedEv++
		case EvWorkflowResumed:
			resumedEv++
			if ev.Detail["step"] != "b" {
				t.Fatalf("resume event missing step detail: %+v", ev.Detail)
			}
		case EvWorkflowCompleted:
			completedEv++
		}
	}
	if failedEv != 1 || resumedEv != 1 || completedEv != 1 {
		t.Fatalf("event counts wrong: failed=%d resumed=%d completed=%d", failedEv, resumedEv, completedEv)
	}
	t.Logf("scenario=resume-continue result=completed attempts=%d resumes=%d side_effects=%d",
		st.Steps[1].Attempt, len(st.Resumes), env.se.count())
}

// TestResumeRequiresExplicitBudget 续跑必须显式给出正的追加预算，否则拒绝。
func TestResumeRequiresExplicitBudget(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w1", time.Minute)
	mustDef(e, resumeTestDef())
	var mu sync.Mutex
	calls := 0
	e.RegisterActivity("echo", echoActivity(env.se))
	e.RegisterActivity("flakyb", flakyNActivity(env.se, 99, &calls, &mu))
	if _, err := e.Start(ctx, "wf-r2", "resume-wf", "in"); err != nil {
		t.Fatal(err)
	}
	if st := drainToCompletion(t, e, ctx, "wf-r2", 20); st.Status != WFFailed {
		t.Fatalf("precondition: expected failed, got %s", st.Status)
	}
	for _, n := range []int{0, -1, -100} {
		_, err := e.Resume(ctx, "wf-r2", n)
		if conflictKind(err) != ConflictInvalidExtraAttempts || !errors.Is(err, ErrInvalidExtraAttempts) {
			t.Fatalf("extraAttempts=%d: expected invalid_extra_attempts, got %v", n, err)
		}
	}
	// 不存在的实例。
	if _, err := e.Resume(ctx, "missing", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	// 拒绝不产生任何状态变化。
	st, _ := e.Get(ctx, "wf-r2")
	if st.Status != WFFailed || len(st.Resumes) != 0 {
		t.Fatalf("rejected resume mutated state: %+v", st)
	}
	t.Logf("scenario=resume-invalid-budget result=rejected")
}

// TestResumeCompletedAndRunningRejected 已成功实例与运行中实例都不能续跑，且原因可区分。
func TestResumeCompletedAndRunningRejected(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w1", time.Minute)
	e.RegisterActivity("echo", echoActivity(env.se))
	if _, err := e.Start(ctx, "wf-done", "echo", "x"); err != nil {
		t.Fatal(err)
	}
	if st := drainToCompletion(t, e, ctx, "wf-done", 5); st.Status != WFCompleted {
		t.Fatalf("precondition: expected completed, got %s", st.Status)
	}
	_, err := e.Resume(ctx, "wf-done", 1)
	if conflictKind(err) != ConflictAlreadyCompleted || !errors.Is(err, ErrWorkflowAlreadyCompleted) {
		t.Fatalf("completed resume: expected workflow_already_completed, got %v", err)
	}

	if _, err := e.Start(ctx, "wf-running", "echo", "y"); err != nil {
		t.Fatal(err)
	}
	_, err = e.Resume(ctx, "wf-running", 1)
	if conflictKind(err) != ConflictNotFailed || !errors.Is(err, ErrWorkflowNotFailed) {
		t.Fatalf("running resume: expected workflow_not_failed, got %v", err)
	}
	t.Logf("scenario=resume-wrong-state completed=rejected(already_completed) running=rejected(not_failed)")
}

// TestConcurrentResumeOnlyOneWins 并发续跑同一失败实例：恰好一个提交成功。
func TestConcurrentResumeOnlyOneWins(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w1", time.Minute)
	mustDef(e, resumeTestDef())
	var mu sync.Mutex
	calls := 0
	e.RegisterActivity("echo", echoActivity(env.se))
	e.RegisterActivity("flakyb", flakyNActivity(env.se, 99, &calls, &mu))
	if _, err := e.Start(ctx, "wf-r3", "resume-wf", "in"); err != nil {
		t.Fatal(err)
	}
	if st := drainToCompletion(t, e, ctx, "wf-r3", 20); st.Status != WFFailed {
		t.Fatalf("precondition: expected failed, got %s", st.Status)
	}

	const n = 8
	var wg sync.WaitGroup
	var okCount int
	var okMu sync.Mutex
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := e.Resume(ctx, "wf-r3", 1); err == nil {
				okMu.Lock()
				okCount++
				okMu.Unlock()
			} else if k := conflictKind(err); k != ConflictVersion && k != ConflictNotFailed {
				t.Errorf("unexpected resume error: %v", err)
			}
		}()
	}
	wg.Wait()
	if okCount != 1 {
		t.Fatalf("expected exactly 1 successful resume, got %d", okCount)
	}
	st, _ := e.Get(ctx, "wf-r3")
	if len(st.Resumes) != 1 || st.Steps[1].ExtraAttempts != 1 {
		t.Fatalf("resume applied more than once: resumes=%+v extra=%d", st.Resumes, st.Steps[1].ExtraAttempts)
	}
	t.Logf("scenario=concurrent-resume result=exactly-one-winner")
}

// TestResumeThenConcurrentTicksSingleAdvancer 续跑后两个执行者并发调度：
// 同一步骤任意时刻只有一个执行者推进，活动调用次数与尝试编号严格对应。
func TestResumeThenConcurrentTicksSingleAdvancer(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e1 := env.newEngine(t, "w1", 200*time.Millisecond)
	e2 := env.newEngine(t, "w2", 200*time.Millisecond)
	for _, e := range []*Engine{e1, e2} {
		mustDef(e, resumeTestDef())
	}
	var mu sync.Mutex
	calls := 0
	act := flakyNActivity(env.se, 2, &calls, &mu) // 前 2 次失败（耗尽预算），第 3 次成功
	e1.RegisterActivity("echo", echoActivity(env.se))
	e1.RegisterActivity("flakyb", act)
	e2.RegisterActivity("echo", echoActivity(env.se))
	e2.RegisterActivity("flakyb", act)

	if _, err := e1.Start(ctx, "wf-r4", "resume-wf", "in"); err != nil {
		t.Fatal(err)
	}
	if st := drainToCompletion(t, e1, ctx, "wf-r4", 20); st.Status != WFFailed {
		t.Fatalf("precondition: expected failed, got %s", st.Status)
	}
	if _, err := e1.Resume(ctx, "wf-r4", 1); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	// 两个执行者并发 Tick 多轮，直到终态。
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); _, _ = e1.Tick(ctx) }()
		go func() { defer wg.Done(); _, _ = e2.Tick(ctx) }()
		wg.Wait()
		env.clock.Advance(time.Second)
		st, _ := e1.Get(ctx, "wf-r4")
		if st.Status.Terminal() {
			break
		}
	}
	st, _ := e1.Get(ctx, "wf-r4")
	if st.Status != WFCompleted {
		t.Fatalf("expected completed, got %s", st.Status)
	}
	mu.Lock()
	gotCalls := calls
	mu.Unlock()
	if gotCalls != 3 || st.Steps[1].Attempt != 3 {
		t.Fatalf("activity calls (%d) must equal attempt count (%d)", gotCalls, st.Steps[1].Attempt)
	}
	if env.se.count() != 2 {
		t.Fatalf("side effects=%d, want 2 (no duplicate execution)", env.se.count())
	}
	t.Logf("scenario=resume-concurrent-ticks result=completed calls=%d attempt=%d", gotCalls, st.Steps[1].Attempt)
}

// TestCrashInterruptedAttemptBudgetAccounting 崩溃留下的在途尝试已用掉最后一次预算：
// 接管后不得再发起活动调用，直接落账预算耗尽；续跑 1 次后从下一编号继续并成功。
func TestCrashInterruptedAttemptBudgetAccounting(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e1 := env.newEngine(t, "old-worker", 10*time.Second)
	def := WorkflowDef{Name: "crash-wf", Steps: []StepDef{
		{Name: "only", Activity: "crasher", Retry: RetryPolicy{MaxAttempts: 2, InitialBackoff: 10 * time.Millisecond, Multiplier: 1}},
	}}
	mustDef(e1, def)

	var mu sync.Mutex
	calls := 0
	started := make(chan struct{})
	gate := make(chan struct{})
	act := func(ctx ActivityContext, in ActivityInput) (string, error) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		switch n {
		case 1:
			return "", Transientf("boom #1")
		case 2:
			close(started)
			<-gate // 模拟崩溃：永不返回，直到测试收尾放行
			return "", Transientf("abandoned")
		default:
			env.se.apply(in.IdempotencyToken)
			return "ok", nil
		}
	}
	e1.RegisterActivity("crasher", act)
	if _, err := e1.Start(ctx, "wf-r5", "crash-wf", "x"); err != nil {
		t.Fatal(err)
	}
	// 第 1 次尝试：瞬时失败进入退避。
	if _, err := e1.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	env.clock.Advance(20 * time.Millisecond)
	// 第 2 次尝试：活动执行中“进程崩溃”。
	advDone := make(chan error, 1)
	go func() {
		_, err := e1.Tick(ctx)
		advDone <- err
	}()
	<-started
	if err := env.store.Close(); err != nil {
		t.Fatal(err)
	}

	// 新进程接管：租约过期后 Tick。
	store, err := NewFileStore(env.dir)
	if err != nil {
		t.Fatal(err)
	}
	env.store = store
	e2 := NewEngine(store, Options{WorkerID: "new-worker", LeaseDuration: 10 * time.Second, Clock: env.clock, Logger: env.log})
	mustDef(e2, def)
	e2.RegisterActivity("crasher", act)
	env.clock.Advance(11 * time.Second)
	if _, err := e2.Tick(ctx); err != nil {
		t.Fatalf("takeover tick: %v", err)
	}

	// 预算（2）已被尝试 1、2 用完：接管不得发起第 3 次活动调用，直接落账失败。
	mu.Lock()
	if calls != 2 {
		t.Fatalf("budget exceeded: activity called %d times, want 2", calls)
	}
	mu.Unlock()
	st, _ := e2.Get(ctx, "wf-r5")
	if st.Status != WFFailed || st.Steps[0].Attempt != 2 {
		t.Fatalf("expected failed with attempt=2, got %s attempt=%d", st.Status, st.Steps[0].Attempt)
	}
	if !strings.Contains(st.Failure.Message, "interrupted") {
		t.Fatalf("expected interrupted-exhaustion reason, got %q", st.Failure.Message)
	}
	if len(st.Steps[0].Failures) != 2 {
		t.Fatalf("interrupted attempt not accounted: %+v", st.Steps[0].Failures)
	}

	// 续跑 1 次：从尝试 3 继续并成功，总调用 3 次，不超预算。
	if _, err := e2.Resume(ctx, "wf-r5", 1); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if _, err := e2.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	st, _ = e2.Get(ctx, "wf-r5")
	if st.Status != WFCompleted || st.Steps[0].Attempt != 3 {
		t.Fatalf("expected completed at attempt=3, got %s attempt=%d", st.Status, st.Steps[0].Attempt)
	}
	mu.Lock()
	if calls != 3 {
		t.Fatalf("activity calls=%d, want 3", calls)
	}
	mu.Unlock()
	if env.se.count() != 1 {
		t.Fatalf("side effects=%d, want 1", env.se.count())
	}

	// 放行被遗弃的旧协程：其结果必须被丢弃，不影响终态。
	close(gate)
	select {
	case <-advDone:
	case <-time.After(2 * time.Second):
		t.Fatal("abandoned tick did not return")
	}
	st, _ = e2.Get(ctx, "wf-r5")
	if st.Status != WFCompleted {
		t.Fatalf("stale result overwrote state: %s", st.Status)
	}
	t.Logf("scenario=crash-interrupted-budget result=exhausted-then-resumed attempts=%d calls=%d", st.Steps[0].Attempt, calls)
}

// TestResumeTerminatedInstance 被人工终止的实例可以续跑，且终止历史保留。
func TestResumeTerminatedInstance(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w1", time.Minute)
	e.RegisterActivity("echo", echoActivity(env.se))
	if _, err := e.Start(ctx, "wf-r6", "echo", "x"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Terminate(ctx, "wf-r6", "operator abort"); err != nil {
		t.Fatal(err)
	}
	st, err := e.Resume(ctx, "wf-r6", 1)
	if err != nil {
		t.Fatalf("Resume terminated: %v", err)
	}
	if st.Status != WFRunning || len(st.Resumes) != 1 {
		t.Fatalf("bad resumed state: %+v", st)
	}
	st = drainToCompletion(t, e, ctx, "wf-r6", 5)
	if st.Status != WFCompleted || st.Result != "echo:x" {
		t.Fatalf("expected completed, got %s result=%q", st.Status, st.Result)
	}
	// 终止时的失败记录仍在步骤历史里。
	if len(st.Steps[0].Failures) != 1 || !strings.Contains(st.Steps[0].Failures[0].Message, "terminated") {
		t.Fatalf("termination history lost: %+v", st.Steps[0].Failures)
	}
	events, _ := e.History(ctx, "wf-r6")
	var failed, resumed, completed bool
	for _, ev := range events {
		switch ev.Type {
		case EvWorkflowFailed:
			failed = true
		case EvWorkflowResumed:
			resumed = true
		case EvWorkflowCompleted:
			completed = true
		}
	}
	if !failed || !resumed || !completed {
		t.Fatalf("history incomplete: failed=%v resumed=%v completed=%v", failed, resumed, completed)
	}
	t.Logf("scenario=resume-terminated result=completed")
}
