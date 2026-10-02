package workflow

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"
)

// TestAuditTrailMatchesFinalState 审计轨迹与最终状态对得上：
// 每次尝试的编号/分类/剩余预算、续跑记录都能从持久化数据重建，且重启后结果一致。
func TestAuditTrailMatchesFinalState(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w1", time.Minute)
	mustDef(e, resumeTestDef())
	var mu sync.Mutex
	calls := 0
	e.RegisterActivity("echo", echoActivity(env.se))
	e.RegisterActivity("flakyb", flakyNActivity(env.se, 3, &calls, &mu)) // 前 3 次失败，第 4 次成功

	if _, err := e.Start(ctx, "wf-a1", "resume-wf", "in"); err != nil {
		t.Fatal(err)
	}
	if st := drainToCompletion(t, e, ctx, "wf-a1", 20); st.Status != WFFailed {
		t.Fatalf("precondition: expected failed, got %s", st.Status)
	}
	if _, err := e.Resume(ctx, "wf-a1", 2); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	st := drainToCompletion(t, e, ctx, "wf-a1", 20)
	if st.Status != WFCompleted {
		t.Fatalf("expected completed, got %s", st.Status)
	}

	rep, err := e.Audit(ctx, "wf-a1")
	if err != nil {
		t.Fatalf("Audit: %v", err)
	}
	if rep.WorkflowID != "wf-a1" || rep.Status != WFCompleted {
		t.Fatalf("report header wrong: %+v", rep)
	}
	// 步骤 a 1 次尝试 + 步骤 b 4 次尝试。
	if len(rep.Attempts) != 5 {
		t.Fatalf("expected 5 attempt records, got %d: %+v", len(rep.Attempts), rep.Attempts)
	}
	type want struct {
		step, attempt int
		outcome       AttemptOutcome
		class         FailureClass
		remaining     int
	}
	wants := []want{
		{0, 1, OutcomeSucceeded, FailureNone, 0},   // a: maxAttempts=1
		{1, 1, OutcomeFailed, FailureTransient, 1}, // b: 预算 2，第 1 次后剩 1
		{1, 2, OutcomeFailed, FailureTransient, 0}, // b: 预算耗尽
		{1, 3, OutcomeFailed, FailureTransient, 1}, // b: 续跑 +2 后预算 4，第 3 次后剩 1
		{1, 4, OutcomeSucceeded, FailureNone, 0},
	}
	for i, w := range wants {
		got := rep.Attempts[i]
		if got.StepIndex != w.step || got.Attempt != w.attempt || got.Outcome != w.outcome ||
			got.Class != w.class || got.RemainingBudget != w.remaining {
			t.Errorf("attempt[%d] = %+v, want step=%d attempt=%d outcome=%s class=%q remaining=%d",
				i, got, w.step, w.attempt, w.outcome, w.class, w.remaining)
		}
		if got.EndedAt == nil || got.Duration < 0 {
			t.Errorf("attempt[%d] not closed properly: %+v", i, got)
		}
	}
	// 续跑历史：1 次，追加 2。
	if len(rep.Resumes) != 1 || rep.Resumes[0].AddedAttempts != 2 || rep.Resumes[0].StepIndex != 1 {
		t.Fatalf("resume history wrong: %+v", rep.Resumes)
	}
	// 轨迹与最终状态一致：最后一条记录成功 ⇒ 工作流完成。
	last := rep.Attempts[len(rep.Attempts)-1]
	if last.Outcome != OutcomeSucceeded || rep.Status != WFCompleted {
		t.Fatalf("trail does not explain final state: last=%+v status=%s", last, rep.Status)
	}

	// 重启（新 Store/Engine）后重建的轨迹必须逐字段一致：只依赖持久化数据。
	e2 := env.reopen(t, "w2")
	mustDef(e2, resumeTestDef())
	rep2, err := e2.Audit(ctx, "wf-a1")
	if err != nil {
		t.Fatalf("Audit after restart: %v", err)
	}
	if !reflect.DeepEqual(rep, rep2) {
		t.Fatalf("audit not stable across restart:\n before=%+v\n after=%+v", rep, rep2)
	}
	t.Logf("scenario=audit-trail result=consistent attempts=%d resumes=%d stable_after_restart=true",
		len(rep.Attempts), len(rep.Resumes))
}

// TestAuditMarksInterruptedAttempt 崩溃中断的在途尝试在审计中被标记为 interrupted，
// 且已计入预算消耗；接管后的新尝试编号连续。
func TestAuditMarksInterruptedAttempt(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e1 := env.newEngine(t, "old-worker", 10*time.Second)
	def := WorkflowDef{Name: "audit-crash", Steps: []StepDef{
		{Name: "only", Activity: "gate", Retry: RetryPolicy{MaxAttempts: 3}},
	}}
	mustDef(e1, def)
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
			return "", Transientf("abandoned")
		}
		return "", Transientf("boom #%d", n)
	}
	e1.RegisterActivity("gate", act)
	if _, err := e1.Start(ctx, "wf-a2", "audit-crash", "x"); err != nil {
		t.Fatal(err)
	}
	advDone := make(chan error, 1)
	go func() {
		_, err := e1.Tick(ctx)
		advDone <- err
	}()
	<-started
	if err := env.store.Close(); err != nil { // 崩溃：尝试 1 在途
		t.Fatal(err)
	}
	store, err := NewFileStore(env.dir)
	if err != nil {
		t.Fatal(err)
	}
	env.store = store
	e2 := NewEngine(store, Options{WorkerID: "new-worker", LeaseDuration: 10 * time.Second, Clock: env.clock, Logger: env.log})
	mustDef(e2, def)
	e2.RegisterActivity("gate", act)
	env.clock.Advance(11 * time.Second) // 租约过期，接管
	if _, err := e2.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	st, _ := e2.Get(ctx, "wf-a2")
	if st.Steps[0].Attempt != 2 || st.Steps[0].Status != StepWaiting {
		t.Fatalf("precondition: attempt=2 waiting, got %+v", st.Steps[0])
	}

	rep, err := e2.Audit(ctx, "wf-a2")
	if err != nil {
		t.Fatalf("Audit: %v", err)
	}
	if len(rep.Attempts) != 2 {
		t.Fatalf("expected 2 attempt records, got %+v", rep.Attempts)
	}
	a1, a2 := rep.Attempts[0], rep.Attempts[1]
	if a1.Outcome != OutcomeInterrupted || a1.Attempt != 1 || a1.RemainingBudget != 2 {
		t.Fatalf("attempt 1 should be interrupted with remaining=2, got %+v", a1)
	}
	if a1.EndedAt != nil {
		t.Fatalf("interrupted attempt must have no end time: %+v", a1)
	}
	if a2.Outcome != OutcomeFailed || a2.Class != FailureTransient || a2.RemainingBudget != 1 {
		t.Fatalf("attempt 2 should be failed transient with remaining=1, got %+v", a2)
	}
	close(gate)
	select {
	case <-advDone:
	case <-time.After(2 * time.Second):
		t.Fatal("abandoned tick did not return")
	}
	t.Logf("scenario=audit-interrupted result=attempt1=interrupted attempt2=failed remaining=[%d,%d]",
		a1.RemainingBudget, a2.RemainingBudget)
}

// TestAuditInProgressAttempt 正在执行中的尝试标记为 in_progress（有活跃租约）。
func TestAuditInProgressAttempt(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	e := env.newEngine(t, "w1", time.Minute)
	mustDef(e, WorkflowDef{Name: "audit-live", Steps: []StepDef{
		{Name: "only", Activity: "gate", Retry: RetryPolicy{MaxAttempts: 1}},
	}})
	started := make(chan struct{})
	release := make(chan struct{})
	e.RegisterActivity("gate", func(ctx ActivityContext, in ActivityInput) (string, error) {
		close(started)
		<-release
		return "ok", nil
	})
	if _, err := e.Start(ctx, "wf-a3", "audit-live", "x"); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _, _ = e.Tick(ctx); close(done) }()
	<-started

	rep, err := e.Audit(ctx, "wf-a3")
	if err != nil {
		t.Fatalf("Audit: %v", err)
	}
	if len(rep.Attempts) != 1 || rep.Attempts[0].Outcome != OutcomeInProgress {
		t.Fatalf("expected single in_progress attempt, got %+v", rep.Attempts)
	}
	close(release)
	<-done
	t.Logf("scenario=audit-in-progress result=in_progress")
}
