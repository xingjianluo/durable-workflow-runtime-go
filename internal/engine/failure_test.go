package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/xingjianluo/durable-workflow-runtime-go/internal/activity"
	"github.com/xingjianluo/durable-workflow-runtime-go/internal/engine"
	"github.com/xingjianluo/durable-workflow-runtime-go/internal/workflow"
)

// TestTransientRetryAndBackoff 瞬时错误按策略重试并最终成功；
// 失败历史与重试依据（退避时刻）可查询、可复现。
func TestTransientRetryAndBackoff(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	// 前两次瞬时失败，第三次成功。
	h.reg.MustRegister(activity.NewFuncActivity("flaky", func(ctx context.Context, in []byte) ([]byte, error) {
		n := h.counters.Effect("flaky")
		if n <= 2 {
			return nil, activity.NewFailure(workflow.FailureTransient, "TEMP_UNAVAILABLE", "temporary outage")
		}
		return []byte(`"ok"`), nil
	}))

	eng := h.newEngine("worker-1", 30*time.Second)
	def := &workflow.Definition{
		Name: "retry-wf",
		Steps: []workflow.StepSpec{{
			Name:     "s1",
			Activity: "flaky",
			RetryPolicy: workflow.RetryPolicy{
				MaxAttempts:     3,
				InitialInterval: time.Second,
				Multiplier:      2,
				MaxInterval:     10 * time.Second,
			},
		}},
	}
	if err := eng.Start(ctx, def, "inst-retry", nil); err != nil {
		t.Fatalf("start: %v", err)
	}

	t0 := h.clock.Now()
	// 第 1 次尝试：失败，安排 1s 后重试。
	if err := eng.Tick(ctx); err != nil {
		t.Fatalf("tick1: %v", err)
	}
	inst, _ := eng.Get(ctx, "inst-retry")
	s1 := inst.Steps[0]
	if s1.State != workflow.StepPending || s1.Attempt != 1 {
		t.Fatalf("after attempt1: state=%s attempt=%d", s1.State, s1.Attempt)
	}
	wantNext := t0.Add(1 * time.Second)
	if !s1.NextAttemptAt.Equal(wantNext) {
		t.Fatalf("next attempt at %s, want %s", s1.NextAttemptAt, wantNext)
	}
	// 未到退避时间：拒绝推进，原因可区分。
	_, _, err := eng.AcquireStep(ctx, "inst-retry")
	assertCode(t, err, workflow.CodeRetryNotDue)

	// 第 2 次尝试：退避 2s。
	h.clock.Advance(1 * time.Second)
	if err := eng.Tick(ctx); err != nil {
		t.Fatalf("tick2: %v", err)
	}
	inst, _ = eng.Get(ctx, "inst-retry")
	s1 = inst.Steps[0]
	wantNext = t0.Add(1*time.Second + 2*time.Second)
	if !s1.NextAttemptAt.Equal(wantNext) {
		t.Fatalf("next attempt at %s, want %s", s1.NextAttemptAt, wantNext)
	}

	// 第 3 次尝试：成功。
	h.clock.Advance(2 * time.Second)
	if err := eng.Tick(ctx); err != nil {
		t.Fatalf("tick3: %v", err)
	}
	inst, _ = eng.Get(ctx, "inst-retry")
	if inst.State != workflow.StateCompleted {
		t.Fatalf("state = %s", inst.State)
	}
	s1 = inst.Steps[0]
	// 失败历史可查询：2 次 TRANSIENT + 1 次成功记录。
	if len(s1.History) != 3 {
		t.Fatalf("history len = %d, want 3", len(s1.History))
	}
	for i := 0; i < 2; i++ {
		rec := s1.History[i]
		if rec.Class != workflow.FailureTransient || rec.ErrorCode != "TEMP_UNAVAILABLE" {
			t.Fatalf("history[%d] = %+v", i, rec)
		}
		t.Logf("workflow=%s/%s step=s1 attempt=%d failure_class=%s code=%s",
			inst.WorkflowID, inst.InstanceID, rec.Attempt, rec.Class, rec.ErrorCode)
	}
	if n := h.counters.Effects("flaky"); n != 3 {
		t.Fatalf("activity executions = %d, want 3", n)
	}
}

// TestActivityTimeout 活动执行超时被归类为 TIMEOUT，按策略重试，
// 预算耗尽后工作流终止且原因可区分。
func TestActivityTimeout(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	// 总是超时的活动：忽略 ctx，睡眠超过超时上限。
	h.reg.MustRegister(activity.NewFuncActivity("slow", func(ctx context.Context, in []byte) ([]byte, error) {
		h.counters.Call("slow")
		select {
		case <-time.After(200 * time.Millisecond):
			return []byte(`"late"`), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}))

	eng := engine.New(h.st, h.reg, engine.Config{
		WorkerID: "worker-1",
		LeaseTTL: 30 * time.Second,
		Now:      h.clock.Now,
		LogOut:   &testLog{t: t},
	})
	def := &workflow.Definition{
		Name: "timeout-wf",
		Steps: []workflow.StepSpec{{
			Name:     "s1",
			Activity: "slow",
			RetryPolicy: workflow.RetryPolicy{
				MaxAttempts: 2,
				Timeout:     50 * time.Millisecond,
			},
		}},
	}
	if err := eng.Start(ctx, def, "inst-timeout", nil); err != nil {
		t.Fatalf("start: %v", err)
	}

	// 两次尝试都会超时（真实时钟等待 50ms 超时）。
	for i := 0; i < 2; i++ {
		if err := eng.Tick(ctx); err != nil {
			t.Fatalf("tick %d: %v", i, err)
		}
	}
	inst, err := eng.Get(ctx, "inst-timeout")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if inst.State != workflow.StateFailed {
		t.Fatalf("state = %s, want FAILED", inst.State)
	}
	s1 := inst.Steps[0]
	if s1.State != workflow.StepFailed {
		t.Fatalf("step state = %s", s1.State)
	}
	if s1.Failure == nil || s1.Failure.Class != workflow.FailureTimeout {
		t.Fatalf("final failure = %+v", s1.Failure)
	}
	// 历史：2 次超时尝试。
	timeouts := 0
	for _, rec := range s1.History {
		if rec.TimedOut {
			timeouts++
		}
		t.Logf("workflow=%s/%s step=s1 attempt=%d failure_class=%s timed_out=%v",
			inst.WorkflowID, inst.InstanceID, rec.Attempt, rec.Class, rec.TimedOut)
	}
	if timeouts != 2 {
		t.Fatalf("timeout records = %d, want 2", timeouts)
	}
	if n := h.counters.Calls("slow"); n != 2 {
		t.Fatalf("activity executions = %d, want 2", n)
	}
}

// TestPermanentFailureNoRetry 永久错误不得重试：一次失败后立即终止。
func TestPermanentFailureNoRetry(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.reg.MustRegister(activity.NewFuncActivity("bad-input", func(ctx context.Context, in []byte) ([]byte, error) {
		h.counters.Call("bad-input")
		return nil, activity.NewFailure(workflow.FailurePermanent, "VALIDATION", "input schema invalid")
	}))

	eng := h.newEngine("worker-1", 30*time.Second)
	def := &workflow.Definition{
		Name: "perm-wf",
		Steps: []workflow.StepSpec{{
			Name:     "s1",
			Activity: "bad-input",
			RetryPolicy: workflow.RetryPolicy{
				MaxAttempts:     5, // 即使预算充足也不得重试
				InitialInterval: time.Second,
			},
		}},
	}
	if err := eng.Start(ctx, def, "inst-perm", nil); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := eng.Tick(ctx); err != nil {
		t.Fatalf("tick: %v", err)
	}
	inst, err := eng.Get(ctx, "inst-perm")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if inst.State != workflow.StateFailed {
		t.Fatalf("state = %s, want FAILED", inst.State)
	}
	s1 := inst.Steps[0]
	if s1.Failure == nil || s1.Failure.Class != workflow.FailurePermanent {
		t.Fatalf("failure = %+v", s1.Failure)
	}
	if n := h.counters.Calls("bad-input"); n != 1 {
		t.Fatalf("permanent failure retried: executions = %d, want 1", n)
	}
	// 推进时钟后也不会再调度。
	h.clock.Advance(time.Hour)
	if err := eng.Tick(ctx); err != nil {
		t.Fatalf("tick2: %v", err)
	}
	if n := h.counters.Calls("bad-input"); n != 1 {
		t.Fatalf("permanent failure retried after advance: executions = %d", n)
	}
	t.Logf("workflow=%s/%s step=s1 permanent failure, no retry, state=%s",
		inst.WorkflowID, inst.InstanceID, inst.State)
}

// TestRetryExhausted 瞬时错误重试预算耗尽后终止，重试依据可查询。
func TestRetryExhausted(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.reg.MustRegister(activity.NewFuncActivity("always-flaky", func(ctx context.Context, in []byte) ([]byte, error) {
		h.counters.Call("always-flaky")
		return nil, activity.NewFailure(workflow.FailureTransient, "DOWNSTREAM", "downstream 503")
	}))

	eng := h.newEngine("worker-1", 30*time.Second)
	def := &workflow.Definition{
		Name: "exhaust-wf",
		Steps: []workflow.StepSpec{{
			Name:     "s1",
			Activity: "always-flaky",
			RetryPolicy: workflow.RetryPolicy{
				MaxAttempts:     3,
				InitialInterval: 100 * time.Millisecond,
				Multiplier:      2,
			},
		}},
	}
	if err := eng.Start(ctx, def, "inst-exhaust", nil); err != nil {
		t.Fatalf("start: %v", err)
	}
	// 每次失败后推进足够时钟再 tick。
	for i := 0; i < 3; i++ {
		if err := eng.Tick(ctx); err != nil {
			t.Fatalf("tick %d: %v", i, err)
		}
		h.clock.Advance(10 * time.Second)
	}
	inst, err := eng.Get(ctx, "inst-exhaust")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if inst.State != workflow.StateFailed {
		t.Fatalf("state = %s", inst.State)
	}
	s1 := inst.Steps[0]
	if n := h.counters.Calls("always-flaky"); n != 3 {
		t.Fatalf("executions = %d, want 3", n)
	}
	// 历史包含 3 次 TRANSIENT 尝试失败 + 1 条终结记录（RETRY_EXHAUSTED），
	// 重试依据（每次失败分类与退避）可复现。
	if len(s1.History) != 4 {
		t.Fatalf("history len = %d, want 4", len(s1.History))
	}
	for i, rec := range s1.History {
		if rec.Class != workflow.FailureTransient {
			t.Fatalf("unexpected class %s at %d", rec.Class, i)
		}
	}
	last := s1.History[len(s1.History)-1]
	if last.ErrorCode != string(workflow.CodeRetryExhausted) {
		t.Fatalf("terminal record code = %s, want RETRY_EXHAUSTED", last.ErrorCode)
	}
	t.Logf("workflow=%s/%s retry exhausted after %d attempts, state=%s",
		inst.WorkflowID, inst.InstanceID, s1.Attempt, inst.State)
}
