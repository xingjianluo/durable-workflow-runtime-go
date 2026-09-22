package engine

import (
	"context"
	"time"

	"github.com/xingjianluo/durable-workflow-runtime-go/internal/activity"
	"github.com/xingjianluo/durable-workflow-runtime-go/internal/workflow"
)

// ExecuteLeased 执行本引擎持有租约的步骤并原子提交结果事件。
//
// 幂等与去副作用保证：
//   - 只有租约持有者且租约未过期才能执行；
//   - 提交走全局 seq CAS：执行期间租约被接管时提交会失败并中止，
//     STEP_SUCCEEDED 永远只会被提交一次，重复调度不会重复确认副作用；
//   - 活动应使外部副作用幂等（最坏情况下活动函数可能已执行但提交失败）。
func (e *Engine) ExecuteLeased(ctx context.Context, instanceID, stepName string) error {
	return e.casLoop(ctx, instanceID, func(_ int64) (bool, []*workflow.Event, int64, error) {
		events, seq, err := e.store.ReadEvents(ctx, instanceID)
		if err != nil {
			return true, nil, 0, err
		}
		if len(events) == 0 {
			return true, nil, 0, workflow.NewError(workflow.CodeNotFound, "instance %q not found", instanceID)
		}
		inst := workflow.Project(events)
		cur := inst.FindStep(stepName)
		if cur == nil {
			return true, nil, 0, workflow.NewError(workflow.CodeNotFound, "step %q not found", stepName)
		}
		if cur.State == workflow.StepSucceeded {
			// 幂等去重：已成功的步骤永远拒绝重复提交，原因可区分。
			return true, nil, 0, workflow.NewError(workflow.CodeAlreadySucceeded,
				"step %q already succeeded", stepName)
		}
		if inst.State != workflow.StateRunning {
			return true, nil, 0, workflow.NewError(workflow.CodeWorkflowFinished,
				"instance %q is %s", instanceID, inst.State)
		}
		switch {
		case cur.State != workflow.StepRunning:
			return true, nil, 0, workflow.NewError(workflow.CodeInvalidState,
				"step %q is %s, expected RUNNING", stepName, cur.State)
		case cur.LeaseHolder != e.id:
			return true, nil, 0, workflow.NewError(workflow.CodeLeaseHeldByOther,
				"step %q leased by %q, not %q", stepName, cur.LeaseHolder, e.id)
		}

		def, derr := e.definition(inst.WorkflowID)
		if derr != nil {
			return true, nil, 0, derr
		}
		idx := stepIndex(def, stepName)
		if idx < 0 {
			return true, nil, 0, workflow.NewError(workflow.CodeInvalidState,
				"step %q missing from definition", stepName)
		}
		spec := &def.Steps[idx]

		act, ok := e.registry.Lookup(spec.Activity)
		if !ok {
			return true, nil, 0, workflow.NewError(workflow.CodeDefinitionNotFound,
				"activity %q not registered", spec.Activity)
		}
		in := stepInput(def, idx, inst.InputJSON, inst)

		// 执行前再次确认租约未过期。
		now := e.now()
		if !now.Before(cur.LeaseExpiresAt) {
			return true, nil, 0, workflow.NewError(workflow.CodeLeaseExpired,
				"step %q lease expired at %s", stepName, cur.LeaseExpiresAt.Format("15:04:05.000"))
		}

		p := policyOrDefault(spec.RetryPolicy)
		out, runErr := e.runActivity(ctx, act, in, p.Timeout)

		// 重新读时间，并在提交前最后确认租约仍属于自己（CAS 是最终防线）。
		finishAt := e.now()
		if !finishAt.Before(cur.LeaseExpiresAt) {
			return true, nil, 0, workflow.NewError(workflow.CodeLeaseExpired,
				"step %q lease expired during execution", stepName)
		}
		_ = finishAt

		evs, fatal := e.outcomeEvents(inst, cur, def, idx, p, out, runErr, seq)
		return fatal, evs, seq, nil
	})
}

// runActivity 在超时控制下执行活动；活动忽略 ctx 时用 goroutine + select 兜底。
func (e *Engine) runActivity(ctx context.Context, act activity.Activity, in []byte, timeout time.Duration) ([]byte, error) {
	runCtx := ctx
	var cancel context.CancelFunc
	if timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	type result struct {
		out []byte
		err error
	}
	ch := make(chan result, 1)
	go func() {
		out, err := act.Execute(runCtx, in)
		ch <- result{out, err}
	}()
	if timeout <= 0 {
		r := <-ch
		return r.out, r.err
	}
	select {
	case r := <-ch:
		return r.out, r.err
	case <-runCtx.Done():
		return nil, activity.NewFailure(workflow.FailureTimeout, "ACTIVITY_TIMEOUT",
			"activity timed out after "+timeout.String())
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// outcomeEvents 依据执行结果与重试策略构造提交事件批次。
func (e *Engine) outcomeEvents(
	inst *workflow.Instance,
	cur *workflow.StepStatus,
	def *workflow.Definition,
	idx int,
	p workflow.RetryPolicy,
	out []byte,
	runErr error,
	_ int64,
) ([]*workflow.Event, bool) {
	now := e.now()

	if runErr == nil {
		succeeded := &workflow.Event{
			WorkflowID: inst.WorkflowID,
			InstanceID: inst.InstanceID,
			Type:       workflow.EventStepSucceeded,
			StepName:   cur.Name,
			Attempt:    cur.Attempt,
			ResultJSON: string(out),
			Timestamp:  now,
		}
		e.logf(inst, cur.Name, workflow.EventStepSucceeded, cur.Attempt, "", "bytes="+itoa(len(out)))
		if idx == len(def.Steps)-1 {
			completed := &workflow.Event{
				WorkflowID: inst.WorkflowID,
				InstanceID: inst.InstanceID,
				Type:       workflow.EventWorkflowCompleted,
				ResultJSON: string(out),
				Timestamp:  now,
			}
			e.logf(inst, "", workflow.EventWorkflowCompleted, 0, "", "state="+string(workflow.StateCompleted))
			return []*workflow.Event{succeeded, completed}, false
		}
		next := def.Steps[idx+1]
		scheduled := &workflow.Event{
			WorkflowID:    inst.WorkflowID,
			InstanceID:    inst.InstanceID,
			Type:          workflow.EventStepScheduled,
			StepName:      next.Name,
			Activity:      next.Activity,
			NextAttemptAt: now,
			Timestamp:     now,
		}
		e.logf(inst, next.Name, workflow.EventStepScheduled, 0, "", "state="+string(workflow.StepPending))
		return []*workflow.Event{succeeded, scheduled}, false
	}

	class := activity.Classify(runErr)
	code := activity.CodeOf(runErr)
	msg := runErr.Error()
	recordType := workflow.EventAttemptFailed
	if class == workflow.FailureTimeout {
		recordType = workflow.EventStepTimedOut
	}
	attemptFailed := &workflow.Event{
		WorkflowID:   inst.WorkflowID,
		InstanceID:   inst.InstanceID,
		Type:         recordType,
		StepName:     cur.Name,
		Attempt:      cur.Attempt,
		FailureClass: class,
		ErrorCode:    code,
		Message:      msg,
		Timestamp:    now,
	}
	e.logf(inst, cur.Name, recordType, cur.Attempt, class, "code="+code)

	// 永久错误立即终止，不得重试。
	terminal := class == workflow.FailurePermanent || cur.Attempt >= p.MaxAttempts
	if terminal {
		reason := string(workflow.CodePermanentFailure)
		if class != workflow.FailurePermanent {
			reason = string(workflow.CodeRetryExhausted)
		}
		failed := &workflow.Event{
			WorkflowID:   inst.WorkflowID,
			InstanceID:   inst.InstanceID,
			Type:         workflow.EventStepFailed,
			StepName:     cur.Name,
			Attempt:      cur.Attempt,
			FailureClass: class,
			ErrorCode:    reason,
			Message:      msg,
			Timestamp:    now,
		}
		wfFailed := e.workflowFailedEvent(inst, now)
		e.logf(inst, cur.Name, workflow.EventStepFailed, cur.Attempt, class, "terminal="+reason)
		return []*workflow.Event{attemptFailed, failed, wfFailed}, true
	}

	d := backoff(p.InitialInterval, p.MaxInterval, p.Multiplier, cur.Attempt)
	retry := &workflow.Event{
		WorkflowID:    inst.WorkflowID,
		InstanceID:    inst.InstanceID,
		Type:          workflow.EventRetryScheduled,
		StepName:      cur.Name,
		Attempt:       cur.Attempt,
		NextAttemptAt: now.Add(d),
		Message:       "retry scheduled",
		Timestamp:     now,
	}
	e.logf(inst, cur.Name, workflow.EventRetryScheduled, cur.Attempt, class,
		"next_attempt_at="+retry.NextAttemptAt.Format("15:04:05.000")+" backoff="+d.String())
	return []*workflow.Event{attemptFailed, retry}, false
}

func (e *Engine) workflowFailedEvent(inst *workflow.Instance, now time.Time) *workflow.Event {
	return &workflow.Event{
		WorkflowID: inst.WorkflowID,
		InstanceID: inst.InstanceID,
		Type:       workflow.EventWorkflowFailed,
		Message:    "workflow terminated due to step failure",
		Timestamp:  now,
	}
}

func stepIndex(def *workflow.Definition, name string) int {
	for i := range def.Steps {
		if def.Steps[i].Name == name {
			return i
		}
	}
	return -1
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
