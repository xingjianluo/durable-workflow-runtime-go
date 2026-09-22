package engine

import (
	"context"
	"time"

	"github.com/xingjianluo/durable-workflow-runtime-go/internal/workflow"
)

// AcquireStep 尝试领取实例当前就绪步骤的执行租约。
//
// 状态规则：
//   - PENDING 且到达 NextAttemptAt：领取，追加 STEP_LEASED(attempt+1, holder=self)；
//   - RUNNING 且持有者是自己且未过期：返回 CodeAlreadyLeased（重复调度被拒绝且幂等）；
//   - RUNNING 且持有者是他人且未过期：返回 CodeLeaseHeldByOther（拒绝并发执行）；
//   - RUNNING 且租约已过期：本 worker 接管。旧 attempt 以 TIMEOUT 计入历史，
//     若重试预算耗尽则终结工作流，否则以新 attempt 立即重新领取；
//   - 工作流已终结：返回 CodeWorkflowFinished。
//
// 领取成功返回 (stepName, true, nil)。
func (e *Engine) AcquireStep(ctx context.Context, instanceID string) (string, bool, error) {
	var acquiredName string
	var acquired bool
	err := e.casLoop(ctx, instanceID, func(_ int64) (bool, []*workflow.Event, int64, error) {
		events, seq, err := e.store.ReadEvents(ctx, instanceID)
		if err != nil {
			return true, nil, 0, err
		}
		if len(events) == 0 {
			return true, nil, 0, workflow.NewError(workflow.CodeNotFound, "instance %q not found", instanceID)
		}
		inst := workflow.Project(events)
		if inst.State != workflow.StateRunning {
			return true, nil, 0, workflow.NewError(workflow.CodeWorkflowFinished,
				"instance %q is %s", instanceID, inst.State)
		}
		cur := inst.CurrentStepStatus()
		if cur == nil {
			return true, nil, 0, workflow.NewError(workflow.CodeWorkflowFinished,
				"instance %q has no runnable step", instanceID)
		}

		def, derr := e.definition(inst.WorkflowID)
		if derr != nil {
			return true, nil, 0, derr
		}
		spec := stepSpecByName(def, cur.Name)
		now := e.now()

		switch cur.State {
		case workflow.StepPending:
			if !cur.NextAttemptAt.IsZero() && now.Before(cur.NextAttemptAt) {
				return true, nil, 0, workflow.NewError(workflow.CodeRetryNotDue,
					"step %q retry not due until %s", cur.Name, cur.NextAttemptAt.Format("15:04:05.000"))
			}
			ev := e.leasedEvent(inst, cur.Name, cur.Attempt+1, now)
			e.logf(inst, cur.Name, workflow.EventStepLeased, ev.Attempt, "", "holder="+e.id)
			acquiredName, acquired = cur.Name, true
			return false, []*workflow.Event{ev}, seq, nil

		case workflow.StepRunning:
			if cur.LeaseHolder == e.id && now.Before(cur.LeaseExpiresAt) {
				return true, nil, 0, workflow.NewError(workflow.CodeAlreadyLeased,
					"step %q already leased by %s until %s",
					cur.Name, cur.LeaseHolder, cur.LeaseExpiresAt.Format("15:04:05.000"))
			}
			if now.Before(cur.LeaseExpiresAt) {
				return true, nil, 0, workflow.NewError(workflow.CodeLeaseHeldByOther,
					"step %q leased by %s until %s",
					cur.Name, cur.LeaseHolder, cur.LeaseExpiresAt.Format("15:04:05.000"))
			}
			// 租约过期 => 崩溃接管。
			evs, fatal, fail := e.takeoverEvents(inst, cur, spec, now, seq)
			if fail == nil {
				acquiredName, acquired = cur.Name, true
			}
			return fatal, evs, seq, fail

		default:
			return true, nil, 0, workflow.NewError(workflow.CodeInvalidState,
				"step %q in state %s cannot be acquired", cur.Name, cur.State)
		}
	})
	if err != nil {
		return "", false, err
	}
	return acquiredName, acquired, nil
}

// Takeover 显式接管一个租约已过期的步骤；未过期返回 CodeLeaseNotExpired。
func (e *Engine) Takeover(ctx context.Context, instanceID, stepName string) error {
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
		now := e.now()
		switch {
		case cur.State != workflow.StepRunning:
			return true, nil, 0, workflow.NewError(workflow.CodeInvalidState,
				"step %q is %s, not running", stepName, cur.State)
		case cur.LeaseHolder == e.id:
			return true, nil, 0, workflow.NewError(workflow.CodeAlreadyLeased,
				"step %q already held by %s", stepName, e.id)
		case now.Before(cur.LeaseExpiresAt):
			return true, nil, 0, workflow.NewError(workflow.CodeLeaseNotExpired,
				"step %q lease held by %s until %s",
				stepName, cur.LeaseHolder, cur.LeaseExpiresAt.Format("15:04:05.000"))
		}
		def, derr := e.definition(inst.WorkflowID)
		if derr != nil {
			return true, nil, 0, derr
		}
		spec := stepSpecByName(def, stepName)
		evs, fatal, fail := e.takeoverEvents(inst, cur, spec, now, seq)
		return fatal, evs, seq, fail
	})
}

func (e *Engine) leasedEvent(inst *workflow.Instance, stepName string, attempt int, now time.Time) *workflow.Event {
	return &workflow.Event{
		WorkflowID:     inst.WorkflowID,
		InstanceID:     inst.InstanceID,
		Type:           workflow.EventStepLeased,
		StepName:       stepName,
		Attempt:        attempt,
		LeaseHolder:    e.id,
		LeaseExpiresAt: now.Add(e.leaseTTL),
		Timestamp:      now,
	}
}

// takeoverEvents 构造租约过期接管的事件批次。
// 崩溃的 attempt 归类为 TIMEOUT 并写入历史：
//   - 重试耗尽：STEP_TIMED_OUT + STEP_FAILED + WORKFLOW_FAILED；
//   - 仍可重试：STEP_TIMED_OUT + 新 attempt 的 STEP_LEASED（持有者为本引擎）。
func (e *Engine) takeoverEvents(
	inst *workflow.Instance,
	cur *workflow.StepStatus,
	spec *workflow.StepSpec,
	now time.Time,
	_ int64,
) ([]*workflow.Event, bool, error) {
	p := policyOrDefault(spec.RetryPolicy)
	timedOut := &workflow.Event{
		WorkflowID:   inst.WorkflowID,
		InstanceID:   inst.InstanceID,
		Type:         workflow.EventStepTimedOut,
		StepName:     cur.Name,
		Attempt:      cur.Attempt,
		FailureClass: workflow.FailureTimeout,
		ErrorCode:    "LEASE_TIMEOUT",
		Message:      "lease expired; previous worker presumed crashed",
		Timestamp:    now,
	}
	e.logf(inst, cur.Name, workflow.EventStepTimedOut, cur.Attempt, workflow.FailureTimeout,
		"lost holder="+cur.LeaseHolder)

	if cur.Attempt >= p.MaxAttempts {
		failed := &workflow.Event{
			WorkflowID:   inst.WorkflowID,
			InstanceID:   inst.InstanceID,
			Type:         workflow.EventStepFailed,
			StepName:     cur.Name,
			Attempt:      cur.Attempt,
			FailureClass: workflow.FailureTimeout,
			ErrorCode:    string(workflow.CodeRetryExhausted),
			Message:      "retry budget exhausted after lease timeout",
			Timestamp:    now,
		}
		wfFailed := e.workflowFailedEvent(inst, now)
		e.logf(inst, cur.Name, workflow.EventStepFailed, cur.Attempt, workflow.FailureTimeout,
			"terminal: retry exhausted")
		return []*workflow.Event{timedOut, failed, wfFailed}, true, nil
	}

	leased := e.leasedEvent(inst, cur.Name, cur.Attempt+1, now)
	e.logf(inst, cur.Name, workflow.EventLeaseTakenOver, leased.Attempt, "",
		"takeover by="+e.id)
	return []*workflow.Event{timedOut, leased}, false, nil
}
