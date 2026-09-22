package workflow

import (
	"context"
	"time"
)

// Advance 在持有的租约下把工作流推进至多一个逻辑步骤（执行一次活动尝试）。
// 幂等与并发语义：
//   - 已成功的步骤不会再执行（游标只在成功提交后前移）；
//   - 等待退避（NextAttemptAt 未到）返回 ConflictError(ConflictNoRunnable)；
//   - 租约失效/被接管（token 或 fence 不匹配、已过期、版本被他人推进）一律拒绝提交；
//   - 同一步骤所有尝试共享稳定 IdempotencyToken，活动据此保证业务副作用只发生一次。
func (e *Engine) Advance(ctx context.Context, id string, token string, fence int64) (*WorkflowState, error) {
	st, _, err := e.store.Load(ctx, id)
	if err != nil {
		return nil, err
	}
	def, ok := e.defs[st.DefName]
	if !ok {
		return nil, unknownDefErr(st.DefName)
	}
	now := e.clock.Now()
	if st.Status.Terminal() {
		return st, conflict(ConflictTerminal, ErrWorkflowCompleted)
	}
	if err := checkLease(st, e.workerID, token, fence, now); err != nil {
		return nil, err
	}
	cursor := st.Cursor
	step := st.Steps[cursor]
	sd := def.Steps[cursor]
	if step.Status == StepWaiting && step.NextAttemptAt != nil && step.NextAttemptAt.After(now) {
		return st, conflictMsg(ConflictNoRunnable, "step waiting for backoff until "+step.NextAttemptAt.Format(time.RFC3339Nano))
	}

	// 标记尝试开始并先落盘：即使随后进程崩溃，接管者也能看到“进行中”的尝试编号，
	// 且由于幂等令牌只依赖 (workflow, step)，接管重试不会产生新的业务副作用身份。
	attempt := step.Attempt + 1
	step.Attempt = attempt
	step.Status = StepRunning
	step.NextAttemptAt = nil
	startAt := now
	step.StartedAt = &startAt
	st.UpdatedAt = now
	startEvent := Event{WorkflowID: id, StepIndex: cursor, Type: EvStepStarted, At: now, Attempt: attempt, LeaseID: token,
		Detail: map[string]any{"step": step.Name, "activity": step.Activity, "idempotency_token": idempotencyToken(id, cursor)}}
	committed, err := e.store.Commit(ctx, id, st.Version, st, []Event{startEvent})
	if err != nil {
		return nil, err
	}
	st = committed
	e.log.Logf("info", id, cursor, "step started", "status", StepRunning, "attempt", attempt,
		"activity", step.Activity, "lease", token, "fence", fence)

	// 执行活动（按策略施加单次尝试超时；计时器来自注入的 Clock，可确定性测试）。
	input := st.Input
	if cursor > 0 {
		input = st.Steps[cursor-1].Result
	}
	result, actErr := e.runActivity(ctx, sd, ActivityInput{
		WorkflowID:       id,
		StepName:         step.Name,
		Attempt:          attempt,
		Input:            input,
		IdempotencyToken: idempotencyToken(id, cursor),
	})

	// 重新加载并再次校验租约/版本：活动执行期间可能被接管。expectedVersion 必须仍是
	// “尝试开始”提交后的版本；若期间有任何其他提交，这里直接拒绝，绝不静默覆盖。
	latest, _, err := e.store.Load(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := checkLease(latest, e.workerID, token, fence, e.clock.Now()); err != nil {
		e.log.Logf("warn", id, cursor, "result discarded: lease lost during activity",
			"attempt", attempt, "lease", token, "fence", fence)
		return nil, err
	}
	if latest.Version != st.Version {
		return nil, conflict(ConflictVersion, ErrVersionConflict)
	}
	st = latest
	step = st.Steps[cursor]

	if actErr == nil {
		return e.commitSucceeded(ctx, st, sd, attempt, token, result, input)
	}
	return e.commitFailed(ctx, st, sd, attempt, token, actErr)
}

func unknownDefErr(name string) error {
	return &ConflictError{Kind: ConflictInvalidTransition, Msg: "unknown definition: " + name, Err: ErrInvalidState}
}

// runActivity 执行一次活动尝试，AttemptTimeout>0 时用 Clock 驱动超时取消。
func (e *Engine) runActivity(parent context.Context, sd StepDef, in ActivityInput) (string, error) {
	timeout := sd.Retry.AttemptTimeout
	if timeout <= 0 {
		actCtx := &activityContext{Context: parent, attempt: in.Attempt, token: in.IdempotencyToken}
		return e.callActivity(sd, actCtx, in)
	}
	actCtx, cancel := context.WithCancel(parent)
	defer cancel()
	timer := e.clock.NewTimer(timeout)
	defer timer.Stop()
	resultCh := make(chan activityResult, 1)
	go func() {
		r, err := e.callActivity(sd, &activityContext{Context: actCtx, attempt: in.Attempt, token: in.IdempotencyToken}, in)
		resultCh <- activityResult{result: r, err: err}
	}()
	select {
	case <-parent.Done():
		return "", parent.Err()
	case <-timer.C():
		cancel()
		return "", &activityError{class: FailureTimeout, message: "attempt timeout after " + timeout.String(), cause: context.DeadlineExceeded}
	case res := <-resultCh:
		return res.result, res.err
	}
}

type activityResult struct {
	result string
	err    error
}

func (e *Engine) callActivity(sd StepDef, actCtx ActivityContext, in ActivityInput) (string, error) {
	fn, ok := e.activities[sd.Activity]
	if !ok {
		// 未注册活动是确定性的配置错误：永久失败，立即终止，不做重试。
		return "", Permanentf("activity %q not registered", sd.Activity)
	}
	return fn(actCtx, in)
}

// commitSucceeded 提交步骤成功：写入结果、前移游标、调度下一步或完成工作流，并释放租约。
func (e *Engine) commitSucceeded(ctx context.Context, st *WorkflowState, sd StepDef, attempt int, token, result, input string) (*WorkflowState, error) {
	id := st.ID
	cursor := st.Cursor
	step := st.Steps[cursor]
	now := e.clock.Now()

	endAt := now
	step.Status = StepSucceeded
	step.Result = result
	step.EndedAt = &endAt
	step.FinalClass = FailureNone
	st.Result = result

	events := []Event{{WorkflowID: id, StepIndex: cursor, Type: EvStepSucceeded, At: now, Attempt: attempt, LeaseID: token,
		Detail: map[string]any{"step": step.Name, "result": result, "input": input}}}

	if cursor+1 < len(st.Steps) {
		next := st.Steps[cursor+1]
		st.Cursor = cursor + 1
		next.Status = StepPending
		events = append(events, Event{WorkflowID: id, StepIndex: cursor + 1, Type: EvStepScheduled, At: now, LeaseID: token,
			Detail: map[string]any{"step": next.Name, "activity": next.Activity}})
	} else {
		st.Status = WFCompleted
		st.Cursor = len(st.Steps)
		events = append(events, Event{WorkflowID: id, Type: EvWorkflowCompleted, At: now, LeaseID: token,
			Detail: map[string]any{"result": result}})
	}
	st.Lease = nil
	st.UpdatedAt = now
	events = append(events, Event{WorkflowID: id, Type: EvLeaseReleased, At: now, LeaseID: token,
		Detail: map[string]any{"holder": e.workerID, "reason": "step_advanced"}})

	committed, err := e.store.Commit(ctx, id, st.Version, st, events)
	if err != nil {
		return nil, err
	}
	e.log.Logf("info", id, cursor, "step succeeded", "status", StepSucceeded, "attempt", attempt, "result", result)
	if cursor+1 < len(st.Steps) {
		e.log.Logf("info", id, cursor+1, "step scheduled", "status", StepPending)
	} else {
		e.log.Logf("info", id, -1, "workflow completed", "status", WFCompleted)
	}
	e.log.Logf("info", id, -1, "lease released", "holder", e.workerID, "reason", "step_advanced")
	return committed, nil
}

// commitFailed 提交尝试失败：瞬时错误且仍有预算则进入退避等待；
// 永久错误或预算耗尽则步骤永久失败、工作流终止。无论哪条路都释放租约。
func (e *Engine) commitFailed(ctx context.Context, st *WorkflowState, sd StepDef, attempt int, token string, actErr error) (*WorkflowState, error) {
	id := st.ID
	cursor := st.Cursor
	step := st.Steps[cursor]
	now := e.clock.Now()

	class, msg := classify(actErr)
	timedOut := class == FailureTimeout
	f := Failure{Attempt: attempt, Class: class, Message: msg, At: now, DuringLease: token, TimedOut: timedOut}
	step.Failures = append(step.Failures, f)
	endAt := now

	maxAttempts := sd.Retry.MaxAttemptsVal()
	canRetry := class != FailurePermanent && attempt < maxAttempts

	events := []Event{{WorkflowID: id, StepIndex: cursor, Type: EvStepFailed, At: now, Attempt: attempt, LeaseID: token,
		Detail: map[string]any{"step": step.Name, "class": class, "attempt": attempt, "max_attempts": maxAttempts,
			"timed_out": timedOut, "message": msg, "will_retry": canRetry}}}

	if canRetry {
		backoff := sd.Retry.Backoff(attempt)
		next := now.Add(backoff)
		step.Status = StepWaiting
		step.NextAttemptAt = &next
		step.EndedAt = nil
		events = append(events, Event{WorkflowID: id, StepIndex: cursor, Type: EvStepRetryWait, At: now, Attempt: attempt, LeaseID: token,
			Detail: map[string]any{"step": step.Name, "backoff": backoff.String(), "next_attempt_at": next.Format(time.RFC3339Nano),
				"attempts_used": attempt, "max_attempts": maxAttempts}})
		st.Lease = nil
		st.UpdatedAt = now
		events = append(events, Event{WorkflowID: id, Type: EvLeaseReleased, At: now, LeaseID: token,
			Detail: map[string]any{"holder": e.workerID, "reason": "retry_wait"}})
		committed, err := e.store.Commit(ctx, id, st.Version, st, events)
		if err != nil {
			return nil, err
		}
		e.log.Logf("warn", id, cursor, "step failed", "status", StepWaiting, "attempt", attempt,
			"class", class, "timed_out", timedOut, "will_retry", true, "backoff", backoff.String(), "reason", msg)
		e.log.Logf("info", id, -1, "lease released", "holder", e.workerID, "reason", "retry_wait")
		return committed, nil
	}

	// 永久失败或重试预算耗尽：终态必须可解释——记录最终分类与依据。
	finalClass := class
	reason := msg
	if class != FailurePermanent && attempt >= maxAttempts {
		finalClass = FailureTransient
		reason = "retry budget exhausted after " + itoa(attempt) + " attempt(s): " + msg
	}
	step.Status = StepFailed
	step.EndedAt = &endAt
	step.FinalClass = finalClass
	step.FinalReason = reason
	st.Status = WFFailed
	finalFailure := Failure{Attempt: attempt, Class: finalClass, Message: reason, At: now, DuringLease: token, TimedOut: timedOut}
	st.Failure = &finalFailure
	st.Lease = nil
	st.UpdatedAt = now
	events = append(events,
		Event{WorkflowID: id, Type: EvWorkflowFailed, At: now, LeaseID: token,
			Detail: map[string]any{"class": finalClass, "reason": reason, "attempts": attempt}},
		Event{WorkflowID: id, Type: EvLeaseReleased, At: now, LeaseID: token,
			Detail: map[string]any{"holder": e.workerID, "reason": "workflow_failed"}})

	committed, err := e.store.Commit(ctx, id, st.Version, st, events)
	if err != nil {
		return nil, err
	}
	e.log.Logf("error", id, cursor, "step failed", "status", StepFailed, "attempt", attempt,
		"class", finalClass, "will_retry", false, "reason", reason)
	e.log.Logf("error", id, -1, "workflow failed", "status", WFFailed, "class", finalClass, "attempts", attempt)
	e.log.Logf("info", id, -1, "lease released", "holder", e.workerID, "reason", "workflow_failed")
	return committed, nil
}
