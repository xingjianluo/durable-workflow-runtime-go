package workflow

import (
	"context"
)

// Resume 让调用方显式地接着跑一个已失败（failed）的实例：
// 从卡住的那一步往后继续，已成功的步骤不重跑，实例身份与全部历史保持不变。
//
// 语义与约束：
//   - extraAttempts 是本次愿意追加的尝试预算，必须 >= 1；不指定（0）或负数一律拒绝
//     （ConflictError(ConflictResumeInvalidBudget)），绝不默认放开；
//   - 已成功完成的实例拒绝续跑（ConflictError(ConflictResumeCompleted)），
//     仍处于 running 的实例同样拒绝（ConflictError(ConflictResumeNotFailed)）；
//   - 续跑把当前失败步骤重置为待调度，并为其追加 extraAttempts 的尝试预算
//     （有效预算 = RetryPolicy.MaxAttempts + 累计追加），已消耗的尝试编号继续累进；
//   - 每次续跑都追加一条 ResumeRecord 与 workflow_resumed 事件，历史一条不丢。
func (e *Engine) Resume(ctx context.Context, id string, extraAttempts int) (*WorkflowState, error) {
	if extraAttempts <= 0 {
		return nil, &ConflictError{Kind: ConflictResumeInvalidBudget, Err: ErrResumeInvalidBudget,
			Msg: "extra attempts must be >= 1"}
	}
	st, _, err := e.store.Load(ctx, id)
	if err != nil {
		return nil, err
	}
	switch st.Status {
	case WFCompleted:
		return st, conflict(ConflictResumeCompleted, ErrWorkflowCompleted)
	case WFFailed:
		// 只有失败态允许续跑。
	default:
		return st, conflict(ConflictResumeNotFailed, ErrWorkflowNotFailed)
	}
	now := e.clock.Now()
	if leaseActive(st.Lease, now) && st.Lease.Holder != e.workerID {
		return nil, conflict(ConflictLeaseHeldByOther, ErrLeaseOwnedByOther)
	}
	if st.Cursor >= len(st.Steps) {
		return nil, conflictMsg(ConflictInvalidTransition, "failed workflow has no resumable step: "+id)
	}

	cursor := st.Cursor
	step := st.Steps[cursor]
	// 回到待调度：已成功的步骤（游标之前）不动，失败步骤从当前尝试编号继续累进。
	step.Status = StepPending
	step.NextAttemptAt = nil
	step.EndedAt = nil
	step.ExtraAttempts += extraAttempts

	rec := ResumeRecord{
		Seq:           len(st.Resumes) + 1,
		At:            now,
		By:            e.workerID,
		ExtraAttempts: extraAttempts,
		StepIndex:     cursor,
	}
	st.Resumes = append(st.Resumes, rec)
	st.Status = WFRunning
	st.Failure = nil // 终态失败结论已沉淀在 step.Failures 与事件历史中，快照不再携带
	st.Lease = nil
	st.UpdatedAt = now

	detail := map[string]any{
		"step":           step.Name,
		"resume_seq":     rec.Seq,
		"extra_attempts": extraAttempts,
		"attempts_used":  step.Attempt,
		"by":             e.workerID,
	}
	if def, ok := e.defs[st.DefName]; ok && cursor < len(def.Steps) {
		detail["effective_max_attempts"] = def.Steps[cursor].Retry.MaxAttemptsVal() + step.ExtraAttempts
	}
	ev := Event{WorkflowID: id, StepIndex: cursor, Type: EvWorkflowResumed, At: now, Detail: detail}
	committed, err := e.store.Commit(ctx, id, st.Version, st, []Event{ev})
	if err != nil {
		return nil, err
	}
	e.log.Logf("info", id, cursor, "workflow resumed", "status", WFRunning,
		"resume_seq", rec.Seq, "extra_attempts", extraAttempts, "attempts_used", step.Attempt)
	return committed, nil
}
