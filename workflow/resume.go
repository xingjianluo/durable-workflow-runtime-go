package workflow

import (
	"context"
)

// Resume 让已失败的实例从卡住的步骤接着跑：不新建实例、不重跑已成功步骤、
// 历史（事件、失败记录、续跑记录）全部保留。extraAttempts 是调用方本次明确
// 追加的尝试预算，必须为正数，否则拒绝（不默认放开）。
//
// 语义：
//   - 已成功完成的实例拒绝续跑（ConflictAlreadyCompleted），仍在运行的实例
//     拒绝续跑（ConflictNotFailed），二者可区分；
//   - 续跑把 extraAttempts 追加到卡住步骤的预算（StepState.ExtraAttempts），
//     有效预算 = RetryPolicy.MaxAttempts + ExtraAttempts；尝试编号从既有
//     Attempt 继续递增，不重置；
//   - 续跑本身不执行活动，只把实例置回 running；之后由 Tick/Advance 正常推进，
//     租约 + fencing + 乐观版本的互斥约束不变；
//   - 并发续跑只有一个能提交（乐观版本），其余收到 version_conflict；
//     串行的重复续跑会因实例已不在 failed 而被拒绝。
func (e *Engine) Resume(ctx context.Context, id string, extraAttempts int) (*WorkflowState, error) {
	if extraAttempts <= 0 {
		return nil, conflict(ConflictInvalidExtraAttempts, ErrInvalidExtraAttempts)
	}
	st, _, err := e.store.Load(ctx, id)
	if err != nil {
		return nil, err
	}
	switch st.Status {
	case WFCompleted:
		return st, conflict(ConflictAlreadyCompleted, ErrWorkflowAlreadyCompleted)
	case WFRunning:
		return st, conflict(ConflictNotFailed, ErrWorkflowNotFailed)
	}
	if st.Cursor >= len(st.Steps) {
		// failed 但游标越界属于数据异常，明确拒绝而不是 panic。
		return st, conflictMsg(ConflictInvalidTransition, "failed workflow has no resumable step: "+st.ID)
	}
	now := e.clock.Now()
	if leaseActive(st.Lease, now) && st.Lease.Holder != e.workerID {
		return nil, conflict(ConflictLeaseHeldByOther, ErrLeaseOwnedByOther)
	}

	step := st.Steps[st.Cursor]
	step.ExtraAttempts += extraAttempts
	effectiveMax := step.effectiveMaxAttempts(e.defs[st.DefName])
	// 步骤回到待调度：不清空 Attempt（尝试编号连续）、不清 Failures（历史保留），
	// 只解除终态标记，等待下一次 Advance 从 Attempt+1 继续。
	step.Status = StepPending
	step.NextAttemptAt = nil
	step.EndedAt = nil
	step.FinalClass = FailureNone
	step.FinalReason = ""

	st.Status = WFRunning
	st.Failure = nil // 终态失败结论已写入事件与步骤失败历史；实例不再是终态
	st.Lease = nil
	st.UpdatedAt = now

	rec := ResumeRecord{
		Seq:           len(st.Resumes) + 1,
		StepIndex:     st.Cursor,
		AddedAttempts: extraAttempts,
		At:            now,
		By:            e.workerID,
	}
	st.Resumes = append(st.Resumes, rec)

	ev := Event{WorkflowID: id, Type: EvWorkflowResumed, At: now, StepIndex: st.Cursor,
		Detail: map[string]any{
			"step":                   step.Name,
			"resume_seq":             rec.Seq,
			"added_attempts":         extraAttempts,
			"extra_attempts_total":   step.ExtraAttempts,
			"effective_max_attempts": effectiveMax,
			"attempts_used":          step.Attempt,
			"by":                     e.workerID,
		}}
	committed, err := e.store.Commit(ctx, id, st.Version, st, []Event{ev})
	if err != nil {
		return nil, err
	}
	e.log.Logf("info", id, st.Cursor, "workflow resumed", "status", WFRunning,
		"resume_seq", rec.Seq, "added_attempts", extraAttempts,
		"effective_max_attempts", effectiveMax, "attempts_used", step.Attempt)
	return committed, nil
}

// effectiveMaxAttempts 返回步骤当前的有效尝试预算：
// 定义中的 MaxAttempts（归一化）加上历次续跑追加的 ExtraAttempts。
// def 为 nil（定义未注册）时只按已追加预算计，保证不panic；正常推进路径要求定义已注册。
func (s *StepState) effectiveMaxAttempts(def *WorkflowDef) int {
	base := 0
	if def != nil && s.Index < len(def.Steps) {
		base = def.Steps[s.Index].Retry.MaxAttemptsVal()
	}
	return base + s.ExtraAttempts
}
