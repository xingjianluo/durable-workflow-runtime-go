package workflow

import (
	"context"
	"time"
)

// AttemptOutcome 是一次尝试的最终去向分类。
type AttemptOutcome string

const (
	OutcomeSucceeded   AttemptOutcome = "succeeded"   // 活动成功
	OutcomeFailed      AttemptOutcome = "failed"      // 活动失败（含退避等待中的失败）
	OutcomeInterrupted AttemptOutcome = "interrupted" // 崩溃/接管中断，结果未提交
	OutcomeInProgress  AttemptOutcome = "in_progress" // 仍在执行中
)

// AttemptRecord 是审计轨迹中一次尝试的完整记录。
type AttemptRecord struct {
	StepIndex int            `json:"step_index"`
	StepName  string         `json:"step_name"`
	Attempt   int            `json:"attempt"`
	StartedAt time.Time      `json:"started_at"`
	EndedAt   *time.Time     `json:"ended_at,omitempty"`
	Duration  time.Duration  `json:"duration,omitempty"`
	Outcome   AttemptOutcome `json:"outcome"`
	Class     FailureClass   `json:"class,omitempty"`
	LeaseID   string         `json:"lease_id,omitempty"`
	// RemainingBudget 是该次尝试开始时、按当时预算计算扣掉本次后剩余的可用尝试次数；
	// -1 表示无法从持久化数据推断（如定义未注册且事件缺少预算信息）。
	RemainingBudget int `json:"remaining_budget"`
}

// AuditReport 是一个实例的完整复盘报告：每一轮尝试的轨迹 + 续跑历史。
// 全部从持久化的事件与快照重建，不依赖进程内存，重启后查询结果一致。
type AuditReport struct {
	WorkflowID string          `json:"workflow_id"`
	Status     WorkflowStatus  `json:"status"`
	Attempts   []AttemptRecord `json:"attempts"`
	Resumes    []ResumeRecord  `json:"resumes,omitempty"`
}

// Audit 重建实例的完整尝试轨迹与续跑历史，用于解释“当时为什么继续、为什么终止”：
//   - 每次 step_started 开一条尝试记录，step_succeeded/step_failed 将其闭环；
//   - 没有闭环的尝试：若仍是当前在途尝试则为 in_progress，否则为 interrupted
//     （崩溃或接管中断——该尝试已用掉一次预算，但不会重复产生副作用）；
//   - 剩余预算优先取 step_started 事件中持久化的 max_attempts，缺失时按
//     “定义基准 + 历次续跑追加”的时间线推算；
//   - 续跑历史取自快照中持久化的 Resumes，与 workflow_resumed 事件一一对应。
func (e *Engine) Audit(ctx context.Context, id string) (*AuditReport, error) {
	st, events, err := e.store.Load(ctx, id)
	if err != nil {
		return nil, err
	}
	rep := &AuditReport{WorkflowID: id, Status: st.Status, Resumes: append([]ResumeRecord(nil), st.Resumes...)}

	// 每步预算时间线：定义基准（若已注册）+ 续跑事件追加。
	def := e.defs[st.DefName]
	budget := map[int]int{}
	baseOf := func(stepIdx int) (int, bool) {
		if def == nil || stepIdx >= len(def.Steps) {
			return 0, false
		}
		return def.Steps[stepIdx].Retry.MaxAttemptsVal(), true
	}

	// open 记录尚未闭环的尝试：key 为 stepIndex*1e6+attempt 之类易错，改用二维映射。
	type key struct {
		step, attempt int
	}
	open := map[key]int{} // key -> rep.Attempts 下标

	for _, ev := range events {
		switch ev.Type {
		case EvWorkflowResumed:
			added := detailInt(ev.Detail, "added_attempts")
			budget[ev.StepIndex] = budgetAt(budget, ev.StepIndex, baseOf) + added
		case EvStepStarted:
			max, known := detailIntOK(ev.Detail, "max_attempts")
			if !known {
				max, known = budgetAtOK(budget, ev.StepIndex, baseOf)
			}
			remaining := -1
			if known {
				remaining = max - ev.Attempt
			}
			name, _ := ev.Detail["step"].(string)
			rep.Attempts = append(rep.Attempts, AttemptRecord{
				StepIndex: ev.StepIndex, StepName: name, Attempt: ev.Attempt,
				StartedAt: ev.At, Outcome: OutcomeInProgress, LeaseID: ev.LeaseID,
				RemainingBudget: remaining,
			})
			open[key{ev.StepIndex, ev.Attempt}] = len(rep.Attempts) - 1
		case EvStepSucceeded, EvStepFailed:
			idx, ok := open[key{ev.StepIndex, ev.Attempt}]
			if !ok {
				continue
			}
			rec := &rep.Attempts[idx]
			end := ev.At
			rec.EndedAt = &end
			rec.Duration = end.Sub(rec.StartedAt)
			if ev.Type == EvStepSucceeded {
				rec.Outcome = OutcomeSucceeded
			} else {
				rec.Outcome = OutcomeFailed
				rec.Class = FailureClass(detailString(ev.Detail, "class"))
			}
			delete(open, key{ev.StepIndex, ev.Attempt})
		}
	}

	// 未闭环的尝试：仍是当前在途尝试（步骤 running 且租约活跃）→ in_progress；
	// 其余（崩溃/接管中断，或状态已前移）→ interrupted。
	now := e.clock.Now()
	for k, idx := range open {
		rec := &rep.Attempts[idx]
		if k.step == st.Cursor && k.step < len(st.Steps) &&
			st.Steps[k.step].Status == StepRunning && leaseActive(st.Lease, now) {
			rec.Outcome = OutcomeInProgress
		} else {
			rec.Outcome = OutcomeInterrupted
		}
	}
	return rep, nil
}

func budgetAt(budget map[int]int, step int, baseOf func(int) (int, bool)) int {
	v, _ := budgetAtOK(budget, step, baseOf)
	return v
}

func budgetAtOK(budget map[int]int, step int, baseOf func(int) (int, bool)) (int, bool) {
	if v, ok := budget[step]; ok {
		return v, true
	}
	return baseOf(step)
}

// detailIntOK 读取事件 Detail 中的整数（JSON 反序列化后为 float64）。
func detailIntOK(d map[string]any, k string) (int, bool) {
	if d == nil {
		return 0, false
	}
	switch v := d[k].(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	}
	return 0, false
}

func detailInt(d map[string]any, k string) int {
	v, _ := detailIntOK(d, k)
	return v
}

func detailString(d map[string]any, k string) string {
	if d == nil {
		return ""
	}
	s, _ := d[k].(string)
	return s
}
