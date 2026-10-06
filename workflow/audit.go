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

// Audit 重建实例的完整尝试轨迹与续跑历史，用于解释“当时为什么继续、为什么终止”。
// 全部结论只来自已落盘的事件与快照，进程重启后或实例到达终态后再查，结果逐字段一致。
//
// 尝试去向（Outcome）的判定口径：
//   - succeeded：有 step_succeeded 闭环，活动确实返回了成功结果；
//   - failed：有 step_failed 闭环且事件未标记 interrupted——活动确实执行完并返回了错误；
//   - interrupted（被中断，执行被打断、根本没跑出结果）有两种落盘形态：
//     1. step_failed 事件带 interrupted=true：接管/推进时发现上一次在途尝试已把最后一次
//     预算用掉，直接补记落账“预算耗尽”，期间没有发起新的活动调用（见 commitBudgetExhausted）；
//     2. 只有 step_started、没有任何闭环事件，且又不是当前活跃在途尝试——即崩溃/接管后
//     被遗弃的历史尝试（它的预算在 step_started 落盘时就已消耗）；
//   - in_progress：同一步骤同一时刻至多一条。必须同时满足：该步骤是当前游标步骤、
//     快照状态为 running、快照中的 Attempt 与本记录编号一致、租约仍活跃、且租约 token
//     与本 step_started 事件记录的 lease 一致。接管后新尝试在跑时，旧尝试因编号/token
//     都对不上而归入 interrupted，不会出现两条 in_progress。
//
// 剩余可用次数（RemainingBudget）口径：取该次 step_started 事件持久化的 max_attempts
// （即当时的有效预算 = 定义基准 MaxAttempts + 截至当时历次续跑追加）减去本尝试编号，
// 含义是“本次尝试开始后，之后还剩几次可用”；事件缺少该明细时按“定义基准 + 续跑时间线”
// 推算，仍无法推断（定义未注册）时为 -1。
//
// 续跑历史取自快照中持久化的 Resumes，与 workflow_resumed 事件一一对应。
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
			switch {
			case ev.Type == EvStepSucceeded:
				rec.Outcome = OutcomeSucceeded
			case detailBool(ev.Detail, "interrupted"):
				// 崩溃/接管留下的在途尝试用掉了最后一次预算：没有活动结果，
				// 是被中断而非业务失败；Class 留空以与真实错误区分。
				rec.Outcome = OutcomeInterrupted
			default:
				rec.Outcome = OutcomeFailed
				rec.Class = FailureClass(detailString(ev.Detail, "class"))
			}
			delete(open, key{ev.StepIndex, ev.Attempt})
		}
	}

	// 未闭环的尝试：只有“当前真正在跑的那一次”才是 in_progress，
	// 同一步骤同一时刻至多一条；其余（崩溃/接管后被遗弃的历史尝试，或状态已前移）
	// 一律为 interrupted。判定只依赖快照：游标步骤、running、Attempt 编号一致、
	// 租约活跃且 token 与本尝试 step_started 时记录的 lease 一致。
	now := e.clock.Now()
	for k, idx := range open {
		rec := &rep.Attempts[idx]
		inProgress := false
		if k.step == st.Cursor && k.step < len(st.Steps) {
			cur := st.Steps[k.step]
			if cur.Status == StepRunning && cur.Attempt == k.attempt &&
				leaseActive(st.Lease, now) && rec.LeaseID != "" && rec.LeaseID == st.Lease.Token {
				inProgress = true
			}
		}
		if inProgress {
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

// detailBool 读取事件 Detail 中的布尔值（JSON 反序列化后为 bool）；缺省为 false。
func detailBool(d map[string]any, k string) bool {
	if d == nil {
		return false
	}
	b, _ := d[k].(bool)
	return b
}
