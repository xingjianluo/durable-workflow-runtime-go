package workflow

import (
	"context"
	"time"
)

// AttemptOutcome 是一次尝试的最终去向分类。
type AttemptOutcome string

const (
	OutcomeSucceeded   AttemptOutcome = "succeeded"   // 活动返回成功
	OutcomeFailed      AttemptOutcome = "failed"      // 活动返回失败（含超时）
	OutcomeInterrupted AttemptOutcome = "interrupted" // 崩溃/接管导致在途尝试中断
	OutcomeInFlight    AttemptOutcome = "in_flight"   // 已开始、尚无结局（持有者在跑或崩溃未被发现）
)

// AttemptRecord 是一轮尝试的完整轨迹，全部来自持久化数据，可稳定重建。
type AttemptRecord struct {
	StepIndex int            `json:"step_index"`
	StepName  string         `json:"step_name"`
	Attempt   int            `json:"attempt"`
	StartedAt time.Time      `json:"started_at"`
	EndedAt   *time.Time     `json:"ended_at,omitempty"`
	Duration  time.Duration  `json:"duration,omitempty"`
	Outcome   AttemptOutcome `json:"outcome"`
	Class     FailureClass   `json:"class,omitempty"`   // 失败/中断时的分类
	Message   string         `json:"message,omitempty"` // 失败/中断时的原因
	LeaseID   string         `json:"lease_id,omitempty"`
	// BudgetRemaining 是该尝试结束后、按当时有效预算计算的剩余可用尝试次数。
	BudgetRemaining int `json:"budget_remaining"`
}

// AuditReport 是一个实例的完整审计轨迹：每一轮尝试 + 每一次续跑。
// 只依赖持久化的快照与事件历史重建，重启之后查询结果稳定一致。
type AuditReport struct {
	WorkflowID string          `json:"workflow_id"`
	Status     WorkflowStatus  `json:"status"`
	Attempts   []AttemptRecord `json:"attempts"`
	Resumes    []ResumeRecord  `json:"resumes,omitempty"`
	// EffectiveMaxAttempts 是每个步骤当前的有效尝试预算（下标对齐步骤）。
	EffectiveMaxAttempts []int `json:"effective_max_attempts"`
}

// Audit 重建实例每一轮尝试的完整轨迹与续跑历史：第几次尝试、起止时间、
// 结局分类、是否被崩溃/接管中断、当时剩余预算，以及每次续跑追加的预算。
// 数据全部来自持久化的事件历史与快照，不依赖任何进程内存。
func (e *Engine) Audit(ctx context.Context, id string) (*AuditReport, error) {
	st, events, err := e.store.Load(ctx, id)
	if err != nil {
		return nil, err
	}
	def, ok := e.defs[st.DefName]
	if !ok {
		return nil, unknownDefErr(st.DefName)
	}

	report := &AuditReport{
		WorkflowID:           id,
		Status:               st.Status,
		Resumes:              append([]ResumeRecord(nil), st.Resumes...),
		EffectiveMaxAttempts: make([]int, len(st.Steps)),
	}
	for i := range st.Steps {
		base := 1
		if i < len(def.Steps) {
			base = def.Steps[i].Retry.MaxAttemptsVal()
		}
		report.EffectiveMaxAttempts[i] = base + st.Steps[i].ExtraAttempts
	}

	// 按事件顺序重建：step_started 开启一轮尝试，succeeded/failed/interrupted 收尾；
	// 续跑事件推进“当时的有效预算”，使 BudgetRemaining 反映历史时刻的真实余量。
	extraSoFar := map[int]int{}
	type key struct {
		step, attempt int
	}
	open := map[key]int{} // key -> report.Attempts 下标
	baseMax := func(step int) int {
		if step < len(def.Steps) {
			return def.Steps[step].Retry.MaxAttemptsVal()
		}
		return 1
	}
	budgetRemaining := func(step, attempt int) int {
		r := baseMax(step) + extraSoFar[step] - attempt
		if r < 0 {
			r = 0
		}
		return r
	}
	for _, ev := range events {
		switch ev.Type {
		case EvWorkflowResumed:
			extraSoFar[ev.StepIndex] += detailInt(ev.Detail, "extra_attempts")
		case EvStepStarted:
			rec := AttemptRecord{
				StepIndex: ev.StepIndex,
				StepName:  detailStr(ev.Detail, "step"),
				Attempt:   ev.Attempt,
				StartedAt: ev.At,
				Outcome:   OutcomeInFlight,
				LeaseID:   ev.LeaseID,
			}
			open[key{ev.StepIndex, ev.Attempt}] = len(report.Attempts)
			report.Attempts = append(report.Attempts, rec)
		case EvStepSucceeded, EvStepFailed, EvStepAttemptInterrupted:
			idx, ok := open[key{ev.StepIndex, ev.Attempt}]
			if !ok {
				continue // 已有结局（例如中断记账后又补的终止事件），不重复收尾
			}
			delete(open, key{ev.StepIndex, ev.Attempt})
			rec := &report.Attempts[idx]
			end := ev.At
			rec.EndedAt = &end
			rec.Duration = end.Sub(rec.StartedAt)
			rec.BudgetRemaining = budgetRemaining(ev.StepIndex, ev.Attempt)
			switch ev.Type {
			case EvStepSucceeded:
				rec.Outcome = OutcomeSucceeded
			case EvStepAttemptInterrupted:
				rec.Outcome = OutcomeInterrupted
				rec.Class = FailureTransient
				rec.Message = detailStr(ev.Detail, "message")
			default:
				rec.Outcome = OutcomeFailed
				rec.Class = FailureClass(detailStr(ev.Detail, "class"))
				rec.Message = detailStr(ev.Detail, "message")
			}
		}
	}
	// 仍未收尾的尝试：按当前预算给出剩余次数（崩溃未被发现或正在执行）。
	for _, idx := range open {
		rec := &report.Attempts[idx]
		rec.BudgetRemaining = budgetRemaining(rec.StepIndex, rec.Attempt)
	}
	return report, nil
}

func detailStr(d map[string]any, k string) string {
	if d == nil {
		return ""
	}
	if s, ok := d[k].(string); ok {
		return s
	}
	return ""
}

func detailInt(d map[string]any, k string) int {
	if d == nil {
		return 0
	}
	switch v := d[k].(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	}
	return 0
}
