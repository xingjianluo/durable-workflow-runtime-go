package workflow

import "time"

// Project 将一个实例的事件日志重放为当前状态快照。
// 纯函数：不访问存储，崩溃恢复时重建内存状态即调用本函数。
// 重放规则保证：事件顺序确定 => 投影结果确定且可复现。
func Project(events []*Event) *Instance {
	if len(events) == 0 {
		return nil
	}
	inst := &Instance{}
	var stepByName = map[string]*StepStatus{}

	getStep := func(name string) *StepStatus {
		if s, ok := stepByName[name]; ok {
			return s
		}
		s := &StepStatus{Name: name, State: StepPending}
		stepByName[name] = s
		inst.Steps = append(inst.Steps, s)
		return s
	}

	for _, ev := range events {
		inst.WorkflowID = ev.WorkflowID
		inst.InstanceID = ev.InstanceID
		inst.UpdatedAt = ev.Timestamp

		switch ev.Type {
		case EventWorkflowStarted:
			inst.State = StateRunning
			inst.InputJSON = ev.InputJSON
			inst.CurrentStep = 0
			inst.Steps = nil
			stepByName = map[string]*StepStatus{}
			for _, n := range ev.StepNames {
				getStep(n)
			}

		case EventStepScheduled:
			s := getStep(ev.StepName)
			s.Activity = ev.Activity
			s.NextAttemptAt = ev.NextAttemptAt

		case EventAttemptFailed:
			// 一次尝试失败：租约释放，最终走向由随后的
			// RETRY_SCHEDULED / STEP_FAILED 决定。
			s := getStep(ev.StepName)
			s.LeaseHolder = ""
			s.LeaseExpiresAt = time.Time{}

		case EventStepLeased:
			s := getStep(ev.StepName)
			s.State = StepRunning
			s.Attempt = ev.Attempt
			s.LeaseHolder = ev.LeaseHolder
			s.LeaseExpiresAt = ev.LeaseExpiresAt
			s.NextAttemptAt = time.Time{}

		case EventLeaseTakenOver:
			s := getStep(ev.StepName)
			s.State = StepRunning
			s.LeaseHolder = ev.LeaseHolder
			s.LeaseExpiresAt = ev.LeaseExpiresAt

		case EventStepSucceeded:
			s := getStep(ev.StepName)
			s.State = StepSucceeded
			s.ResultJSON = ev.ResultJSON
			s.LeaseHolder = ""
			s.LeaseExpiresAt = time.Time{}
			s.NextAttemptAt = time.Time{}
			// 推进当前步骤指针
			inst.CurrentStep = indexOf(inst.Steps, s) + 1

		case EventStepFailed:
			s := getStep(ev.StepName)
			s.State = StepFailed
			s.LeaseHolder = ""
			s.LeaseExpiresAt = time.Time{}
			s.NextAttemptAt = time.Time{}

		case EventStepTimedOut:
			// 超时本身不改变步骤最终态（仍为 RUNNING/待接管），仅记录历史；
			// 是否可重试由后续 RETRY_SCHEDULED 或 STEP_FAILED 表达。
			s := getStep(ev.StepName)
			s.LeaseHolder = ""
			s.LeaseExpiresAt = time.Time{}

		case EventRetryScheduled:
			s := getStep(ev.StepName)
			s.State = StepPending
			s.LeaseHolder = ""
			s.LeaseExpiresAt = time.Time{}
			s.NextAttemptAt = ev.NextAttemptAt

		case EventWorkflowCompleted:
			inst.State = StateCompleted
			inst.ResultJSON = ev.ResultJSON
			if inst.CurrentStep < len(inst.Steps) {
				inst.CurrentStep = len(inst.Steps)
			}

		case EventWorkflowFailed:
			inst.State = StateFailed
			inst.ResultJSON = ev.ResultJSON

		case EventWorkflowCanceled:
			inst.State = StateCanceled
		}

		// 失败/超时/成功都追加到尝试历史
		if ev.StepName != "" && isAttemptRecord(ev.Type) {
			s := getStep(ev.StepName)
			rec := AttemptRecord{
				Attempt:    ev.Attempt,
				Class:      ev.FailureClass,
				ErrorCode:  ev.ErrorCode,
				Message:    ev.Message,
				FinishedAt: ev.Timestamp,
				TimedOut:   ev.Type == EventStepTimedOut,
			}
			s.History = append(s.History, rec)
			if ev.Type == EventStepFailed || ev.Type == EventStepTimedOut {
				cp := rec
				s.Failure = &cp
			}
		}
	}
	return inst
}

func isAttemptRecord(t EventType) bool {
	return t == EventStepSucceeded || t == EventStepFailed ||
		t == EventStepTimedOut || t == EventAttemptFailed
}

func indexOf(steps []*StepStatus, target *StepStatus) int {
	for i, s := range steps {
		if s == target {
			return i
		}
	}
	return -1
}

// FindStep 返回指定名称的步骤状态。
func (in *Instance) FindStep(name string) *StepStatus {
	for _, s := range in.Steps {
		if s.Name == name {
			return s
		}
	}
	return nil
}

// CurrentStepSpec 返回当前步骤；已全部完成时返回 nil。
func (in *Instance) CurrentStepStatus() *StepStatus {
	if in.CurrentStep < 0 || in.CurrentStep >= len(in.Steps) {
		return nil
	}
	return in.Steps[in.CurrentStep]
}
