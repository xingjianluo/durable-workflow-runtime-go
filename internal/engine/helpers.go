package engine

import (
	"time"

	"github.com/xingjianluo/durable-workflow-runtime-go/internal/workflow"
)

// stepSpecByName 在定义中查找步骤规格。
func stepSpecByName(def *workflow.Definition, name string) *workflow.StepSpec {
	for i := range def.Steps {
		if def.Steps[i].Name == name {
			return &def.Steps[i]
		}
	}
	return nil
}

// policyOrDefault 返回生效的重试策略（填充零值默认）。
func policyOrDefault(p workflow.RetryPolicy) workflow.RetryPolicy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 1
	}
	if p.Multiplier <= 0 {
		p.Multiplier = 2
	}
	return p
}

// backoff 计算第 failedAttempt 次失败（从 1 开始）后的退避时长。
// 纯函数：固定策略与失败次数 => 固定退避，结论可复现。
func backoff(initial, max time.Duration, multiplier float64, failedAttempt int) time.Duration {
	if initial <= 0 || failedAttempt <= 0 {
		return 0
	}
	d := float64(initial)
	for i := 1; i < failedAttempt; i++ {
		d *= multiplier
		if max > 0 && d > float64(max) {
			d = float64(max)
			break
		}
	}
	dur := time.Duration(d)
	if max > 0 && dur > max {
		dur = max
	}
	return dur
}
