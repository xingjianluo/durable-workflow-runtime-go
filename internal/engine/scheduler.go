package engine

import (
	"context"
	"time"

	"github.com/xingjianluo/durable-workflow-runtime-go/internal/workflow"
)

// benignTickErrors 是调度扫描时可预期、无需打印为错误的原因码。
var benignTickErrors = map[workflow.ErrorCode]bool{
	workflow.CodeWorkflowFinished: true,
	workflow.CodeRetryNotDue:      true,
	workflow.CodeLeaseHeldByOther: true,
	workflow.CodeAlreadyLeased:    true,
}

// Tick 执行一次调度扫描：遍历全部实例，尝试领取（含崩溃接管）并执行
// 每个处于 RUNNING 的工作流的当前到期步骤。
// 冲突（他人持租约、重试未到期、已终结等）不会被当作错误中断扫描。
func (e *Engine) Tick(ctx context.Context) error {
	ids, err := e.ListInstances(ctx)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		e.tickInstance(ctx, id)
	}
	return nil
}

func (e *Engine) tickInstance(ctx context.Context, id string) {
	inst, err := e.Get(ctx, id)
	if err != nil {
		e.logger.Printf("workflow=/%s tick load error: %v", id, err)
		return
	}
	if inst.State != workflow.StateRunning {
		return
	}
	stepName, acquired, err := e.AcquireStep(ctx, id)
	if err != nil {
		code := workflow.GetErrorCode(err)
		if benignTickErrors[code] {
			e.logger.Printf("workflow=%s/%s tick skip: code=%s %v",
				inst.WorkflowID, id, code, err)
		} else {
			e.logger.Printf("workflow=%s/%s tick acquire error: code=%s %v",
				inst.WorkflowID, id, code, err)
		}
		return
	}
	if !acquired {
		return
	}
	if err := e.ExecuteLeased(ctx, id, stepName); err != nil {
		code := workflow.GetErrorCode(err)
		e.logger.Printf("workflow=%s/%s step=%s execute result: code=%s %v",
			inst.WorkflowID, id, stepName, code, err)
	}
}

// Run 以固定间隔运行 Tick，直到 ctx 被取消。
func (e *Engine) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := e.Tick(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			e.logger.Printf("scheduler tick error: %v", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
