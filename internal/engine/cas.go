package engine

import (
	"context"

	"github.com/xingjianluo/durable-workflow-runtime-go/internal/workflow"
)

// casLoop 封装“读取-判定-条件追加”的乐观并发循环。
// fn 返回要追加的事件批次与所基于的全局 seq；
// 返回 fatal=true 时即使 CAS 冲突也不再重试（如 AlreadyStarted）。
func (e *Engine) casLoop(
	ctx context.Context,
	instanceID string,
	fn func(lastReadSeq int64) (fatal bool, events []*workflow.Event, baseSeq int64, err error),
) error {
	for attempt := 0; attempt <= defaultCASRetries; attempt++ {
		fatal, events, baseSeq, err := fn(0)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return nil
		}
		err = e.store.Append(ctx, instanceID, events, baseSeq)
		if err == nil {
			return nil
		}
		if workflow.GetErrorCode(err) != workflow.CodeConcurrentModification || fatal {
			return err
		}
		// CAS 冲突：重读后由 fn 重新判定，不静默覆盖。
	}
	return workflow.NewError(workflow.CodeConcurrentModification,
		"instance %q contended beyond retry budget", instanceID)
}
