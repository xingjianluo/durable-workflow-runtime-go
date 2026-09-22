package store

import (
	"context"

	"github.com/xingjianluo/durable-workflow-runtime-go/internal/workflow"
)

// Store 是工作流事件日志的持久化抽象。
// 所有写操作必须满足：Append 是原子且具备串行化语义的（单文件互斥锁），
// 保证多个引擎实例并发访问时事件不丢失、seq 单调。
type Store interface {
	// Append 以条件事务方式追加事件。
	// expectLastSeq 为调用方读取到的最后一条事件 seq（空日志传 0）；
	// 若日志尾部已被其他写者推进，返回 CodeConcurrentModification。
	Append(ctx context.Context, instanceID string, events []*workflow.Event, expectLastSeq int64) error

	// ReadEvents 读取某实例的全部事件（按 seq 升序）。
	ReadEvents(ctx context.Context, instanceID string) ([]*workflow.Event, int64, error)

	// ListInstances 列出所有实例 ID。
	ListInstances(ctx context.Context) ([]string, error)

	// LastSeq 返回整个日志当前的最大 seq（空日志为 0）。
	LastSeq(ctx context.Context) (int64, error)

	// Close 释放底层资源。
	Close() error
}
