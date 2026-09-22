package workflow

import "context"

// Store 是工作流状态的持久化抽象。所有写入都必须在返回前落盘，
// 保证进程重启后 Load 能读到最新已确认状态。
type Store interface {
	// Create 幂等性由实现保证：相同 ID 已存在时返回 ConflictError(ConflictStartAlreadyExists)。
	Create(ctx context.Context, st *WorkflowState, events []Event) (*WorkflowState, error)
	// Commit 在乐观版本 expectedVersion 上提交：状态被替换为 st（st.Version 会被置为新版本），
	// events 被追加。版本不匹配返回 ConflictError(ConflictVersion)。
	Commit(ctx context.Context, id string, expectedVersion int64, st *WorkflowState, events []Event) (*WorkflowState, error)
	// Load 读取最新快照与全部事件历史。
	Load(ctx context.Context, id string) (*WorkflowState, []Event, error)
	// List 返回所有工作流最新快照（无序，调用方按需排序）。
	List(ctx context.Context) ([]*WorkflowState, error)
	// Close 释放底层资源（如文件锁）。
	Close() error
}
