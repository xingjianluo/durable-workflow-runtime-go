package workflow

import "errors"

// 可区分原因的哨兵错误。调用方可用 errors.Is 判定，也可用 errors.As
// 拿到 *ConflictError 读取 Kind 字段做细粒度处理。
var (
	ErrNotFound             = errors.New("workflow: not found")
	ErrAlreadyExists        = errors.New("workflow: already exists")
	ErrInvalidState         = errors.New("workflow: invalid state transition")
	ErrStepAlreadyCompleted = errors.New("workflow: step already completed")
	ErrLeaseNotHeld         = errors.New("workflow: lease not held by caller")
	ErrLeaseOwnedByOther    = errors.New("workflow: lease owned by another worker")
	ErrVersionConflict      = errors.New("workflow: optimistic version conflict")
	ErrUnknownActivity      = errors.New("workflow: unknown activity")
	ErrWorkflowCompleted    = errors.New("workflow: workflow already terminal")
	ErrNoRunnableStep       = errors.New("workflow: no runnable step now")
	// ErrWorkflowAlreadyCompleted 续跑一个已成功完成的实例（与失败终止可区分）。
	ErrWorkflowAlreadyCompleted = errors.New("workflow: already completed, cannot resume")
	// ErrWorkflowNotFailed 续跑一个并未失败的实例（仍在运行）。
	ErrWorkflowNotFailed = errors.New("workflow: not in failed state, cannot resume")
	// ErrInvalidExtraAttempts 续跑时未显式给出有效的追加尝试次数。
	ErrInvalidExtraAttempts = errors.New("workflow: extra attempts must be a positive number")
)

// ConflictKind 标识冲突/非法状态迁移的具体原因。
type ConflictKind string

const (
	ConflictInvalidTransition  ConflictKind = "invalid_transition"
	ConflictStepDone           ConflictKind = "step_already_completed"
	ConflictLeaseHeldByOther   ConflictKind = "lease_held_by_another_worker"
	ConflictLeaseNotHeld       ConflictKind = "lease_not_held"
	ConflictLeaseExpired       ConflictKind = "lease_expired"
	ConflictLeaseVersionStale  ConflictKind = "lease_version_stale"
	ConflictVersion            ConflictKind = "version_conflict"
	ConflictTerminal           ConflictKind = "workflow_terminal"
	ConflictNoRunnable         ConflictKind = "no_runnable_step"
	ConflictNotTerminal        ConflictKind = "not_terminal"
	ConflictStartAlreadyExists ConflictKind = "workflow_already_exists"
	// ConflictAlreadyCompleted 续跑已成功完成的实例。
	ConflictAlreadyCompleted ConflictKind = "workflow_already_completed"
	// ConflictNotFailed 续跑未处于 failed 的实例（如仍在运行）。
	ConflictNotFailed ConflictKind = "workflow_not_failed"
	// ConflictInvalidExtraAttempts 续跑追加预算缺失或非法（<=0）。
	ConflictInvalidExtraAttempts ConflictKind = "invalid_extra_attempts"
)

// ConflictError 包装一个可区分原因的冲突。
type ConflictError struct {
	Kind ConflictKind
	Err  error
	Msg  string
}

func (e *ConflictError) Error() string {
	if e.Msg != "" {
		return "workflow: " + string(e.Kind) + ": " + e.Msg
	}
	return "workflow: " + string(e.Kind) + ": " + e.Err.Error()
}

func (e *ConflictError) Unwrap() error { return e.Err }

func conflict(kind ConflictKind, err error) *ConflictError {
	return &ConflictError{Kind: kind, Err: err}
}

func conflictMsg(kind ConflictKind, msg string) *ConflictError {
	return &ConflictError{Kind: kind, Msg: msg, Err: ErrInvalidState}
}
