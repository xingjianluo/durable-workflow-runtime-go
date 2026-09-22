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
