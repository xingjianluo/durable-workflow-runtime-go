package workflow

import "fmt"

// ErrorCode 用可区分的代码标识冲突或非法状态迁移原因。
type ErrorCode string

const (
	CodeNotFound               ErrorCode = "NOT_FOUND"
	CodeAlreadyStarted         ErrorCode = "ALREADY_STARTED"
	CodeInvalidState           ErrorCode = "INVALID_STATE_TRANSITION"
	CodeStepNotReady           ErrorCode = "STEP_NOT_READY"
	CodeLeaseHeldByOther       ErrorCode = "LEASE_HELD_BY_OTHER"
	CodeLeaseNotExpired        ErrorCode = "LEASE_NOT_EXPIRED"
	CodeConcurrentModification ErrorCode = "CONCURRENT_MODIFICATION"
	CodeRetryExhausted         ErrorCode = "RETRY_EXHAUSTED"
	CodePermanentFailure       ErrorCode = "PERMANENT_FAILURE"
	CodeRetryNotDue            ErrorCode = "RETRY_NOT_DUE"
	CodeAlreadySucceeded       ErrorCode = "ALREADY_SUCCEEDED"
	CodeAlreadyLeased          ErrorCode = "ALREADY_LEASED"
	CodeWorkflowFinished       ErrorCode = "WORKFLOW_FINISHED"
	CodeLeaseExpired           ErrorCode = "LEASE_EXPIRED"
	CodeDefinitionNotFound     ErrorCode = "DEFINITION_NOT_FOUND"
)

// Error 携带可区分原因码的引擎错误。
type Error struct {
	Code    ErrorCode
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// NewError 构造一个带原因码的错误。
func NewError(code ErrorCode, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// GetErrorCode 从错误中提取原因码；普通错误返回空串。
func GetErrorCode(err error) ErrorCode {
	if err == nil {
		return ""
	}
	if e, ok := err.(*Error); ok {
		return e.Code
	}
	return ""
}
