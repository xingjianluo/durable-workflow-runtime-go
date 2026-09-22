package workflow

import (
	"context"
	"errors"
	"fmt"
)

// ActivityContext 携带单次尝试的 context（尝试超时/租约丢失会取消它）。
type ActivityContext interface {
	context.Context
	// Attempt 当前尝试序号（从 1 开始）。
	Attempt() int
	// IdempotencyToken 同一逻辑步骤所有尝试稳定不变的去重令牌。
	IdempotencyToken() string
}

type activityContext struct {
	context.Context
	attempt int
	token   string
}

func (c *activityContext) Attempt() int             { return c.attempt }
func (c *activityContext) IdempotencyToken() string { return c.token }

// activityError 携带失败分类。
type activityError struct {
	class   FailureClass
	message string
	cause   error
}

func (e *activityError) Error() string {
	if e.cause != nil {
		return string(e.class) + ": " + e.message + ": " + e.cause.Error()
	}
	return string(e.class) + ": " + e.message
}

func (e *activityError) Unwrap() error { return e.cause }

// Class 返回错误的失败分类。
func (e *activityError) Class() FailureClass { return e.class }

// Transient 包装一个瞬时错误：可以重试。
func Transient(err error) error {
	if err == nil {
		return nil
	}
	return &activityError{class: FailureTransient, message: err.Error(), cause: err}
}

// Transientf 构造一个瞬时错误。
func Transientf(format string, args ...any) error {
	return &activityError{class: FailureTransient, message: fmt.Sprintf(format, args...)}
}

// Permanent 包装一个永久错误：不得重试，立即终止该步骤与工作流。
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &activityError{class: FailurePermanent, message: err.Error(), cause: err}
}

// Permanentf 构造一个永久错误。
func Permanentf(format string, args ...any) error {
	return &activityError{class: FailurePermanent, message: fmt.Sprintf(format, args...)}
}

// classify 把活动返回的错误归一化为 FailureClass。
// 规则：
//   - 显式 Permanent 包装 → permanent，立即终止；
//   - 显式 Transient 包装或 context.DeadlineExceeded → transient/timeout，可重试；
//   - 未分类的普通错误默认按瞬时处理（受 MaxAttempts 限制，因此不会无限重试）。
func classify(err error) (FailureClass, string) {
	if err == nil {
		return FailureNone, ""
	}
	var ae *activityError
	if errors.As(err, &ae) {
		return ae.class, ae.Error()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return FailureTimeout, err.Error()
	}
	return FailureTransient, err.Error()
}
