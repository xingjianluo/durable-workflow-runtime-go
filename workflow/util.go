package workflow

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

func newToken(prefix string) string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand 失败在正常环境下不可能发生；直接 panic 暴露平台问题。
		panic("workflow: crypto/rand failed: " + err.Error())
	}
	return prefix + "-" + hex.EncodeToString(b[:])
}

func ptrTime(t time.Time) *time.Time { return &t }

// idempotencyToken 是逻辑步骤（而非单次尝试）的稳定去重令牌：
// 只依赖工作流 ID 与步骤下标，因此崩溃接管后的重试尝试拿到的仍是同一个令牌，
// 活动可据此保证同一逻辑步骤只产生一次业务副作用。
func idempotencyToken(workflowID string, stepIndex int) string {
	return workflowID + "#step" + itoa(stepIndex)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [24]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
