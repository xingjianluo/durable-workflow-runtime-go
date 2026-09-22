package workflow

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
)

// Logger 接收引擎内部的结构化日志。测试可注入打印工作流标识、
// 步骤、状态变化与失败分类的 Logger。
type Logger interface {
	// Logf 记录一条日志。stepIndex<0 表示工作流级别事件；kv 为 key/value 对。
	Logf(level, workflowID string, stepIndex int, msg string, kv ...any)
}

// stdLogger 用标准库 log 输出 key=value 风格日志。
type stdLogger struct {
	l *log.Logger
}

// NewStdLogger 创建输出到 stderr 的默认 Logger。
func NewStdLogger() Logger {
	return NewStdLoggerWriter(os.Stderr)
}

// NewStdLoggerWriter 创建输出到指定 writer 的 Logger。
func NewStdLoggerWriter(w io.Writer) Logger {
	return &stdLogger{l: log.New(w, "[workflow] ", log.LstdFlags|log.Lmicroseconds)}
}

func (s *stdLogger) Logf(level, workflowID string, stepIndex int, msg string, kv ...any) {
	var b strings.Builder
	b.WriteString("level=")
	b.WriteString(level)
	b.WriteString(" workflow=")
	b.WriteString(workflowID)
	if stepIndex >= 0 {
		fmt.Fprintf(&b, " step=%d", stepIndex)
	}
	b.WriteString(" msg=\"")
	b.WriteString(msg)
	b.WriteString("\"")
	for i := 0; i+1 < len(kv); i += 2 {
		b.WriteString(" ")
		fmt.Fprintf(&b, "%v=%v", kv[i], kv[i+1])
	}
	s.l.Println(b.String())
}

// nopLogger 丢弃日志。
type nopLogger struct{}

// NopLogger 返回空日志器。
func NopLogger() Logger { return nopLogger{} }

func (nopLogger) Logf(level, workflowID string, stepIndex int, msg string, kv ...any) {}

// LogEntry 是测试日志器捕获的一条日志。
type LogEntry struct {
	Level      string
	WorkflowID string
	StepIndex  int
	Msg        string
	KV         []any
}

func (e LogEntry) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "level=%s workflow=%s", e.Level, e.WorkflowID)
	if e.StepIndex >= 0 {
		fmt.Fprintf(&b, " step=%d", e.StepIndex)
	}
	fmt.Fprintf(&b, " msg=%q", e.Msg)
	for i := 0; i+1 < len(e.KV); i += 2 {
		fmt.Fprintf(&b, " %v=%v", e.KV[i], e.KV[i+1])
	}
	return b.String()
}

// TestLogger 同时通过 t.Logf 打印（工作流标识/步骤/状态变化/失败分类）
// 并在内存中捕获全部条目，便于断言。
type TestLogger interface {
	Logger
	Entries() []LogEntry
}

type testLogger struct {
	mu      sync.Mutex
	entries []LogEntry
	print   func(string)
}

// NewTestLogger 创建测试日志器；print 通常传 t.Logf 的适配器，可为 nil。
func NewTestLogger(print func(string)) TestLogger {
	return &testLogger{print: print}
}

func (l *testLogger) Logf(level, workflowID string, stepIndex int, msg string, kv ...any) {
	e := LogEntry{Level: level, WorkflowID: workflowID, StepIndex: stepIndex, Msg: msg, KV: append([]any(nil), kv...)}
	l.mu.Lock()
	l.entries = append(l.entries, e)
	l.mu.Unlock()
	if l.print != nil {
		l.print(e.String())
	}
}

func (l *testLogger) Entries() []LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]LogEntry(nil), l.entries...)
}
