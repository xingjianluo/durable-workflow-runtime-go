package engine_test

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/durable-workflow-runtime-go/internal/activity"
	"github.com/xingjianluo/durable-workflow-runtime-go/internal/engine"
	"github.com/xingjianluo/durable-workflow-runtime-go/internal/store"
	"github.com/xingjianluo/durable-workflow-runtime-go/internal/workflow"
)

// ---- 测试基础设施：虚拟时钟、可分类的测试活动、日志捕获 ----

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)} }
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// testLog 同时输出到 t.Log 与缓冲区，便于断言。
type testLog struct {
	t   *testing.T
	buf bytes.Buffer
	mu  sync.Mutex
}

func (l *testLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf.Write(p)
	l.t.Logf("%s", bytes.TrimRight(p, "\n"))
	return len(p), nil
}
func (l *testLog) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.buf.String() }

type testHarness struct {
	t        *testing.T
	dir      string
	st       *store.FileStore
	reg      *activity.Registry
	clock    *fakeClock
	counters *counters
}

type counters struct {
	mu      sync.Mutex
	effects map[string]int // key: input; 活动外部副作用计数
	calls   map[string]int // key: activity:"stepinput"
}

func newCounters() *counters { return &counters{effects: map[string]int{}, calls: map[string]int{}} }
func (c *counters) Effect(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.effects[key]++
	return c.effects[key]
}
func (c *counters) Effects(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.effects[key]
}
func (c *counters) Call(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls[key]++
}
func (c *counters) Calls(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[key]
}

func newHarness(t *testing.T) *testHarness {
	t.Helper()
	dir := t.TempDir()
	st, err := store.OpenFileStore(context.Background(), dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	h := &testHarness{
		t:        t,
		dir:      dir,
		st:       st,
		reg:      activity.NewRegistry(),
		clock:    newFakeClock(),
		counters: newCounters(),
	}
	h.reg.MustRegister(activity.NewFuncActivity("echo", func(ctx context.Context, in []byte) ([]byte, error) {
		h.counters.Effect(string(in))
		return in, nil
	}))
	return h
}

func (h *testHarness) newEngine(workerID string, ttl time.Duration) *engine.Engine {
	return engine.New(h.st, h.reg, engine.Config{
		WorkerID: workerID,
		LeaseTTL: ttl,
		Now:      h.clock.Now,
		LogOut:   &testLog{t: h.t},
	})
}

// reopenEngine 模拟进程重启：关闭并重新打开存储、新建引擎。
func (h *testHarness) reopenEngine(workerID string, ttl time.Duration) *engine.Engine {
	if err := h.st.Close(); err != nil {
		h.t.Fatalf("close store: %v", err)
	}
	st, err := store.OpenFileStore(context.Background(), h.dir)
	if err != nil {
		h.t.Fatalf("reopen store: %v", err)
	}
	h.st = st
	return engine.New(st, h.reg, engine.Config{
		WorkerID: workerID,
		LeaseTTL: ttl,
		Now:      h.clock.Now,
		LogOut:   &testLog{t: h.t},
	})
}

func linearDef(stepNames ...string) *workflow.Definition {
	steps := make([]workflow.StepSpec, len(stepNames))
	for i, n := range stepNames {
		steps[i] = workflow.StepSpec{Name: n, Activity: "echo"}
	}
	return &workflow.Definition{Name: "linear-wf", Steps: steps}
}

func (h *testHarness) runUntilTerminal(eng *engine.Engine, instanceID string, maxTicks int) *workflow.Instance {
	ctx := context.Background()
	for i := 0; i < maxTicks; i++ {
		if err := eng.Tick(ctx); err != nil {
			h.t.Fatalf("tick: %v", err)
		}
		inst, err := eng.Get(ctx, instanceID)
		if err != nil {
			h.t.Fatalf("get: %v", err)
		}
		if inst.State == workflow.StateCompleted || inst.State == workflow.StateFailed || inst.State == workflow.StateCanceled {
			return inst
		}
	}
	h.t.Fatalf("instance %s not terminal after %d ticks", instanceID, maxTicks)
	return nil
}

func assertCode(t *testing.T, err error, want workflow.ErrorCode) {
	t.Helper()
	if got := workflow.GetErrorCode(err); got != want {
		t.Fatalf("error code = %q, want %q (err=%v)", got, want, err)
	}
}
