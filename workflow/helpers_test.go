package workflow

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

// testSideEffect 模拟业务副作用存储：以活动的幂等令牌为键。
// 引擎保证同一逻辑步骤的所有尝试共享同一令牌，因此活动用“已存在则不重复发放”
// 的方式实现恰好一次语义；测试据此断言副作用只发生一次。
type testSideEffect struct {
	mu     sync.Mutex
	values map[string]int
}

func newSideEffect() *testSideEffect { return &testSideEffect{values: map[string]int{}} }

func (s *testSideEffect) apply(token string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.values[token]; ok {
		return v
	}
	v := len(s.values) + 1
	s.values[token] = v
	return v
}

func (s *testSideEffect) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.values)
}

func (s *testSideEffect) tokens() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.values))
	for k := range s.values {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

type testEnv struct {
	dir   string
	store *FileStore
	log   TestLogger
	clock *FakeClock
	se    *testSideEffect
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "wfstore")
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	tl := NewTestLogger(func(s string) { t.Log(s) })
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	env := &testEnv{dir: dir, store: store, log: tl, clock: NewFakeClock(base), se: newSideEffect()}
	t.Cleanup(func() { _ = store.Close() })
	return env
}

func (env *testEnv) newEngine(t *testing.T, workerID string, leaseTTL time.Duration) *Engine {
	t.Helper()
	if workerID == "" {
		workerID = newToken("worker")
	}
	e := NewEngine(env.store, Options{WorkerID: workerID, LeaseDuration: leaseTTL, Clock: env.clock, Logger: env.log})
	registerTestDef(e)
	return e
}

// registerTestDef 注册测试用工作流定义：
// echo（成功）、flaky（前 failUntil 次瞬时失败，由闭包计数）、
// perm（永久失败）、slow（等到 ctx 超时/取消）。
func registerTestDef(e *Engine) {
	mustDef(e, WorkflowDef{Name: "echo", Steps: []StepDef{
		{Name: "only", Activity: "echo", Retry: RetryPolicy{MaxAttempts: 1}},
	}})
}

func mustDef(e *Engine, def WorkflowDef) {
	if err := e.RegisterDef(def); err != nil {
		panic(err)
	}
}

func conflictKind(err error) ConflictKind {
	var ce *ConflictError
	if errors.As(err, &ce) {
		return ce.Kind
	}
	return ""
}

func mustAcquire(t *testing.T, e *Engine, ctx context.Context, id string) *Lease {
	t.Helper()
	_, l, err := e.AcquireLease(ctx, id)
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	return l
}

func drainToCompletion(t *testing.T, e *Engine, ctx context.Context, id string, maxTicks int) *WorkflowState {
	t.Helper()
	var st *WorkflowState
	for i := 0; i < maxTicks; i++ {
		if _, err := e.Tick(ctx); err != nil {
			t.Fatalf("Tick: %v", err)
		}
		var err error
		st, _, err = e.store.Load(ctx, id)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if st.Status.Terminal() {
			return st
		}
		// 推进假时钟以经过退避/租约。
		e.clock.(*FakeClock).Advance(time.Second)
	}
	t.Fatalf("workflow %s did not finish in %d ticks; status=%s", id, maxTicks, st.Status)
	return nil
}
