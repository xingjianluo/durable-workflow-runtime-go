package engine_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/durable-workflow-runtime-go/internal/engine"
	"github.com/xingjianluo/durable-workflow-runtime-go/internal/workflow"
)

// TestLeaseMutualExclusion 两个引擎实例竞争同一工作流：
// 未过期租约持有者互斥，他人领取被拒绝且原因可区分。
func TestLeaseMutualExclusion(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	e1 := h.newEngine("worker-A", 30*time.Second)
	e2 := h.newEngine("worker-B", 30*time.Second)

	def := linearDef("s1")
	if err := e1.Start(ctx, def, "inst-mutex", nil); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := e2.RegisterDefinition(def); err != nil {
		t.Fatalf("register on e2: %v", err)
	}

	name, ok, err := e1.AcquireStep(ctx, "inst-mutex")
	if err != nil || !ok || name != "s1" {
		t.Fatalf("e1 acquire: %v", err)
	}
	// e2 在租约未过期时领取：被拒绝。
	_, _, err = e2.AcquireStep(ctx, "inst-mutex")
	assertCode(t, err, workflow.CodeLeaseHeldByOther)
	// e2 显式接管未过期租约：被拒绝。
	err = e2.Takeover(ctx, "inst-mutex", "s1")
	assertCode(t, err, workflow.CodeLeaseNotExpired)
	// e2 执行他人持有的步骤：被拒绝。
	err = e2.ExecuteLeased(ctx, "inst-mutex", "s1")
	assertCode(t, err, workflow.CodeLeaseHeldByOther)

	inst, _ := e1.Get(ctx, "inst-mutex")
	if inst.Steps[0].LeaseHolder != "worker-A" {
		t.Fatalf("lease holder = %q", inst.Steps[0].LeaseHolder)
	}
	t.Logf("workflow=%s/%s step=s1 lease held by worker-A, worker-B rejected",
		inst.WorkflowID, inst.InstanceID)
}

// TestCrashTakeover worker 崩溃（租约过期）后另一 worker 接管，
// 步骤继续执行，无并发执行、无状态回退、无结果丢失。
func TestCrashTakeover(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	e1 := h.newEngine("worker-A", 10*time.Second)
	e2 := h.newEngine("worker-B", 10*time.Second)

	// 允许多次尝试，使租约超时后接管走重新领取路径。
	def := &workflow.Definition{Name: "linear-wf", Steps: []workflow.StepSpec{
		{Name: "s1", Activity: "echo", RetryPolicy: workflow.RetryPolicy{MaxAttempts: 3}},
		{Name: "s2", Activity: "echo", RetryPolicy: workflow.RetryPolicy{MaxAttempts: 3}},
	}}
	if err := e1.Start(ctx, def, "inst-takeover", []byte(`"go"`)); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := e2.RegisterDefinition(def); err != nil {
		t.Fatalf("register on e2: %v", err)
	}

	// worker-A 领取 s1 后“崩溃”（不再执行、不再续约）。
	if _, _, err := e1.AcquireStep(ctx, "inst-takeover"); err != nil {
		t.Fatalf("e1 acquire: %v", err)
	}
	inst, _ := e1.Get(ctx, "inst-takeover")
	if inst.Steps[0].LeaseHolder != "worker-A" {
		t.Fatalf("holder = %q", inst.Steps[0].LeaseHolder)
	}

	// 租约过期前，worker-B 不能接管。
	_, _, err := e2.AcquireStep(ctx, "inst-takeover")
	assertCode(t, err, workflow.CodeLeaseHeldByOther)

	// 时钟前进超过 TTL：worker-B 接管并执行。
	h.clock.Advance(11 * time.Second)
	name, ok, err := e2.AcquireStep(ctx, "inst-takeover")
	if err != nil || !ok || name != "s1" {
		t.Fatalf("e2 takeover acquire: name=%q ok=%v err=%v", name, ok, err)
	}
	inst, _ = e2.Get(ctx, "inst-takeover")
	s1 := inst.Steps[0]
	if s1.LeaseHolder != "worker-B" || s1.Attempt != 2 {
		t.Fatalf("after takeover: holder=%q attempt=%d", s1.LeaseHolder, s1.Attempt)
	}
	// 崩溃的 attempt 以 TIMEOUT 计入历史。
	if len(s1.History) != 1 || !s1.History[0].TimedOut {
		t.Fatalf("history = %+v", s1.History)
	}

	// 崩溃的 worker-A 醒来尝试提交：租约已不属于它，被拒绝。
	err = e1.ExecuteLeased(ctx, "inst-takeover", "s1")
	assertCode(t, err, workflow.CodeLeaseHeldByOther)

	// worker-B 完成整个工作流。
	if err := e2.ExecuteLeased(ctx, "inst-takeover", "s1"); err != nil {
		t.Fatalf("e2 execute s1: %v", err)
	}
	final := h.runUntilTerminal(e2, "inst-takeover", 10)
	if final.State != workflow.StateCompleted {
		t.Fatalf("state = %s", final.State)
	}
	// 副作用恰好每步一次（接管不重复执行已提交结果）。
	if n := h.counters.Effects(`"go"`); n != 2 {
		t.Fatalf("side effects = %d, want 2 (s1+s2)", n)
	}
	t.Logf("workflow=%s/%s takeover by worker-B complete, state=%s",
		final.WorkflowID, final.InstanceID, final.State)
}

// TestConcurrentEnginesNoDuplicateCommit 多引擎并发 Tick 同一实例：
// 步骤成功提交恰好一次，副作用不重复，冲突被显式处理。
func TestConcurrentEnginesNoDuplicateCommit(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	def := linearDef("s1", "s2", "s3")

	const workers = 4
	var es []*engine.Engine
	for i := 0; i < workers; i++ {
		e := h.newEngine(fmt.Sprintf("worker-%d", i), 5*time.Second)
		if err := e.RegisterDefinition(def); err != nil {
			t.Fatalf("register: %v", err)
		}
		es = append(es, e)
	}
	if err := es[0].Start(ctx, def, "inst-conc", []byte(`"p"`)); err != nil {
		t.Fatalf("start: %v", err)
	}

	// 多轮并发 Tick，直到终结。
	var final *workflow.Instance
	for round := 0; round < 50; round++ {
		var wg sync.WaitGroup
		for _, e := range es {
			wg.Add(1)
			go func(e *engine.Engine) {
				defer wg.Done()
				_ = e.Tick(ctx)
			}(e)
		}
		wg.Wait()
		inst, err := es[0].Get(ctx, "inst-conc")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if inst.State == workflow.StateCompleted || inst.State == workflow.StateFailed {
			final = inst
			break
		}
		h.clock.Advance(6 * time.Second) // 推进时钟以允许租约过期接管
	}
	if final == nil {
		t.Fatal("workflow did not terminate")
	}
	if final.State != workflow.StateCompleted {
		t.Fatalf("state = %s", final.State)
	}
	// 每步副作用恰好一次：并发调度没有造成重复提交。
	if n := h.counters.Effects(`"p"`); n != 3 {
		t.Fatalf("side effects = %d, want 3", n)
	}
	for _, s := range final.Steps {
		if s.State != workflow.StepSucceeded {
			t.Fatalf("step %s = %s", s.Name, s.State)
		}
	}
	t.Logf("workflow=%s/%s %d workers concurrent, state=%s, effects=%d",
		final.WorkflowID, final.InstanceID, workers, final.State, h.counters.Effects(`"p"`))
}

// TestConcurrentStartSameInstance 并发启动同一实例：只有一个成功，
// 其余收到可区分的 ALREADY_STARTED 或 CONCURRENT_MODIFICATION。
func TestConcurrentStartSameInstance(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	def := linearDef("s1")

	const n = 8
	var wg sync.WaitGroup
	codes := make([]workflow.ErrorCode, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e := h.newEngine(fmt.Sprintf("starter-%d", i), time.Second)
			err := e.Start(ctx, def, "inst-race", nil)
			codes[i] = workflow.GetErrorCode(err)
		}(i)
	}
	wg.Wait()

	succeeded := 0
	for i, c := range codes {
		switch c {
		case "":
			succeeded++
		case workflow.CodeAlreadyStarted, workflow.CodeConcurrentModification:
			// 允许的冲突结果
		default:
			t.Fatalf("goroutine %d unexpected code %q", i, c)
		}
	}
	if succeeded != 1 {
		t.Fatalf("successful starts = %d, want 1", succeeded)
	}
	inst, err := h.newEngine("checker", time.Second).Get(ctx, "inst-race")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if inst.State != workflow.StateRunning {
		t.Fatalf("state = %s", inst.State)
	}
	t.Logf("workflow=%s/%s concurrent start: 1 succeeded, %d rejected",
		inst.WorkflowID, inst.InstanceID, n-1)
}
