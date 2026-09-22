package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/xingjianluo/durable-workflow-runtime-go/internal/workflow"
)

// TestRestartRecovery 模拟进程重启：引擎崩溃后重新打开存储，
// 状态从事件日志重放恢复，工作流继续推进且副作用不重复。
func TestRestartRecovery(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	eng := h.newEngine("worker-1", 30*time.Second)

	def := linearDef("s1", "s2", "s3")
	if err := eng.Start(ctx, def, "inst-restart", []byte(`"in"`)); err != nil {
		t.Fatalf("start: %v", err)
	}

	// 完成第一步后“崩溃”（不执行 Tick 的剩余部分）。
	if _, _, err := eng.AcquireStep(ctx, "inst-restart"); err != nil {
		t.Fatalf("acquire s1: %v", err)
	}
	if err := eng.ExecuteLeased(ctx, "inst-restart", "s1"); err != nil {
		t.Fatalf("execute s1: %v", err)
	}
	inst, err := eng.Get(ctx, "inst-restart")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if inst.CurrentStep != 1 || inst.Steps[0].State != workflow.StepSucceeded {
		t.Fatalf("after s1: currentStep=%d s1=%s", inst.CurrentStep, inst.Steps[0].State)
	}
	t.Logf("workflow=%s/%s before crash: state=%s currentStep=%d", inst.WorkflowID, inst.InstanceID, inst.State, inst.CurrentStep)

	// 进程重启：重新打开存储、新建引擎（同一 worker 名，模拟同一进程重启）。
	// 定义是代码的一部分，重启后必须重新注册。
	eng2 := h.reopenEngine("worker-1", 30*time.Second)
	if err := eng2.RegisterDefinition(def); err != nil {
		t.Fatalf("re-register definition: %v", err)
	}
	inst2, err := eng2.Get(ctx, "inst-restart")
	if err != nil {
		t.Fatalf("get after restart: %v", err)
	}
	if inst2.CurrentStep != 1 || inst2.State != workflow.StateRunning {
		t.Fatalf("recovered state: currentStep=%d state=%s", inst2.CurrentStep, inst2.State)
	}

	// 重启后继续推进到完成。
	final := h.runUntilTerminal(eng2, "inst-restart", 10)
	if final.State != workflow.StateCompleted {
		t.Fatalf("final state = %s", final.State)
	}
	for _, s := range final.Steps {
		if s.State != workflow.StepSucceeded {
			t.Fatalf("step %s state = %s", s.Name, s.State)
		}
	}
	// 三个步骤输入相同（线性数据流），副作用总次数必须恰好等于步骤数，
	// 即重启恢复没有导致任何步骤被重复执行。
	if n := h.counters.Effects(`"in"`); n != len(final.Steps) {
		t.Fatalf("total side effects = %d, want %d (one per step)", n, len(final.Steps))
	}
	t.Logf("workflow=%s/%s recovered and completed: state=%s", final.WorkflowID, final.InstanceID, final.State)
}

// TestDuplicateScheduling 重复调度不得重复产生副作用：
// 重复 Start、重复 Acquire、重复 Execute 均被拒绝或幂等。
func TestDuplicateScheduling(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	eng := h.newEngine("worker-1", 30*time.Second)
	def := linearDef("s1")

	if err := eng.Start(ctx, def, "inst-dup", []byte(`"x"`)); err != nil {
		t.Fatalf("start: %v", err)
	}
	// 重复启动被拒绝，原因可区分。
	err := eng.Start(ctx, def, "inst-dup", []byte(`"x"`))
	assertCode(t, err, workflow.CodeAlreadyStarted)

	// 首次领取成功。
	name, ok, err := eng.AcquireStep(ctx, "inst-dup")
	if err != nil || !ok || name != "s1" {
		t.Fatalf("acquire: name=%q ok=%v err=%v", name, ok, err)
	}
	// 同一 worker 重复领取：幂等拒绝。
	_, _, err = eng.AcquireStep(ctx, "inst-dup")
	assertCode(t, err, workflow.CodeAlreadyLeased)

	// 执行成功。
	if err := eng.ExecuteLeased(ctx, "inst-dup", "s1"); err != nil {
		t.Fatalf("execute: %v", err)
	}
	// 重复执行已成功的步骤：被拒绝。
	err = eng.ExecuteLeased(ctx, "inst-dup", "s1")
	assertCode(t, err, workflow.CodeAlreadySucceeded)

	// 工作流已终结，再领取被拒绝。
	_, _, err = eng.AcquireStep(ctx, "inst-dup")
	assertCode(t, err, workflow.CodeWorkflowFinished)

	inst, err := eng.Get(ctx, "inst-dup")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if inst.State != workflow.StateCompleted {
		t.Fatalf("state = %s", inst.State)
	}
	if n := h.counters.Effects(inst.Steps[0].ResultJSON); n != 1 {
		t.Fatalf("side effect count = %d, want 1", n)
	}
	t.Logf("workflow=%s/%s duplicate scheduling rejected, state=%s", inst.WorkflowID, inst.InstanceID, inst.State)
}

// TestCancelTerminalConflict 取消与终结态的非法迁移被拒绝且原因可区分。
func TestCancelTerminalConflict(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	eng := h.newEngine("worker-1", 30*time.Second)
	def := linearDef("s1")

	if err := eng.Start(ctx, def, "inst-cancel", nil); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := eng.Cancel(ctx, "inst-cancel"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	// 重复取消：非法状态迁移。
	err := eng.Cancel(ctx, "inst-cancel")
	assertCode(t, err, workflow.CodeInvalidState)
	// 已取消实例不能推进。
	_, _, err = eng.AcquireStep(ctx, "inst-cancel")
	assertCode(t, err, workflow.CodeWorkflowFinished)
	// 不存在的实例。
	err = eng.Cancel(ctx, "no-such")
	assertCode(t, err, workflow.CodeNotFound)

	inst, err := eng.Get(ctx, "inst-cancel")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if inst.State != workflow.StateCanceled {
		t.Fatalf("state = %s", inst.State)
	}
	t.Logf("workflow=%s/%s canceled; illegal transitions rejected", inst.WorkflowID, inst.InstanceID)
}
