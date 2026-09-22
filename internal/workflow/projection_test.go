package workflow_test

import (
	"testing"
	"time"

	"github.com/xingjianluo/durable-workflow-runtime-go/internal/workflow"
)

func TestProjectHappyPath(t *testing.T) {
	now := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	events := []*workflow.Event{
		{Seq: 1, WorkflowID: "wf", InstanceID: "i", Type: workflow.EventWorkflowStarted,
			StepNames: []string{"s1", "s2"}, InputJSON: `"in"`, Timestamp: now},
		{Seq: 2, WorkflowID: "wf", InstanceID: "i", Type: workflow.EventStepScheduled,
			StepName: "s1", Activity: "echo", Timestamp: now},
		{Seq: 3, WorkflowID: "wf", InstanceID: "i", Type: workflow.EventStepLeased,
			StepName: "s1", Attempt: 1, LeaseHolder: "w1", LeaseExpiresAt: now.Add(time.Second), Timestamp: now},
		{Seq: 4, WorkflowID: "wf", InstanceID: "i", Type: workflow.EventStepSucceeded,
			StepName: "s1", Attempt: 1, ResultJSON: `"r1"`, Timestamp: now},
		{Seq: 5, WorkflowID: "wf", InstanceID: "i", Type: workflow.EventStepScheduled,
			StepName: "s2", Activity: "echo", Timestamp: now},
	}
	inst := workflow.Project(events)
	if inst.State != workflow.StateRunning || inst.CurrentStep != 1 || inst.InputJSON != `"in"` {
		t.Fatalf("projected: state=%s current=%d input=%s", inst.State, inst.CurrentStep, inst.InputJSON)
	}
	if inst.Steps[0].State != workflow.StepSucceeded || inst.Steps[0].ResultJSON != `"r1"` {
		t.Fatalf("s1 = %+v", inst.Steps[0])
	}
	if cur := inst.CurrentStepStatus(); cur == nil || cur.Name != "s2" {
		t.Fatalf("current step = %+v", cur)
	}

	// 确定性：同序事件重放结果一致。
	inst2 := workflow.Project(events)
	if inst2.CurrentStep != inst.CurrentStep || inst2.State != inst.State {
		t.Fatal("projection not deterministic")
	}
}

func TestProjectFailureHistory(t *testing.T) {
	now := time.Now()
	events := []*workflow.Event{
		{Seq: 1, WorkflowID: "wf", InstanceID: "i", Type: workflow.EventWorkflowStarted,
			StepNames: []string{"s1"}, Timestamp: now},
		{Seq: 2, WorkflowID: "wf", InstanceID: "i", Type: workflow.EventStepScheduled,
			StepName: "s1", Activity: "a", Timestamp: now},
		{Seq: 3, WorkflowID: "wf", InstanceID: "i", Type: workflow.EventStepLeased,
			StepName: "s1", Attempt: 1, LeaseHolder: "w", LeaseExpiresAt: now.Add(time.Second), Timestamp: now},
		{Seq: 4, WorkflowID: "wf", InstanceID: "i", Type: workflow.EventAttemptFailed,
			StepName: "s1", Attempt: 1, FailureClass: workflow.FailureTransient, ErrorCode: "E", Timestamp: now},
		{Seq: 5, WorkflowID: "wf", InstanceID: "i", Type: workflow.EventRetryScheduled,
			StepName: "s1", Attempt: 1, NextAttemptAt: now.Add(time.Second), Timestamp: now},
		{Seq: 6, WorkflowID: "wf", InstanceID: "i", Type: workflow.EventStepLeased,
			StepName: "s1", Attempt: 2, LeaseHolder: "w", LeaseExpiresAt: now.Add(2 * time.Second), Timestamp: now},
		{Seq: 7, WorkflowID: "wf", InstanceID: "i", Type: workflow.EventAttemptFailed,
			StepName: "s1", Attempt: 2, FailureClass: workflow.FailurePermanent, ErrorCode: "P", Timestamp: now},
		{Seq: 8, WorkflowID: "wf", InstanceID: "i", Type: workflow.EventStepFailed,
			StepName: "s1", Attempt: 2, FailureClass: workflow.FailurePermanent, ErrorCode: "PERMANENT_FAILURE", Timestamp: now},
		{Seq: 9, WorkflowID: "wf", InstanceID: "i", Type: workflow.EventWorkflowFailed, Timestamp: now},
	}
	inst := workflow.Project(events)
	if inst.State != workflow.StateFailed {
		t.Fatalf("state = %s", inst.State)
	}
	s1 := inst.Steps[0]
	if s1.State != workflow.StepFailed || s1.Attempt != 2 {
		t.Fatalf("s1 state=%s attempt=%d", s1.State, s1.Attempt)
	}
	// 历史 = 2 次尝试失败 + 1 条终结 STEP_FAILED 记录。
	if len(s1.History) != 3 {
		t.Fatalf("history = %d, want 3", len(s1.History))
	}
	if s1.Failure == nil || s1.Failure.Class != workflow.FailurePermanent {
		t.Fatalf("failure = %+v", s1.Failure)
	}
}
