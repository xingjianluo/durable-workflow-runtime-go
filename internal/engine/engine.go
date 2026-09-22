package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/xingjianluo/durable-workflow-runtime-go/internal/activity"
	"github.com/xingjianluo/durable-workflow-runtime-go/internal/store"
	"github.com/xingjianluo/durable-workflow-runtime-go/internal/workflow"
)

// defaultCASRetries 是乐观并发冲突时的重读重试次数。
const defaultCASRetries = 5

// Engine 是本地持久化工作流执行引擎。
// 多个 Engine（同进程或跨进程）可指向同一数据目录，通过租约实现崩溃接管。
type Engine struct {
	id       string
	store    store.Store
	registry *activity.Registry
	leaseTTL time.Duration
	now      func() time.Time
	logger   *log.Logger

	defsMu sync.RWMutex
	defs   map[string]*workflow.Definition
}

// Config 是引擎配置。
type Config struct {
	WorkerID string
	LeaseTTL time.Duration
	Now      func() time.Time
	LogOut   io.Writer
}

// New 创建引擎。
func New(st store.Store, registry *activity.Registry, cfg Config) *Engine {
	workerID := cfg.WorkerID
	if workerID == "" {
		workerID = fmt.Sprintf("worker-%d", os.Getpid())
	}
	leaseTTL := cfg.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = 30 * time.Second
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	out := cfg.LogOut
	if out == nil {
		out = os.Stderr
	}
	return &Engine{
		id:       workerID,
		store:    st,
		registry: registry,
		leaseTTL: leaseTTL,
		now:      now,
		logger:   log.New(out, "[durable-wf] ", log.LstdFlags|log.Lmicroseconds),
		defs:     map[string]*workflow.Definition{},
	}
}

// ID 返回引擎（worker）标识，即租约持有者标识。
func (e *Engine) ID() string { return e.id }

// RegisterDefinition 注册工作流定义。定义必须在进程重启后以相同内容
// 重新注册（代码即定义），名称相同但内容冲突会被拒绝。
func (e *Engine) RegisterDefinition(def *workflow.Definition) error {
	if def == nil || def.Name == "" {
		return workflow.NewError(workflow.CodeInvalidState, "definition name must not be empty")
	}
	if len(def.Steps) == 0 {
		return workflow.NewError(workflow.CodeInvalidState, "workflow %q has no steps", def.Name)
	}
	seen := map[string]struct{}{}
	for _, st := range def.Steps {
		if st.Name == "" {
			return workflow.NewError(workflow.CodeInvalidState, "workflow %q has unnamed step", def.Name)
		}
		if _, dup := seen[st.Name]; dup {
			return workflow.NewError(workflow.CodeInvalidState, "duplicate step name %q in %q", st.Name, def.Name)
		}
		seen[st.Name] = struct{}{}
		if _, ok := e.registry.Lookup(st.Activity); !ok {
			return workflow.NewError(workflow.CodeDefinitionNotFound,
				"activity %q used by step %q is not registered", st.Activity, st.Name)
		}
	}
	e.defsMu.Lock()
	defer e.defsMu.Unlock()
	if existing, ok := e.defs[def.Name]; ok {
		if !sameShape(existing, def) {
			return workflow.NewError(workflow.CodeInvalidState,
				"workflow definition %q re-registered with different shape", def.Name)
		}
	}
	e.defs[def.Name] = def
	return nil
}

func sameShape(a, b *workflow.Definition) bool {
	if len(a.Steps) != len(b.Steps) {
		return false
	}
	for i := range a.Steps {
		if a.Steps[i].Name != b.Steps[i].Name || a.Steps[i].Activity != b.Steps[i].Activity {
			return false
		}
	}
	return true
}

func (e *Engine) definition(name string) (*workflow.Definition, error) {
	e.defsMu.RLock()
	defer e.defsMu.RUnlock()
	def, ok := e.defs[name]
	if !ok {
		return nil, workflow.NewError(workflow.CodeDefinitionNotFound,
			"workflow definition %q is not registered on this engine", name)
	}
	return def, nil
}

// Start 启动一个工作流实例：持久化 WORKFLOW_STARTED 与首个 STEP_SCHEDULED。
// 重复启动同一 instanceID 会被拒绝（CodeAlreadyStarted）。
func (e *Engine) Start(ctx context.Context, def *workflow.Definition, instanceID string, input []byte) error {
	if err := e.RegisterDefinition(def); err != nil {
		return err
	}
	if instanceID == "" {
		return workflow.NewError(workflow.CodeInvalidState, "instanceID must not be empty")
	}

	return e.casLoop(ctx, instanceID, func(lastSeq int64) (bool, []*workflow.Event, int64, error) {
		events, seq, err := e.store.ReadEvents(ctx, instanceID)
		if err != nil {
			return false, nil, 0, err
		}
		if len(events) > 0 {
			return false, nil, 0, workflow.NewError(workflow.CodeAlreadyStarted,
				"instance %q already exists", instanceID)
		}
		names := make([]string, len(def.Steps))
		for i, st := range def.Steps {
			names[i] = st.Name
		}
		now := e.now()
		started := &workflow.Event{
			WorkflowID: def.Name,
			InstanceID: instanceID,
			Type:       workflow.EventWorkflowStarted,
			StepNames:  names,
			InputJSON:  string(input),
			Timestamp:  now,
		}
		first := def.Steps[0]
		scheduled := &workflow.Event{
			WorkflowID:    def.Name,
			InstanceID:    instanceID,
			Type:          workflow.EventStepScheduled,
			StepName:      first.Name,
			Activity:      first.Activity,
			NextAttemptAt: now,
			Timestamp:     now,
		}
		return false, []*workflow.Event{started, scheduled}, seq, nil
	})
}

// Get 查询实例当前投影状态（每次都从持久化事件重放）。
func (e *Engine) Get(ctx context.Context, instanceID string) (*workflow.Instance, error) {
	events, _, err := e.store.ReadEvents(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, workflow.NewError(workflow.CodeNotFound, "instance %q not found", instanceID)
	}
	return workflow.Project(events), nil
}

// ListInstances 列出全部实例 ID。
func (e *Engine) ListInstances(ctx context.Context) ([]string, error) {
	return e.store.ListInstances(ctx)
}

// Cancel 终止一个未终结的工作流；已终结实例返回 CodeInvalidState。
func (e *Engine) Cancel(ctx context.Context, instanceID string) error {
	return e.casLoop(ctx, instanceID, func(lastSeq int64) (bool, []*workflow.Event, int64, error) {
		events, seq, err := e.store.ReadEvents(ctx, instanceID)
		if err != nil {
			return false, nil, 0, err
		}
		if len(events) == 0 {
			return false, nil, 0, workflow.NewError(workflow.CodeNotFound, "instance %q not found", instanceID)
		}
		inst := workflow.Project(events)
		if inst.State == workflow.StateCanceled || inst.State == workflow.StateCompleted || inst.State == workflow.StateFailed {
			return false, nil, 0, workflow.NewError(workflow.CodeInvalidState,
				"instance %q is already %s", instanceID, inst.State)
		}
		ev := &workflow.Event{
			WorkflowID: inst.WorkflowID,
			InstanceID: instanceID,
			Type:       workflow.EventWorkflowCanceled,
			Timestamp:  e.now(),
			Message:    "canceled by request",
		}
		e.logf(inst, "", workflow.EventWorkflowCanceled, 0, "", "cancel")
		return false, []*workflow.Event{ev}, seq, nil
	})
}

// marshalInput 计算步骤输入：显式输入优先；否则首步用工作流输入，
// 后续步骤用上一步结果（简单线性数据流）。
func stepInput(def *workflow.Definition, idx int, workflowInput string, inst *workflow.Instance) []byte {
	spec := def.Steps[idx]
	if spec.Input != nil {
		b, err := json.Marshal(spec.Input)
		if err != nil {
			return []byte(fmt.Sprintf("%v", spec.Input))
		}
		return b
	}
	if idx == 0 {
		return []byte(workflowInput)
	}
	if prev := inst.Steps[idx-1]; prev != nil {
		return []byte(prev.ResultJSON)
	}
	return nil
}

// logf 打印统一格式的状态变化日志（工作流/步骤/状态/失败分类）。
func (e *Engine) logf(inst *workflow.Instance, step string, evType workflow.EventType, attempt int, class workflow.FailureClass, detail string) {
	wf := ""
	if inst != nil {
		wf = inst.WorkflowID + "/" + inst.InstanceID
	}
	e.logger.Printf("workflow=%s step=%s event=%s attempt=%d failure_class=%s %s",
		wf, step, evType, attempt, class, detail)
}
