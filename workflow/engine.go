package workflow

import (
	"context"
	"fmt"
	"time"
)

const (
	defaultLeaseDuration = 30 * time.Second
	tickInterval         = 100 * time.Millisecond
)

// Options 配置引擎。
type Options struct {
	// WorkerID 当前执行者标识，用于租约归属排查；默认自动生成。
	WorkerID string
	// LeaseDuration 租约时长；过期后其他执行者可接管，默认 30s。
	LeaseDuration time.Duration
	// Clock 时间来源，默认系统时钟。
	Clock Clock
	// Logger 日志器，默认输出 stderr。
	Logger Logger
}

// Engine 是本地持久化工作流执行引擎。
type Engine struct {
	store      Store
	defs       map[string]*WorkflowDef
	activities map[string]ActivityFunc
	workerID   string
	leaseTTL   time.Duration
	clock      Clock
	log        Logger
}

// NewEngine 创建引擎。所有实例状态都在 Store 中，引擎本身无易失状态，
// 因此用同一目录重新 NewEngine 即等价于“进程重启后的恢复”。
func NewEngine(store Store, opts Options) *Engine {
	clock := opts.Clock
	if clock == nil {
		clock = SystemClock{}
	}
	logger := opts.Logger
	if logger == nil {
		logger = NewStdLogger()
	}
	workerID := opts.WorkerID
	if workerID == "" {
		workerID = newToken("worker")
	}
	leaseTTL := opts.LeaseDuration
	if leaseTTL <= 0 {
		leaseTTL = defaultLeaseDuration
	}
	return &Engine{
		store:      store,
		defs:       map[string]*WorkflowDef{},
		activities: map[string]ActivityFunc{},
		workerID:   workerID,
		leaseTTL:   leaseTTL,
		clock:      clock,
		log:        logger,
	}
}

// WorkerID 返回本引擎的执行者标识。
func (e *Engine) WorkerID() string { return e.workerID }

// RegisterDef 注册工作流定义（名字非空、步骤非空、步骤名不重复）。
// 每步未显式配置的 Retry 使用 DefaultRetryPolicy。
func (e *Engine) RegisterDef(def WorkflowDef) error {
	if def.Name == "" {
		return fmt.Errorf("workflow: definition name is empty")
	}
	if len(def.Steps) == 0 {
		return fmt.Errorf("workflow: definition %q has no steps", def.Name)
	}
	seen := map[string]bool{}
	for i := range def.Steps {
		if def.Steps[i].Name == "" {
			return fmt.Errorf("workflow: definition %q step %d has empty name", def.Name, i)
		}
		if def.Steps[i].Activity == "" {
			return fmt.Errorf("workflow: definition %q step %q has empty activity", def.Name, def.Steps[i].Name)
		}
		if seen[def.Steps[i].Name] {
			return fmt.Errorf("workflow: duplicate step name %q in definition %q", def.Steps[i].Name, def.Name)
		}
		seen[def.Steps[i].Name] = true
		if def.Steps[i].Retry.MaxAttempts == 0 {
			def.Steps[i].Retry = DefaultRetryPolicy()
		}
	}
	d := def
	e.defs[d.Name] = &d
	return nil
}

// RegisterActivity 注册活动实现。
func (e *Engine) RegisterActivity(name string, fn ActivityFunc) {
	if fn == nil {
		delete(e.activities, name)
		return
	}
	e.activities[name] = fn
}

// Start 启动工作流实例：持久化初始快照与 workflow_started/step_scheduled 事件。
// 相同 id 重复启动被拒绝（ConflictStartAlreadyExists），已有实例不受影响。
func (e *Engine) Start(ctx context.Context, id, defName, input string) (*WorkflowState, error) {
	def, ok := e.defs[defName]
	if !ok {
		return nil, fmt.Errorf("workflow: unknown definition %q", defName)
	}
	if id == "" {
		return nil, fmt.Errorf("workflow: empty workflow id")
	}
	now := e.clock.Now()
	st := &WorkflowState{
		ID:        id,
		DefName:   defName,
		Status:    WFRunning,
		Steps:     make([]*StepState, 0, len(def.Steps)),
		Version:   0,
		Cursor:    0,
		CreatedAt: now,
		UpdatedAt: now,
		Input:     input,
	}
	for i, sd := range def.Steps {
		st.Steps = append(st.Steps, &StepState{
			Index: i, Name: sd.Name, Activity: sd.Activity, Status: StepPending,
		})
	}
	events := []Event{{
		WorkflowID: id, Type: EvWorkflowStarted, At: now,
		Detail: map[string]any{"definition": defName, "input": input, "steps": len(def.Steps)},
	}, {
		WorkflowID: id, StepIndex: 0, Type: EvStepScheduled, At: now,
		Detail: map[string]any{"step": def.Steps[0].Name, "activity": def.Steps[0].Activity},
	}}
	created, err := e.store.Create(ctx, st, events)
	if err != nil {
		return nil, err
	}
	e.log.Logf("info", id, -1, "workflow started", "status", WFRunning, "steps", len(def.Steps))
	e.log.Logf("info", id, 0, "step scheduled", "status", StepPending)
	return created, nil
}

// Get 查询最新快照。
func (e *Engine) Get(ctx context.Context, id string) (*WorkflowState, error) {
	st, _, err := e.store.Load(ctx, id)
	if err != nil {
		return nil, err
	}
	return st, nil
}

// History 返回追加事件历史：每次状态变化都在此可查。
func (e *Engine) History(ctx context.Context, id string) ([]Event, error) {
	_, events, err := e.store.Load(ctx, id)
	if err != nil {
		return nil, err
	}
	return events, nil
}

// List 列出全部实例快照（按 ID 排序）。
func (e *Engine) List(ctx context.Context) ([]*WorkflowState, error) {
	return e.store.List(ctx)
}

// Result 返回终态工作流结果。未到终态时返回最新快照与 ConflictError(ConflictNotTerminal)。
func (e *Engine) Result(ctx context.Context, id string) (string, *WorkflowState, error) {
	st, _, err := e.store.Load(ctx, id)
	if err != nil {
		return "", nil, err
	}
	if !st.Status.Terminal() {
		return "", st, conflictMsg(ConflictNotTerminal, "workflow not terminal: "+st.ID)
	}
	return st.Result, st, nil
}

// Failures 返回某步骤的失败历史；stepIndex<0 时返回工作流终态失败（0 或 1 条）。
func (e *Engine) Failures(ctx context.Context, id string, stepIndex int) ([]Failure, error) {
	st, _, err := e.store.Load(ctx, id)
	if err != nil {
		return nil, err
	}
	if stepIndex < 0 {
		if st.Failure != nil {
			return []Failure{*st.Failure}, nil
		}
		return nil, nil
	}
	if stepIndex >= len(st.Steps) {
		return nil, fmt.Errorf("workflow: step index %d out of range", stepIndex)
	}
	return append([]Failure(nil), st.Steps[stepIndex].Failures...), nil
}

// leaseActive 判断租约在 now 时刻是否仍有效。
func leaseActive(l *Lease, now time.Time) bool {
	return l != nil && l.ExpiresAt.After(now)
}

// AcquireLease 获取执行租约；若自己已持有则幂等返回现有租约。
// 他人持有有效租约时返回 ConflictError(ConflictLeaseHeldByOther)；
// 接管过期租约会产生 lease_taken_over 事件并递增 fencing 令牌。
func (e *Engine) AcquireLease(ctx context.Context, id string) (*WorkflowState, *Lease, error) {
	st, _, err := e.store.Load(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	if st.Status.Terminal() {
		return st, nil, conflict(ConflictTerminal, ErrWorkflowCompleted)
	}
	now := e.clock.Now()
	if leaseActive(st.Lease, now) {
		if st.Lease.Holder == e.workerID {
			// 同一执行者重复领取：幂等返回原租约，不改变版本/fencing。
			return st, cloneLease(st.Lease), nil
		}
		return st, nil, conflict(ConflictLeaseHeldByOther, ErrLeaseOwnedByOther)
	}

	takeover := st.Lease != nil
	prevFence := int64(0)
	prevHolder := ""
	if takeover {
		prevFence = st.Lease.Fence
		prevHolder = st.Lease.Holder
	}
	lease := &Lease{
		Holder:     e.workerID,
		Token:      newToken("lease"),
		AcquiredAt: now,
		ExpiresAt:  now.Add(e.leaseTTL),
		Fence:      prevFence + 1,
	}
	st.Lease = lease
	st.UpdatedAt = now
	evType := EvLeaseAcquired
	detail := map[string]any{"holder": e.workerID, "fence": lease.Fence, "ttl": e.leaseTTL.String()}
	if takeover {
		evType = EvLeaseTakenOver
		detail["previous_holder"] = prevHolder
	}
	ev := Event{WorkflowID: id, Type: evType, At: now, LeaseID: lease.Token, Detail: detail}
	committed, err := e.store.Commit(ctx, id, st.Version, st, []Event{ev})
	if err != nil {
		return nil, nil, err
	}
	e.log.Logf("info", id, -1, evName(evType), "holder", e.workerID, "fence", lease.Fence,
		"expires_at", lease.ExpiresAt.Format(time.RFC3339Nano))
	return committed, cloneLease(lease), nil
}

func evName(t EventType) string { return string(t) }

func cloneLease(l *Lease) *Lease {
	if l == nil {
		return nil
	}
	cp := *l
	return &cp
}

// checkLease 校验调用方仍持有匹配 holder/token/fence 且未过期的租约。
func checkLease(st *WorkflowState, holder, token string, fence int64, now time.Time) error {
	l := st.Lease
	if l == nil {
		return conflict(ConflictLeaseNotHeld, ErrLeaseNotHeld)
	}
	if l.Holder != holder {
		return conflict(ConflictLeaseHeldByOther, ErrLeaseOwnedByOther)
	}
	if !l.ExpiresAt.After(now) {
		return conflict(ConflictLeaseExpired, ErrLeaseNotHeld)
	}
	if l.Token != token || l.Fence != fence {
		return conflict(ConflictLeaseVersionStale, ErrLeaseNotHeld)
	}
	return nil
}

// RenewLease 续租。租约不属于调用方、已过期或已被接管（fence 变化）都明确拒绝。
func (e *Engine) RenewLease(ctx context.Context, id string, token string, fence int64) (*Lease, error) {
	st, _, err := e.store.Load(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := checkLease(st, e.workerID, token, fence, e.clock.Now()); err != nil {
		return nil, err
	}
	now := e.clock.Now()
	st.Lease.ExpiresAt = now.Add(e.leaseTTL)
	st.UpdatedAt = now
	ev := Event{WorkflowID: id, Type: EvLeaseRenewed, At: now, LeaseID: token,
		Detail: map[string]any{"holder": e.workerID, "fence": fence, "expires_at": st.Lease.ExpiresAt.Format(time.RFC3339Nano)}}
	committed, err := e.store.Commit(ctx, id, st.Version, st, []Event{ev})
	if err != nil {
		return nil, err
	}
	e.log.Logf("info", id, -1, "lease renewed", "holder", e.workerID, "fence", fence)
	return cloneLease(committed.Lease), nil
}

// ReleaseLease 主动释放租约；租约不存在视为已释放（幂等），
// 归属他人或 fencing 不匹配则拒绝，绝不静默覆盖。
func (e *Engine) ReleaseLease(ctx context.Context, id string, token string, fence int64) error {
	st, _, err := e.store.Load(ctx, id)
	if err != nil {
		return err
	}
	if st.Lease == nil {
		return nil
	}
	if st.Lease.Holder != e.workerID {
		return conflict(ConflictLeaseHeldByOther, ErrLeaseOwnedByOther)
	}
	if st.Lease.Token != token || st.Lease.Fence != fence {
		return conflict(ConflictLeaseVersionStale, ErrLeaseNotHeld)
	}
	now := e.clock.Now()
	st.Lease = nil
	st.UpdatedAt = now
	ev := Event{WorkflowID: id, Type: EvLeaseReleased, At: now, LeaseID: token,
		Detail: map[string]any{"holder": e.workerID, "fence": fence}}
	if _, err := e.store.Commit(ctx, id, st.Version, st, []Event{ev}); err != nil {
		return err
	}
	e.log.Logf("info", id, -1, "lease released", "holder", e.workerID, "fence", fence)
	return nil
}

// Terminate 以永久失败终止实例。终态实例返回其快照且不重复处理；
// 存在他人活跃租约时拒绝（应先等接管/过期，避免与正在执行的步骤互相覆盖）。
func (e *Engine) Terminate(ctx context.Context, id, reason string) (*WorkflowState, error) {
	st, _, err := e.store.Load(ctx, id)
	if err != nil {
		return nil, err
	}
	if st.Status.Terminal() {
		return st, nil
	}
	now := e.clock.Now()
	if leaseActive(st.Lease, now) && st.Lease.Holder != e.workerID {
		return nil, conflict(ConflictLeaseHeldByOther, ErrLeaseOwnedByOther)
	}
	step := st.Steps[st.Cursor]
	f := Failure{Attempt: step.Attempt, Class: FailurePermanent,
		Message: "terminated: " + reason, At: now, DuringLease: leaseID(st.Lease)}
	step.Status = StepFailed
	step.FinalClass = FailurePermanent
	step.FinalReason = f.Message
	step.EndedAt = &now
	step.Failures = append(step.Failures, f)
	st.Status = WFFailed
	st.Failure = &f
	st.Lease = nil
	st.UpdatedAt = now
	events := []Event{
		{WorkflowID: id, StepIndex: st.Cursor, Type: EvStepFailed, At: now, Attempt: step.Attempt,
			Detail: map[string]any{"step": step.Name, "class": FailurePermanent, "reason": f.Message}},
		{WorkflowID: id, Type: EvWorkflowFailed, At: now,
			Detail: map[string]any{"reason": f.Message, "class": FailurePermanent}},
	}
	committed, err := e.store.Commit(ctx, id, st.Version, st, events)
	if err != nil {
		return nil, err
	}
	e.log.Logf("error", id, st.Cursor, "step failed", "status", StepFailed, "class", FailurePermanent, "reason", f.Message)
	e.log.Logf("error", id, -1, "workflow failed", "status", WFFailed, "class", FailurePermanent)
	return committed, nil
}

func leaseID(l *Lease) string {
	if l == nil {
		return ""
	}
	return l.Token
}

// Tick 扫描全部非终态实例：退避已到期且能获得租约的，各推进至多一步。
// 重复调度安全：已成功的步骤不会重跑（游标已前移），等待退避的步骤跳过，
// 他人活跃租约的实例跳过。返回本轮实际推进过的实例 ID。
func (e *Engine) Tick(ctx context.Context) ([]string, error) {
	list, err := e.store.List(ctx)
	if err != nil {
		return nil, err
	}
	now := e.clock.Now()
	var advanced []string
	for _, st := range list {
		if ctx.Err() != nil {
			return advanced, ctx.Err()
		}
		if st.Status.Terminal() {
			continue
		}
		step := st.Steps[st.Cursor]
		if step.Status == StepWaiting && step.NextAttemptAt != nil && step.NextAttemptAt.After(now) {
			continue
		}
		// 已被他人持有的活跃租约不抢占；自己持有则直接推进（幂等的重复调度）。
		if leaseActive(st.Lease, now) && st.Lease.Holder != e.workerID {
			continue
		}
		lease, err := e.acquireForTick(ctx, st.ID)
		if err != nil {
			if isBenignTickConflict(err) {
				continue
			}
			return advanced, err
		}
		if _, err := e.Advance(ctx, st.ID, lease.Token, lease.Fence); err != nil {
			if isBenignTickConflict(err) {
				// 并发推进者已赢下本轮：跳过，下一轮重读即可。
				continue
			}
			return advanced, err
		}
		advanced = append(advanced, st.ID)
	}
	return advanced, nil
}

func asConflict(err error, target **ConflictError) bool {
	for err != nil {
		if ce, ok := err.(*ConflictError); ok {
			*target = ce
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// acquireForTick 在调度循环中获取租约：与其他执行者并发竞争时，
// 领取提交可能撞上版本冲突，此时重读一次；若已被他人抢先拿到租约则返回冲突由调用方跳过。
func (e *Engine) acquireForTick(ctx context.Context, id string) (*Lease, error) {
	_, lease, err := e.AcquireLease(ctx, id)
	if err == nil {
		return lease, nil
	}
	if !isKind(err, ConflictVersion) {
		return nil, err
	}
	st, _, lerr := e.store.Load(ctx, id)
	if lerr != nil {
		return nil, lerr
	}
	now := e.clock.Now()
	if leaseActive(st.Lease, now) {
		return nil, conflict(ConflictLeaseHeldByOther, ErrLeaseOwnedByOther)
	}
	return nil, conflict(ConflictVersion, ErrVersionConflict)
}

// isBenignTickConflict 判断调度循环中哪些冲突只代表“本轮没抢到”，可安全跳过。
func isBenignTickConflict(err error) bool {
	var ce *ConflictError
	if !asConflict(err, &ce) {
		return false
	}
	switch ce.Kind {
	case ConflictVersion, ConflictLeaseVersionStale, ConflictLeaseHeldByOther,
		ConflictLeaseExpired, ConflictLeaseNotHeld, ConflictNoRunnable, ConflictTerminal:
		return true
	}
	return false
}

func isKind(err error, kind ConflictKind) bool {
	var ce *ConflictError
	if asConflict(err, &ce) {
		return ce.Kind == kind
	}
	return false
}

// Run 按 interval（<=0 用 100ms）循环 Tick，直到 ctx 取消；计时器来自 Clock，
// 因此用 FakeClock 时可确定性驱动。
func (e *Engine) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = tickInterval
	}
	timer := e.clock.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C():
			if _, err := e.Tick(ctx); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				e.log.Logf("error", "", -1, "tick failed", "err", err.Error())
			}
			timer.Reset(interval)
		}
	}
}
