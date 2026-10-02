package workflow

import "time"

// WorkflowStatus 是工作流实例的生命周期状态。
type WorkflowStatus string

const (
	// WFRunning 表示至少还有一个步骤未结束（可能在等待重试退避）。
	WFRunning WorkflowStatus = "running"
	// WFCompleted 所有步骤均成功完成。
	WFCompleted WorkflowStatus = "completed"
	// WFFailed 某步骤遇到永久失败，或重试预算耗尽。
	WFFailed WorkflowStatus = "failed"
)

func (s WorkflowStatus) Terminal() bool {
	return s == WFCompleted || s == WFFailed
}

// StepStatus 是单个逻辑步骤的状态。
type StepStatus string

const (
	StepPending   StepStatus = "pending" // 尚未开始
	StepRunning   StepStatus = "running" // 租约持有者正在执行（含失败后等待退避的间歇）
	StepWaiting   StepStatus = "waiting" // 瞬时失败，等待退避后重试
	StepSucceeded StepStatus = "succeeded"
	StepFailed    StepStatus = "failed" // 永久失败 / 重试耗尽
)

// FailureClass 是活动失败的分类，决定重试依据，且可复现。
type FailureClass string

const (
	FailureNone      FailureClass = ""
	FailureTransient FailureClass = "transient" // 可重试
	FailurePermanent FailureClass = "permanent" // 不可重试
	FailureTimeout   FailureClass = "timeout"   // 按瞬时处理
)

// Failure 是一次尝试的失败记录，属于可查询的失败历史。
type Failure struct {
	Attempt     int          `json:"attempt"`
	Class       FailureClass `json:"class"`
	Message     string       `json:"message"`
	At          time.Time    `json:"at"`
	DuringLease string       `json:"lease_id,omitempty"`
	TimedOut    bool         `json:"timed_out,omitempty"`
	// Interrupted 为 true 表示该尝试并非活动返回失败，而是进程崩溃/租约被接管
	// 导致在途尝试中断；它同样计为一次已消耗的尝试。
	Interrupted bool `json:"interrupted,omitempty"`
}

// EventType 是事件日志中记录的状态变化种类。
type EventType string

const (
	EvWorkflowStarted   EventType = "workflow_started"
	EvStepScheduled     EventType = "step_scheduled"
	EvStepStarted       EventType = "step_started"
	EvStepSucceeded     EventType = "step_succeeded"
	EvStepFailed        EventType = "step_failed"
	EvStepRetryWait     EventType = "step_retry_wait"
	EvWorkflowCompleted EventType = "workflow_completed"
	EvWorkflowFailed    EventType = "workflow_failed"
	EvLeaseAcquired     EventType = "lease_acquired"
	EvLeaseRenewed      EventType = "lease_renewed"
	EvLeaseReleased     EventType = "lease_released"
	EvLeaseTakenOver    EventType = "lease_taken_over"
	// EvStepAttemptInterrupted 记录一次在途尝试因崩溃/接管被中断的记账事件。
	EvStepAttemptInterrupted EventType = "step_attempt_interrupted"
	// EvWorkflowResumed 记录一次显式续跑（追加预算、失败态回到 running）。
	EvWorkflowResumed EventType = "workflow_resumed"
)

// Event 是一次持久化的状态变化记录（追加写）。
type Event struct {
	Seq        int64          `json:"seq"`
	WorkflowID string         `json:"workflow_id"`
	StepIndex  int            `json:"step_index,omitempty"`
	Type       EventType      `json:"type"`
	At         time.Time      `json:"at"`
	Attempt    int            `json:"attempt,omitempty"`
	LeaseID    string         `json:"lease_id,omitempty"`
	Detail     map[string]any `json:"detail,omitempty"`
}

// StepState 是单个步骤的持久化状态。
type StepState struct {
	Index         int          `json:"index"`
	Name          string       `json:"name"`
	Activity      string       `json:"activity"`
	Status        StepStatus   `json:"status"`
	Attempt       int          `json:"attempt"` // 已开始执行的尝试次数（首次执行为 1）
	StartedAt     *time.Time   `json:"started_at,omitempty"`
	EndedAt       *time.Time   `json:"ended_at,omitempty"`
	NextAttemptAt *time.Time   `json:"next_attempt_at,omitempty"`
	Result        string       `json:"result,omitempty"` // 活动的字符串结果（幂等去重令牌的载体）
	Failures      []Failure    `json:"failures,omitempty"`
	FinalClass    FailureClass `json:"final_class,omitempty"`
	FinalReason   string       `json:"final_reason,omitempty"`
	// ExtraAttempts 是续跑为该步骤追加的尝试预算累计值；
	// 有效预算 = RetryPolicy.MaxAttempts + ExtraAttempts。
	ExtraAttempts int `json:"extra_attempts,omitempty"`
}

// ResumeRecord 记录一次显式续跑：何时、由哪个执行者、追加了多少尝试预算。
type ResumeRecord struct {
	Seq           int       `json:"seq"` // 第几次续跑（从 1 开始）
	At            time.Time `json:"at"`
	By            string    `json:"by"`             // 发起续跑的执行者
	ExtraAttempts int       `json:"extra_attempts"` // 本次追加的尝试预算
	StepIndex     int       `json:"step_index"`     // 续跑时卡住的步骤下标
}

// Lease 是工作流实例的执行租约。
type Lease struct {
	Holder     string    `json:"holder"`
	Token      string    `json:"token"`
	AcquiredAt time.Time `json:"acquired_at"`
	ExpiresAt  time.Time `json:"expires_at"`
	Fence      int64     `json:"fence"` // 单调递增的 fencing 令牌
}

// WorkflowState 是工作流实例的完整可恢复快照。
type WorkflowState struct {
	ID        string         `json:"id"`
	DefName   string         `json:"def_name"`
	Status    WorkflowStatus `json:"status"`
	Steps     []*StepState   `json:"steps"`
	Version   int64          `json:"version"` // 乐观并发版本，每次提交 +1
	Cursor    int            `json:"cursor"`  // 下一个待处理步骤下标
	Lease     *Lease         `json:"lease,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	Result    string         `json:"result,omitempty"` // 末步结果即工作流结果
	Failure   *Failure       `json:"failure,omitempty"`
	Input     string         `json:"input,omitempty"`
	// Resumes 是该实例被显式续跑的完整记录（追加写，永不删除）。
	Resumes []ResumeRecord `json:"resumes,omitempty"`
}

// RetryPolicy 描述步骤的重试、超时与终止策略。
type RetryPolicy struct {
	// MaxAttempts 最大尝试次数（含首次）；<=0 表示 1（不重试）。
	MaxAttempts int
	// InitialBackoff 首次重试前的退避。
	InitialBackoff time.Duration
	// MaxBackoff 退避上限。
	MaxBackoff time.Duration
	// Multiplier 退避乘数。
	Multiplier float64
	// AttemptTimeout 单次尝试超时；<=0 表示不限制。
	AttemptTimeout time.Duration
}

// DefaultRetryPolicy 是未显式指定时使用的策略：瞬时错误最多 3 次，指数退避。
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		Multiplier:     2,
	}
}

// Backoff 返回第 attempt 次尝试失败后、下一次尝试前的等待时长。
// attempt 从 1 开始（即第 1 次尝试失败后的退避）。
func (p RetryPolicy) Backoff(attempt int) time.Duration {
	mult := p.Multiplier
	if mult <= 1 {
		mult = 2
	}
	d := p.InitialBackoff
	for i := 1; i < attempt; i++ {
		d = time.Duration(float64(d) * mult)
		if p.MaxBackoff > 0 && d > p.MaxBackoff {
			d = p.MaxBackoff
		}
	}
	if p.MaxBackoff > 0 && d > p.MaxBackoff {
		d = p.MaxBackoff
	}
	if d < 0 {
		d = 0
	}
	return d
}

// MaxAttempts 返回归一化后的最大尝试次数（至少为 1）。
func (p RetryPolicy) MaxAttemptsVal() int {
	if p.MaxAttempts <= 0 {
		return 1
	}
	return p.MaxAttempts
}

// StepDef 定义一个线性步骤。
type StepDef struct {
	Name     string
	Activity string
	Retry    RetryPolicy
}

// WorkflowDef 定义一个线性工作流：步骤按顺序执行，上一步结果作为下一步输入。
type WorkflowDef struct {
	Name  string
	Steps []StepDef
}

// ActivityInput 是活动函数收到的输入。
type ActivityInput struct {
	WorkflowID string
	StepName   string
	Attempt    int
	// Input 对第一步是启动输入，之后是上一步结果。
	Input string
	// IdempotencyToken 在同一逻辑步骤的所有尝试间保持稳定，
	// 活动应用它保证“同一逻辑步骤只产生一次业务副作用”。
	IdempotencyToken string
}

// ActivityFunc 是活动实现。返回 error 为 nil 表示成功（result 为业务结果）；
// 返回的 error 可用 TransientError / PermanentError 分类，未分类错误按瞬时处理。
type ActivityFunc func(ctx ActivityContext, in ActivityInput) (result string, err error)

// TerminalStep 报告步骤状态是否为终态。
func (s StepStatus) TerminalStep() bool { return s == StepSucceeded || s == StepFailed }
