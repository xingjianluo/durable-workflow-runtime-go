package workflow

import "time"

// InstanceState 是工作流实例的总体状态。
type InstanceState string

const (
	StatePending   InstanceState = "PENDING"
	StateRunning   InstanceState = "RUNNING"
	StateCompleted InstanceState = "COMPLETED"
	StateFailed    InstanceState = "FAILED"
	StateCanceled  InstanceState = "CANCELED"
)

// StepState 是单个步骤（活动）的状态。
type StepState string

const (
	StepPending   StepState = "PENDING"
	StepRunning   StepState = "RUNNING"
	StepSucceeded StepState = "SUCCEEDED"
	StepFailed    StepState = "FAILED" // 永久失败或重试耗尽
	StepSkipped   StepState = "SKIPPED"
)

// FailureClass 描述活动失败的分类，决定重试策略。
type FailureClass string

const (
	FailureTransient FailureClass = "TRANSIENT" // 瞬时错误，可重试
	FailurePermanent FailureClass = "PERMANENT" // 永久错误，不得重试
	FailureTimeout   FailureClass = "TIMEOUT"   // 执行超时，按瞬时处理但可区分
)

// EventType 是持久化事件的类型。
type EventType string

const (
	EventWorkflowStarted   EventType = "WORKFLOW_STARTED"
	EventWorkflowCompleted EventType = "WORKFLOW_COMPLETED"
	EventStepScheduled     EventType = "STEP_SCHEDULED"
	EventStepLeased        EventType = "STEP_LEASED"
	EventStepSucceeded     EventType = "STEP_SUCCEEDED"
	EventStepFailed        EventType = "STEP_FAILED"
	EventAttemptFailed     EventType = "ATTEMPT_FAILED"
	EventRetryScheduled    EventType = "RETRY_SCHEDULED"
	EventStepTimedOut      EventType = "STEP_TIMED_OUT"
	EventWorkflowFailed    EventType = "WORKFLOW_FAILED"
	EventWorkflowCanceled  EventType = "WORKFLOW_CANCELED"
	EventLeaseTakenOver    EventType = "LEASE_TAKEN_OVER"
)

// Event 是持久化到事件日志中的单条不可变记录。
type Event struct {
	Seq            int64        `json:"seq"`
	WorkflowID     string       `json:"workflow_id"`
	InstanceID     string       `json:"instance_id"`
	StepName       string       `json:"step_name,omitempty"`
	Type           EventType    `json:"type"`
	Attempt        int          `json:"attempt,omitempty"`
	StepNames      []string     `json:"step_names,omitempty"`
	Activity       string       `json:"activity,omitempty"`
	InputJSON      string       `json:"input_json,omitempty"`
	LeaseHolder    string       `json:"lease_holder,omitempty"`
	LeaseExpiresAt time.Time    `json:"lease_expires_at,omitempty"`
	NextAttemptAt  time.Time    `json:"next_attempt_at,omitempty"`
	FailureClass   FailureClass `json:"failure_class,omitempty"`
	ErrorCode      string       `json:"error_code,omitempty"`
	Message        string       `json:"message,omitempty"`
	ResultJSON     string       `json:"result_json,omitempty"`
	Timestamp      time.Time    `json:"timestamp"`
}

// RetryPolicy 定义步骤的重试、超时与终止策略。
type RetryPolicy struct {
	MaxAttempts     int           // 含首次执行；<=0 表示 1
	Timeout         time.Duration // 单次执行超时；<=0 表示不超时
	InitialInterval time.Duration // 首次重试退避基数
	MaxInterval     time.Duration // 退避上限
	Multiplier      float64       // 退避乘数；<=0 表示 2
}

// StepSpec 描述工作流中的一个步骤。
type StepSpec struct {
	Name        string
	Activity    string
	RetryPolicy RetryPolicy
	Input       any
}

// Definition 是工作流的静态定义：线性步骤序列。
type Definition struct {
	Name  string
	Steps []StepSpec
}

// AttemptRecord 是一次尝试的失败/成功记录。
type AttemptRecord struct {
	Attempt    int
	Class      FailureClass
	ErrorCode  string
	Message    string
	StartedAt  time.Time
	FinishedAt time.Time
	TimedOut   bool
}

// StepStatus 是投影出的单个步骤运行时状态。
type StepStatus struct {
	Name           string
	Activity       string
	State          StepState
	Attempt        int
	ResultJSON     string
	Failure        *AttemptRecord
	History        []AttemptRecord
	LeaseHolder    string
	LeaseExpiresAt time.Time
	NextAttemptAt  time.Time
}

// Instance 是从事件日志投影出的工作流实例状态。
type Instance struct {
	WorkflowID  string
	InstanceID  string
	State       InstanceState
	InputJSON   string
	CurrentStep int // 当前待执行/执行中的步骤下标
	Steps       []*StepStatus
	ResultJSON  string
	UpdatedAt   time.Time
}
