package activity

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/xingjianluo/durable-workflow-runtime-go/internal/workflow"
)

// Activity 是可被工作流调度的业务活动。
// 契约：
//   - 对相同输入应给出可复现的分类结论（成功 / TRANSIENT / PERMANENT）；
//   - 活动内部的外部副作用应尽量幂等。引擎保证同一逻辑步骤的成功结果
//     只提交一次（STEP_SUCCEEDED 是去重点），活动不会被重复并发执行。
type Activity interface {
	Name() string
	// Execute 执行业务逻辑。返回 *Failure 以区分瞬时/永久失败；
	// 返回普通 error 时引擎按瞬时失败处理。
	Execute(ctx context.Context, input []byte) ([]byte, error)
}

// Failure 携带失败分类，供引擎决定是否重试。
type Failure struct {
	Class   workflow.FailureClass
	Code    string
	Message string
}

func (f *Failure) Error() string {
	return fmt.Sprintf("activity failure class=%s code=%s: %s", f.Class, f.Code, f.Message)
}

// NewFailure 构造一个分类失败。
func NewFailure(class workflow.FailureClass, code, message string) *Failure {
	return &Failure{Class: class, Code: code, Message: message}
}

// Classify 把任意 error 归类。
func Classify(err error) workflow.FailureClass {
	if err == nil {
		return ""
	}
	if f, ok := err.(*Failure); ok {
		return f.Class
	}
	return workflow.FailureTransient
}

// CodeOf 提取错误码；非 *Failure 返回 "UNCLASSIFIED"。
func CodeOf(err error) string {
	if f, ok := err.(*Failure); ok {
		return f.Code
	}
	return "UNCLASSIFIED"
}

// Registry 按名称注册与查找活动，并发安全。
type Registry struct {
	mu sync.RWMutex
	m  map[string]Activity
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{m: map[string]Activity{}}
}

// Register 注册活动；重名返回错误。
func (r *Registry) Register(a Activity) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.m[a.Name()]; exists {
		return fmt.Errorf("activity %q already registered", a.Name())
	}
	r.m[a.Name()] = a
	return nil
}

// MustRegister 注册活动，重名 panic。
func (r *Registry) MustRegister(a Activity) {
	if err := r.Register(a); err != nil {
		panic(err)
	}
}

// Lookup 按名称查找活动。
func (r *Registry) Lookup(name string) (Activity, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.m[name]
	return a, ok
}

// Names 返回已注册活动名（有序，便于日志）。
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.m))
	for n := range r.m {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// RunFunc 是把普通函数包装成 Activity 的函数签名。
type RunFunc func(ctx context.Context, input []byte) ([]byte, error)

// FuncActivity 将函数适配为 Activity。
type FuncActivity struct {
	name string
	fn   RunFunc
}

// NewFuncActivity 创建函数式活动。
func NewFuncActivity(name string, fn RunFunc) *FuncActivity {
	return &FuncActivity{name: name, fn: fn}
}

func (a *FuncActivity) Name() string { return a.name }

func (a *FuncActivity) Execute(ctx context.Context, input []byte) ([]byte, error) {
	return a.fn(ctx, input)
}
