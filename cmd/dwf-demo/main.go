// dwf-demo 是持久化工作流引擎的最小端到端示例：
//
//	dwf-demo -dir ./wfdata -id order-42 -input "hello"
//
// 它注册一个三步线性工作流（charge -> reserve -> ship），其中 charge 配置了
// 瞬时失败重试与尝试超时。重复运行同一命令是安全的：已完成的步骤不会重跑，
// 所有状态都从 -dir 指向的本地目录恢复。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/xingjianluo/durable-workflow-runtime-go/workflow"
)

func main() {
	dir := flag.String("dir", "./wfdata", "持久化状态目录")
	id := flag.String("id", "demo-1", "工作流实例 ID")
	input := flag.String("input", "hello", "工作流输入")
	flag.Parse()

	store, err := workflow.NewFileStore(*dir)
	if err != nil {
		fatal(err)
	}
	defer store.Close()

	eng := workflow.NewEngine(store, workflow.Options{
		WorkerID:      "demo-worker",
		LeaseDuration: 5 * time.Second,
		Logger:        workflow.NewStdLogger(),
	})

	def := workflow.WorkflowDef{
		Name: "order-fulfillment",
		Steps: []workflow.StepDef{
			{Name: "charge", Activity: "charge", Retry: workflow.RetryPolicy{
				MaxAttempts: 3, AttemptTimeout: 2 * time.Second,
				InitialBackoff: 200 * time.Millisecond, MaxBackoff: 2 * time.Second, Multiplier: 2,
			}},
			{Name: "reserve", Activity: "reserve", Retry: workflow.DefaultRetryPolicy()},
			{Name: "ship", Activity: "ship", Retry: workflow.RetryPolicy{MaxAttempts: 1}},
		},
	}
	if err := eng.RegisterDef(def); err != nil {
		fatal(err)
	}
	registerActivities(eng)

	ctx := context.Background()
	if _, err := eng.Start(ctx, *id, def.Name, *input); err != nil {
		// 重复启动（含进程重启后再次运行）是被拒绝的可区分冲突，直接走恢复路径。
		var ce *workflow.ConflictError
		if !asConflict(err, &ce) || ce.Kind != workflow.ConflictStartAlreadyExists {
			fatal(err)
		}
		fmt.Println("实例已存在，进入恢复/继续推进模式")
	}

	// 用调度循环把工作流跑到终态（退避由真实时间驱动）。
	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := eng.Tick(runCtx); err != nil {
			fatal(err)
		}
		st, err := eng.Get(runCtx, *id)
		if err != nil {
			fatal(err)
		}
		if st.Status.Terminal() {
			break
		}
		select {
		case <-runCtx.Done():
			fatal(runCtx.Err())
		case <-ticker.C:
		}
	}

	st, err := eng.Get(ctx, *id)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("最终状态: %s\n", st.Status)
	if st.Status == workflow.WFCompleted {
		fmt.Printf("结果: %s\n", st.Result)
	} else if st.Failure != nil {
		fmt.Printf("失败分类: %s 原因: %s\n", st.Failure.Class, st.Failure.Message)
	}
}

// registerActivities 演示幂等令牌与失败分类的用法。
func registerActivities(eng *workflow.Engine) {
	eng.RegisterActivity("charge", func(ctx workflow.ActivityContext, in workflow.ActivityInput) (string, error) {
		// 真实系统里应以 in.IdempotencyToken 做去重：先查是否已扣款，已扣款直接返回原结果。
		fmt.Printf("[charge] attempt=%d token=%s input=%s\n", ctx.Attempt(), in.IdempotencyToken, in.Input)
		return "charged:" + in.Input, nil
	})
	eng.RegisterActivity("reserve", func(ctx workflow.ActivityContext, in workflow.ActivityInput) (string, error) {
		fmt.Printf("[reserve] attempt=%d token=%s input=%s\n", ctx.Attempt(), in.IdempotencyToken, in.Input)
		return "reserved:" + in.Input, nil
	})
	eng.RegisterActivity("ship", func(ctx workflow.ActivityContext, in workflow.ActivityInput) (string, error) {
		fmt.Printf("[ship] attempt=%d token=%s input=%s\n", ctx.Attempt(), in.IdempotencyToken, in.Input)
		return "shipped:" + in.Input, nil
	})
}

func asConflict(err error, target **workflow.ConflictError) bool {
	type unwrapper interface{ Unwrap() error }
	for err != nil {
		if ce, ok := err.(*workflow.ConflictError); ok {
			*target = ce
			return true
		}
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "错误:", err)
	os.Exit(1)
}
