// Command demo 演示本地持久化工作流引擎的关键语义：
//
//	go run ./cmd/demo start   # 启动并完成前两步后“崩溃”
//	go run ./cmd/demo resume  # 重启进程，从事件日志恢复并跑完全程
//
// 数据写入 ./.demo-data/events.jsonl，可直接用文本工具查看。
package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/xingjianluo/durable-workflow-runtime-go/internal/activity"
	"github.com/xingjianluo/durable-workflow-runtime-go/internal/engine"
	"github.com/xingjianluo/durable-workflow-runtime-go/internal/store"
	"github.com/xingjianluo/durable-workflow-runtime-go/internal/workflow"
)

const dataDir = "./.demo-data"
const instanceID = "demo-instance-1"

func definition(flaky *atomic.Int32) *workflow.Definition {
	return &workflow.Definition{
		Name: "demo-order-wf",
		Steps: []workflow.StepSpec{
			{
				Name:        "charge",
				Activity:    "echo",
				RetryPolicy: workflow.RetryPolicy{MaxAttempts: 1},
			},
			{
				Name:     "flaky-notify",
				Activity: "flaky-notify",
				// 瞬时错误重试 3 次，1s 起步指数退避，单次 5s 超时。
				RetryPolicy: workflow.RetryPolicy{
					MaxAttempts:     3,
					Timeout:         5 * time.Second,
					InitialInterval: time.Second,
					MaxInterval:     10 * time.Second,
					Multiplier:      2,
				},
			},
			{
				Name:        "ship",
				Activity:    "echo",
				RetryPolicy: workflow.RetryPolicy{MaxAttempts: 1},
			},
		},
	}
}

func main() {
	mode := "start"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}

	ctx := context.Background()
	st, err := store.OpenFileStore(ctx, dataDir)
	must(err)

	var flaky atomic.Int32
	reg := activity.NewRegistry()
	reg.MustRegister(activity.NewFuncActivity("echo", func(ctx context.Context, in []byte) ([]byte, error) {
		fmt.Printf(">> side effect executed with input %s\n", string(in))
		return in, nil
	}))
	reg.MustRegister(activity.NewFuncActivity("flaky-notify", func(ctx context.Context, in []byte) ([]byte, error) {
		n := flaky.Add(1)
		if n == 1 {
			return nil, activity.NewFailure(workflow.FailureTransient, "TEMP_UNAVAILABLE", "first attempt fails transiently")
		}
		return []byte(`"notified"`), nil
	}))

	eng := engine.New(st, reg, engine.Config{
		WorkerID: fmt.Sprintf("demo-worker-%d", os.Getpid()),
		LeaseTTL: 30 * time.Second,
	})

	def := definition(&flaky)
	must(eng.RegisterDefinition(def))

	switch mode {
	case "start":
		if err := eng.Start(ctx, def, instanceID, []byte(`{"order":"A-1001"}`)); err != nil {
			if workflow.GetErrorCode(err) == workflow.CodeAlreadyStarted {
				fmt.Println("instance already exists; run with: resume")
				return
			}
			must(err)
		}
		// 只推进一个 tick 后模拟崩溃（charge 完成、flaky-notify 已调度）。
		must(eng.Tick(ctx))
		fmt.Println("[demo] process crashing after first step; state is persisted")
		must(st.Close())
		os.Exit(0)

	case "resume":
		// 运行调度循环直到终结（真实进程内可直接使用 eng.Run）。
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			must(eng.Tick(ctx))
			inst, err := eng.Get(ctx, instanceID)
			must(err)
			fmt.Printf("[demo] %s: state=%s currentStep=%d\n", time.Now().Format("15:04:05"), inst.State, inst.CurrentStep)
			if inst.State == workflow.StateCompleted {
				fmt.Println("[demo] workflow completed after restart recovery")
				return
			}
			if inst.State == workflow.StateFailed {
				fmt.Println("[demo] workflow failed terminally")
				os.Exit(1)
			}
			time.Sleep(100 * time.Millisecond)
		}
		fmt.Println("[demo] did not finish in time")
		os.Exit(1)

	default:
		fmt.Println("usage: go run ./cmd/demo [start|resume]")
		os.Exit(2)
	}
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
