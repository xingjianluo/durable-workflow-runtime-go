package store_test

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/durable-workflow-runtime-go/internal/store"
	"github.com/xingjianluo/durable-workflow-runtime-go/internal/workflow"
)

func ev(inst string, seqHint int64, t workflow.EventType) *workflow.Event {
	return &workflow.Event{
		WorkflowID: "wf",
		InstanceID: inst,
		Type:       t,
		Timestamp:  time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC),
	}
}

func TestAppendReadAndReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	st, err := store.OpenFileStore(ctx, dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := st.Append(ctx, "a", []*workflow.Event{ev("a", 1, workflow.EventWorkflowStarted)}, 0); err != nil {
		t.Fatalf("append1: %v", err)
	}
	if err := st.Append(ctx, "b", []*workflow.Event{ev("b", 2, workflow.EventWorkflowStarted)}, 1); err != nil {
		t.Fatalf("append2: %v", err)
	}

	events, last, err := st.ReadEvents(ctx, "a")
	if err != nil || len(events) != 1 || last != 2 {
		t.Fatalf("read a: n=%d last=%d err=%v", len(events), last, err)
	}
	ids, err := st.ListInstances(ctx)
	if err != nil || len(ids) != 2 || ids[0] != "a" || ids[1] != "b" {
		t.Fatalf("list: %v err=%v", ids, err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 重新打开：事件仍在（重启恢复语义）。
	st2, err := store.OpenFileStore(ctx, dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	events, last, err = st2.ReadEvents(ctx, "a")
	if err != nil || len(events) != 1 || events[0].Seq != 1 || last != 2 {
		t.Fatalf("read after reopen: n=%d seq=%d last=%d err=%v", len(events), func() int64 {
			if len(events) > 0 {
				return events[0].Seq
			}
			return -1
		}(), last, err)
	}

	// 文件内容是逐行 JSON，可读、可解释。
	f, err := os.Open(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatalf("open log file: %v", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	lines := 0
	for sc.Scan() {
		if !strings.Contains(sc.Text(), `"seq":`) {
			t.Fatalf("line missing seq: %s", sc.Text())
		}
		lines++
	}
	if lines != 2 {
		t.Fatalf("log lines = %d, want 2", lines)
	}
}

func TestCASConflictRejected(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	st, err := store.OpenFileStore(ctx, dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	if err := st.Append(ctx, "a", []*workflow.Event{ev("a", 1, workflow.EventWorkflowStarted)}, 0); err != nil {
		t.Fatalf("append: %v", err)
	}
	// 过期的期望 seq：必须被拒绝，不得静默覆盖。
	err = st.Append(ctx, "a", []*workflow.Event{ev("a", 2, workflow.EventWorkflowCanceled)}, 0)
	if code := workflow.GetErrorCode(err); code != workflow.CodeConcurrentModification {
		t.Fatalf("conflict code = %q, want CONCURRENT_MODIFICATION", code)
	}
	// 正确 seq：成功。
	if err := st.Append(ctx, "a", []*workflow.Event{ev("a", 2, workflow.EventWorkflowCanceled)}, 1); err != nil {
		t.Fatalf("append with correct seq: %v", err)
	}
}

func TestConcurrentAppendsMonotonicSeq(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	st, err := store.OpenFileStore(ctx, dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	const goroutines = 16
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// 读最新 seq 后追加；遇到冲突重读重试（CAS 标准模式）。
			for i := 0; i < 1000; i++ {
				last, err := st.LastSeq(ctx)
				if err != nil {
					errs <- err
					return
				}
				err = st.Append(ctx, "a", []*workflow.Event{ev("a", last+1, workflow.EventStepLeased)}, last)
				if err == nil {
					return
				}
				if workflow.GetErrorCode(err) != workflow.CodeConcurrentModification {
					errs <- err
					return
				}
			}
			errs <- workflow.NewError(workflow.CodeConcurrentModification, "gave up after 1000 retries")
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatalf("concurrent append: %v", e)
	}
	last, err := st.LastSeq(ctx)
	if err != nil {
		t.Fatalf("last seq: %v", err)
	}
	if last != goroutines {
		t.Fatalf("final seq = %d, want %d", last, goroutines)
	}
	events, _, err := st.ReadEvents(ctx, "a")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	seen := map[int64]bool{}
	for _, e := range events {
		if seen[e.Seq] {
			t.Fatalf("duplicate seq %d", e.Seq)
		}
		seen[e.Seq] = true
	}
}
