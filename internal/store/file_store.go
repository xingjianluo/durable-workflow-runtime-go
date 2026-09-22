package store

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"

	"github.com/xingjianluo/durable-workflow-runtime-go/internal/workflow"
)

const (
	logFileName  = "events.jsonl"
	lockFileName = "events.lock"
)

// FileStore 基于 JSON Lines 文件 + 目录锁的本地持久化事件存储。
// 同一数据目录可被同一进程内或不同进程的多个 FileStore 并发打开：
// 通过进程内互斥锁 + flock 跨进程排他锁，保证 Append 的读-判-写是
// 串行化的临界区，事件不丢失、全局 seq 单调。
type FileStore struct {
	dir      string
	logPath  string
	lockFile *os.File
	mu       sync.Mutex
	closed   bool
}

// openGuard 使同进程内相同目录的存储共享一把内存锁。
var (
	openGuardMu sync.Mutex
	openGuards  = map[string]*sync.Mutex{}
)

func guardFor(dir string) *sync.Mutex {
	openGuardMu.Lock()
	defer openGuardMu.Unlock()
	g, ok := openGuards[dir]
	if !ok {
		g = &sync.Mutex{}
		openGuards[dir] = g
	}
	return g
}

// OpenFileStore 打开（必要时创建）数据目录下的事件日志。
func OpenFileStore(ctx context.Context, dir string) (*FileStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("store dir must not be empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}
	lockPath := filepath.Join(dir, lockFileName)
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	// 必须持有 *os.File 本身，避免其 finalizer 关闭底层 fd
	//（fd 号可能被后续 open 复用，导致“bad file descriptor”）。
	return &FileStore{
		dir:      dir,
		logPath:  filepath.Join(dir, logFileName),
		lockFile: f,
	}, nil
}

func (s *FileStore) lock() (unlock func(), err error) {
	// 进程内串行；随后再获取跨进程排他锁。
	guardFor(s.dir).Lock()
	if err := syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_EX); err != nil {
		guardFor(s.dir).Unlock()
		return nil, fmt.Errorf("acquire flock: %w", err)
	}
	return func() {
		_ = syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN)
		guardFor(s.dir).Unlock()
	}, nil
}

// readAllLocked 在持锁状态下读取全部事件并返回全局最大 seq。
func (s *FileStore) readAllLocked() ([]*workflow.Event, int64, error) {
	f, err := os.Open(s.logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	defer f.Close()

	var events []*workflow.Event
	var lastSeq int64
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		ev := &workflow.Event{}
		if err := json.Unmarshal(line, ev); err != nil {
			return nil, 0, fmt.Errorf("corrupt event at line %d: %w", lineNo, err)
		}
		events = append(events, ev)
		if ev.Seq > lastSeq {
			lastSeq = ev.Seq
		}
	}
	if err := sc.Err(); err != nil {
		return nil, 0, err
	}
	return events, lastSeq, nil
}

// Append 以条件事务方式追加事件。
// expectLastSeq 为调用方观察到的全局最大 seq（空日志传 0）。
// 若日志尾部已被其他写者推进，返回 CodeConcurrentModification。
func (s *FileStore) Append(ctx context.Context, instanceID string, events []*workflow.Event, expectLastSeq int64) error {
	if len(events) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	unlock, err := s.lock()
	if err != nil {
		return err
	}
	defer unlock()

	_, lastSeq, err := s.readAllLocked()
	if err != nil {
		return err
	}
	if lastSeq != expectLastSeq {
		return workflow.NewError(workflow.CodeConcurrentModification,
			"expected last seq %d but store is at %d", expectLastSeq, lastSeq)
	}

	f, err := os.OpenFile(s.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open event log for append: %w", err)
	}
	bw := bufio.NewWriter(f)
	seq := lastSeq
	for _, ev := range events {
		seq++
		ev.Seq = seq
		if ev.InstanceID == "" {
			ev.InstanceID = instanceID
		}
		line, err := json.Marshal(ev)
		if err != nil {
			f.Close()
			return fmt.Errorf("marshal event: %w", err)
		}
		if _, err := bw.Write(append(line, '\n')); err != nil {
			f.Close()
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	// fsync 目录，保证新建日志文件的目录项持久。
	if df, derr := os.Open(s.dir); derr == nil {
		_ = df.Sync()
		_ = df.Close()
	}
	return nil
}

// ReadEvents 读取某实例的全部事件（按 seq 升序），并返回全局最大 seq。
func (s *FileStore) ReadEvents(ctx context.Context, instanceID string) ([]*workflow.Event, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := s.lock()
	if err != nil {
		return nil, 0, err
	}
	defer unlock()

	all, lastSeq, err := s.readAllLocked()
	if err != nil {
		return nil, 0, err
	}
	var out []*workflow.Event
	for _, ev := range all {
		if ev.InstanceID == instanceID {
			out = append(out, ev)
		}
	}
	return out, lastSeq, nil
}

// readAllNoLock 供 ListInstances / LastSeq 使用。
func (s *FileStore) snapshot() ([]*workflow.Event, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := s.lock()
	if err != nil {
		return nil, 0, err
	}
	defer unlock()
	return s.readAllLocked()
}

// ListInstances 列出所有实例 ID（去重、有序）。
func (s *FileStore) ListInstances(ctx context.Context) ([]string, error) {
	all, _, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	var out []string
	for _, ev := range all {
		if _, ok := seen[ev.InstanceID]; !ok {
			seen[ev.InstanceID] = struct{}{}
			out = append(out, ev.InstanceID)
		}
	}
	sort.Strings(out)
	return out, nil
}

// LastSeq 返回整个日志当前的最大 seq（空日志为 0）。
func (s *FileStore) LastSeq(ctx context.Context) (int64, error) {
	_, seq, err := s.snapshot()
	return seq, err
}

// Path 返回日志文件路径，便于诊断。
func (s *FileStore) Path() string { return s.logPath }

// Close 释放底层资源。
func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	_ = syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN)
	return s.lockFile.Close()
}

var _ io.Closer = (*FileStore)(nil)
