package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// FileStore 把每个工作流持久化为单个原子文件 <dir>/<id>.json，
// 文件内容同时包含最新快照 state 与追加事件 events。
// 写入方式：同目录临时文件 fsync 后 rename，保证“状态与历史要么一起可见、要么都不可见”，
// 进程在任意时刻崩溃都不会产生撕裂文件。
// 并发控制采用两层：
//   - 进程内 sync.Mutex：串行化同一 FileStore 实例的调用；
//   - 每次写操作短暂持有目录 flock（LOCK_EX）：跨进程/跨 FileStore 实例串行化
//     “读取当前版本 → 校验版本 → 原子替换”这一临界区，杜绝两个进程同时通过版本检查。
//
// 构造时不再长期持有 flock（否则不同实例间的锁会永久互相阻塞）。
type FileStore struct {
	dir string
	mu  sync.Mutex
}

type fileEnvelope struct {
	State  *WorkflowState `json:"state"`
	Events []Event        `json:"events"`
}

// NewFileStore 打开（必要时创建）存储目录并做基本校验。
// 真正的跨进程排他锁在每次写操作的临界区内短暂获取。
func NewFileStore(dir string) (*FileStore, error) {
	if dir == "" {
		return nil, errors.New("workflow: empty store dir")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("workflow: mkdir store: %w", err)
	}
	if f, err := os.Open(dir); err != nil {
		return nil, fmt.Errorf("workflow: open store dir: %w", err)
	} else {
		_ = f.Close()
	}
	return &FileStore{dir: dir}, nil
}

func (s *FileStore) path(id string) string {
	return filepath.Join(s.dir, filepath.Clean(id)+".json")
}

func (s *FileStore) readEnvelopeLocked(id string) (*fileEnvelope, error) {
	b, err := os.ReadFile(s.path(id))
	if err != nil {
		return nil, err
	}
	var env fileEnvelope
	if err := json.Unmarshal(b, &env); err != nil {
		return nil, fmt.Errorf("workflow: corrupt state file %s: %w", id, err)
	}
	return &env, nil
}

// withDirLock 打开目录 fd、加排他 flock，执行 fn，随后释放并关闭。
func (s *FileStore) withDirLock(fn func() error) error {
	df, err := os.Open(s.dir)
	if err != nil {
		return fmt.Errorf("workflow: open store dir: %w", err)
	}
	defer df.Close()
	if err := lockDir(df); err != nil {
		return fmt.Errorf("workflow: lock store dir: %w", err)
	}
	defer func() { _ = unlockDir(df) }()
	return fn()
}

// writeFile 原子落盘 envelope：临时文件 + fsync + rename + fsync 目录。调用方须持锁。
func (s *FileStore) writeFile(id string, env *fileEnvelope) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(env); err != nil {
		return fmt.Errorf("workflow: encode state: %w", err)
	}
	final := s.path(id)
	tmp, err := os.CreateTemp(s.dir, ".tmp-"+filepath.Clean(id)+"-*")
	if err != nil {
		return fmt.Errorf("workflow: create temp: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(buf.Bytes()); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("workflow: write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("workflow: fsync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("workflow: close temp: %w", err)
	}
	if err := os.Rename(tmpName, final); err != nil {
		cleanup()
		return fmt.Errorf("workflow: rename state: %w", err)
	}
	if df, err := os.Open(s.dir); err == nil {
		_ = df.Sync()
		_ = df.Close()
	}
	return nil
}

func (s *FileStore) Create(ctx context.Context, st *WorkflowState, events []Event) (*WorkflowState, error) {
	if st == nil {
		return nil, errors.New("workflow: nil state")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.withDirLock(func() error {
		if _, err := s.readEnvelopeLocked(st.ID); err == nil {
			return conflict(ConflictStartAlreadyExists, ErrAlreadyExists)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		st.Version = 1
		st.UpdatedAt = st.CreatedAt
		for i := range events {
			events[i].Seq = int64(i + 1)
		}
		env := &fileEnvelope{State: cloneState(st), Events: append([]Event(nil), events...)}
		return s.writeFile(st.ID, env)
	})
	if err != nil {
		return nil, err
	}
	return cloneState(st), nil
}

func (s *FileStore) Commit(ctx context.Context, id string, expectedVersion int64, st *WorkflowState, events []Event) (*WorkflowState, error) {
	if st == nil {
		return nil, errors.New("workflow: nil state")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var committed *WorkflowState
	err := s.withDirLock(func() error {
		env, err := s.readEnvelopeLocked(id)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return conflictMsg(ConflictInvalidTransition, "workflow does not exist: "+id)
			}
			return err
		}
		if env.State == nil || env.State.Version != expectedVersion {
			got := int64(-1)
			if env.State != nil {
				got = env.State.Version
			}
			c := conflict(ConflictVersion, ErrVersionConflict)
			c.Msg = fmt.Sprintf("expected version %d, stored version %d", expectedVersion, got)
			return c
		}
		st.ID = id
		st.Version = expectedVersion + 1
		base := int64(len(env.Events))
		for i := range events {
			events[i].Seq = base + int64(i) + 1
		}
		env.State = cloneState(st)
		env.Events = append(env.Events, events...)
		if err := s.writeFile(id, env); err != nil {
			return err
		}
		committed = cloneState(st)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return committed, nil
}

func (s *FileStore) Load(ctx context.Context, id string) (*WorkflowState, []Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	env, err := s.readEnvelopeLocked(id)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, err
	}
	return cloneState(env.State), append([]Event(nil), env.Events...), nil
}

func (s *FileStore) List(ctx context.Context) ([]*WorkflowState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("workflow: list store: %w", err)
	}
	var out []*WorkflowState
	for _, ent := range entries {
		name := ent.Name()
		if ent.IsDir() || filepath.Ext(name) != ".json" || isTmpFile(name) {
			continue
		}
		id := name[:len(name)-len(".json")]
		env, err := s.readEnvelopeLocked(id)
		if err != nil {
			return nil, err
		}
		out = append(out, cloneState(env.State))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func isTmpFile(name string) bool {
	// 临时文件名形如 .tmp-<id>-<random>，没有 .json 后缀，天然会被 Ext 过滤；
	// 这里额外显式排除，防止命名变化后误读。
	return len(name) > 5 && name[:5] == ".tmp-"
}

// Close 仅为满足 Store 接口资源释放约定；锁按操作获取/释放，无需清理。
func (s *FileStore) Close() error { return nil }

// cloneState 用 JSON 往返做深拷贝，防止调用方在提交后继续修改内部对象，
// 也防止 FileStore 内存对象被外部别名污染。
func cloneState(st *WorkflowState) *WorkflowState {
	if st == nil {
		return nil
	}
	b, err := json.Marshal(st)
	if err != nil {
		panic("workflow: marshal state: " + err.Error())
	}
	var out WorkflowState
	if err := json.Unmarshal(b, &out); err != nil {
		panic("workflow: unmarshal state: " + err.Error())
	}
	return &out
}
