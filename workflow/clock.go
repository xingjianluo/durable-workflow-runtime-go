package workflow

import (
	"sort"
	"sync"
	"time"
)

// Clock 抽象时间来源，便于用假时钟确定性地测试租约过期与退避。
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
}

// Timer 与 time.Timer 的用法对齐。
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

// SystemClock 使用真实墙钟。
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now() }

func (SystemClock) NewTimer(d time.Duration) Timer {
	return &systemTimer{t: time.NewTimer(d)}
}

type systemTimer struct {
	t *time.Timer
}

func (s *systemTimer) C() <-chan time.Time        { return s.t.C }
func (s *systemTimer) Stop() bool                 { return s.t.Stop() }
func (s *systemTimer) Reset(d time.Duration) bool { return s.t.Reset(d) }

// FakeClock 是手动推进的假时钟。NewTimer 产生的计时器只在 Advance
// 推进到其到期时刻后才触发，从而让租约过期、退避、尝试超时都可确定性测试。
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
	seq    int64
}

// NewFakeClock 从 start 时刻创建假时钟。
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{
		c:        make(chan time.Time, 1),
		clock:    c,
		deadline: c.now.Add(d),
		active:   true,
	}
	if d <= 0 {
		t.active = false
		t.fired = true
		t.c <- c.now
		return t
	}
	c.timers = append(c.timers, t)
	return t
}

// Advance 把时钟向前推进 d，并按到期顺序触发所有已到期的计时器。
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	due := make([]*fakeTimer, 0, len(c.timers))
	rest := c.timers[:0]
	for _, t := range c.timers {
		if t.active && !t.deadline.After(c.now) {
			due = append(due, t)
		} else if t.active {
			rest = append(rest, t)
		}
	}
	c.timers = rest
	c.mu.Unlock()

	sort.Slice(due, func(i, j int) bool { return due[i].deadline.Before(due[j].deadline) })
	for _, t := range due {
		t.fire(c.Now())
	}
}

type fakeTimer struct {
	c        chan time.Time
	clock    *FakeClock
	deadline time.Time
	active   bool
	fired    bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.c }

func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	wasActive := t.active
	t.active = false
	if wasActive {
		for i, x := range t.clock.timers {
			if x == t {
				t.clock.timers = append(t.clock.timers[:i], t.clock.timers[i+1:]...)
				break
			}
		}
	}
	return wasActive
}

func (t *fakeTimer) Reset(d time.Duration) bool {
	t.clock.mu.Lock()
	wasActive := t.active
	t.active = true
	t.fired = false
	t.deadline = t.clock.now.Add(d)
	// 排空可能残留的触发值，语义对齐 time.Timer.Reset 的常见用法。
	select {
	case <-t.c:
	default:
	}
	if d <= 0 {
		t.active = false
		t.fired = true
		now := t.clock.now
		t.clock.mu.Unlock()
		t.c <- now
		return wasActive
	}
	if !wasActive {
		t.clock.timers = append(t.clock.timers, t)
	}
	t.clock.mu.Unlock()
	return wasActive
}

func (t *fakeTimer) fire(at time.Time) {
	t.clock.mu.Lock()
	if !t.active {
		t.clock.mu.Unlock()
		return
	}
	t.active = false
	t.fired = true
	t.clock.mu.Unlock()
	// 通道容量为 1，不会阻塞；无人读取也不丢下一次 Reset 的正确性。
	select {
	case t.c <- at:
	default:
	}
}
