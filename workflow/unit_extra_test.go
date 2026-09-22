package workflow

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRetryPolicyBackoff(t *testing.T) {
	p := RetryPolicy{InitialBackoff: 100 * time.Millisecond, MaxBackoff: time.Second, Multiplier: 2}
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, time.Second}
	for i, w := range want {
		if got := p.Backoff(i + 1); got != w {
			t.Fatalf("backoff(%d)=%s want %s", i+1, got, w)
		}
	}
	if (RetryPolicy{MaxAttempts: 0}).MaxAttemptsVal() != 1 {
		t.Fatal("MaxAttempts<=0 must normalize to 1")
	}
	if (RetryPolicy{MaxAttempts: 7}).MaxAttemptsVal() != 7 {
		t.Fatal("MaxAttempts normalization wrong")
	}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		err   error
		class FailureClass
	}{
		{nil, FailureNone},
		{Transientf("x"), FailureTransient},
		{Permanentf("y"), FailurePermanent},
		{context.DeadlineExceeded, FailureTimeout},
		{errors.New("plain"), FailureTransient},
	}
	for _, c := range cases {
		got, _ := classify(c.err)
		if got != c.class {
			t.Fatalf("classify(%v)=%q want %q", c.err, got, c.class)
		}
	}
}

func TestFakeClockTimer(t *testing.T) {
	clk := NewFakeClock(time.Unix(1000, 0))
	tm := clk.NewTimer(5 * time.Second)
	select {
	case <-tm.C():
		t.Fatal("timer fired early")
	default:
	}
	clk.Advance(4 * time.Second)
	select {
	case <-tm.C():
		t.Fatal("timer fired before deadline")
	default:
	}
	clk.Advance(2 * time.Second)
	select {
	case <-tm.C():
	default:
		t.Fatal("timer did not fire after deadline")
	}
	if tm.Stop() {
		t.Fatal("Stop on fired timer should report false")
	}
}

func TestRunStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	env := newTestEnv(t)
	e := env.newEngine(t, "w-run", time.Minute)
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx, 10*time.Millisecond) }()
	env.clock.Advance(time.Second)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}

func TestStdLoggerFormat(t *testing.T) {
	var b strings.Builder
	l := NewStdLoggerWriter(&b)
	l.Logf("warn", "wf-x", 3, "step failed", "class", FailurePermanent, "attempt", 2)
	out := b.String()
	for _, frag := range []string{"level=warn", "workflow=wf-x", "step=3", "step failed",
		"class=permanent", "attempt=2"} {
		if !strings.Contains(out, frag) {
			t.Fatalf("log %q missing %q", out, frag)
		}
	}
}
