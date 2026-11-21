package realtime

import (
	"testing"
	"time"
)

func TestBreakerOpensAfterThresholdAndRecovers(t *testing.T) {
	now := time.Unix(1000, 0)
	var transitions []BreakerState
	b := newBreaker(3, 5*time.Second, func(_, to BreakerState) { transitions = append(transitions, to) })
	b.now = func() time.Time { return now }

	for i := 0; i < 2; i++ {
		if !b.allow() {
			t.Fatal("closed breaker must allow")
		}
		b.failure()
	}
	if b.current() != StateClosed {
		t.Fatalf("opened before threshold: %s", b.current())
	}
	b.failure()
	if b.current() != StateOpen {
		t.Fatalf("want open, got %s", b.current())
	}
	if b.allow() {
		t.Fatal("open breaker must reject during cooldown")
	}

	now = now.Add(5 * time.Second)
	if !b.allow() {
		t.Fatal("expected half-open probe after cooldown")
	}
	if b.allow() {
		t.Fatal("only one probe allowed while half-open")
	}
	b.failure() // probe fails → open again
	if b.current() != StateOpen || b.allow() {
		t.Fatal("failed probe must reopen")
	}

	now = now.Add(5 * time.Second)
	if !b.allow() {
		t.Fatal("expected second probe")
	}
	b.success()
	if b.current() != StateClosed || !b.allow() {
		t.Fatal("successful probe must close")
	}
	want := []BreakerState{StateOpen, StateHalfOpen, StateOpen, StateHalfOpen, StateClosed}
	if len(transitions) != len(want) {
		t.Fatalf("transitions %v", transitions)
	}
	for i := range want {
		if transitions[i] != want[i] {
			t.Fatalf("transitions %v want %v", transitions, want)
		}
	}
}

func TestBreakerSuccessResetsFailureCount(t *testing.T) {
	b := newBreaker(2, time.Second, nil)
	b.failure()
	b.success()
	b.failure()
	if b.current() != StateClosed {
		t.Fatal("non-consecutive failures must not open")
	}
}
