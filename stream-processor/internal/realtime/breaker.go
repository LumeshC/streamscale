package realtime

import (
	"sync"
	"time"
)

// BreakerState is the circuit breaker state.
type BreakerState string

const (
	StateClosed   BreakerState = "closed"    // Redis healthy, all calls go through
	StateOpen     BreakerState = "open"      // Redis considered down, calls skipped
	StateHalfOpen BreakerState = "half_open" // one probe call allowed
)

// breaker is a small consecutive-failure circuit breaker. It exists so that a
// dead Redis costs the hot path ~nothing (no dial, no timeout wait) instead of
// OpTimeout per event, which at 1k events/s would otherwise stall the consumer.
type breaker struct {
	mu        sync.Mutex
	threshold int
	cooldown  time.Duration
	now       func() time.Time

	state    BreakerState
	failures int
	openedAt time.Time
	probing  bool
	onChange func(from, to BreakerState)
}

func newBreaker(threshold int, cooldown time.Duration, onChange func(from, to BreakerState)) *breaker {
	return &breaker{
		threshold: threshold,
		cooldown:  cooldown,
		now:       time.Now,
		state:     StateClosed,
		onChange:  onChange,
	}
}

// allow reports whether a call may proceed.
func (b *breaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case StateClosed:
		return true
	case StateOpen:
		if b.now().Sub(b.openedAt) < b.cooldown {
			return false
		}
		b.transition(StateHalfOpen)
		b.probing = true
		return true
	default: // half-open: only the single in-flight probe
		if b.probing {
			return false
		}
		b.probing = true
		return true
	}
}

func (b *breaker) success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.probing = false
	if b.state != StateClosed {
		b.transition(StateClosed)
	}
}

func (b *breaker) failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probing = false
	b.failures++
	if b.state == StateHalfOpen || (b.state == StateClosed && b.failures >= b.threshold) {
		b.openedAt = b.now()
		b.transition(StateOpen)
	}
}

func (b *breaker) current() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *breaker) transition(to BreakerState) {
	from := b.state
	b.state = to
	if b.onChange != nil && from != to {
		b.onChange(from, to)
	}
}
