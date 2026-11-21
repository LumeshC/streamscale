package realtime

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/LumeshC/streamscale/pkg/event"
	"github.com/redis/go-redis/v9"
)

// ErrUnavailable is returned by Tracker reads while the breaker is open.
var ErrUnavailable = errors.New("realtime state unavailable (redis circuit open)")

// Stats are process-local counters for the real-time path.
type Stats struct {
	Applied     int64        `json:"applied"`
	Duplicates  int64        `json:"duplicates"`
	RateLimited int64        `json:"rate_limited"`
	Failures    int64        `json:"failures"`
	Skipped     int64        `json:"skipped_circuit_open"`
	State       BreakerState `json:"circuit"`
}

// Tracker is the processor-facing real-time sink. Its contract with the
// consumer loop is: Observe never fails the event and never blocks longer than
// OpTimeout. Redis is an accelerator, not a dependency of correctness.
type Tracker struct {
	store   *Store
	rdb     *redis.Client
	cfg     Config
	log     *slog.Logger
	breaker *breaker

	applied, duplicates, rateLimited, failures, skipped atomic.Int64
}

func NewTracker(rdb *redis.Client, cfg Config, log *slog.Logger) *Tracker {
	cfg = cfg.withDefaults()
	if log == nil {
		log = slog.Default()
	}
	t := &Tracker{store: NewStore(rdb, cfg), rdb: rdb, cfg: cfg, log: log}
	t.breaker = newBreaker(cfg.BreakerThreshold, cfg.BreakerCooldown, func(from, to BreakerState) {
		// Log transitions only — a per-event error log during an outage would
		// flood at the event rate.
		switch to {
		case StateOpen:
			log.Warn("redis circuit open; real-time state degraded, durable path unaffected",
				"from", from, "cooldown", cfg.BreakerCooldown.String())
		case StateClosed:
			log.Info("redis circuit closed; real-time state recovered", "from", from)
		}
	})
	return t
}

// Store exposes the underlying store (reads, tests).
func (t *Tracker) Store() *Store { return t.store }

// Observe applies a newly persisted event to Redis on a best-effort basis.
func (t *Tracker) Observe(ctx context.Context, e event.Event) {
	if !t.breaker.allow() {
		t.skipped.Add(1)
		return
	}
	opCtx, cancel := context.WithTimeout(ctx, t.cfg.OpTimeout)
	defer cancel()
	res, err := t.store.Apply(opCtx, e)
	if err != nil {
		if ctx.Err() != nil {
			// Shutdown, not a Redis fault: don't trip the breaker.
			return
		}
		t.failures.Add(1)
		t.breaker.failure()
		t.log.Debug("redis apply failed", "event_id", e.EventID, "err", err)
		return
	}
	t.breaker.success()
	if !res.Applied {
		t.duplicates.Add(1)
		return
	}
	t.applied.Add(1)
	if res.RateLimited {
		t.rateLimited.Add(1)
	}
}

// Snapshot reads the rolling view through the breaker.
func (t *Tracker) Snapshot(ctx context.Context, window time.Duration) (Snapshot, error) {
	var out Snapshot
	err := t.guard(ctx, func(c context.Context) (err error) {
		out, err = t.store.Snapshot(c, window)
		return err
	})
	return out, err
}

// UserState reads one user's live state through the breaker.
func (t *Tracker) UserState(ctx context.Context, userID string) (UserState, bool, error) {
	var (
		out   UserState
		found bool
	)
	err := t.guard(ctx, func(c context.Context) (err error) {
		out, found, err = t.store.UserState(c, userID)
		return err
	})
	return out, found, err
}

// Check is an informational health check: it reports degraded state but the
// processor stays ready without Redis.
func (t *Tracker) Check(ctx context.Context) error {
	if t.breaker.current() == StateOpen {
		return ErrUnavailable
	}
	return t.guard(ctx, t.store.Ping)
}

func (t *Tracker) Stats() Stats {
	return Stats{
		Applied:     t.applied.Load(),
		Duplicates:  t.duplicates.Load(),
		RateLimited: t.rateLimited.Load(),
		Failures:    t.failures.Load(),
		Skipped:     t.skipped.Load(),
		State:       t.breaker.current(),
	}
}

func (t *Tracker) Close() error { return t.rdb.Close() }

func (t *Tracker) guard(ctx context.Context, fn func(context.Context) error) error {
	if !t.breaker.allow() {
		return ErrUnavailable
	}
	c, cancel := context.WithTimeout(ctx, t.cfg.OpTimeout)
	defer cancel()
	if err := fn(c); err != nil {
		if ctx.Err() == nil {
			t.breaker.failure()
		}
		return err
	}
	t.breaker.success()
	return nil
}
