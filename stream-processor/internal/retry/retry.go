package retry

import (
	"context"
	"time"

	"github.com/LumeshC/streamscale/stream-processor/internal/classify"
)

type Config struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

func DefaultConfig() Config {
	return Config{
		MaxAttempts: 3,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    2 * time.Second,
	}
}

// Do runs fn until it succeeds, a non-transient error occurs, or attempts are exhausted.
// On transient exhaustion it returns classify.Transient with reason "retries_exhausted".
func Do(ctx context.Context, cfg Config, fn func(attempt int) error) (attempts int, err error) {
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = 100 * time.Millisecond
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 2 * time.Second
	}

	var last error
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return attempt - 1, classify.Transient("context_done", err)
		}
		last = fn(attempt)
		if last == nil {
			return attempt, nil
		}
		c := classify.Of(last)
		if c.Class != classify.ClassTransient {
			return attempt, c
		}
		if attempt == cfg.MaxAttempts {
			return attempt, classify.Transient("retries_exhausted", last)
		}
		delay := cfg.BaseDelay * time.Duration(1<<(attempt-1))
		if delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
		}
		select {
		case <-ctx.Done():
			return attempt, classify.Transient("context_done", ctx.Err())
		case <-time.After(delay):
		}
	}
	return cfg.MaxAttempts, last
}
