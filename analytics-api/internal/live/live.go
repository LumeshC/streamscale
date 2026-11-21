// Package live reads the processor's Redis real-time state (read-only).
// Redis is optional for the API: every call has its own short timeout and
// failures surface as "degraded", never as a failed request.
package live

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/streamscale/streamscale/pkg/rtstate"
)

// ErrDisabled is returned when Redis is turned off by configuration.
var ErrDisabled = errors.New("redis disabled")

// Source provides the rolling real-time view.
type Source interface {
	Snapshot(ctx context.Context, window time.Duration) (rtstate.Snapshot, error)
	Ping(ctx context.Context) error
}

// Redis implements Source with a pooled client.
type Redis struct {
	rdb     *redis.Client
	timeout time.Duration
	now     func() time.Time
}

// Options for the read-only Redis client.
type Options struct {
	Addr     string
	Password string
	DB       int
	Timeout  time.Duration
}

func NewRedis(o Options) *Redis {
	if o.Timeout <= 0 {
		o.Timeout = 300 * time.Millisecond
	}
	return &Redis{
		rdb: redis.NewClient(&redis.Options{
			Addr:                  o.Addr,
			Password:              o.Password,
			DB:                    o.DB,
			PoolSize:              10,
			MinIdleConns:          1,
			DialTimeout:           o.Timeout,
			ReadTimeout:           o.Timeout,
			WriteTimeout:          o.Timeout,
			PoolTimeout:           o.Timeout,
			MaxRetries:            1, // reads are idempotent; one quick retry
			ContextTimeoutEnabled: true,
		}),
		timeout: o.Timeout,
		now:     time.Now,
	}
}

func (r *Redis) Snapshot(ctx context.Context, window time.Duration) (rtstate.Snapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	return rtstate.ReadSnapshot(ctx, r.rdb, r.now().UTC(), window, rtstate.ReadOptions{})
}

func (r *Redis) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	return r.rdb.Ping(ctx).Err()
}

func (r *Redis) Close() error { return r.rdb.Close() }

// Disabled is a Source used when REDIS_ENABLED=false.
type Disabled struct{}

func (Disabled) Snapshot(context.Context, time.Duration) (rtstate.Snapshot, error) {
	return rtstate.Snapshot{}, ErrDisabled
}
func (Disabled) Ping(context.Context) error { return ErrDisabled }
