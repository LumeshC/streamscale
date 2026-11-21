package realtime

import (
	"time"

	"github.com/redis/go-redis/v9"
)

// Config controls the Redis connection and real-time state retention.
type Config struct {
	Addr     string
	Password string
	DB       int

	// Connection pool. go-redis keeps PoolSize connections per process;
	// MinIdleConns are pre-dialed so the first events after idle do not pay a
	// dial. PoolTimeout bounds how long a caller waits for a free connection.
	PoolSize        int
	MinIdleConns    int
	PoolTimeout     time.Duration
	ConnMaxIdleTime time.Duration

	// Socket timeouts. Kept tight: Redis is an in-memory hop; anything slower
	// than a few hundred ms is treated as unavailable.
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// OpTimeout is the total per-event budget including go-redis retries.
	// The Kafka loop never waits longer than this on Redis.
	OpTimeout time.Duration

	// Retries inside go-redis for network errors/timeouts. Safe because the
	// apply script is idempotent per event_id (see AppliedKey).
	MaxRetries      int
	MinRetryBackoff time.Duration
	MaxRetryBackoff time.Duration

	// Circuit breaker: after BreakerThreshold consecutive failures, skip Redis
	// for BreakerCooldown, then let one probe through.
	BreakerThreshold int
	BreakerCooldown  time.Duration

	// Retention.
	BucketRetention time.Duration // ss:rt:sec:* lifetime past their second
	ActiveWindow    time.Duration // "active user" horizon
	UserStateTTL    time.Duration // idle-session timeout for ss:rt:user:*
	DedupTTL        time.Duration // ss:rt:applied:* lifetime

	// UserRateLimit is the max events per user per RateWindow before the user
	// is flagged as rate-limited. 0 disables.
	UserRateLimit int
	RateWindow    time.Duration
}

func DefaultConfig() Config {
	return Config{
		Addr:             "localhost:6379",
		PoolSize:         20,
		MinIdleConns:     2,
		PoolTimeout:      300 * time.Millisecond,
		ConnMaxIdleTime:  5 * time.Minute,
		DialTimeout:      500 * time.Millisecond,
		ReadTimeout:      200 * time.Millisecond,
		WriteTimeout:     200 * time.Millisecond,
		OpTimeout:        300 * time.Millisecond,
		MaxRetries:       2,
		MinRetryBackoff:  8 * time.Millisecond,
		MaxRetryBackoff:  64 * time.Millisecond,
		BreakerThreshold: 3,
		BreakerCooldown:  5 * time.Second,
		BucketRetention:  15 * time.Minute,
		ActiveWindow:     5 * time.Minute,
		UserStateTTL:     30 * time.Minute,
		DedupTTL:         10 * time.Minute,
		UserRateLimit:    50,
		RateWindow:       time.Second,
	}
}

// withDefaults fills zero-valued fields from DefaultConfig.
func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.Addr == "" {
		c.Addr = d.Addr
	}
	if c.PoolSize <= 0 {
		c.PoolSize = d.PoolSize
	}
	if c.MinIdleConns < 0 {
		c.MinIdleConns = 0
	}
	if c.PoolTimeout <= 0 {
		c.PoolTimeout = d.PoolTimeout
	}
	if c.ConnMaxIdleTime <= 0 {
		c.ConnMaxIdleTime = d.ConnMaxIdleTime
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = d.DialTimeout
	}
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = d.ReadTimeout
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = d.WriteTimeout
	}
	if c.OpTimeout <= 0 {
		c.OpTimeout = d.OpTimeout
	}
	if c.MaxRetries < 0 {
		c.MaxRetries = 0
	}
	if c.MinRetryBackoff <= 0 {
		c.MinRetryBackoff = d.MinRetryBackoff
	}
	if c.MaxRetryBackoff <= 0 {
		c.MaxRetryBackoff = d.MaxRetryBackoff
	}
	if c.BreakerThreshold <= 0 {
		c.BreakerThreshold = d.BreakerThreshold
	}
	if c.BreakerCooldown <= 0 {
		c.BreakerCooldown = d.BreakerCooldown
	}
	if c.BucketRetention < time.Minute {
		c.BucketRetention = d.BucketRetention
	}
	if c.ActiveWindow <= 0 {
		c.ActiveWindow = d.ActiveWindow
	}
	if c.UserStateTTL <= 0 {
		c.UserStateTTL = d.UserStateTTL
	}
	if c.DedupTTL <= 0 {
		c.DedupTTL = d.DedupTTL
	}
	if c.UserRateLimit < 0 {
		c.UserRateLimit = 0
	}
	if c.RateWindow < time.Second {
		c.RateWindow = time.Second
	}
	return c
}

// NewClient builds a pooled go-redis client from cfg. It does not dial; the
// pool connects lazily, so a Redis outage at startup does not block the
// processor.
func NewClient(cfg Config) *redis.Client {
	cfg = cfg.withDefaults()
	// MaxRetries in go-redis: 0 means default (3), -1 disables.
	maxRetries := cfg.MaxRetries
	if maxRetries == 0 {
		maxRetries = -1
	}
	return redis.NewClient(&redis.Options{
		Addr:            cfg.Addr,
		Password:        cfg.Password,
		DB:              cfg.DB,
		PoolSize:        cfg.PoolSize,
		MinIdleConns:    cfg.MinIdleConns,
		PoolTimeout:     cfg.PoolTimeout,
		ConnMaxIdleTime: cfg.ConnMaxIdleTime,
		DialTimeout:     cfg.DialTimeout,
		ReadTimeout:     cfg.ReadTimeout,
		WriteTimeout:    cfg.WriteTimeout,
		MaxRetries:      maxRetries,
		MinRetryBackoff: cfg.MinRetryBackoff,
		MaxRetryBackoff: cfg.MaxRetryBackoff,
		// Respect the caller's context deadline (OpTimeout) for socket I/O.
		ContextTimeoutEnabled: true,
	})
}
