package config

import (
	"fmt"
	"time"

	"github.com/streamscale/streamscale/pkg/envcfg"
	"github.com/streamscale/streamscale/pkg/ktopic"
	"github.com/streamscale/streamscale/stream-processor/internal/realtime"
)

type Config struct {
	Brokers          []string
	Topic            string
	Partitions       int
	DLQTopic         string
	DLQPartitions    int
	GroupID          string
	DatabaseURL      string
	HTTPAddr         string
	MaxRetries       int
	RetryBase        time.Duration
	RetryMax         time.Duration
	AllowedLateness  time.Duration
	AnalyticsEnabled bool
	RedisEnabled     bool
	Redis            realtime.Config
}

func ms(key string, def time.Duration) time.Duration {
	return time.Duration(envcfg.Int(key, int(def/time.Millisecond))) * time.Millisecond
}

func secs(key string, def time.Duration) time.Duration {
	return time.Duration(envcfg.Int(key, int(def/time.Second))) * time.Second
}

func loadRedis() realtime.Config {
	d := realtime.DefaultConfig()
	return realtime.Config{
		Addr:             envcfg.String("REDIS_ADDR", d.Addr),
		Password:         envcfg.String("REDIS_PASSWORD", ""),
		DB:               envcfg.Int("REDIS_DB", 0),
		PoolSize:         envcfg.Int("REDIS_POOL_SIZE", d.PoolSize),
		MinIdleConns:     envcfg.Int("REDIS_MIN_IDLE_CONNS", d.MinIdleConns),
		PoolTimeout:      ms("REDIS_POOL_TIMEOUT_MS", d.PoolTimeout),
		ConnMaxIdleTime:  d.ConnMaxIdleTime,
		DialTimeout:      ms("REDIS_DIAL_TIMEOUT_MS", d.DialTimeout),
		ReadTimeout:      ms("REDIS_READ_TIMEOUT_MS", d.ReadTimeout),
		WriteTimeout:     ms("REDIS_WRITE_TIMEOUT_MS", d.WriteTimeout),
		OpTimeout:        ms("REDIS_OP_TIMEOUT_MS", d.OpTimeout),
		MaxRetries:       envcfg.Int("REDIS_MAX_RETRIES", d.MaxRetries),
		MinRetryBackoff:  d.MinRetryBackoff,
		MaxRetryBackoff:  d.MaxRetryBackoff,
		BreakerThreshold: envcfg.Int("REDIS_BREAKER_THRESHOLD", d.BreakerThreshold),
		BreakerCooldown:  ms("REDIS_BREAKER_COOLDOWN_MS", d.BreakerCooldown),
		BucketRetention:  secs("REALTIME_BUCKET_RETENTION_S", d.BucketRetention),
		ActiveWindow:     secs("REALTIME_ACTIVE_WINDOW_S", d.ActiveWindow),
		UserStateTTL:     secs("REALTIME_USER_STATE_TTL_S", d.UserStateTTL),
		DedupTTL:         secs("REALTIME_DEDUP_TTL_S", d.DedupTTL),
		UserRateLimit:    envcfg.Int("REALTIME_USER_RATE_LIMIT", d.UserRateLimit),
		RateWindow:       secs("REALTIME_RATE_WINDOW_S", d.RateWindow),
	}
}

func Load() (Config, error) {
	c := Config{
		Brokers:          envcfg.CSV("KAFKA_BROKERS", "localhost:9092"),
		Topic:            envcfg.String("KAFKA_TOPIC", ktopic.DefaultName),
		Partitions:       envcfg.Int("KAFKA_PARTITIONS", ktopic.DefaultPartitions),
		DLQTopic:         envcfg.String("KAFKA_DLQ_TOPIC", ktopic.DefaultDLQName),
		DLQPartitions:    envcfg.Int("KAFKA_DLQ_PARTITIONS", ktopic.DefaultDLQPartitions),
		GroupID:          envcfg.String("KAFKA_GROUP_ID", "streamscale-processor"),
		DatabaseURL:      envcfg.String("DATABASE_URL", "postgres://streamscale:streamscale@localhost:5432/streamscale?sslmode=disable"),
		HTTPAddr:         envcfg.String("HTTP_ADDR", ":8081"),
		MaxRetries:       envcfg.Int("PROCESSOR_MAX_RETRIES", 3),
		RetryBase:        time.Duration(envcfg.Int("PROCESSOR_RETRY_BASE_MS", 100)) * time.Millisecond,
		RetryMax:         time.Duration(envcfg.Int("PROCESSOR_RETRY_MAX_MS", 2000)) * time.Millisecond,
		AllowedLateness:  time.Duration(envcfg.Int("ANALYTICS_ALLOWED_LATENESS_MS", 30000)) * time.Millisecond,
		AnalyticsEnabled: envcfg.String("ANALYTICS_ENABLED", "true") != "false",
		RedisEnabled:     envcfg.String("REDIS_ENABLED", "true") != "false",
		Redis:            loadRedis(),
	}
	if len(c.Brokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS is required")
	}
	if c.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	if c.MaxRetries < 1 {
		c.MaxRetries = 1
	}
	return c, nil
}
