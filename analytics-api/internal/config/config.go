// Package config loads analytics-api settings from environment variables.
package config

import (
	"fmt"
	"time"

	"github.com/streamscale/streamscale/pkg/envcfg"
)

type Config struct {
	HTTPAddr string

	DatabaseURL string
	DBMaxConns  int32
	// DBStatementTimeout is enforced server-side by PostgreSQL as a backstop
	// in case a client-side deadline is not honored.
	DBStatementTimeout time.Duration

	RedisEnabled bool
	RedisAddr    string
	RedisPass    string
	RedisDB      int
	RedisTimeout time.Duration // per live read; Redis is optional

	// Kafka is used only to read consumer-group lag (optional).
	KafkaEnabled bool
	KafkaBrokers []string
	KafkaGroupID string
	KafkaTopic   string
	KafkaTimeout time.Duration

	// LatencyMaxRange caps /metrics/latency, which computes percentiles over raw rows.
	LatencyMaxRange time.Duration

	// RequestTimeout bounds each request end to end (handler context deadline).
	RequestTimeout time.Duration
	// ShutdownTimeout bounds graceful drain of in-flight requests.
	ShutdownTimeout time.Duration

	DefaultRange time.Duration // when ?from is omitted
	MaxRange     time.Duration // largest allowed to-from
}

func Load() (Config, error) {
	c := Config{
		HTTPAddr:           envcfg.String("API_HTTP_ADDR", ":8082"),
		DatabaseURL:        envcfg.String("DATABASE_URL", "postgres://streamscale:streamscale@localhost:5432/streamscale?sslmode=disable"),
		DBMaxConns:         int32(envcfg.Int("API_DB_MAX_CONNS", 10)),
		DBStatementTimeout: ms("API_DB_STATEMENT_TIMEOUT_MS", 10*time.Second),
		RedisEnabled:       envcfg.String("REDIS_ENABLED", "true") != "false",
		RedisAddr:          envcfg.String("REDIS_ADDR", "localhost:6379"),
		RedisPass:          envcfg.String("REDIS_PASSWORD", ""),
		RedisDB:            envcfg.Int("REDIS_DB", 0),
		RedisTimeout:       ms("API_REDIS_TIMEOUT_MS", 300*time.Millisecond),
		KafkaEnabled:       envcfg.String("API_KAFKA_ENABLED", "true") != "false",
		KafkaBrokers:       envcfg.CSV("KAFKA_BROKERS", "localhost:9092"),
		KafkaGroupID:       envcfg.String("KAFKA_GROUP_ID", "streamscale-processor"),
		KafkaTopic:         envcfg.String("KAFKA_TOPIC", "events"),
		KafkaTimeout:       ms("API_KAFKA_TIMEOUT_MS", 2*time.Second),
		LatencyMaxRange:    time.Duration(envcfg.Int("API_LATENCY_MAX_RANGE_MINUTES", 180)) * time.Minute,
		RequestTimeout:     ms("API_REQUEST_TIMEOUT_MS", 5*time.Second),
		ShutdownTimeout:    ms("API_SHUTDOWN_TIMEOUT_MS", 10*time.Second),
		DefaultRange:       time.Duration(envcfg.Int("API_DEFAULT_RANGE_MINUTES", 60)) * time.Minute,
		MaxRange:           time.Duration(envcfg.Int("API_MAX_RANGE_HOURS", 168)) * time.Hour,
	}
	if c.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	if c.DBMaxConns < 1 {
		return Config{}, fmt.Errorf("API_DB_MAX_CONNS must be >= 1")
	}
	if c.RequestTimeout <= 0 || c.RedisTimeout <= 0 || c.DBStatementTimeout <= 0 {
		return Config{}, fmt.Errorf("timeouts must be positive")
	}
	if c.KafkaEnabled && len(c.KafkaBrokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS is required when API_KAFKA_ENABLED")
	}
	if c.LatencyMaxRange <= 0 {
		return Config{}, fmt.Errorf("API_LATENCY_MAX_RANGE_MINUTES must be positive")
	}
	if c.DefaultRange <= 0 || c.MaxRange <= 0 || c.DefaultRange > c.MaxRange {
		return Config{}, fmt.Errorf("API_DEFAULT_RANGE_MINUTES must be positive and <= API_MAX_RANGE_HOURS")
	}
	return c, nil
}

func ms(key string, def time.Duration) time.Duration {
	return time.Duration(envcfg.Int(key, int(def/time.Millisecond))) * time.Millisecond
}
