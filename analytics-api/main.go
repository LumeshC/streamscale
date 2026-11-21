// analytics-api serves read-only REST endpoints over StreamScale analytics.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/LumeshC/streamscale/analytics-api/api"
	"github.com/LumeshC/streamscale/analytics-api/internal/config"
	"github.com/LumeshC/streamscale/analytics-api/internal/handler"
	"github.com/LumeshC/streamscale/analytics-api/internal/kafkalag"
	"github.com/LumeshC/streamscale/analytics-api/internal/live"
	"github.com/LumeshC/streamscale/analytics-api/internal/repo"
	"github.com/LumeshC/streamscale/pkg/logging"
)

func main() {
	log := logging.New("analytics-api")
	cfg, err := config.Load()
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := repo.NewPool(ctx, repo.PoolConfig{
		URL: cfg.DatabaseURL, MaxConns: cfg.DBMaxConns, StatementTimeout: cfg.DBStatementTimeout,
	})
	if err != nil {
		log.Error("postgres pool", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	var src live.Source = live.Disabled{}
	if cfg.RedisEnabled {
		r := live.NewRedis(live.Options{Addr: cfg.RedisAddr, Password: cfg.RedisPass, DB: cfg.RedisDB, Timeout: cfg.RedisTimeout})
		defer func() { _ = r.Close() }()
		src = r
	}

	var lag kafkalag.Source = kafkalag.Disabled{}
	if cfg.KafkaEnabled {
		lag = kafkalag.New(kafkalag.Options{Brokers: cfg.KafkaBrokers, GroupID: cfg.KafkaGroupID, Topic: cfg.KafkaTopic, Timeout: cfg.KafkaTimeout})
	}

	a := handler.New(handler.Options{
		Repo:            repo.NewPostgres(pool),
		Live:            src,
		Lag:             lag,
		LatencyMaxRange: cfg.LatencyMaxRange,
		Log:             log,
		DefaultRange:    cfg.DefaultRange,
		MaxRange:        cfg.MaxRange,
		OpenAPI:         api.OpenAPI,
	})

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           a.Router(log, cfg.RequestTimeout),
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       5 * time.Second,
		// Must exceed the per-request budget so a timed-out handler can still
		// write its 504 envelope.
		WriteTimeout: cfg.RequestTimeout + 5*time.Second,
		IdleTimeout:  60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.HTTPAddr, "redis_enabled", cfg.RedisEnabled,
			"request_timeout", cfg.RequestTimeout.String())
		errCh <- srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		log.Info("shutdown signal")
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server", "err", err)
			os.Exit(1)
		}
	}

	// Stop accepting connections and drain in-flight requests.
	shCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shCtx); err != nil {
		log.Error("graceful shutdown", "err", err)
	}
	log.Info("shutdown complete")
}
