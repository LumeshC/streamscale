package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/streamscale/streamscale/pkg/analytics"
	"github.com/streamscale/streamscale/pkg/health"
	"github.com/streamscale/streamscale/pkg/ktopic"
	"github.com/streamscale/streamscale/pkg/logging"
	procanalytics "github.com/streamscale/streamscale/stream-processor/internal/analytics"
	"github.com/streamscale/streamscale/stream-processor/internal/config"
	"github.com/streamscale/streamscale/stream-processor/internal/consume"
	"github.com/streamscale/streamscale/stream-processor/internal/dlq"
	"github.com/streamscale/streamscale/stream-processor/internal/realtime"
	"github.com/streamscale/streamscale/stream-processor/internal/retry"
	"github.com/streamscale/streamscale/stream-processor/internal/store"
)

func main() {
	log := logging.New("stream-processor")
	cfg, err := config.Load()
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := connectDB(ctx, cfg.DatabaseURL, log)
	if err != nil {
		log.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := store.Migrate(ctx, pool); err != nil {
		log.Error("migrate", "err", err)
		os.Exit(1)
	}
	log.Info("migrations applied")

	topics := map[string]int{
		cfg.Topic:    cfg.Partitions,
		cfg.DLQTopic: cfg.DLQPartitions,
	}
	if err := ktopic.EnsureMany(ctx, cfg.Brokers, topics, log); err != nil {
		log.Error("kafka topics", "err", err)
		os.Exit(1)
	}

	pg := store.New(pool)
	dlqPub := dlq.New(cfg.Brokers, cfg.DLQTopic, log)

	var engine analytics.Engine
	if cfg.AnalyticsEnabled {
		engine = procanalytics.NewMemoryEngine(procanalytics.Config{
			Specs:           analytics.DefaultSpecs(),
			AllowedLateness: cfg.AllowedLateness,
			Sink:            store.NewWindowSink(pg),
		})
		log.Info("analytics engine enabled",
			"allowed_lateness", cfg.AllowedLateness.String(),
			"specs", len(analytics.DefaultSpecs()),
		)
	}

	// Redis is optional at runtime: the client dials lazily and the tracker's
	// circuit breaker degrades to "durable path only" when Redis is down.
	var tracker *realtime.Tracker
	var rtSink consume.RealtimeSink
	healthOpts := []health.Option{}
	if cfg.RedisEnabled {
		tracker = realtime.NewTracker(realtime.NewClient(cfg.Redis), cfg.Redis, log)
		defer func() { _ = tracker.Close() }()
		rtSink = tracker
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		if err := tracker.Check(pingCtx); err != nil {
			log.Warn("redis not reachable at startup; continuing degraded", "addr", cfg.Redis.Addr, "err", err)
		} else {
			log.Info("redis connected", "addr", cfg.Redis.Addr, "pool_size", cfg.Redis.PoolSize)
		}
		cancel()
		healthOpts = append(healthOpts,
			health.WithInfo("redis", tracker.Check),
			health.WithRoutes(tracker.Routes),
		)
	}

	proc := consume.New(consume.Options{
		Brokers: cfg.Brokers,
		Topic:   cfg.Topic,
		GroupID: cfg.GroupID,
		Retry: retry.Config{
			MaxAttempts: cfg.MaxRetries,
			BaseDelay:   cfg.RetryBase,
			MaxDelay:    cfg.RetryMax,
		},
		Store:     pg,
		DLQ:       dlqPub,
		Analytics: engine,
		Realtime:  rtSink,
		Log:       log,
	})
	defer func() { _ = proc.Close() }()

	srv := health.Server(cfg.HTTPAddr, func(c context.Context) error {
		return pg.Ping(c)
	}, healthOpts...)
	go func() {
		log.Info("health server", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health server", "err", err)
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		errCh <- proc.Run(ctx)
	}()

	select {
	case <-ctx.Done():
		log.Info("shutdown signal")
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Error("processor stopped", "err", err)
			shutdownHTTP(srv)
			os.Exit(1)
		}
	}

	shutdownHTTP(srv)
	log.Info("shutdown complete")
}

func connectDB(ctx context.Context, url string, log *slog.Logger) (*pgxpool.Pool, error) {
	var last error
	for i := 0; i < 30; i++ {
		pool, err := pgxpool.New(ctx, url)
		if err != nil {
			last = err
		} else if pingErr := pool.Ping(ctx); pingErr != nil {
			last = pingErr
			pool.Close()
		} else {
			log.Info("postgres connected")
			return pool, nil
		}
		log.Warn("waiting for postgres", "attempt", i+1, "err", last)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return nil, last
}

func shutdownHTTP(srv *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}
