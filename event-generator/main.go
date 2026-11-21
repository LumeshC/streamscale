package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/streamscale/streamscale/event-generator/internal/config"
	"github.com/streamscale/streamscale/event-generator/internal/generate"
	"github.com/streamscale/streamscale/event-generator/internal/produce"
	"github.com/streamscale/streamscale/pkg/health"
	"github.com/streamscale/streamscale/pkg/ktopic"
	"github.com/streamscale/streamscale/pkg/logging"
)

func main() {
	log := logging.New("event-generator")
	cfg, err := config.Parse()
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(2)
	}
	if cfg.Seed == 0 {
		cfg.Seed = time.Now().UnixNano()
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := ktopic.Ensure(ctx, cfg.Brokers, cfg.Topic, cfg.Partitions, log); err != nil {
		log.Error("kafka topic", "err", err)
		os.Exit(1)
	}

	producer := produce.New(cfg.Brokers, cfg.Topic, log)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Error("close producer", "err", err)
		}
	}()

	gen := generate.New(cfg.Seed, cfg.UserPool)
	ready := func(context.Context) error { return nil }
	srv := health.Server(cfg.HTTPAddr, ready)
	go func() {
		log.Info("health server", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health server", "err", err)
		}
	}()

	log.Info("producing events",
		"brokers", cfg.Brokers,
		"topic", cfg.Topic,
		"rate", cfg.Rate,
		"count", cfg.Count,
		"seed", cfg.Seed,
	)

	interval := time.Second / time.Duration(cfg.Rate)
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	produced := 0
	for {
		select {
		case <-ctx.Done():
			shutdown(srv)
			log.Info("shutdown", "produced", produced)
			return
		case <-ticker.C:
			e := gen.Next()
			if err := producer.Write(ctx, e); err != nil {
				if ctx.Err() != nil {
					shutdown(srv)
					log.Info("shutdown", "produced", produced)
					return
				}
				log.Error("produce failed", "err", err, "event_id", e.EventID)
				continue
			}
			produced++
			if produced%100 == 0 {
				log.Info("produce progress", "produced", produced, "event_type", e.EventType)
			}
			if cfg.Count > 0 && produced >= cfg.Count {
				shutdown(srv)
				log.Info("count reached", "produced", produced)
				return
			}
		}
	}
}

func shutdown(srv *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}
