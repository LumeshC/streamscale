// Phase 2: duplicates, DLQ for malformed events, idempotent persistence.
package e2e_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"github.com/streamscale/streamscale/pkg/event"
	"github.com/streamscale/streamscale/stream-processor/internal/consume"
	"github.com/streamscale/streamscale/stream-processor/internal/dlq"
	"github.com/streamscale/streamscale/stream-processor/internal/retry"
	"github.com/streamscale/streamscale/stream-processor/internal/store"
)

func TestPhase2IdempotencyAndDLQ(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pgURL, brokers, cleanup := startInfra(t, ctx)
	defer cleanup()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ensureTopics(t, ctx, brokers, log)

	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := store.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	st := store.New(pool)
	dlqPub := dlq.New(brokers, "events.dlq", log)

	procCtx, procCancel := context.WithCancel(ctx)
	defer procCancel()
	proc := consume.New(consume.Options{
		Brokers: brokers,
		Topic:   "events",
		GroupID: "streamscale-processor-phase2",
		Store:   st,
		DLQ:     dlqPub,
		Log:     log,
		Retry:   retry.Config{MaxAttempts: 3, BaseDelay: 20 * time.Millisecond, MaxDelay: 50 * time.Millisecond},
	})
	defer func() { _ = proc.Close() }()
	go func() { _ = proc.Run(procCtx) }()

	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "events",
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
	}
	defer w.Close()

	dupID := uuid.NewString()
	e := event.Event{
		EventID:   dupID,
		EventType: event.TypePurchase,
		UserID:    uuid.NewString(),
		Timestamp: time.Now().UTC(),
		Region:    "us-east-1",
		Metadata:  map[string]string{"source": "phase2-e2e"},
		ProductID: "sku_test",
		Amount:    "42.00",
		Currency:  "USD",
	}
	payload, err := event.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := w.WriteMessages(ctx, kafka.Message{Key: []byte(e.UserID), Value: payload}); err != nil {
			t.Fatalf("produce duplicate %d: %v", i, err)
		}
	}

	if err := w.WriteMessages(ctx, kafka.Message{Key: []byte("bad"), Value: []byte(`{"event_id":"not-valid"`)}); err != nil {
		t.Fatalf("produce malformed: %v", err)
	}

	neg := map[string]any{
		"event_id":   uuid.NewString(),
		"event_type": "PURCHASE",
		"user_id":    uuid.NewString(),
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
		"region":     "us-east-1",
		"metadata":   map[string]string{},
		"product_id": "sku",
		"amount":     "-10.00",
		"currency":   "USD",
	}
	negBytes, _ := json.Marshal(neg)
	if err := w.WriteMessages(ctx, kafka.Message{Key: []byte("neg"), Value: negBytes}); err != nil {
		t.Fatalf("produce negative amount: %v", err)
	}

	deadline := time.Now().Add(90 * time.Second)
	var eventCount, dupStatus, dlqStatus int64
	for {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: events=%d dup_status=%d dlq_status=%d", eventCount, dupStatus, dlqStatus)
		}
		var err error
		eventCount, err = st.Count(ctx)
		if err != nil {
			t.Fatal(err)
		}
		dupStatus, err = st.CountByStatus(ctx, store.StatusDuplicate)
		if err != nil {
			t.Fatal(err)
		}
		dlqStatus, err = st.CountByStatus(ctx, store.StatusDLQ)
		if err != nil {
			t.Fatal(err)
		}
		if eventCount == 1 && dupStatus >= 2 && dlqStatus >= 2 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       "events.dlq",
		GroupID:     "phase2-dlq-reader",
		StartOffset: kafka.FirstOffset,
		MaxWait:     time.Second,
	})
	defer r.Close()
	dlqMsgs := 0
	readDeadline := time.Now().Add(30 * time.Second)
	for dlqMsgs < 2 && time.Now().Before(readDeadline) {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			continue
		}
		dlqMsgs++
		_ = r.CommitMessages(ctx, m)
		hasReason := false
		for _, h := range m.Headers {
			if h.Key == "dlq.reason" && len(h.Value) > 0 {
				hasReason = true
			}
		}
		if !hasReason {
			t.Fatalf("dlq message missing reason header: %+v", m.Headers)
		}
	}
	if dlqMsgs < 2 {
		t.Fatalf("expected >=2 dlq kafka messages, got %d", dlqMsgs)
	}
	t.Logf("idempotency ok: events=%d duplicates=%d dlq_status=%d dlq_kafka=%d", eventCount, dupStatus, dlqStatus, dlqMsgs)
}
