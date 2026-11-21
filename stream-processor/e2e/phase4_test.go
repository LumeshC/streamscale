// Phase 4: Redis real-time state — integration tests against real Redis (and
// Kafka + PostgreSQL for the pipeline test) via testcontainers.
package e2e_test

import (
	"context"
	"io"
	"log/slog"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/streamscale/streamscale/pkg/event"
	"github.com/streamscale/streamscale/pkg/rtstate"
	"github.com/streamscale/streamscale/stream-processor/internal/consume"
	"github.com/streamscale/streamscale/stream-processor/internal/dlq"
	"github.com/streamscale/streamscale/stream-processor/internal/realtime"
	"github.com/streamscale/streamscale/stream-processor/internal/retry"
	"github.com/streamscale/streamscale/stream-processor/internal/store"
)

func startRedis(t *testing.T, ctx context.Context) string {
	t.Helper()
	c, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("redis container: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	cs, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(cs)
	if err != nil {
		t.Fatal(err)
	}
	return u.Host
}

func testRedisConfig(addr string) realtime.Config {
	cfg := realtime.DefaultConfig()
	cfg.Addr = addr
	cfg.BreakerThreshold = 2
	cfg.BreakerCooldown = 500 * time.Millisecond
	cfg.UserRateLimit = 5
	return cfg
}

func rtEvent(typ event.Type, user string, ts time.Time) event.Event {
	e := event.Event{
		EventID:   uuid.NewString(),
		EventType: typ,
		UserID:    user,
		Timestamp: ts,
		Region:    "eu-west-1",
		Metadata:  map[string]string{"source": "phase4"},
	}
	if typ.RequiresCommerce() {
		e.ProductID, e.Amount, e.Currency = "sku_1", "10.25", "USD"
	}
	return e
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestPhase4RedisState(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	addr := startRedis(t, ctx)
	admin := redis.NewClient(&redis.Options{Addr: addr})
	defer admin.Close()

	newTracker := func(t *testing.T, cfg realtime.Config) *realtime.Tracker {
		t.Helper()
		if err := admin.FlushDB(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		tr := realtime.NewTracker(realtime.NewClient(cfg), cfg, quietLog())
		t.Cleanup(func() { _ = tr.Close() })
		return tr
	}

	t.Run("rolling metrics are idempotent per event and carry TTLs", func(t *testing.T) {
		cfg := testRedisConfig(addr)
		tr := newTracker(t, cfg)
		st := tr.Store()
		now := time.Now().UTC()
		u := uuid.NewString()

		evs := []event.Event{
			rtEvent(event.TypePurchase, u, now),
			rtEvent(event.TypePurchase, u, now),
			rtEvent(event.TypePaymentSuccess, u, now),
			rtEvent(event.TypePaymentFailure, u, now),
		}
		for _, e := range evs {
			r, err := st.Apply(ctx, e)
			if err != nil || !r.Applied {
				t.Fatalf("apply: %+v %v", r, err)
			}
		}
		// Simulated client retry of an ambiguous timeout: must be a no-op.
		r, err := st.Apply(ctx, evs[0])
		if err != nil || r.Applied {
			t.Fatalf("retry must not re-apply: %+v %v", r, err)
		}

		snap, err := tr.Snapshot(ctx, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if snap.Events != 4 || snap.Purchases != 2 || snap.Revenue != "20.50" {
			t.Fatalf("snapshot %+v", snap)
		}
		if snap.PaymentSuccess != 1 || snap.PaymentFailure != 1 || snap.PaymentSuccessRate != 0.5 {
			t.Fatalf("payments %+v", snap)
		}
		if snap.ByType["PURCHASE"] != 2 || snap.ByRegion["eu-west-1"] != 4 || snap.ActiveUsers != 1 {
			t.Fatalf("breakdowns %+v", snap)
		}

		ttl := func(key string) time.Duration {
			d, err := admin.TTL(ctx, key).Result()
			if err != nil {
				t.Fatal(err)
			}
			return d
		}
		if d := ttl(rtstate.SecondBucketKey(now.Unix())); d <= 0 || d > cfg.BucketRetention {
			t.Fatalf("bucket ttl %s", d)
		}
		if d := ttl(rtstate.AppliedKey(evs[0].EventID)); d <= 0 || d > cfg.DedupTTL {
			t.Fatalf("applied ttl %s", d)
		}
		if d := ttl(rtstate.UserStateKey(u)); d <= 0 || d > cfg.UserStateTTL {
			t.Fatalf("user ttl %s", d)
		}
		if d := ttl(rtstate.ActiveUsersKey()); d <= 0 || d > 2*cfg.ActiveWindow {
			t.Fatalf("active ttl %s", d)
		}
		// No key without a TTL (memory bounded by traffic, not history).
		keys, err := admin.Keys(ctx, "ss:*").Result()
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range keys {
			if ttl(k) < 0 {
				t.Fatalf("key %s has no TTL", k)
			}
		}
	})

	t.Run("stale events are not resurrected into real-time state", func(t *testing.T) {
		tr := newTracker(t, testRedisConfig(addr))
		old := rtEvent(event.TypeLogin, uuid.NewString(), time.Now().Add(-2*time.Hour))
		if _, err := tr.Store().Apply(ctx, old); err != nil {
			t.Fatal(err)
		}
		n, err := admin.Exists(ctx,
			rtstate.SecondBucketKey(old.Timestamp.Unix()),
			rtstate.UserStateKey(old.UserID),
			rtstate.ActiveUsersKey()).Result()
		if err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("old event created %d real-time keys", n)
		}
	})

	t.Run("active users and session state follow login/logout", func(t *testing.T) {
		tr := newTracker(t, testRedisConfig(addr))
		st := tr.Store()
		now := time.Now().UTC()
		u1, u2 := uuid.NewString(), uuid.NewString()
		apply := func(e event.Event) {
			if _, err := st.Apply(ctx, e); err != nil {
				t.Fatal(err)
			}
		}
		apply(rtEvent(event.TypeLogin, u1, now.Add(-3*time.Second)))
		apply(rtEvent(event.TypePageView, u1, now.Add(-2*time.Second)))
		apply(rtEvent(event.TypeLogin, u2, now.Add(-2*time.Second)))

		us, ok, err := tr.UserState(ctx, u1)
		if err != nil || !ok || !us.LoggedIn || us.SessionEvents != 2 || us.LastEventType != "PAGE_VIEW" {
			t.Fatalf("user state %+v ok=%v err=%v", us, ok, err)
		}

		apply(rtEvent(event.TypeLogout, u1, now.Add(-time.Second)))
		// Out-of-order event older than the logout must not re-activate u1.
		apply(rtEvent(event.TypeSearch, u1, now.Add(-1500*time.Millisecond)))

		snap, err := tr.Snapshot(ctx, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if snap.ActiveUsers != 1 {
			t.Fatalf("active users %d want 1", snap.ActiveUsers)
		}
		us, _, _ = tr.UserState(ctx, u1)
		if us.LoggedIn || us.LastEventType != "LOGOUT" {
			t.Fatalf("after logout %+v", us)
		}
		if _, ok, _ := tr.UserState(ctx, uuid.NewString()); ok {
			t.Fatal("unknown user must be not found")
		}
	})

	t.Run("per-user rate limit is shared across trackers and never drops metrics", func(t *testing.T) {
		cfg := testRedisConfig(addr)
		cfg.UserRateLimit = 5
		trA := newTracker(t, cfg)
		trB := realtime.NewTracker(realtime.NewClient(cfg), cfg, quietLog()) // second "pod"
		defer trB.Close()

		u := uuid.NewString()
		// Stay inside one rate window: wait for a fresh second.
		time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(time.Second)))
		limited := 0
		for i := 0; i < 8; i++ {
			tr := trA
			if i%2 == 1 {
				tr = trB
			}
			r, err := tr.Store().Apply(ctx, rtEvent(event.TypePageView, u, time.Now()))
			if err != nil {
				t.Fatal(err)
			}
			if r.RateLimited {
				limited++
			}
		}
		if limited != 3 {
			t.Fatalf("rate limited %d want 3", limited)
		}
		snap, err := trA.Snapshot(ctx, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if snap.Events != 8 || snap.RateLimited != 3 {
			t.Fatalf("all events must be counted; limited flagged: %+v", snap)
		}
		us, _, _ := trA.UserState(ctx, u)
		if us.SessionEvents != 5 || us.RateLimited != 3 {
			t.Fatalf("user state %+v", us)
		}
	})

	t.Run("timeouts open the circuit, then it recovers without double counting", func(t *testing.T) {
		cfg := testRedisConfig(addr)
		cfg.UserRateLimit = 0
		tr := newTracker(t, cfg)
		u := uuid.NewString()

		// Freeze Redis for all clients: every command blocks past OpTimeout.
		if err := admin.Do(ctx, "CLIENT", "PAUSE", 1500, "ALL").Err(); err != nil {
			t.Fatal(err)
		}
		sent := map[string]event.Event{}
		start := time.Now()
		for i := 0; i < 20; i++ {
			e := rtEvent(event.TypePageView, u, time.Now())
			sent[e.EventID] = e
			tr.Observe(ctx, e)
		}
		elapsed := time.Since(start)
		// 2 failures × OpTimeout, the rest skipped instantly by the breaker.
		if elapsed > 2*cfg.OpTimeout+500*time.Millisecond {
			t.Fatalf("observe blocked too long while redis paused: %s", elapsed)
		}
		s := tr.Stats()
		if s.State != realtime.StateOpen || s.Failures != 2 || s.Skipped != 18 {
			t.Fatalf("stats during outage %+v", s)
		}
		if _, err := tr.Snapshot(ctx, time.Minute); err == nil {
			t.Fatal("reads must report degraded while circuit open")
		}

		// After the pause and cooldown, the next event is the probe and closes the circuit.
		deadline := time.Now().Add(10 * time.Second)
		for tr.Stats().State != realtime.StateClosed {
			if time.Now().After(deadline) {
				t.Fatalf("circuit did not recover: %+v", tr.Stats())
			}
			time.Sleep(200 * time.Millisecond)
			e := rtEvent(event.TypePageView, u, time.Now())
			sent[e.EventID] = e
			tr.Observe(ctx, e)
		}
		// Timed-out scripts may still run once Redis resumes (ambiguous
		// outcome); retries re-sending them must be no-ops.
		for _, e := range sent {
			tr.Observe(ctx, e)
		}
		snap, err := tr.Snapshot(ctx, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if snap.Events > int64(len(sent)) {
			t.Fatalf("double counted: redis=%d distinct=%d", snap.Events, len(sent))
		}
		if snap.Events != int64(len(sent)) {
			t.Fatalf("after re-observe all events should be present once: redis=%d distinct=%d", snap.Events, len(sent))
		}
	})
}

// Full pipeline: Kafka → processor → PostgreSQL (durable) + Redis (real-time),
// including a Redis stall mid-stream that must not affect durable writes.
func TestPhase4PipelineWithRedis(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	pgURL, brokers, cleanup := startInfra(t, ctx)
	defer cleanup()
	redisAddr := startRedis(t, ctx)

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ensureTopics(t, ctx, brokers, log)

	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := store.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	pg := store.New(pool)

	cfg := testRedisConfig(redisAddr)
	cfg.UserRateLimit = 0
	tr := realtime.NewTracker(realtime.NewClient(cfg), cfg, log)
	defer tr.Close()

	procCtx, procCancel := context.WithCancel(ctx)
	defer procCancel()
	proc := consume.New(consume.Options{
		Brokers:  brokers,
		Topic:    "events",
		GroupID:  "streamscale-processor-phase4",
		Store:    pg,
		DLQ:      dlq.New(brokers, "events.dlq", log),
		Realtime: tr,
		Log:      log,
		Retry:    retry.Config{MaxAttempts: 3, BaseDelay: 20 * time.Millisecond, MaxDelay: 50 * time.Millisecond},
	})
	defer func() { _ = proc.Close() }()
	go func() { _ = proc.Run(procCtx) }()

	w := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: "events", Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireOne}
	defer w.Close()
	produce := func(evs ...event.Event) {
		t.Helper()
		msgs := make([]kafka.Message, 0, len(evs))
		for _, e := range evs {
			b, err := event.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			msgs = append(msgs, kafka.Message{Key: []byte(e.UserID), Value: b})
		}
		if err := w.WriteMessages(ctx, msgs...); err != nil {
			t.Fatal(err)
		}
	}
	waitPG := func(want int64) {
		t.Helper()
		deadline := time.Now().Add(90 * time.Second)
		for {
			n, err := pg.Count(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if n == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("postgres events=%d want %d", n, want)
			}
			time.Sleep(300 * time.Millisecond)
		}
	}

	// Batch 1: 3 users, 10 events, each produced twice (redelivery/duplicates).
	users := []string{uuid.NewString(), uuid.NewString(), uuid.NewString()}
	var batch1 []event.Event
	for i := 0; i < 10; i++ {
		typ := event.TypePageView
		if i%5 == 0 {
			typ = event.TypePurchase
		}
		batch1 = append(batch1, rtEvent(typ, users[i%3], time.Now().UTC()))
	}
	produce(batch1...)
	produce(batch1...)
	waitPG(10)

	snap, err := tr.Snapshot(ctx, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Events != 10 || snap.Purchases != 2 || snap.ActiveUsers != 3 {
		t.Fatalf("redis must mirror inserted (not duplicate) events: %+v", snap)
	}

	// Batch 2 while Redis is frozen: PostgreSQL must still get every event.
	admin := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer admin.Close()
	if err := admin.Do(ctx, "CLIENT", "PAUSE", 3000, "ALL").Err(); err != nil {
		t.Fatal(err)
	}
	var batch2 []event.Event
	for i := 0; i < 20; i++ {
		batch2 = append(batch2, rtEvent(event.TypeSearch, users[i%3], time.Now().UTC()))
	}
	produce(batch2...)
	waitPG(30)
	if tr.Stats().Failures == 0 {
		t.Fatalf("expected redis failures during pause: %+v", tr.Stats())
	}

	// Redis recovers; new events flow into real-time state again.
	time.Sleep(3 * time.Second)
	recovered := rtEvent(event.TypeLogin, uuid.NewString(), time.Now().UTC())
	produce(recovered)
	waitPG(31)
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, ok, err := tr.UserState(ctx, recovered.UserID); err == nil && ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("real-time state did not recover: %+v", tr.Stats())
		}
		time.Sleep(300 * time.Millisecond)
	}
	snap, err = tr.Snapshot(ctx, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	// Redis is best-effort: it may miss events from the outage but never exceeds PG.
	if snap.Events > 31 || snap.Events < 11 {
		t.Fatalf("redis events=%d, want between 11 and 31 (pg=31)", snap.Events)
	}
	t.Logf("pipeline ok: pg=31 redis=%d stats=%+v", snap.Events, tr.Stats())
}
