package e2e_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/streamscale/streamscale/analytics-api/api"
	"github.com/streamscale/streamscale/analytics-api/internal/handler"
	"github.com/streamscale/streamscale/analytics-api/internal/kafkalag"
	"github.com/streamscale/streamscale/analytics-api/internal/live"
	"github.com/streamscale/streamscale/analytics-api/internal/repo"
)

// TestOperationalEndpoints covers /metrics/latency and /errors against real
// PostgreSQL (with migration 0004's indexes) and /metrics/consumer-lag against
// a real Kafka broker with a consumer group that has committed part of the topic.
func TestOperationalEndpoints(t *testing.T) {
	requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := setup(t, ctx)

	w := mustPool(t, ctx, e.pgURL) // e.pool is read-only; seed through a writer

	// ---- seed events with known latency: 100 events ingested in the last
	// minutes, latency = 10ms, 20ms, ... 1000ms.
	base := time.Now().UTC().Add(-5 * time.Minute)
	for i := 1; i <= 100; i++ {
		ing := base.Add(time.Duration(i) * time.Second)
		ts := ing.Add(-time.Duration(i*10) * time.Millisecond)
		if _, err := w.Exec(ctx, `
			INSERT INTO events (event_id, event_type, user_id, ts, region, metadata, ingested_at)
			VALUES ($1, 'PAGE_VIEW', $2, $3, 'us-east-1', '{}', $4)`,
			uuid.New(), uuid.New(), ts, ing); err != nil {
			t.Fatal(err)
		}
	}
	// ---- seed processing outcomes: 90 processed, 3 malformed, 2 permanent DLQ.
	insStatus := func(off int, status string, class, reason *string, at time.Time) {
		if _, err := w.Exec(ctx, `
			INSERT INTO processing_status (event_id, kafka_topic, kafka_partition, kafka_offset, status,
				failure_class, failure_reason, attempts, created_at, updated_at)
			VALUES ($1, 'events', 0, $2, $3, $4, $5, 1, $6, $6)`,
			nil, off, status, class, reason, at); err != nil {
			t.Fatal(err)
		}
	}
	mal, perm := "malformed", "permanent"
	invalid, check := "invalid_json", "check_violation"
	for i := 0; i < 90; i++ {
		insStatus(i, "processed", nil, nil, base)
	}
	for i := 0; i < 3; i++ {
		insStatus(100+i, "dlq", &mal, &invalid, base.Add(time.Duration(i)*time.Minute))
	}
	for i := 0; i < 2; i++ {
		insStatus(200+i, "dlq", &perm, &check, base.Add(time.Duration(i)*time.Minute))
	}

	// ---- Kafka: 3 partitions, 30 messages, group commits 10 of them.
	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1")
	if err != nil {
		t.Fatalf("kafka: %v", err)
	}
	t.Cleanup(func() { _ = kc.Terminate(context.Background()) })
	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.CreateTopics(kafka.TopicConfig{Topic: "events", NumPartitions: 3, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	kw := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: "events", Balancer: &kafka.RoundRobin{}, RequiredAcks: kafka.RequireAll}
	for i := 0; i < 30; i++ {
		if err := kw.WriteMessages(ctx, kafka.Message{Value: []byte(fmt.Sprintf("m%d", i))}); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	_ = kw.Close()
	rd := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, GroupID: "test-group", Topic: "events",
		StartOffset: kafka.FirstOffset, MaxWait: 200 * time.Millisecond})
	for i := 0; i < 10; i++ {
		m, err := rd.FetchMessage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := rd.CommitMessages(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	_ = rd.Close()

	lag := kafkalag.New(kafkalag.Options{Brokers: brokers, GroupID: "test-group", Topic: "events", Timeout: 5 * time.Second})
	a := handler.New(handler.Options{Repo: repo.NewPostgres(e.pool), Live: live.Disabled{}, Lag: lag,
		Log: quiet, OpenAPI: api.OpenAPI})
	srv := httptest.NewServer(a.Router(quiet, 10*time.Second))
	defer srv.Close()

	t.Run("consumer lag matches committed offsets", func(t *testing.T) {
		code, body := getJSON(t, srv.URL, "/metrics/consumer-lag")
		if code != 200 || num(body, "total_lag") != 20 || num(body, "end_offset_total") != 30 ||
			len(body["partitions"].([]any)) != 3 || body["group"] != "test-group" {
			t.Fatalf("%d %v", code, body)
		}
		var committed float64
		for _, p := range body["partitions"].([]any) {
			pm := p.(map[string]any)
			if num(pm, "lag") != num(pm, "end_offset")-maxf(num(pm, "committed_offset"), num(pm, "start_offset")) {
				t.Fatalf("partition math %v", pm)
			}
			committed += maxf(num(pm, "committed_offset"), 0)
		}
		if committed != 10 {
			t.Fatalf("committed sum %v want 10", committed)
		}
		_, h := getJSON(t, srv.URL, "/health")
		if h["checks"].(map[string]any)["kafka"].(map[string]any)["status"] != "ok" {
			t.Fatalf("health %v", h)
		}
	})

	t.Run("consumer lag for a group that never committed counts the whole log", func(t *testing.T) {
		fresh := kafkalag.New(kafkalag.Options{Brokers: brokers, GroupID: "never-ran", Topic: "events", Timeout: 5 * time.Second})
		rep, err := fresh.Lag(ctx)
		if err != nil || rep.TotalLag != 30 {
			t.Fatalf("%+v %v", rep, err)
		}
	})

	t.Run("latency percentiles are exact", func(t *testing.T) {
		q := url.Values{"from": {base.Format(time.RFC3339)}, "to": {time.Now().UTC().Add(time.Minute).Format(time.RFC3339)}}
		code, body := getJSON(t, srv.URL, "/metrics/latency?"+q.Encode())
		s := body["summary"].(map[string]any)
		// latencies 10..1000 step 10 → p50 = 505, p95 = 950.5, p99 = 990.1, max = 1000
		if code != 200 || num(s, "count") != 100 || num(s, "p50_ms") != 505 || num(s, "p95_ms") != 950.5 ||
			num(s, "p99_ms") != 990.1 || num(s, "max_ms") != 1000 {
			t.Fatalf("%d %v", code, s)
		}
		var n float64
		for _, p := range body["data"].([]any) {
			n += num(p.(map[string]any), "count")
		}
		if n != 100 || len(body["data"].([]any)) < 2 {
			t.Fatalf("per-minute buckets %v", body["data"])
		}
	})

	t.Run("errors list, reasons, rate and keyset paging", func(t *testing.T) {
		code, body := getJSON(t, srv.URL, "/errors?limit=2")
		tot := body["totals"].(map[string]any)
		if code != 200 || num(tot, "dlq") != 5 || num(tot, "processed") != 90 || num(tot, "error_rate") != 0.052632 {
			t.Fatalf("%d %v", code, body)
		}
		br := body["by_reason"].([]any)
		if len(br) != 2 || br[0].(map[string]any)["failure_reason"] != "invalid_json" || num(br[0].(map[string]any), "count") != 3 {
			t.Fatalf("by_reason %v", br)
		}
		seen := 0
		target := "/errors?limit=2"
		for i := 0; i < 5; i++ {
			_, b := getJSON(t, srv.URL, target)
			seen += len(b["data"].([]any))
			next, ok := b["page"].(map[string]any)["next_cursor"].(string)
			if !ok {
				break
			}
			target = "/errors?limit=2&cursor=" + url.QueryEscape(next)
		}
		if seen != 5 {
			t.Fatalf("paged %d errors, want 5", seen)
		}
		_, b := getJSON(t, srv.URL, "/errors?class=permanent")
		if len(b["data"].([]any)) != 2 {
			t.Fatalf("class filter %v", b["data"])
		}
	})

	t.Run("kafka down is 503 and health degraded", func(t *testing.T) {
		dead := kafkalag.New(kafkalag.Options{Brokers: []string{unusedAddr(t)}, GroupID: "g", Topic: "events", Timeout: 500 * time.Millisecond})
		a := handler.New(handler.Options{Repo: repo.NewPostgres(e.pool), Live: live.Disabled{}, Lag: dead, Log: quiet, OpenAPI: api.OpenAPI})
		s := httptest.NewServer(a.Router(quiet, 5*time.Second))
		defer s.Close()
		code, body := getJSON(t, s.URL, "/metrics/consumer-lag")
		if code != 503 || body["error"].(map[string]any)["code"] != "unavailable" {
			t.Fatalf("%d %v", code, body)
		}
		code, body = getJSON(t, s.URL, "/health")
		if code != 200 || body["status"] != "degraded" {
			t.Fatalf("health %d %v", code, body)
		}
	})
}

func maxf(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func mustPool(t *testing.T, ctx context.Context, url string) *pgxpool.Pool {
	t.Helper()
	p, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}
