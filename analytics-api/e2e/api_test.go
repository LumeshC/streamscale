// Integration tests: the real analytics-api router against real PostgreSQL and
// Redis (testcontainers), with the stream-processor's own migrations applied
// and deterministic metrics_windows / Redis state seeded.
package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/streamscale/streamscale/analytics-api/api"
	"github.com/streamscale/streamscale/analytics-api/internal/handler"
	"github.com/streamscale/streamscale/analytics-api/internal/live"
	"github.com/streamscale/streamscale/analytics-api/internal/repo"
	"github.com/streamscale/streamscale/pkg/rtstate"
)

func requireDocker(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not installed")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skipf("docker daemon not available: %v", err)
	}
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// env is shared by all subtests: one Postgres + one Redis container.
type env struct {
	pool      *pgxpool.Pool
	pgURL     string
	redisAddr string
	now       time.Time
}

func setup(t *testing.T, ctx context.Context) *env {
	t.Helper()
	pgC, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("streamscale"),
		tcpostgres.WithUsername("streamscale"),
		tcpostgres.WithPassword("streamscale"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("postgres: %v", err)
	}
	t.Cleanup(func() { _ = pgC.Terminate(context.Background()) })
	rdC, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("redis: %v", err)
	}
	t.Cleanup(func() { _ = rdC.Terminate(context.Background()) })

	pgURL, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	rcs, err := rdC.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ru, _ := url.Parse(rcs)

	admin, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	applyProcessorMigrations(t, ctx, admin)

	// Read-only pool exactly as production builds it.
	pool, err := repo.NewPool(ctx, repo.PoolConfig{URL: pgURL, MaxConns: 5, StatementTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	e := &env{pool: pool, pgURL: pgURL, redisAddr: ru.Host, now: time.Now().UTC().Truncate(time.Minute)}
	seed(t, ctx, admin, e.now)
	return e
}

// applyProcessorMigrations runs the stream-processor's migration files, so the
// API is tested against the real schema the processor writes.
func applyProcessorMigrations(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "..", "stream-processor", "internal", "store", "migrations", "*.sql"))
	if err != nil || len(files) == 0 {
		t.Fatalf("migrations not found: %v", err)
	}
	sort.Strings(files)
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(b)); err != nil {
			t.Fatalf("migration %s: %v", filepath.Base(f), err)
		}
	}
}

// Seed: 90 one-minute windows ending at now (1000 events each, a spike of 4000
// at minute -10), 120 one-second windows, 10 five-minute sliding windows.
const minutesSeeded = 90

func seed(t *testing.T, ctx context.Context, pool *pgxpool.Pool, now time.Time) {
	t.Helper()
	ins := func(spec, kind string, size, slide time.Duration, start time.Time, events, purchases int64, revenue string,
		ok, fail, active int64, byRegion, byType string) {
		end := start.Add(size)
		min := size.Minutes()
		_, err := pool.Exec(ctx, `
			INSERT INTO metrics_windows (spec_name, window_kind, window_size_ms, window_slide_ms,
				window_start, window_end, event_count, purchase_count, revenue,
				payment_success_count, payment_failure_count, active_users,
				events_per_second, events_per_minute, purchases_per_minute, revenue_per_minute,
				payment_success_rate, payment_failure_rate, by_region, by_event_type)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::numeric,$10,$11,$12,$13,$14,$15,($9::numeric / $16),$17,$18,$19::jsonb,$20::jsonb)`,
			spec, kind, size.Milliseconds(), slide.Milliseconds(), start, end, events, purchases, revenue,
			ok, fail, active, float64(events)/size.Seconds(), float64(events)/min, float64(purchases)/min, min,
			float64(ok)/float64(ok+fail), float64(fail)/float64(ok+fail), byRegion, byType)
		if err != nil {
			t.Fatalf("seed %s: %v", spec, err)
		}
	}
	for i := 0; i < minutesSeeded; i++ {
		start := now.Add(-time.Duration(minutesSeeded-i) * time.Minute)
		events := int64(1000 + i%3 - 1) // 999/1000/1001
		if i == minutesSeeded-10 {
			events = 4000
		}
		ins("metrics_1m", "tumbling", time.Minute, time.Minute, start, events, 10, "100.00", 45, 5, 50,
			fmt.Sprintf(`{"us-east-1": %d, "eu-west-1": %d}`, events-events/4, events/4),
			fmt.Sprintf(`{"PAGE_VIEW": %d, "PURCHASE": 10, "PAYMENT_SUCCESS": 45, "PAYMENT_FAILURE": 5}`, events-60))
	}
	for i := 0; i < 120; i++ {
		start := now.Add(-time.Duration(120-i) * time.Second)
		ins("eps_1s", "tumbling", time.Second, time.Second, start, 17, 0, "0.00", 1, 0, 5, `{}`, `{}`)
	}
	for i := 0; i < 10; i++ {
		start := now.Add(-time.Duration(14-i) * time.Minute)
		ins("metrics_5m_slide_1m", "sliding", 5*time.Minute, time.Minute, start, 5000, 50, "500.00", 225, 25, 120, `{}`, `{}`)
	}
}

func server(t *testing.T, r repo.Repository, src live.Source, timeout time.Duration) *httptest.Server {
	t.Helper()
	a := handler.New(handler.Options{Repo: r, Live: src, Log: quiet, OpenAPI: api.OpenAPI,
		DefaultRange: time.Hour, MaxRange: 7 * 24 * time.Hour})
	srv := httptest.NewServer(a.Router(quiet, timeout))
	t.Cleanup(srv.Close)
	return srv
}

func getJSON(t *testing.T, base, path string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(base + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("%s: decode: %v", path, err)
	}
	if resp.Header.Get("X-Request-ID") == "" {
		t.Fatalf("%s: missing X-Request-ID", path)
	}
	return resp.StatusCode, body
}

func num(m map[string]any, k string) float64 { v, _ := m[k].(float64); return v }

func unusedAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func TestAnalyticsAPIIntegration(t *testing.T) {
	requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	e := setup(t, ctx)

	rdb := redis.NewClient(&redis.Options{Addr: e.redisAddr})
	defer rdb.Close()
	src := live.NewRedis(live.Options{Addr: e.redisAddr, Timeout: 500 * time.Millisecond})
	defer src.Close()
	pg := repo.NewPostgres(e.pool)
	srv := server(t, pg, src, 5*time.Second)

	from := e.now.Add(-60 * time.Minute).Format(time.RFC3339)
	to := e.now.Format(time.RFC3339)
	rng := "from=" + from + "&to=" + to

	t.Run("health ok with postgres and redis", func(t *testing.T) {
		code, body := getJSON(t, srv.URL, "/health")
		if code != 200 || body["status"] != "ok" {
			t.Fatalf("%d %v", code, body)
		}
	})

	t.Run("overview totals match seeded windows and include live redis view", func(t *testing.T) {
		// Seed live state the way the processor writes it.
		sec := time.Now().Unix()
		if err := rdb.HSet(ctx, rtstate.SecondBucketKey(sec), "events", 42, "purchases", 3, "revenue_cents", 1999).Err(); err != nil {
			t.Fatal(err)
		}
		if err := rdb.ZAdd(ctx, rtstate.ActiveUsersKey(), redis.Z{Score: float64(time.Now().UnixMilli()), Member: "u1"}).Err(); err != nil {
			t.Fatal(err)
		}
		code, body := getJSON(t, srv.URL, "/metrics/overview?"+rng)
		if code != 200 {
			t.Fatalf("%d %v", code, body)
		}
		tot := body["totals"].(map[string]any)
		// last 60 minutes: 59 windows ≈1000 (999/1000/1001 cycle) + one 4000 spike.
		var want int64
		for i := minutesSeeded - 60; i < minutesSeeded; i++ {
			v := int64(1000 + i%3 - 1)
			if i == minutesSeeded-10 {
				v = 4000
			}
			want += v
		}
		if int64(num(tot, "events")) != want || num(tot, "windows") != 60 || tot["revenue"] != "6000.00" ||
			num(tot, "payment_failure_rate") != 0.1 || num(tot, "peak_active_users_per_minute") != 50 {
			t.Fatalf("totals %v (want events %d)", tot, want)
		}
		lv, ok := body["live"].(map[string]any)
		if !ok || num(lv, "events") != 42 || lv["revenue"] != "19.99" || num(lv, "active_users") != 1 {
			t.Fatalf("live %v", body["live"])
		}
	})

	t.Run("event series paginates through every window exactly once", func(t *testing.T) {
		seen := map[string]bool{}
		target := "/metrics/events?limit=7&" + rng
		for pages := 0; pages < 20; pages++ {
			code, body := getJSON(t, srv.URL, target)
			if code != 200 {
				t.Fatalf("%d %v", code, body)
			}
			for _, d := range body["data"].([]any) {
				ws := d.(map[string]any)["window_start"].(string)
				if seen[ws] {
					t.Fatalf("duplicate window %s across pages", ws)
				}
				seen[ws] = true
			}
			next, ok := body["page"].(map[string]any)["next_cursor"].(string)
			if !ok {
				break
			}
			target = "/metrics/events?limit=7&cursor=" + url.QueryEscape(next) + "&" + rng
		}
		if len(seen) != 60 {
			t.Fatalf("saw %d windows, want 60", len(seen))
		}
	})

	t.Run("intervals select the right window spec", func(t *testing.T) {
		_, body := getJSON(t, srv.URL, "/metrics/events?interval=1s&limit=1000&from="+e.now.Add(-time.Minute).Format(time.RFC3339)+"&to="+to)
		if n := len(body["data"].([]any)); n != 60 {
			t.Fatalf("1s windows in last minute: %d", n)
		}
		_, body = getJSON(t, srv.URL, "/metrics/events?interval=5m&"+rng)
		if n := len(body["data"].([]any)); n != 10 {
			t.Fatalf("5m sliding windows: %d", n)
		}
		// Summaries ignore interval: sliding windows must not inflate totals.
		_, b1 := getJSON(t, srv.URL, "/metrics/revenue?interval=1m&"+rng)
		_, b5 := getJSON(t, srv.URL, "/metrics/revenue?interval=5m&"+rng)
		s1, s5 := b1["summary"].(map[string]any), b5["summary"].(map[string]any)
		if s1["revenue"] != s5["revenue"] {
			t.Fatalf("summary differs by interval: %v vs %v", s1, s5)
		}
		// 60 windows × (10 purchases, 100.00 revenue).
		if s1["revenue"] != "6000.00" || num(s1, "purchases") != 600 ||
			s1["average_order_value"] != "10.00" || s1["revenue_per_minute"] != "100.00" {
			t.Fatalf("revenue summary %v", s1)
		}
	})

	t.Run("payments summary", func(t *testing.T) {
		_, body := getJSON(t, srv.URL, "/metrics/payments?"+rng)
		s := body["summary"].(map[string]any)
		if num(s, "success") != 2700 || num(s, "failure") != 300 || num(s, "success_rate") != 0.9 {
			t.Fatalf("%v", s)
		}
	})

	t.Run("region and event-type breakdowns add up to total events", func(t *testing.T) {
		_, ov := getJSON(t, srv.URL, "/metrics/overview?"+rng)
		total := num(ov["totals"].(map[string]any), "events")
		for _, p := range []string{"/metrics/regions", "/metrics/event-types"} {
			_, body := getJSON(t, srv.URL, p+"?"+rng)
			var sum, share float64
			for _, d := range body["data"].([]any) {
				sum += num(d.(map[string]any), "events")
				share += num(d.(map[string]any), "share")
			}
			if sum != total || num(body, "total_events") != total || share < 0.999 || share > 1.001 {
				t.Fatalf("%s: sum=%v total=%v share=%v", p, sum, total, share)
			}
		}
		_, body := getJSON(t, srv.URL, "/metrics/regions?"+rng)
		if first := body["data"].([]any)[0].(map[string]any); first["key"] != "us-east-1" {
			t.Fatalf("regions not sorted by events: %v", body["data"])
		}
	})

	t.Run("anomalies finds the seeded spike", func(t *testing.T) {
		code, body := getJSON(t, srv.URL, "/anomalies?kind=event_rate_spike&"+rng)
		data := body["data"].([]any)
		if code != 200 || len(data) != 1 {
			t.Fatalf("%d %v", code, body)
		}
		a := data[0].(map[string]any)
		spikeAt := e.now.Add(-10 * time.Minute).Format(time.RFC3339)
		if a["window_start"] != spikeAt || a["severity"] != "critical" || num(a, "observed") != 4000 {
			t.Fatalf("anomaly %v (want start %s)", a, spikeAt)
		}
	})

	t.Run("validation errors use the envelope", func(t *testing.T) {
		code, body := getJSON(t, srv.URL, "/metrics/events?limit=0&from=bad")
		errObj := body["error"].(map[string]any)
		if code != 400 || errObj["code"] != "invalid_parameter" || len(errObj["details"].([]any)) != 2 {
			t.Fatalf("%d %v", code, body)
		}
	})

	t.Run("openapi document is served", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/openapi.yaml")
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || !strings.Contains(string(b), "openapi: 3.0.3") {
			t.Fatalf("%d", resp.StatusCode)
		}
	})

	t.Run("slow query hits request timeout and returns 504", func(t *testing.T) {
		// Hold an exclusive lock so the API's SELECT blocks.
		lockConn, err := pgxpool.New(ctx, e.pgURL)
		if err != nil {
			t.Fatal(err)
		}
		defer lockConn.Close()
		tx, err := lockConn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, "LOCK TABLE metrics_windows IN ACCESS EXCLUSIVE MODE"); err != nil {
			t.Fatal(err)
		}
		fast := server(t, pg, src, 300*time.Millisecond)
		start := time.Now()
		code, body := getJSON(t, fast.URL, "/metrics/overview?"+rng)
		if code != 504 || body["error"].(map[string]any)["code"] != "timeout" {
			t.Fatalf("%d %v", code, body)
		}
		if el := time.Since(start); el > 2*time.Second {
			t.Fatalf("timeout not enforced: %s", el)
		}
	})

	t.Run("redis down degrades but does not fail", func(t *testing.T) {
		dead := live.NewRedis(live.Options{Addr: unusedAddr(t), Timeout: 200 * time.Millisecond})
		defer dead.Close()
		s := server(t, pg, dead, 5*time.Second)
		code, body := getJSON(t, s.URL, "/health")
		if code != 200 || body["status"] != "degraded" {
			t.Fatalf("health %d %v", code, body)
		}
		code, body = getJSON(t, s.URL, "/metrics/overview?"+rng)
		if code != 200 || body["live"] != nil || body["degraded"].([]any)[0] != "live" {
			t.Fatalf("overview %d %v", code, body)
		}
	})

	t.Run("postgres down is 503 unavailable", func(t *testing.T) {
		badPool, err := repo.NewPool(ctx, repo.PoolConfig{
			URL:      "postgres://x:y@" + unusedAddr(t) + "/db?sslmode=disable&connect_timeout=1",
			MaxConns: 1, StatementTimeout: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer badPool.Close()
		s := server(t, repo.NewPostgres(badPool), src, 5*time.Second)
		code, body := getJSON(t, s.URL, "/health")
		if code != 503 || body["status"] != "unavailable" {
			t.Fatalf("health %d %v", code, body)
		}
		code, body = getJSON(t, s.URL, "/metrics/regions")
		if code != 503 || body["error"].(map[string]any)["code"] != "unavailable" {
			t.Fatalf("regions %d %v", code, body)
		}
	})
}
