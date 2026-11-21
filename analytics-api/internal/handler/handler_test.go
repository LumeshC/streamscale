package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/streamscale/streamscale/analytics-api/internal/live"
	"github.com/streamscale/streamscale/analytics-api/internal/repo"
	"github.com/streamscale/streamscale/pkg/rtstate"
)

var now = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

// fakeRepo serves an in-memory set of 1m windows.
type fakeRepo struct {
	wins      []repo.Window // any order
	pingErr   error
	err       error
	block     bool // block until ctx is done (timeout tests)
	lastQ     repo.SeriesQuery
	breakdown map[repo.Dimension][]repo.BreakdownItem

	latency     repo.LatencyStats
	latencyPts  []repo.LatencyPoint
	lastLatency [2]time.Time
	errs        []repo.ErrorRecord
}

func (f *fakeRepo) wait(ctx context.Context) error {
	if f.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return f.err
}

func (f *fakeRepo) Ping(ctx context.Context) error { return f.pingErr }

func (f *fakeRepo) inRange(from, to time.Time) []repo.Window {
	var out []repo.Window
	for _, w := range f.wins {
		if !w.Start.Before(from) && w.Start.Before(to) {
			out = append(out, w)
		}
	}
	return out
}

func (f *fakeRepo) Summary(ctx context.Context, from, to time.Time) (repo.Summary, error) {
	if err := f.wait(ctx); err != nil {
		return repo.Summary{}, err
	}
	s := repo.Summary{Revenue: "0.00"}
	var cents int64
	for _, w := range f.inRange(from, to) {
		s.Windows++
		s.Events += w.EventCount
		s.Purchases += w.PurchaseCount
		s.PaymentSuccess += w.PaymentSuccess
		s.PaymentFailure += w.PaymentFailure
		if w.ActiveUsers > s.PeakActiveUsers {
			s.PeakActiveUsers = w.ActiveUsers
		}
		var whole, frac int64
		_, _ = fmtSscan(w.Revenue, &whole, &frac)
		cents += whole*100 + frac
	}
	s.Revenue = rtstate.CentsString(cents)
	return s, nil
}

func (f *fakeRepo) Series(ctx context.Context, q repo.SeriesQuery) ([]repo.Window, error) {
	f.lastQ = q
	if err := f.wait(ctx); err != nil {
		return nil, err
	}
	ws := f.inRange(q.From, q.To)
	sort.Slice(ws, func(i, j int) bool { return ws[i].Start.After(ws[j].Start) })
	var out []repo.Window
	for _, w := range ws {
		if q.Before != nil && !w.Start.Before(*q.Before) {
			continue
		}
		out = append(out, w)
		if len(out) == q.Limit+1 {
			break
		}
	}
	return out, nil
}

func (f *fakeRepo) Breakdown(ctx context.Context, dim repo.Dimension, from, to time.Time) ([]repo.BreakdownItem, error) {
	if err := f.wait(ctx); err != nil {
		return nil, err
	}
	return f.breakdown[dim], nil
}

func (f *fakeRepo) MinuteWindows(ctx context.Context, from, to time.Time) ([]repo.Window, error) {
	if err := f.wait(ctx); err != nil {
		return nil, err
	}
	ws := f.inRange(from, to)
	sort.Slice(ws, func(i, j int) bool { return ws[i].Start.Before(ws[j].Start) })
	return ws, nil
}

// fmtSscan parses "12.34" into whole and cents.
func fmtSscan(s string, whole, frac *int64) (int, error) {
	parts := strings.SplitN(s, ".", 2)
	var w, c int64
	for _, ch := range parts[0] {
		w = w*10 + int64(ch-'0')
	}
	if len(parts) == 2 {
		for _, ch := range (parts[1] + "00")[:2] {
			c = c*10 + int64(ch-'0')
		}
	}
	*whole, *frac = w, c
	return 2, nil
}

type fakeLive struct {
	snap rtstate.Snapshot
	err  error
}

func (l fakeLive) Snapshot(context.Context, time.Duration) (rtstate.Snapshot, error) {
	return l.snap, l.err
}
func (l fakeLive) Ping(context.Context) error { return l.err }

// minuteWins makes n consecutive 1m windows ending at `now`, each with the
// given counts.
func minuteWins(n int, events int64) []repo.Window {
	out := make([]repo.Window, n)
	for i := 0; i < n; i++ {
		s := now.Add(-time.Duration(n-i) * time.Minute)
		out[i] = repo.Window{
			Start: s, End: s.Add(time.Minute), EventCount: events, PurchaseCount: 2,
			Revenue: "20.50", PaymentSuccess: 9, PaymentFailure: 1, ActiveUsers: int64(10 + i),
			EventsPerSecond: float64(events) / 60, EventsPerMinute: float64(events),
			PurchasesPerMinute: 2, RevenuePerMinute: "20.50",
			PaymentSuccessRate: 0.9, PaymentFailureRate: 0.1,
		}
	}
	return out
}

func newAPI(r repo.Repository, l live.Source) http.Handler {
	a := New(Options{
		Repo: r, Live: l, Now: func() time.Time { return now },
		DefaultRange: time.Hour, MaxRange: 7 * 24 * time.Hour,
		OpenAPI: []byte("openapi: 3.0.3\n"),
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	return a.Router(slog.New(slog.NewTextHandler(io.Discard, nil)), 200*time.Millisecond)
}

func get(t *testing.T, h http.Handler, target string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	var body map[string]any
	if strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") {
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("%s: invalid json: %v\n%s", target, err, rec.Body.String())
		}
	}
	return rec, body
}

func errCode(body map[string]any) string {
	e, _ := body["error"].(map[string]any)
	c, _ := e["code"].(string)
	return c
}

func errFields(body map[string]any) []string {
	e, _ := body["error"].(map[string]any)
	ds, _ := e["details"].([]any)
	var out []string
	for _, d := range ds {
		out = append(out, d.(map[string]any)["field"].(string))
	}
	sort.Strings(out)
	return out
}

func TestHealth(t *testing.T) {
	cases := []struct {
		name   string
		repo   *fakeRepo
		live   live.Source
		code   int
		status string
		redis  string
	}{
		{"all up", &fakeRepo{}, fakeLive{}, 200, "ok", "ok"},
		{"redis down", &fakeRepo{}, fakeLive{err: errors.New("dial tcp: refused")}, 200, "degraded", "down"},
		{"redis disabled", &fakeRepo{}, live.Disabled{}, 200, "ok", "disabled"},
		{"postgres down", &fakeRepo{pingErr: errors.New("conn refused")}, fakeLive{}, 503, "unavailable", "ok"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec, body := get(t, newAPI(c.repo, c.live), "/health")
			checks := body["checks"].(map[string]any)
			if rec.Code != c.code || body["status"] != c.status || checks["redis"].(map[string]any)["status"] != c.redis {
				t.Fatalf("%d %v", rec.Code, body)
			}
			if rec.Header().Get("X-Request-ID") == "" {
				t.Fatal("missing X-Request-ID")
			}
		})
	}
}

func TestOverviewTotalsAndLive(t *testing.T) {
	r := &fakeRepo{wins: minuteWins(60, 600)}
	l := fakeLive{snap: rtstate.Snapshot{AsOf: now, WindowSeconds: 60, Events: 6000, EventsPerSecond: 100,
		Revenue: "5.00", ActiveUsers: 42, ByType: map[string]int64{}, ByRegion: map[string]int64{}}}
	rec, body := get(t, newAPI(r, l), "/metrics/overview")
	if rec.Code != 200 {
		t.Fatalf("%d %v", rec.Code, body)
	}
	tot := body["totals"].(map[string]any)
	if tot["events"].(float64) != 36000 || tot["events_per_minute"].(float64) != 600 ||
		tot["revenue"] != "1230.00" || tot["revenue_per_minute"] != "20.50" ||
		tot["payment_success_rate"].(float64) != 0.9 || tot["peak_active_users_per_minute"].(float64) != 69 ||
		tot["windows"].(float64) != 60 {
		t.Fatalf("totals %v", tot)
	}
	if body["live"].(map[string]any)["active_users"].(float64) != 42 || len(body["degraded"].([]any)) != 0 {
		t.Fatalf("live %v degraded %v", body["live"], body["degraded"])
	}
}

func TestOverviewDegradesWhenRedisDown(t *testing.T) {
	rec, body := get(t, newAPI(&fakeRepo{wins: minuteWins(5, 10)}, fakeLive{err: errors.New("i/o timeout")}), "/metrics/overview")
	if rec.Code != 200 || body["live"] != nil {
		t.Fatalf("%d %v", rec.Code, body)
	}
	if d := body["degraded"].([]any); len(d) != 1 || d[0] != "live" {
		t.Fatalf("degraded %v", d)
	}
}

func TestValidationErrorsUseEnvelopeWithAllFields(t *testing.T) {
	h := newAPI(&fakeRepo{}, fakeLive{})
	rec, body := get(t, h, "/metrics/events?from=nope&limit=5000&interval=2h&bogus=1")
	if rec.Code != 400 || errCode(body) != "invalid_parameter" {
		t.Fatalf("%d %v", rec.Code, body)
	}
	got := strings.Join(errFields(body), ",")
	if got != "bogus,from,interval,limit" {
		t.Fatalf("fields %s", got)
	}
	if body["request_id"] == "" || body["request_id"] != rec.Header().Get("X-Request-ID") {
		t.Fatalf("request_id mismatch: %v vs %s", body["request_id"], rec.Header().Get("X-Request-ID"))
	}
}

func TestRangeLimits(t *testing.T) {
	h := newAPI(&fakeRepo{}, fakeLive{})
	for _, q := range []string{
		"from=2026-11-20T12:00:00Z&to=2026-11-20T11:00:00Z", // inverted
		"from=2026-01-01T00:00:00Z",                         // > 7 days
	} {
		rec, body := get(t, h, "/metrics/revenue?"+q)
		if rec.Code != 400 || errFields(body)[0] != "from" {
			t.Fatalf("%s → %d %v", q, rec.Code, body)
		}
	}
}

func TestSeriesPaginationWalksAllPages(t *testing.T) {
	r := &fakeRepo{wins: minuteWins(25, 100)}
	h := newAPI(r, fakeLive{})
	var seen []string
	target := "/metrics/events?limit=10"
	for pages := 0; ; pages++ {
		if pages > 5 {
			t.Fatal("pagination did not terminate")
		}
		rec, body := get(t, h, target)
		if rec.Code != 200 {
			t.Fatalf("%d %v", rec.Code, body)
		}
		for _, d := range body["data"].([]any) {
			seen = append(seen, d.(map[string]any)["window_start"].(string))
		}
		page := body["page"].(map[string]any)
		next, ok := page["next_cursor"].(string)
		if !ok {
			break
		}
		target = "/metrics/events?limit=10&cursor=" + url.QueryEscape(next)
	}
	if len(seen) != 25 {
		t.Fatalf("saw %d windows, want 25", len(seen))
	}
	for i := 1; i < len(seen); i++ {
		if seen[i] >= seen[i-1] {
			t.Fatalf("not strictly newest-first / duplicate at %d: %s >= %s", i, seen[i], seen[i-1])
		}
	}
}

func TestIntervalMapsToRepo(t *testing.T) {
	r := &fakeRepo{}
	h := newAPI(r, fakeLive{})
	for _, iv := range []string{"1s", "1m", "5m"} {
		rec, body := get(t, h, "/metrics/events?interval="+iv)
		if rec.Code != 200 || body["interval"] != iv || r.lastQ.Interval != iv {
			t.Fatalf("%s: %d %v", iv, rec.Code, body)
		}
	}
}

func TestRevenueAndPaymentsSummaries(t *testing.T) {
	h := newAPI(&fakeRepo{wins: minuteWins(60, 100)}, fakeLive{})
	_, body := get(t, h, "/metrics/revenue?limit=5")
	s := body["summary"].(map[string]any)
	if s["purchases"].(float64) != 120 || s["revenue"] != "1230.00" || s["average_order_value"] != "10.25" ||
		s["revenue_per_minute"] != "20.50" || len(body["data"].([]any)) != 5 {
		t.Fatalf("revenue %v", body)
	}
	_, body = get(t, h, "/metrics/payments")
	s = body["summary"].(map[string]any)
	if s["success"].(float64) != 540 || s["failure"].(float64) != 60 || s["total"].(float64) != 600 ||
		s["failure_rate"].(float64) != 0.1 {
		t.Fatalf("payments %v", s)
	}
}

func TestRegionsAndEventTypes(t *testing.T) {
	r := &fakeRepo{breakdown: map[repo.Dimension][]repo.BreakdownItem{
		repo.DimRegion:    {{Key: "us-east-1", Count: 75}, {Key: "eu-west-1", Count: 25}},
		repo.DimEventType: {{Key: "PAGE_VIEW", Count: 90}, {Key: "PURCHASE", Count: 10}},
	}}
	h := newAPI(r, fakeLive{})
	_, body := get(t, h, "/metrics/regions")
	data := body["data"].([]any)
	if body["total_events"].(float64) != 100 || len(data) != 2 || data[0].(map[string]any)["share"].(float64) != 0.75 {
		t.Fatalf("regions %v", body)
	}

	_, body = get(t, h, "/metrics/event-types")
	if n := len(body["data"].([]any)); n != 7 {
		t.Fatalf("all 7 types must be listed (zero-filled), got %d", n)
	}
	_, body = get(t, h, "/metrics/event-types?event_type=PURCHASE")
	data = body["data"].([]any)
	if len(data) != 1 || data[0].(map[string]any)["events"].(float64) != 10 || data[0].(map[string]any)["share"].(float64) != 0.1 {
		t.Fatalf("filtered %v", body)
	}
	rec, body := get(t, h, "/metrics/event-types?event_type=CLICK")
	if rec.Code != 400 || errFields(body)[0] != "event_type" {
		t.Fatalf("invalid type: %d %v", rec.Code, body)
	}
	// event_type is not a regions parameter.
	if rec, _ := get(t, h, "/metrics/regions?event_type=PURCHASE"); rec.Code != 400 {
		t.Fatalf("regions must reject event_type: %d", rec.Code)
	}
}

func TestAnomaliesDetectFilterAndPaginate(t *testing.T) {
	wins := minuteWins(90, 1000)
	// Three spikes inside the default 1h range (last 60 windows).
	for _, i := range []int{40, 60, 80} {
		wins[i].EventCount = 3000
	}
	h := newAPI(&fakeRepo{wins: wins}, fakeLive{})

	_, body := get(t, h, "/anomalies?kind=event_rate_spike")
	data := body["data"].([]any)
	if len(data) != 3 {
		t.Fatalf("want 3 spikes, got %v", body)
	}
	det := body["detector"].(map[string]any)
	if det["method"] != "rolling_zscore" || det["baseline_windows"].(float64) != 30 {
		t.Fatalf("detector %v", det)
	}

	// Page through with limit=1.
	var ids []string
	target := "/anomalies?kind=event_rate_spike&limit=1"
	for i := 0; i < 5; i++ {
		_, body := get(t, h, target)
		for _, d := range body["data"].([]any) {
			ids = append(ids, d.(map[string]any)["id"].(string))
		}
		next, ok := body["page"].(map[string]any)["next_cursor"].(string)
		if !ok {
			break
		}
		target = "/anomalies?kind=event_rate_spike&limit=1&cursor=" + url.QueryEscape(next)
	}
	if len(ids) != 3 || ids[0] <= ids[1] || ids[1] <= ids[2] {
		t.Fatalf("paged ids %v", ids)
	}

	rec, body := get(t, h, "/anomalies?min_z=1&severity=fatal")
	if rec.Code != 400 || strings.Join(errFields(body), ",") != "min_z,severity" {
		t.Fatalf("%d %v", rec.Code, body)
	}
}

func TestTimeoutReturns504(t *testing.T) {
	h := newAPI(&fakeRepo{block: true}, fakeLive{})
	start := time.Now()
	rec, body := get(t, h, "/metrics/overview")
	if rec.Code != http.StatusGatewayTimeout || errCode(body) != "timeout" {
		t.Fatalf("%d %v", rec.Code, body)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("took %s", time.Since(start))
	}
}

func TestRepoErrorIs500WithoutLeakingCause(t *testing.T) {
	rec, body := get(t, newAPI(&fakeRepo{err: errors.New("password=hunter2 failed")}, fakeLive{}), "/metrics/regions")
	if rec.Code != 500 || errCode(body) != "internal" || strings.Contains(rec.Body.String(), "hunter2") {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
}

func TestNotFoundAndMethodNotAllowed(t *testing.T) {
	h := newAPI(&fakeRepo{}, fakeLive{})
	rec, body := get(t, h, "/metrics/nope")
	if rec.Code != 404 || errCode(body) != "not_found" {
		t.Fatalf("%d %v", rec.Code, body)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/metrics/events", nil))
	if rec.Code != 405 || rec.Header().Get("Allow") != "GET, HEAD" || !strings.Contains(rec.Body.String(), "method_not_allowed") {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
}

func TestDatabaseOutageIs503(t *testing.T) {
	err := fmt.Errorf("summary: %w: %w", repo.ErrUnavailable, errors.New("dial tcp: connection refused"))
	rec, body := get(t, newAPI(&fakeRepo{err: err}, fakeLive{}), "/metrics/overview")
	if rec.Code != 503 || errCode(body) != "unavailable" || strings.Contains(rec.Body.String(), "dial tcp") {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
}
