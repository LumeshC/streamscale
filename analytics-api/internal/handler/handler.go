// Package handler implements the analytics-api REST endpoints.
package handler

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"math/big"
	"net/http"
	"time"

	"github.com/streamscale/streamscale/analytics-api/internal/anomaly"
	"github.com/streamscale/streamscale/analytics-api/internal/httpx"
	"github.com/streamscale/streamscale/analytics-api/internal/kafkalag"
	"github.com/streamscale/streamscale/analytics-api/internal/live"
	"github.com/streamscale/streamscale/analytics-api/internal/repo"
	"github.com/streamscale/streamscale/pkg/event"
)

// Pagination bounds for list endpoints.
const (
	DefaultLimit = 100
	MaxLimit     = 1000
)

// Options configure the API.
type Options struct {
	Repo         repo.Repository
	Live         live.Source
	Lag          kafkalag.Source // consumer lag; nil = disabled
	Log          *slog.Logger
	DefaultRange time.Duration
	MaxRange     time.Duration
	// LatencyMaxRange caps /metrics/latency (percentiles scan raw rows).
	LatencyMaxRange time.Duration
	Now             func() time.Time
	OpenAPI         []byte // served at GET /openapi.yaml
	Anomaly         anomaly.Options
}

// API holds handler dependencies.
type API struct {
	repo       repo.Repository
	live       live.Source
	lag        kafkalag.Source
	log        *slog.Logger
	defRange   time.Duration
	maxRange   time.Duration
	latencyMax time.Duration
	now        func() time.Time
	openapi    []byte
	anomaly    anomaly.Options
}

func New(o Options) *API {
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Live == nil {
		o.Live = live.Disabled{}
	}
	if o.Lag == nil {
		o.Lag = kafkalag.Disabled{}
	}
	if o.LatencyMaxRange <= 0 {
		o.LatencyMaxRange = 3 * time.Hour
	}
	if o.DefaultRange <= 0 {
		o.DefaultRange = time.Hour
	}
	if o.MaxRange <= 0 {
		o.MaxRange = 7 * 24 * time.Hour
	}
	return &API{
		repo: o.Repo, live: o.Live, lag: o.Lag, log: o.Log, latencyMax: o.LatencyMaxRange,
		defRange: o.DefaultRange, maxRange: o.MaxRange,
		now: o.Now, openapi: o.OpenAPI, anomaly: o.Anomaly,
	}
}

// ---- shared response pieces ----

type rangeJSON struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

func rangeOf(tr httpx.TimeRange) rangeJSON { return rangeJSON{From: tr.From, To: tr.To} }

// seriesCursor is the keyset position for window series.
type seriesCursor struct {
	Before time.Time `json:"b"`
}

// ---- GET /health ----

type checkJSON struct {
	Status    string  `json:"status"`
	LatencyMS float64 `json:"latency_ms,omitempty"`
	Error     string  `json:"error,omitempty"`
}

type healthJSON struct {
	Status string               `json:"status"`
	Checks map[string]checkJSON `json:"checks"`
	Time   time.Time            `json:"time"`
}

// Health: 200 "ok" when everything is up, 200 "degraded" when only the
// optional Redis is down, 503 "unavailable" when PostgreSQL (required) is down.
func (a *API) Health(w http.ResponseWriter, r *http.Request) {
	if err := httpx.ParseQuery(r).Err(); err != nil {
		a.fail(w, r, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	out := healthJSON{Status: "ok", Checks: map[string]checkJSON{}, Time: a.now().UTC()}

	pg := probe(ctx, a.repo.Ping)
	out.Checks["postgres"] = pg
	rd := checkJSON{Status: "disabled"}
	if _, off := a.live.(live.Disabled); !off {
		rd = probe(ctx, a.live.Ping)
	}
	out.Checks["redis"] = rd
	kc := checkJSON{Status: "disabled"}
	if _, off := a.lag.(kafkalag.Disabled); !off {
		kc = probe(ctx, a.lag.Ping)
	}
	out.Checks["kafka"] = kc

	// PostgreSQL is required; Redis and Kafka (lag only) are optional.
	status := http.StatusOK
	switch {
	case pg.Status != "ok":
		out.Status, status = "unavailable", http.StatusServiceUnavailable
	case rd.Status == "down" || kc.Status == "down":
		out.Status = "degraded"
	}
	httpx.WriteJSON(w, status, out)
}

func probe(ctx context.Context, fn func(context.Context) error) checkJSON {
	start := time.Now()
	err := fn(ctx)
	lat := math.Round(float64(time.Since(start).Microseconds())/10) / 100
	if err != nil {
		return checkJSON{Status: "down", LatencyMS: lat, Error: err.Error()}
	}
	return checkJSON{Status: "ok", LatencyMS: lat}
}

// ---- GET /metrics/overview ----

type totalsJSON struct {
	Events                   int64   `json:"events"`
	EventsPerMinute          float64 `json:"events_per_minute"`
	Purchases                int64   `json:"purchases"`
	Revenue                  string  `json:"revenue"`
	RevenuePerMinute         string  `json:"revenue_per_minute"`
	PaymentSuccess           int64   `json:"payment_success"`
	PaymentFailure           int64   `json:"payment_failure"`
	PaymentSuccessRate       float64 `json:"payment_success_rate"`
	PaymentFailureRate       float64 `json:"payment_failure_rate"`
	PeakActiveUsersPerMinute int64   `json:"peak_active_users_per_minute"`
	Windows                  int64   `json:"windows"`
}

type coverageJSON struct {
	FirstWindowStart *time.Time `json:"first_window_start"`
	LastWindowEnd    *time.Time `json:"last_window_end"`
}

type liveJSON struct {
	AsOf               time.Time        `json:"as_of"`
	WindowSeconds      int              `json:"window_seconds"`
	Events             int64            `json:"events"`
	EventsPerSecond    float64          `json:"events_per_second"`
	Purchases          int64            `json:"purchases"`
	Revenue            string           `json:"revenue"`
	PaymentSuccessRate float64          `json:"payment_success_rate"`
	ActiveUsers        int64            `json:"active_users"`
	ByType             map[string]int64 `json:"by_event_type"`
	ByRegion           map[string]int64 `json:"by_region"`
}

type overviewJSON struct {
	Range    rangeJSON    `json:"range"`
	Totals   totalsJSON   `json:"totals"`
	Coverage coverageJSON `json:"coverage"`
	Live     *liveJSON    `json:"live"`
	Degraded []string     `json:"degraded"`
}

// Overview combines durable totals (closed 1m windows, PostgreSQL) with the
// optional live view (Redis). Redis failure degrades the response, never fails it.
func (a *API) Overview(w http.ResponseWriter, r *http.Request) {
	q := httpx.ParseQuery(r, "from", "to", "live_window")
	tr := q.Range(a.now(), a.defRange, a.maxRange)
	liveWin := q.Int("live_window", 60, 1, 900)
	if err := q.Err(); err != nil {
		a.fail(w, r, err)
		return
	}
	s, err := a.repo.Summary(r.Context(), tr.From, tr.To)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	out := overviewJSON{
		Range:    rangeOf(tr),
		Totals:   totals(s, tr),
		Coverage: coverageJSON{FirstWindowStart: utcPtr(s.FirstStart), LastWindowEnd: utcPtr(s.LastEnd)},
		Degraded: []string{},
	}
	snap, lerr := a.live.Snapshot(r.Context(), time.Duration(liveWin)*time.Second)
	switch {
	case lerr == nil:
		out.Live = &liveJSON{
			AsOf: snap.AsOf, WindowSeconds: snap.WindowSeconds, Events: snap.Events,
			EventsPerSecond: round(snap.EventsPerSecond, 3), Purchases: snap.Purchases,
			Revenue: snap.Revenue, PaymentSuccessRate: round(snap.PaymentSuccessRate, 4),
			ActiveUsers: snap.ActiveUsers, ByType: snap.ByType, ByRegion: snap.ByRegion,
		}
	case errors.Is(lerr, live.ErrDisabled):
		// live intentionally off: not degraded
	case r.Context().Err() != nil:
		httpx.WriteError(w, r, r.Context().Err())
		return
	default:
		out.Degraded = append(out.Degraded, "live")
		a.log.Warn("live snapshot unavailable", "request_id", httpx.RequestID(r.Context()), "err", lerr)
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

func totals(s repo.Summary, tr httpx.TimeRange) totalsJSON {
	minutes := tr.To.Sub(tr.From).Minutes()
	t := totalsJSON{
		Events: s.Events, Purchases: s.Purchases, Revenue: s.Revenue,
		PaymentSuccess: s.PaymentSuccess, PaymentFailure: s.PaymentFailure,
		PeakActiveUsersPerMinute: s.PeakActiveUsers, Windows: s.Windows,
		EventsPerMinute:  round(float64(s.Events)/minutes, 3),
		RevenuePerMinute: divDecimal(s.Revenue, minutes),
	}
	t.PaymentSuccessRate, t.PaymentFailureRate = rates(s.PaymentSuccess, s.PaymentFailure)
	return t
}

// ---- GET /metrics/events ----

type eventPointJSON struct {
	WindowStart     time.Time `json:"window_start"`
	WindowEnd       time.Time `json:"window_end"`
	Events          int64     `json:"events"`
	EventsPerSecond float64   `json:"events_per_second"`
	EventsPerMinute float64   `json:"events_per_minute"`
	ActiveUsers     int64     `json:"active_users"`
}

type seriesJSON struct {
	Range    rangeJSON  `json:"range"`
	Interval string     `json:"interval"`
	Summary  any        `json:"summary,omitempty"`
	Data     []any      `json:"data"`
	Page     httpx.Page `json:"page"`
}

func (a *API) Events(w http.ResponseWriter, r *http.Request) {
	a.series(w, r, false, func(win repo.Window) any {
		return eventPointJSON{
			WindowStart: win.Start, WindowEnd: win.End, Events: win.EventCount,
			EventsPerSecond: round(win.EventsPerSecond, 3), EventsPerMinute: round(win.EventsPerMinute, 3),
			ActiveUsers: win.ActiveUsers,
		}
	}, nil)
}

// ---- GET /metrics/revenue ----

type revenuePointJSON struct {
	WindowStart        time.Time `json:"window_start"`
	WindowEnd          time.Time `json:"window_end"`
	Purchases          int64     `json:"purchases"`
	Revenue            string    `json:"revenue"`
	PurchasesPerMinute float64   `json:"purchases_per_minute"`
	RevenuePerMinute   string    `json:"revenue_per_minute"`
}

type revenueSummaryJSON struct {
	Purchases         int64  `json:"purchases"`
	Revenue           string `json:"revenue"`
	RevenuePerMinute  string `json:"revenue_per_minute"`
	AverageOrderValue string `json:"average_order_value"`
}

func (a *API) Revenue(w http.ResponseWriter, r *http.Request) {
	a.series(w, r, true, func(win repo.Window) any {
		return revenuePointJSON{
			WindowStart: win.Start, WindowEnd: win.End, Purchases: win.PurchaseCount,
			Revenue: win.Revenue, PurchasesPerMinute: round(win.PurchasesPerMinute, 3),
			RevenuePerMinute: win.RevenuePerMinute,
		}
	}, func(s repo.Summary, tr httpx.TimeRange) any {
		aov := "0.00"
		if s.Purchases > 0 {
			aov = divDecimal(s.Revenue, float64(s.Purchases))
		}
		return revenueSummaryJSON{
			Purchases: s.Purchases, Revenue: s.Revenue,
			RevenuePerMinute:  divDecimal(s.Revenue, tr.To.Sub(tr.From).Minutes()),
			AverageOrderValue: aov,
		}
	})
}

// ---- GET /metrics/payments ----

type paymentPointJSON struct {
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	Success     int64     `json:"success"`
	Failure     int64     `json:"failure"`
	SuccessRate float64   `json:"success_rate"`
	FailureRate float64   `json:"failure_rate"`
}

type paymentSummaryJSON struct {
	Success     int64   `json:"success"`
	Failure     int64   `json:"failure"`
	Total       int64   `json:"total"`
	SuccessRate float64 `json:"success_rate"`
	FailureRate float64 `json:"failure_rate"`
}

func (a *API) Payments(w http.ResponseWriter, r *http.Request) {
	a.series(w, r, true, func(win repo.Window) any {
		return paymentPointJSON{
			WindowStart: win.Start, WindowEnd: win.End,
			Success: win.PaymentSuccess, Failure: win.PaymentFailure,
			SuccessRate: round(win.PaymentSuccessRate, 4), FailureRate: round(win.PaymentFailureRate, 4),
		}
	}, func(s repo.Summary, _ httpx.TimeRange) any {
		sr, fr := rates(s.PaymentSuccess, s.PaymentFailure)
		return paymentSummaryJSON{
			Success: s.PaymentSuccess, Failure: s.PaymentFailure,
			Total: s.PaymentSuccess + s.PaymentFailure, SuccessRate: sr, FailureRate: fr,
		}
	})
}

// series implements the shared paginated time-series endpoints.
func (a *API) series(w http.ResponseWriter, r *http.Request, withSummary bool,
	point func(repo.Window) any, summary func(repo.Summary, httpx.TimeRange) any) {

	q := httpx.ParseQuery(r, "from", "to", "interval", "limit", "cursor")
	tr := q.Range(a.now(), a.defRange, a.maxRange)
	interval := q.Enum("interval", repo.Interval1m, repo.Intervals()...)
	limit := q.Int("limit", DefaultLimit, 1, MaxLimit)
	var cur seriesCursor
	hasCur := q.Cursor(&cur)
	if err := q.Err(); err != nil {
		a.fail(w, r, err)
		return
	}
	sq := repo.SeriesQuery{Interval: interval, From: tr.From, To: tr.To, Limit: limit}
	if hasCur {
		b := cur.Before
		sq.Before = &b
	}
	wins, err := a.repo.Series(r.Context(), sq)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	page := httpx.Page{Limit: limit}
	if len(wins) > limit {
		wins = wins[:limit]
		page.NextCursor = httpx.EncodeCursor(seriesCursor{Before: wins[len(wins)-1].Start})
	}
	data := make([]any, 0, len(wins))
	for _, win := range wins {
		data = append(data, point(win))
	}
	out := seriesJSON{Range: rangeOf(tr), Interval: interval, Data: data, Page: page}
	if withSummary {
		// Summary covers the whole range (all pages), always from tumbling 1m windows.
		s, err := a.repo.Summary(r.Context(), tr.From, tr.To)
		if err != nil {
			a.fail(w, r, err)
			return
		}
		out.Summary = summary(s, tr)
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

// ---- GET /metrics/regions, /metrics/event-types ----

type breakdownItemJSON struct {
	Key    string  `json:"key"`
	Events int64   `json:"events"`
	Share  float64 `json:"share"`
}

type breakdownJSON struct {
	Range       rangeJSON           `json:"range"`
	Dimension   string              `json:"dimension"`
	TotalEvents int64               `json:"total_events"`
	Data        []breakdownItemJSON `json:"data"`
}

// Regions: bounded cardinality (a handful of regions), so not paginated.
func (a *API) Regions(w http.ResponseWriter, r *http.Request) {
	a.breakdown(w, r, repo.DimRegion, nil)
}

// EventTypes always lists every supported type (zero when absent), so clients
// get a stable shape. Optional ?event_type filters to one type.
func (a *API) EventTypes(w http.ResponseWriter, r *http.Request) {
	all := make([]string, 0, len(event.AllTypes))
	for _, t := range event.AllTypes {
		all = append(all, string(t))
	}
	a.breakdown(w, r, repo.DimEventType, all)
}

func (a *API) breakdown(w http.ResponseWriter, r *http.Request, dim repo.Dimension, fixedKeys []string) {
	allowed := []string{"from", "to"}
	if fixedKeys != nil {
		allowed = append(allowed, "event_type")
	}
	q := httpx.ParseQuery(r, allowed...)
	tr := q.Range(a.now(), a.defRange, a.maxRange)
	filter := ""
	if fixedKeys != nil {
		filter = q.Enum("event_type", "", fixedKeys...)
	}
	if err := q.Err(); err != nil {
		a.fail(w, r, err)
		return
	}
	items, err := a.repo.Breakdown(r.Context(), dim, tr.From, tr.To)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	counts := map[string]int64{}
	var total int64
	order := []string{}
	for _, it := range items {
		if _, seen := counts[it.Key]; !seen {
			order = append(order, it.Key)
		}
		counts[it.Key] += it.Count
		total += it.Count
	}
	for _, k := range fixedKeys {
		if _, ok := counts[k]; !ok {
			counts[k] = 0
			order = append(order, k)
		}
	}
	data := make([]breakdownItemJSON, 0, len(order))
	for _, k := range order {
		if filter != "" && k != filter {
			continue
		}
		share := 0.0
		if total > 0 {
			share = round(float64(counts[k])/float64(total), 4)
		}
		data = append(data, breakdownItemJSON{Key: k, Events: counts[k], Share: share})
	}
	httpx.WriteJSON(w, http.StatusOK, breakdownJSON{
		Range: rangeOf(tr), Dimension: string(dim), TotalEvents: total, Data: data,
	})
}

// ---- GET /anomalies ----

type anomalyCursor struct {
	Start time.Time `json:"s"`
	Kind  string    `json:"k"`
}

type detectorJSON struct {
	Method          string  `json:"method"`
	Interval        string  `json:"interval"`
	BaselineWindows int     `json:"baseline_windows"`
	MinZ            float64 `json:"min_z"`
}

type anomaliesJSON struct {
	Range    rangeJSON         `json:"range"`
	Detector detectorJSON      `json:"detector"`
	Data     []anomaly.Anomaly `json:"data"`
	Page     httpx.Page        `json:"page"`
}

func (a *API) Anomalies(w http.ResponseWriter, r *http.Request) {
	kinds := []string{"all"}
	for _, k := range anomaly.Kinds() {
		kinds = append(kinds, string(k))
	}
	q := httpx.ParseQuery(r, "from", "to", "kind", "severity", "min_z", "limit", "cursor")
	tr := q.Range(a.now(), a.defRange, a.maxRange)
	kind := q.Enum("kind", "all", kinds...)
	severity := q.Enum("severity", "all", "all", "warning", "critical")
	minZ := q.Float("min_z", 3, 2, 10)
	limit := q.Int("limit", DefaultLimit, 1, MaxLimit)
	var cur anomalyCursor
	hasCur := q.Cursor(&cur)
	if err := q.Err(); err != nil {
		a.fail(w, r, err)
		return
	}

	opt := a.anomaly
	opt.MinZ = minZ
	from := tr.From.Add(-anomaly.BaselineLookback(opt))
	wins, err := a.repo.MinuteWindows(r.Context(), from, tr.To)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	pts := make([]anomaly.Point, len(wins))
	for i, win := range wins {
		pts[i] = anomaly.Point{Start: win.Start, End: win.End, Events: win.EventCount,
			PaymentSuccess: win.PaymentSuccess, PaymentFailure: win.PaymentFailure}
	}
	found := anomaly.Detect(pts, tr.From, tr.To, opt)

	// Filter, then keyset-paginate on (window_start desc, kind asc).
	data := make([]anomaly.Anomaly, 0, limit+1)
	for _, an := range found {
		if kind != "all" && string(an.Kind) != kind {
			continue
		}
		if severity != "all" && an.Severity != severity {
			continue
		}
		if hasCur && !afterCursor(an, cur) {
			continue
		}
		data = append(data, an)
		if len(data) > limit {
			break
		}
	}
	page := httpx.Page{Limit: limit}
	if len(data) > limit {
		data = data[:limit]
		last := data[len(data)-1]
		page.NextCursor = httpx.EncodeCursor(anomalyCursor{Start: last.WindowStart, Kind: string(last.Kind)})
	}
	httpx.WriteJSON(w, http.StatusOK, anomaliesJSON{
		Range: rangeOf(tr),
		Detector: detectorJSON{Method: "rolling_zscore", Interval: repo.Interval1m,
			BaselineWindows: opt.Effective().BaselineWindows, MinZ: minZ},
		Data: data, Page: page,
	})
}

func afterCursor(an anomaly.Anomaly, c anomalyCursor) bool {
	if an.WindowStart.Before(c.Start) {
		return true
	}
	return an.WindowStart.Equal(c.Start) && string(an.Kind) > c.Kind
}

// ---- GET /openapi.yaml ----

func (a *API) OpenAPI(w http.ResponseWriter, r *http.Request) {
	if err := httpx.ParseQuery(r).Err(); err != nil {
		a.fail(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/yaml")
	_, _ = w.Write(a.openapi)
}

// ---- helpers ----

// fail writes the error envelope, mapping repository outages to 503.
func (a *API) fail(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, repo.ErrUnavailable) {
		err = httpx.Unavailable("analytics database is unavailable", err)
	}
	httpx.WriteError(w, r, err)
}

func rates(ok, fail int64) (float64, float64) {
	n := ok + fail
	if n == 0 {
		return 0, 0
	}
	return round(float64(ok)/float64(n), 4), round(float64(fail)/float64(n), 4)
}

func round(v float64, places int) float64 {
	m := math.Pow(10, float64(places))
	return math.Round(v*m) / m
}

// divDecimal divides a decimal string exactly and formats to 2 places.
func divDecimal(dec string, by float64) string {
	r, ok := new(big.Rat).SetString(dec)
	if !ok || by <= 0 {
		return "0.00"
	}
	d := new(big.Rat).SetFloat64(by)
	if d == nil || d.Sign() == 0 {
		return "0.00"
	}
	return r.Quo(r, d).FloatString(2)
}

func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}
