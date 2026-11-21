// Package repo is the analytics-api read model over PostgreSQL. It reads only
// closed analytics windows (metrics_windows) written by the stream-processor;
// it never writes and never scans the raw events table.
package repo

import (
	"context"
	"errors"
	"time"
)

// ErrUnavailable marks errors caused by PostgreSQL being unreachable (as
// opposed to a bad query), so the API can answer 503 instead of 500.
var ErrUnavailable = errors.New("database unavailable")

// Interval names exposed by the API, mapped to the processor's window specs
// (pkg/analytics.DefaultSpecs).
const (
	Interval1s = "1s" // tumbling 1s  → eps_1s
	Interval1m = "1m" // tumbling 1m  → metrics_1m
	Interval5m = "5m" // sliding 5m/1m → metrics_5m_slide_1m
)

var specByInterval = map[string]string{
	Interval1s: "eps_1s",
	Interval1m: "metrics_1m",
	Interval5m: "metrics_5m_slide_1m",
}

// Intervals lists valid ?interval values.
func Intervals() []string { return []string{Interval1s, Interval1m, Interval5m} }

// summarySpec is used for every total/breakdown. It must be a *tumbling* spec:
// sliding windows overlap, so summing them would count each event up to 5x.
const summarySpec = "metrics_1m"

// Dimension is a breakdown axis stored as JSONB on metrics_windows.
type Dimension string

const (
	DimRegion    Dimension = "region"
	DimEventType Dimension = "event_type"
)

// Window is one closed analytics window.
type Window struct {
	Start              time.Time
	End                time.Time
	EventCount         int64
	PurchaseCount      int64
	Revenue            string
	PaymentSuccess     int64
	PaymentFailure     int64
	ActiveUsers        int64
	EventsPerSecond    float64
	EventsPerMinute    float64
	PurchasesPerMinute float64
	RevenuePerMinute   string
	PaymentSuccessRate float64
	PaymentFailureRate float64
}

// Summary aggregates tumbling 1m windows over a range.
type Summary struct {
	Windows         int64
	Events          int64
	Purchases       int64
	Revenue         string
	PaymentSuccess  int64
	PaymentFailure  int64
	PeakActiveUsers int64
	FirstStart      *time.Time
	LastEnd         *time.Time
}

// SeriesQuery selects a page of windows, newest first.
type SeriesQuery struct {
	Interval string
	From, To time.Time
	Before   *time.Time // keyset cursor: only windows starting before this
	Limit    int        // rows to return; the repo fetches Limit+1 to detect more
}

// BreakdownItem is the total for one key of a dimension.
type BreakdownItem struct {
	Key   string
	Count int64
}

// LatencyStats are end-to-end ingest latency percentiles (ingested_at - ts),
// in milliseconds, for events ingested in a period.
type LatencyStats struct {
	Count int64
	P50   float64
	P95   float64
	P99   float64
	Max   float64
}

// LatencyPoint is one minute bucket of LatencyStats.
type LatencyPoint struct {
	Minute time.Time
	LatencyStats
}

// ErrorRecord is one dead-lettered message (processing_status.status = 'dlq').
type ErrorRecord struct {
	ID            int64
	EventID       *string
	Topic         string
	Partition     int32
	Offset        int64
	FailureClass  *string
	FailureReason *string
	Attempts      int32
	OccurredAt    time.Time
}

// ErrorQuery selects a page of DLQ records, newest first.
type ErrorQuery struct {
	From, To time.Time
	Class    string // "" = all
	// Keyset cursor (both set or both nil).
	BeforeAt *time.Time
	BeforeID *int64
	Limit    int
}

// ReasonCount is DLQ volume for one (class, reason).
type ReasonCount struct {
	Class  string
	Reason string
	Count  int64
}

// StatusCounts are processing outcomes in a period.
type StatusCounts struct {
	Processed int64
	Duplicate int64
	DLQ       int64
}

// Repository is what handlers depend on (fakes in unit tests, Postgres in prod).
type Repository interface {
	Ping(ctx context.Context) error
	Summary(ctx context.Context, from, to time.Time) (Summary, error)
	// Series returns up to q.Limit+1 windows; a Limit+1th row means "more".
	Series(ctx context.Context, q SeriesQuery) ([]Window, error)
	Breakdown(ctx context.Context, dim Dimension, from, to time.Time) ([]BreakdownItem, error)
	// MinuteWindows returns tumbling 1m windows in [from, to), oldest first.
	MinuteWindows(ctx context.Context, from, to time.Time) ([]Window, error)
	// Latency returns overall stats and per-minute buckets for events ingested in [from, to).
	Latency(ctx context.Context, from, to time.Time) (LatencyStats, []LatencyPoint, error)
	// Errors returns up to q.Limit+1 DLQ records (a Limit+1th row means "more").
	Errors(ctx context.Context, q ErrorQuery) ([]ErrorRecord, error)
	ErrorReasons(ctx context.Context, from, to time.Time, class string) ([]ReasonCount, error)
	StatusCounts(ctx context.Context, from, to time.Time) (StatusCounts, error)
}
