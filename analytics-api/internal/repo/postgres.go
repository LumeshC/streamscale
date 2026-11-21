package repo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PoolConfig configures the read pool.
type PoolConfig struct {
	URL              string
	MaxConns         int32
	StatementTimeout time.Duration
}

// NewPool builds a pgx pool. Connections are opened lazily, so the API starts
// (and reports unhealthy) even while PostgreSQL is down. statement_timeout is
// set server-side as a backstop to the per-request context deadline.
func NewPool(ctx context.Context, c PoolConfig) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(c.URL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	cfg.MaxConns = c.MaxConns
	cfg.MinConns = 0
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second
	cfg.ConnConfig.ConnectTimeout = 3 * time.Second
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = strconv.FormatInt(c.StatementTimeout.Milliseconds(), 10)
	cfg.ConnConfig.RuntimeParams["application_name"] = "analytics-api"
	cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	return pgxpool.NewWithConfig(ctx, cfg)
}

// Postgres implements Repository over metrics_windows.
type Postgres struct{ pool *pgxpool.Pool }

func NewPostgres(pool *pgxpool.Pool) *Postgres { return &Postgres{pool: pool} }

func (p *Postgres) Ping(ctx context.Context) error { return wrap("ping", p.pool.Ping(ctx)) }

// wrap annotates err with the operation and tags connection failures.
func wrap(op string, err error) error {
	if err == nil {
		return nil
	}
	var ce *pgconn.ConnectError
	if errors.As(err, &ce) {
		return fmt.Errorf("%s: %w: %w", op, ErrUnavailable, err)
	}
	return fmt.Errorf("%s: %w", op, err)
}

// Window membership everywhere: window_start in [from, to). A window is
// attributed to the range it starts in, so adjacent ranges never double count.

func (p *Postgres) Summary(ctx context.Context, from, to time.Time) (Summary, error) {
	var s Summary
	err := p.pool.QueryRow(ctx, `
		SELECT COUNT(*),
		       COALESCE(SUM(event_count), 0),
		       COALESCE(SUM(purchase_count), 0),
		       COALESCE(SUM(revenue), 0)::numeric(20,2)::text,
		       COALESCE(SUM(payment_success_count), 0),
		       COALESCE(SUM(payment_failure_count), 0),
		       COALESCE(MAX(active_users), 0),
		       MIN(window_start),
		       MAX(window_end)
		FROM metrics_windows
		WHERE spec_name = $1 AND window_start >= $2 AND window_start < $3`,
		summarySpec, from, to,
	).Scan(&s.Windows, &s.Events, &s.Purchases, &s.Revenue, &s.PaymentSuccess,
		&s.PaymentFailure, &s.PeakActiveUsers, &s.FirstStart, &s.LastEnd)
	if err != nil {
		return Summary{}, wrap("summary", err)
	}
	return s, nil
}

const windowCols = `
	window_start, window_end, event_count, purchase_count, revenue::text,
	payment_success_count, payment_failure_count, active_users,
	events_per_second, events_per_minute, purchases_per_minute,
	revenue_per_minute::text, payment_success_rate, payment_failure_rate`

func scanWindows(rows pgx.Rows) ([]Window, error) {
	defer rows.Close()
	var out []Window
	for rows.Next() {
		var w Window
		if err := rows.Scan(&w.Start, &w.End, &w.EventCount, &w.PurchaseCount, &w.Revenue,
			&w.PaymentSuccess, &w.PaymentFailure, &w.ActiveUsers,
			&w.EventsPerSecond, &w.EventsPerMinute, &w.PurchasesPerMinute,
			&w.RevenuePerMinute, &w.PaymentSuccessRate, &w.PaymentFailureRate); err != nil {
			return nil, err
		}
		w.Start, w.End = w.Start.UTC(), w.End.UTC()
		out = append(out, w)
	}
	return out, rows.Err()
}

// Series uses keyset pagination on window_start (unique per spec): stable
// under concurrent inserts and O(limit) per page via metrics_windows_unique.
func (p *Postgres) Series(ctx context.Context, q SeriesQuery) ([]Window, error) {
	spec, ok := specByInterval[q.Interval]
	if !ok {
		return nil, fmt.Errorf("unknown interval %q", q.Interval)
	}
	rows, err := p.pool.Query(ctx, `
		SELECT `+windowCols+`
		FROM metrics_windows
		WHERE spec_name = $1 AND window_start >= $2 AND window_start < $3
		  AND ($4::timestamptz IS NULL OR window_start < $4)
		ORDER BY window_start DESC
		LIMIT $5`,
		spec, q.From, q.To, q.Before, q.Limit+1)
	if err != nil {
		return nil, wrap("series", err)
	}
	out, err := scanWindows(rows)
	if err != nil {
		return nil, wrap("series", err)
	}
	return out, nil
}

func (p *Postgres) Breakdown(ctx context.Context, dim Dimension, from, to time.Time) ([]BreakdownItem, error) {
	var col string
	switch dim { // whitelist: column names cannot be bound parameters
	case DimRegion:
		col = "by_region"
	case DimEventType:
		col = "by_event_type"
	default:
		return nil, fmt.Errorf("unknown dimension %q", dim)
	}
	rows, err := p.pool.Query(ctx, `
		SELECT kv.key, SUM(kv.value::bigint)
		FROM metrics_windows mw
		CROSS JOIN LATERAL jsonb_each_text(mw.`+col+`) AS kv
		WHERE mw.spec_name = $1 AND mw.window_start >= $2 AND mw.window_start < $3
		GROUP BY kv.key
		ORDER BY 2 DESC, 1`,
		summarySpec, from, to)
	if err != nil {
		return nil, wrap("breakdown", err)
	}
	defer rows.Close()
	var out []BreakdownItem
	for rows.Next() {
		var it BreakdownItem
		if err := rows.Scan(&it.Key, &it.Count); err != nil {
			return nil, wrap("breakdown", err)
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

func (p *Postgres) MinuteWindows(ctx context.Context, from, to time.Time) ([]Window, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT `+windowCols+`
		FROM metrics_windows
		WHERE spec_name = $1 AND window_start >= $2 AND window_start < $3
		ORDER BY window_start ASC`,
		summarySpec, from, to)
	if err != nil {
		return nil, wrap("minute windows", err)
	}
	out, err := scanWindows(rows)
	if err != nil {
		return nil, wrap("minute windows", err)
	}
	return out, nil
}

// latencyExpr is end-to-end ingest latency in ms, clamped at 0 (clock skew).
const latencyExpr = `GREATEST(EXTRACT(EPOCH FROM (ingested_at - ts)) * 1000, 0)::float8`

// Latency uses events_ingested_at_idx (migration 0004) to touch only rows
// ingested in the range.
func (p *Postgres) Latency(ctx context.Context, from, to time.Time) (LatencyStats, []LatencyPoint, error) {
	var s LatencyStats
	var pct []float64
	err := p.pool.QueryRow(ctx, `
		SELECT COUNT(*),
		       COALESCE(percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (ORDER BY lat), ARRAY[0, 0, 0]::float8[]),
		       COALESCE(MAX(lat), 0)
		FROM (SELECT `+latencyExpr+` AS lat FROM events WHERE ingested_at >= $1 AND ingested_at < $2) l`,
		from, to).Scan(&s.Count, &pct, &s.Max)
	if err != nil {
		return LatencyStats{}, nil, wrap("latency", err)
	}
	s.P50, s.P95, s.P99 = pct[0], pct[1], pct[2]

	rows, err := p.pool.Query(ctx, `
		SELECT date_trunc('minute', ingested_at) AS m,
		       COUNT(*),
		       percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (ORDER BY lat),
		       MAX(lat)
		FROM (SELECT ingested_at, `+latencyExpr+` AS lat FROM events WHERE ingested_at >= $1 AND ingested_at < $2) l
		GROUP BY 1
		ORDER BY 1`, from, to)
	if err != nil {
		return LatencyStats{}, nil, wrap("latency series", err)
	}
	defer rows.Close()
	var out []LatencyPoint
	for rows.Next() {
		var lp LatencyPoint
		var pp []float64
		if err := rows.Scan(&lp.Minute, &lp.Count, &pp, &lp.Max); err != nil {
			return LatencyStats{}, nil, wrap("latency series", err)
		}
		lp.Minute = lp.Minute.UTC()
		lp.P50, lp.P95, lp.P99 = pp[0], pp[1], pp[2]
		out = append(out, lp)
	}
	if err := rows.Err(); err != nil {
		return LatencyStats{}, nil, wrap("latency series", err)
	}
	return s, out, nil
}

// Errors pages DLQ rows newest first on (updated_at, id) via
// processing_status_dlq_recent_idx.
func (p *Postgres) Errors(ctx context.Context, q ErrorQuery) ([]ErrorRecord, error) {
	var class any
	if q.Class != "" {
		class = q.Class
	}
	rows, err := p.pool.Query(ctx, `
		SELECT id, event_id::text, kafka_topic, kafka_partition, kafka_offset,
		       failure_class, failure_reason, attempts, updated_at
		FROM processing_status
		WHERE status = 'dlq' AND updated_at >= $1 AND updated_at < $2
		  AND ($3::text IS NULL OR failure_class = $3)
		  AND ($4::timestamptz IS NULL OR (updated_at, id) < ($4, $5::bigint))
		ORDER BY updated_at DESC, id DESC
		LIMIT $6`,
		q.From, q.To, class, q.BeforeAt, q.BeforeID, q.Limit+1)
	if err != nil {
		return nil, wrap("errors", err)
	}
	defer rows.Close()
	var out []ErrorRecord
	for rows.Next() {
		var e ErrorRecord
		if err := rows.Scan(&e.ID, &e.EventID, &e.Topic, &e.Partition, &e.Offset,
			&e.FailureClass, &e.FailureReason, &e.Attempts, &e.OccurredAt); err != nil {
			return nil, wrap("errors", err)
		}
		e.OccurredAt = e.OccurredAt.UTC()
		out = append(out, e)
	}
	return out, wrap("errors", rows.Err())
}

func (p *Postgres) ErrorReasons(ctx context.Context, from, to time.Time, class string) ([]ReasonCount, error) {
	var c any
	if class != "" {
		c = class
	}
	rows, err := p.pool.Query(ctx, `
		SELECT COALESCE(failure_class, 'unknown'), COALESCE(failure_reason, 'unknown'), COUNT(*)
		FROM processing_status
		WHERE status = 'dlq' AND updated_at >= $1 AND updated_at < $2
		  AND ($3::text IS NULL OR failure_class = $3)
		GROUP BY 1, 2
		ORDER BY 3 DESC, 1, 2`, from, to, c)
	if err != nil {
		return nil, wrap("error reasons", err)
	}
	defer rows.Close()
	var out []ReasonCount
	for rows.Next() {
		var r ReasonCount
		if err := rows.Scan(&r.Class, &r.Reason, &r.Count); err != nil {
			return nil, wrap("error reasons", err)
		}
		out = append(out, r)
	}
	return out, wrap("error reasons", rows.Err())
}

func (p *Postgres) StatusCounts(ctx context.Context, from, to time.Time) (StatusCounts, error) {
	var s StatusCounts
	err := p.pool.QueryRow(ctx, `
		SELECT COUNT(*) FILTER (WHERE status = 'processed'),
		       COUNT(*) FILTER (WHERE status = 'duplicate'),
		       COUNT(*) FILTER (WHERE status = 'dlq')
		FROM processing_status
		WHERE updated_at >= $1 AND updated_at < $2`, from, to).Scan(&s.Processed, &s.Duplicate, &s.DLQ)
	return s, wrap("status counts", err)
}

var _ Repository = (*Postgres)(nil)
