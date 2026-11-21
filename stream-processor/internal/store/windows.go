package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/streamscale/streamscale/pkg/analytics"
	"github.com/streamscale/streamscale/stream-processor/internal/classify"
)

// WindowSink persists closed analytics windows to PostgreSQL.
type WindowSink struct {
	pg *Postgres
}

func NewWindowSink(pg *Postgres) *WindowSink {
	return &WindowSink{pg: pg}
}

func (s *WindowSink) PersistClosed(ctx context.Context, windows []analytics.ClosedWindow) error {
	if s == nil || s.pg == nil || len(windows) == 0 {
		return nil
	}
	for _, w := range windows {
		if err := s.upsert(ctx, w); err != nil {
			return err
		}
	}
	return nil
}

func (s *WindowSink) upsert(ctx context.Context, w analytics.ClosedWindow) error {
	byRegion, err := json.Marshal(w.Metrics.ByRegion)
	if err != nil {
		return classify.Permanent("window_region_json", err)
	}
	byType, err := json.Marshal(w.Metrics.ByEventType)
	if err != nil {
		return classify.Permanent("window_type_json", err)
	}
	slide := w.Spec.Slide
	if slide <= 0 {
		slide = w.Spec.Size
	}
	_, err = s.pg.pool.Exec(ctx, `
		INSERT INTO metrics_windows (
			spec_name, window_kind, window_size_ms, window_slide_ms,
			window_start, window_end,
			event_count, purchase_count, revenue,
			payment_success_count, payment_failure_count, active_users,
			events_per_second, events_per_minute, purchases_per_minute,
			revenue_per_minute, payment_success_rate, payment_failure_rate,
			by_region, by_event_type, closed_at
		) VALUES (
			$1, $2, $3, $4,
			$5, $6,
			$7, $8, $9::numeric,
			$10, $11, $12,
			$13, $14, $15,
			$16::numeric, $17, $18,
			$19::jsonb, $20::jsonb, $21
		)
		ON CONFLICT (spec_name, window_start, window_end) DO UPDATE SET
			event_count = EXCLUDED.event_count,
			purchase_count = EXCLUDED.purchase_count,
			revenue = EXCLUDED.revenue,
			payment_success_count = EXCLUDED.payment_success_count,
			payment_failure_count = EXCLUDED.payment_failure_count,
			active_users = EXCLUDED.active_users,
			events_per_second = EXCLUDED.events_per_second,
			events_per_minute = EXCLUDED.events_per_minute,
			purchases_per_minute = EXCLUDED.purchases_per_minute,
			revenue_per_minute = EXCLUDED.revenue_per_minute,
			payment_success_rate = EXCLUDED.payment_success_rate,
			payment_failure_rate = EXCLUDED.payment_failure_rate,
			by_region = EXCLUDED.by_region,
			by_event_type = EXCLUDED.by_event_type,
			closed_at = EXCLUDED.closed_at
	`,
		w.Spec.Name, string(w.Spec.Kind), w.Spec.Size.Milliseconds(), slide.Milliseconds(),
		w.Window.Start, w.Window.End,
		w.Metrics.EventCount, w.Metrics.PurchaseCount, nullDecimal(w.Metrics.Revenue),
		w.Metrics.PaymentSuccessCount, w.Metrics.PaymentFailureCount, w.Metrics.ActiveUsers,
		w.Rates.EventsPerSecond, w.Rates.EventsPerMinute, w.Rates.PurchasesPerMinute,
		nullDecimal(w.Rates.RevenuePerMinute), w.Rates.PaymentSuccessRate, w.Rates.PaymentFailureRate,
		byRegion, byType, w.ClosedAt,
	)
	if err != nil {
		return classify.FromStore(fmt.Errorf("upsert metrics_windows: %w", err))
	}
	return nil
}

func nullDecimal(s string) string {
	if s == "" {
		return "0"
	}
	return s
}

func (p *Postgres) CountWindows(ctx context.Context) (int64, error) {
	var n int64
	err := p.pool.QueryRow(ctx, `SELECT COUNT(*) FROM metrics_windows`).Scan(&n)
	return n, err
}
