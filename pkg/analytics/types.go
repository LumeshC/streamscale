package analytics

import (
	"math/big"
	"strings"
	"time"
)

// Kind identifies the window assignment strategy.
type Kind string

const (
	KindTumbling Kind = "tumbling"
	KindSliding  Kind = "sliding"
)

// Spec describes a window definition. Slide is ignored for tumbling (equals Size).
type Spec struct {
	Name  string        `json:"name"`
	Kind  Kind          `json:"kind"`
	Size  time.Duration `json:"size"`
	Slide time.Duration `json:"slide,omitempty"`
}

// Interval is a half-open time range [Start, End).
type Interval struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

func (i Interval) Duration() time.Duration {
	return i.End.Sub(i.Start)
}

func (i Interval) Contains(t time.Time) bool {
	return !t.Before(i.Start) && t.Before(i.End)
}

// Metrics are the Phase 3 stream analytics aggregates for one window instance.
type Metrics struct {
	EventCount          int64            `json:"event_count"`
	PurchaseCount       int64            `json:"purchase_count"`
	Revenue             string           `json:"revenue"` // decimal string, sum of purchase amounts in window
	PaymentSuccessCount int64            `json:"payment_success_count"`
	PaymentFailureCount int64            `json:"payment_failure_count"`
	ActiveUsers         int64            `json:"active_users"`
	ByRegion            map[string]int64 `json:"by_region"`
	ByEventType         map[string]int64 `json:"by_event_type"`
}

// Rates are derived analytics views over a window.
type Rates struct {
	EventsPerSecond    float64 `json:"events_per_second"`
	EventsPerMinute    float64 `json:"events_per_minute"`
	PurchasesPerMinute float64 `json:"purchases_per_minute"`
	RevenuePerMinute   string  `json:"revenue_per_minute"`
	PaymentSuccessRate float64 `json:"payment_success_rate"`
	PaymentFailureRate float64 `json:"payment_failure_rate"`
}

// Rates derives rate metrics from raw counts and window duration.
func (m Metrics) Rates(window time.Duration) Rates {
	sec := window.Seconds()
	if sec <= 0 {
		sec = 1
	}
	min := window.Minutes()
	if min <= 0 {
		min = sec / 60
		if min <= 0 {
			min = 1
		}
	}
	payments := m.PaymentSuccessCount + m.PaymentFailureCount
	var successRate, failureRate float64
	if payments > 0 {
		successRate = float64(m.PaymentSuccessCount) / float64(payments)
		failureRate = float64(m.PaymentFailureCount) / float64(payments)
	}
	return Rates{
		EventsPerSecond:    float64(m.EventCount) / sec,
		EventsPerMinute:    float64(m.EventCount) / min,
		PurchasesPerMinute: float64(m.PurchaseCount) / min,
		RevenuePerMinute:   scaleRevenuePerMinute(m.Revenue, min),
		PaymentSuccessRate: successRate,
		PaymentFailureRate: failureRate,
	}
}

func scaleRevenuePerMinute(revenue string, minutes float64) string {
	revenue = strings.TrimSpace(revenue)
	if revenue == "" || minutes <= 0 {
		return "0.00"
	}
	r := new(big.Rat)
	if _, ok := r.SetString(revenue); !ok {
		return "0.00"
	}
	div := new(big.Rat).SetFloat64(minutes)
	if div.Sign() == 0 {
		return "0.00"
	}
	r.Quo(r, div)
	return r.FloatString(2)
}

// ClosedWindow is emitted when a window is finalized past the watermark.
type ClosedWindow struct {
	Spec     Spec      `json:"spec"`
	Window   Interval  `json:"window"`
	Metrics  Metrics   `json:"metrics"`
	Rates    Rates     `json:"rates"`
	ClosedAt time.Time `json:"closed_at"`
}
