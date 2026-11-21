// Package anomaly implements on-read, rule-based anomaly detection over closed
// 1-minute analytics windows.
//
// Scope: this is a stateless statistical detector evaluated per request — no
// models, no alerting, no stored anomaly table. It sits behind Detector so a
// later streaming detection pipeline can replace it without changing the API.
package anomaly

import (
	"math"
	"sort"
	"time"
)

// Kind is the anomaly type.
type Kind string

const (
	KindEventRateSpike      Kind = "event_rate_spike"
	KindEventRateDrop       Kind = "event_rate_drop"
	KindPaymentFailureSpike Kind = "payment_failure_spike"
)

// Kinds lists valid kinds (for validation and docs).
func Kinds() []Kind {
	return []Kind{KindEventRateSpike, KindEventRateDrop, KindPaymentFailureSpike}
}

// Point is one 1-minute window's inputs.
type Point struct {
	Start, End     time.Time
	Events         int64
	PaymentSuccess int64
	PaymentFailure int64
}

// Anomaly is one detected deviation.
type Anomaly struct {
	ID          string    `json:"id"`
	Kind        Kind      `json:"kind"`
	Metric      string    `json:"metric"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	Observed    float64   `json:"observed"`
	Expected    float64   `json:"expected"`
	StdDev      float64   `json:"std_dev"`
	ZScore      float64   `json:"z_score"`
	Severity    string    `json:"severity"`
}

// Options tune the detector.
type Options struct {
	BaselineWindows int     // trailing windows forming the baseline (default 30)
	MinBaseline     int     // windows required before judging (default 10)
	MinZ            float64 // |z| threshold (default 3)
	CriticalZ       float64 // |z| at which severity becomes critical (default 5)
	MinPayments     int64   // payments in a window to judge its failure rate (default 20)
	Step            time.Duration
}

func (o Options) withDefaults() Options {
	if o.BaselineWindows <= 0 {
		o.BaselineWindows = 30
	}
	if o.MinBaseline <= 0 {
		o.MinBaseline = 10
	}
	if o.MinBaseline > o.BaselineWindows {
		o.MinBaseline = o.BaselineWindows
	}
	if o.MinZ <= 0 {
		o.MinZ = 3
	}
	if o.CriticalZ <= 0 {
		o.CriticalZ = 5
	}
	if o.MinPayments <= 0 {
		o.MinPayments = 20
	}
	if o.Step <= 0 {
		o.Step = time.Minute
	}
	return o
}

// Detect scans points (any order) and returns anomalies for windows starting
// in [from, to), newest first. Points before `from` serve only as baseline.
//
// Missing minutes between the first and last point are treated as zero-event
// windows: no closed window means no events, which is exactly an event-rate
// drop (e.g. producer or processor outage).
//
// Event rate: z = (x - mean) / sd over the trailing baseline, with
// sd floored at max(sqrt(mean), 1) — the Poisson noise level — so a perfectly
// flat baseline does not turn a one-event wiggle into an "anomaly".
//
// Payment failure rate: compares the window's failure proportion to the
// pooled baseline proportion p, using the binomial standard error
// sqrt(p(1-p)/n) floored at 0.02.
func Detect(points []Point, from, to time.Time, opt Options) []Anomaly {
	opt = opt.withDefaults()
	series := fillGaps(points, opt.Step)
	var out []Anomaly
	for i, cur := range series {
		if cur.Start.Before(from) || !cur.Start.Before(to) {
			continue
		}
		lo := i - opt.BaselineWindows
		if lo < 0 {
			lo = 0
		}
		base := series[lo:i]
		if len(base) < opt.MinBaseline {
			continue
		}
		if a, ok := eventRate(cur, base, opt); ok {
			out = append(out, a)
		}
		if a, ok := paymentFailure(cur, base, opt); ok {
			out = append(out, a)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].WindowStart.Equal(out[j].WindowStart) {
			return out[i].WindowStart.After(out[j].WindowStart)
		}
		return out[i].Kind < out[j].Kind
	})
	return out
}

func eventRate(cur Point, base []Point, opt Options) (Anomaly, bool) {
	var sum, sq float64
	for _, p := range base {
		sum += float64(p.Events)
	}
	mean := sum / float64(len(base))
	for _, p := range base {
		d := float64(p.Events) - mean
		sq += d * d
	}
	sd := math.Sqrt(sq / float64(len(base)))
	sd = math.Max(sd, math.Max(math.Sqrt(mean), 1))
	z := (float64(cur.Events) - mean) / sd
	var kind Kind
	switch {
	case z >= opt.MinZ:
		kind = KindEventRateSpike
	case z <= -opt.MinZ:
		kind = KindEventRateDrop
	default:
		return Anomaly{}, false
	}
	return build(kind, "events_per_minute", cur, float64(cur.Events), mean, sd, z, opt), true
}

func paymentFailure(cur Point, base []Point, opt Options) (Anomaly, bool) {
	n := cur.PaymentSuccess + cur.PaymentFailure
	if n < opt.MinPayments {
		return Anomaly{}, false
	}
	var fail, total int64
	for _, p := range base {
		fail += p.PaymentFailure
		total += p.PaymentSuccess + p.PaymentFailure
	}
	if total < opt.MinPayments {
		return Anomaly{}, false
	}
	p := float64(fail) / float64(total)
	se := math.Max(math.Sqrt(p*(1-p)/float64(n)), 0.02)
	rate := float64(cur.PaymentFailure) / float64(n)
	z := (rate - p) / se
	if z < opt.MinZ {
		return Anomaly{}, false
	}
	return build(KindPaymentFailureSpike, "payment_failure_rate", cur, rate, p, se, z, opt), true
}

func build(kind Kind, metric string, cur Point, observed, expected, sd, z float64, opt Options) Anomaly {
	sev := "warning"
	if math.Abs(z) >= opt.CriticalZ {
		sev = "critical"
	}
	return Anomaly{
		ID:          string(kind) + ":" + cur.Start.UTC().Format(time.RFC3339),
		Kind:        kind,
		Metric:      metric,
		WindowStart: cur.Start.UTC(),
		WindowEnd:   cur.End.UTC(),
		Observed:    round(observed, 4),
		Expected:    round(expected, 4),
		StdDev:      round(sd, 4),
		ZScore:      round(z, 2),
		Severity:    sev,
	}
}

func fillGaps(points []Point, step time.Duration) []Point {
	if len(points) == 0 {
		return nil
	}
	sorted := append([]Point(nil), points...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Start.Before(sorted[j].Start) })
	out := make([]Point, 0, len(sorted))
	for i, p := range sorted {
		if i > 0 {
			for t := out[len(out)-1].Start.Add(step); t.Before(p.Start); t = t.Add(step) {
				out = append(out, Point{Start: t, End: t.Add(step)})
			}
		}
		out = append(out, p)
	}
	return out
}

func round(v float64, places int) float64 {
	m := math.Pow(10, float64(places))
	return math.Round(v*m) / m
}

// Effective returns the options with defaults applied.
func (o Options) Effective() Options { return o.withDefaults() }

// BaselineLookback is how far before `from` callers must fetch points.
func BaselineLookback(opt Options) time.Duration {
	opt = opt.withDefaults()
	return time.Duration(opt.BaselineWindows) * opt.Step
}
