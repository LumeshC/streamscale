package anomaly

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// minutes builds consecutive 1m points with the given event counts.
func minutes(counts ...int64) []Point {
	out := make([]Point, len(counts))
	for i, c := range counts {
		s := t0.Add(time.Duration(i) * time.Minute)
		out[i] = Point{Start: s, End: s.Add(time.Minute), Events: c}
	}
	return out
}

func steady(n int, v int64) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = v + int64(i%3) - 1 // small noise: v-1, v, v+1
	}
	return out
}

func TestEventSpikeAndDrop(t *testing.T) {
	counts := append(steady(30, 1000), 1400, 1000, 500)
	pts := minutes(counts...)
	got := Detect(pts, t0, t0.Add(time.Hour), Options{})
	if len(got) != 2 {
		t.Fatalf("want spike+drop, got %+v", got)
	}
	// newest first
	if got[0].Kind != KindEventRateDrop || got[1].Kind != KindEventRateSpike {
		t.Fatalf("order/kinds: %+v", got)
	}
	if got[1].WindowStart != t0.Add(30*time.Minute) || got[1].Severity != "critical" {
		t.Fatalf("spike: %+v", got[1])
	}
}

func TestPoissonFloorIgnoresSmallWiggleOnFlatBaseline(t *testing.T) {
	flat := make([]int64, 30)
	for i := range flat {
		flat[i] = 100 // stddev 0
	}
	pts := minutes(append(flat, 110)...)
	if got := Detect(pts, t0, t0.Add(time.Hour), Options{}); len(got) != 0 {
		t.Fatalf("+10%% on flat baseline is noise (sqrt(100)=10 → z=1): %+v", got)
	}
	pts = minutes(append(flat, 140)...)
	if got := Detect(pts, t0, t0.Add(time.Hour), Options{}); len(got) != 1 || got[0].ZScore != 4 {
		t.Fatalf("+40%% should be z=4: %+v", got)
	}
}

func TestGapsCountAsZeroEventWindows(t *testing.T) {
	pts := minutes(steady(20, 500)...)
	// Outage: next closed window is 3 minutes later.
	last := pts[len(pts)-1]
	pts = append(pts, Point{Start: last.Start.Add(4 * time.Minute), End: last.Start.Add(5 * time.Minute), Events: 500})
	got := Detect(pts, t0, t0.Add(time.Hour), Options{})
	drops := 0
	for _, a := range got {
		if a.Kind == KindEventRateDrop && a.Observed == 0 {
			drops++
		}
	}
	if drops != 3 {
		t.Fatalf("want 3 zero-minute drops, got %+v", got)
	}
}

func TestNeedsMinimumBaseline(t *testing.T) {
	pts := minutes(append(steady(5, 100), 10000)...)
	if got := Detect(pts, t0, t0.Add(time.Hour), Options{}); len(got) != 0 {
		t.Fatalf("5 baseline windows < MinBaseline 10: %+v", got)
	}
}

func TestOnlyReportsInsideRange(t *testing.T) {
	counts := append(steady(30, 1000), 5000)
	pts := minutes(counts...)
	spikeAt := t0.Add(30 * time.Minute)
	if got := Detect(pts, spikeAt.Add(time.Minute), spikeAt.Add(time.Hour), Options{}); len(got) != 0 {
		t.Fatalf("spike before range must not be reported: %+v", got)
	}
	if got := Detect(pts, spikeAt, spikeAt.Add(time.Minute), Options{}); len(got) != 1 {
		t.Fatalf("spike in range: %+v", got)
	}
}

func TestPaymentFailureSpike(t *testing.T) {
	pts := minutes(steady(30, 1000)...)
	for i := range pts {
		pts[i].PaymentSuccess, pts[i].PaymentFailure = 95, 5 // 5% baseline
	}
	s := t0.Add(30 * time.Minute)
	pts = append(pts,
		Point{Start: s, End: s.Add(time.Minute), Events: 1000, PaymentSuccess: 60, PaymentFailure: 40},
		Point{Start: s.Add(time.Minute), End: s.Add(2 * time.Minute), Events: 1000, PaymentSuccess: 5, PaymentFailure: 5}, // too few payments
	)
	got := Detect(pts, t0, t0.Add(time.Hour), Options{})
	if len(got) != 1 || got[0].Kind != KindPaymentFailureSpike || got[0].Observed != 0.4 || got[0].Expected != 0.05 {
		t.Fatalf("payment spike: %+v", got)
	}
}

func TestMinZRaisesThreshold(t *testing.T) {
	pts := minutes(append(steady(30, 100), 140)...) // z≈4
	if got := Detect(pts, t0, t0.Add(time.Hour), Options{MinZ: 5}); len(got) != 0 {
		t.Fatalf("min_z=5 should suppress z≈4: %+v", got)
	}
}
