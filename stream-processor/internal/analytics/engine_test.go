package analytics

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/streamscale/streamscale/pkg/analytics"
	"github.com/streamscale/streamscale/pkg/event"
)

type memSink struct {
	closed []analytics.ClosedWindow
}

func (m *memSink) PersistClosed(_ context.Context, windows []analytics.ClosedWindow) error {
	m.closed = append(m.closed, windows...)
	return nil
}

func baseEvent(id, user string, ts time.Time, typ event.Type) event.Event {
	e := event.Event{
		EventID:   id,
		EventType: typ,
		UserID:    user,
		Timestamp: ts,
		Region:    "us-east-1",
		Metadata:  map[string]string{"source": "test"},
	}
	if typ.RequiresCommerce() {
		e.ProductID = "sku"
		e.Amount = "10.00"
		e.Currency = "USD"
	}
	return e
}

func TestTumblingMetricsAndClose(t *testing.T) {
	sink := &memSink{}
	eng := NewMemoryEngine(Config{
		Specs: []analytics.Spec{
			{Name: "m1s", Kind: analytics.KindTumbling, Size: time.Second, Slide: time.Second},
		},
		AllowedLateness: 0,
		Sink:            sink,
	})
	t0 := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	ctx := context.Background()

	_, err := eng.OnEvent(ctx, baseEvent("e1", "u1", t0, event.TypePageView))
	if err != nil {
		t.Fatal(err)
	}
	_, err = eng.OnEvent(ctx, baseEvent("e2", "u2", t0.Add(200*time.Millisecond), event.TypePurchase))
	if err != nil {
		t.Fatal(err)
	}

	snaps := eng.OpenSnapshots()
	if len(snaps) != 1 || snaps[0].Metrics.EventCount != 2 {
		t.Fatalf("open=%+v", snaps)
	}
	if snaps[0].Metrics.PurchaseCount != 1 || snaps[0].Metrics.ActiveUsers != 2 {
		t.Fatalf("metrics=%+v", snaps[0].Metrics)
	}
	if snaps[0].Rates.EventsPerSecond != 2 {
		t.Fatalf("eps=%v", snaps[0].Rates.EventsPerSecond)
	}

	closed, err := eng.OnEvent(ctx, baseEvent("e3", "u1", t0.Add(time.Second), event.TypeLogin))
	if err != nil {
		t.Fatal(err)
	}
	if len(closed) != 1 {
		t.Fatalf("closed=%d sink=%d", len(closed), len(sink.closed))
	}
	if closed[0].Metrics.EventCount != 2 {
		t.Fatalf("closed metrics=%+v", closed[0].Metrics)
	}
	if closed[0].Metrics.Revenue != "10.00" {
		t.Fatalf("revenue=%s", closed[0].Metrics.Revenue)
	}
}

func TestSlidingWindowsAccumulate(t *testing.T) {
	eng := NewMemoryEngine(Config{
		Specs: []analytics.Spec{
			{Name: "s5", Kind: analytics.KindSliding, Size: 5 * time.Minute, Slide: time.Minute},
		},
		AllowedLateness: time.Hour,
		Sink:            &memSink{},
	})
	ts := time.Date(2026, 9, 18, 12, 3, 0, 0, time.UTC)
	_, err := eng.OnEvent(context.Background(), baseEvent("e1", "u1", ts, event.TypeSearch))
	if err != nil {
		t.Fatal(err)
	}
	snaps := eng.OpenSnapshots()
	if len(snaps) != 5 {
		t.Fatalf("expected 5 sliding windows, got %d", len(snaps))
	}
	for _, s := range snaps {
		if s.Metrics.EventCount != 1 {
			t.Fatalf("each sliding window should see the event once: %+v", s)
		}
	}
}

func TestLateEventsDropped(t *testing.T) {
	sink := &memSink{}
	eng := NewMemoryEngine(Config{
		Specs: []analytics.Spec{
			{Name: "m1m", Kind: analytics.KindTumbling, Size: time.Minute, Slide: time.Minute},
		},
		AllowedLateness: 10 * time.Second,
		Sink:            sink,
	})
	ctx := context.Background()
	t0 := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)

	if _, err := eng.OnEvent(ctx, baseEvent("e1", "u1", t0, event.TypeLogin)); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.OnEvent(ctx, baseEvent("e2", "u1", t0.Add(2*time.Minute), event.TypeLogout)); err != nil {
		t.Fatal(err)
	}
	lateTS := t0.Add(30 * time.Second)
	if _, err := eng.OnEvent(ctx, baseEvent("e3", "u9", lateTS, event.TypePageView)); err != nil {
		t.Fatal(err)
	}
	if eng.LateDropCount() != 1 {
		t.Fatalf("late drops=%d", eng.LateDropCount())
	}
	for _, s := range eng.OpenSnapshots() {
		if s.Window.Contains(lateTS) && s.Metrics.ByEventType[string(event.TypePageView)] > 0 {
			t.Fatalf("late event incorrectly applied: %+v", s)
		}
	}
}

func TestPaymentRates(t *testing.T) {
	eng := NewMemoryEngine(Config{
		Specs: []analytics.Spec{
			{Name: "m1m", Kind: analytics.KindTumbling, Size: time.Minute, Slide: time.Minute},
		},
		AllowedLateness: time.Hour,
		Sink:            &memSink{},
	})
	ts := time.Date(2026, 9, 18, 12, 0, 10, 0, time.UTC)
	ctx := context.Background()
	for i, typ := range []event.Type{event.TypePaymentSuccess, event.TypePaymentSuccess, event.TypePaymentFailure} {
		e := baseEvent(fmt.Sprintf("pay-%d", i), "u1", ts, typ)
		if _, err := eng.OnEvent(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	snaps := eng.OpenSnapshots()
	if len(snaps) != 1 {
		t.Fatalf("snaps=%d", len(snaps))
	}
	r := snaps[0].Rates
	if r.PaymentSuccessRate < 0.66 || r.PaymentSuccessRate > 0.67 {
		t.Fatalf("success rate=%v", r.PaymentSuccessRate)
	}
	if r.PaymentFailureRate < 0.33 || r.PaymentFailureRate > 0.34 {
		t.Fatalf("failure rate=%v", r.PaymentFailureRate)
	}
}

func TestByRegionAndType(t *testing.T) {
	eng := NewMemoryEngine(Config{
		Specs: []analytics.Spec{
			{Name: "m1m", Kind: analytics.KindTumbling, Size: time.Minute, Slide: time.Minute},
		},
		AllowedLateness: time.Hour,
		Sink:            &memSink{},
	})
	ts := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	e1 := baseEvent("a", "u1", ts, event.TypeLogin)
	e1.Region = "eu-west-1"
	e2 := baseEvent("b", "u2", ts.Add(time.Second), event.TypeSearch)
	e2.Region = "us-east-1"
	ctx := context.Background()
	_, _ = eng.OnEvent(ctx, e1)
	_, _ = eng.OnEvent(ctx, e2)
	m := eng.OpenSnapshots()[0].Metrics
	if m.ByRegion["eu-west-1"] != 1 || m.ByRegion["us-east-1"] != 1 {
		t.Fatalf("%+v", m.ByRegion)
	}
	if m.ByEventType["LOGIN"] != 1 || m.ByEventType["SEARCH"] != 1 {
		t.Fatalf("%+v", m.ByEventType)
	}
}

func TestWindowBoundaryHalfOpen(t *testing.T) {
	eng := NewMemoryEngine(Config{
		Specs: []analytics.Spec{
			{Name: "m1s", Kind: analytics.KindTumbling, Size: time.Second, Slide: time.Second},
		},
		AllowedLateness: 0,
		Sink:            &memSink{},
	})
	t0 := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	ctx := context.Background()
	_, _ = eng.OnEvent(ctx, baseEvent("a", "u1", t0, event.TypeLogin))
	closed, err := eng.OnEvent(ctx, baseEvent("b", "u1", t0.Add(time.Second), event.TypeLogin))
	if err != nil {
		t.Fatal(err)
	}
	if len(closed) != 1 || closed[0].Metrics.EventCount != 1 {
		t.Fatalf("closed=%+v", closed)
	}
	open := eng.OpenSnapshots()
	if len(open) != 1 || open[0].Metrics.EventCount != 1 {
		t.Fatalf("open=%+v", open)
	}
}

func TestDuplicatesDoNotDoubleCountViaEngine(t *testing.T) {
	// Engine itself is not idempotent by event_id; the processor must call it once.
	// This test documents expected single-call semantics for a unique event.
	eng := NewMemoryEngine(Config{
		Specs: []analytics.Spec{
			{Name: "m1m", Kind: analytics.KindTumbling, Size: time.Minute, Slide: time.Minute},
		},
		AllowedLateness: time.Hour,
		Sink:            &memSink{},
	})
	e := baseEvent("same", "u1", time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC), event.TypeLogin)
	_, _ = eng.OnEvent(context.Background(), e)
	_, _ = eng.OnEvent(context.Background(), e) // accidental double call
	if eng.OpenSnapshots()[0].Metrics.EventCount != 2 {
		t.Fatal("engine counts every OnEvent; processor must gate on OutcomeInserted")
	}
}
