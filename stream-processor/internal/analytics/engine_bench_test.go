package analytics

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/streamscale/streamscale/pkg/analytics"
	"github.com/streamscale/streamscale/pkg/event"
)

// BenchmarkOnEvent measures in-process windowing overhead (no Kafka, no Postgres).
func BenchmarkOnEvent(b *testing.B) {
	eng := NewMemoryEngine(Config{
		Specs:           analytics.DefaultSpecs(),
		AllowedLateness: 30 * time.Second,
		Sink:            analytics.NopSink{},
	})
	ctx := context.Background()
	base := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	types := []event.Type{
		event.TypePageView, event.TypeSearch, event.TypePurchase,
		event.TypePaymentSuccess, event.TypePaymentFailure, event.TypeLogin,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts := base.Add(time.Duration(i) * time.Millisecond)
		e := baseEvent(fmt.Sprintf("id-%d", i), fmt.Sprintf("user-%d", i%1000), ts, types[i%len(types)])
		if _, err := eng.OnEvent(ctx, e); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOnEventTumblingOnly(b *testing.B) {
	eng := NewMemoryEngine(Config{
		Specs: []analytics.Spec{
			{Name: "m1s", Kind: analytics.KindTumbling, Size: time.Second, Slide: time.Second},
			{Name: "m1m", Kind: analytics.KindTumbling, Size: time.Minute, Slide: time.Minute},
		},
		AllowedLateness: 30 * time.Second,
		Sink:            analytics.NopSink{},
	})
	ctx := context.Background()
	base := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := baseEvent(fmt.Sprintf("id-%d", i), fmt.Sprintf("u-%d", i%100), base.Add(time.Duration(i)*time.Millisecond), event.TypePageView)
		if _, err := eng.OnEvent(ctx, e); err != nil {
			b.Fatal(err)
		}
	}
}
