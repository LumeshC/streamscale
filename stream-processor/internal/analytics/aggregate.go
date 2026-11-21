package analytics

import (
	"math/big"
	"strings"

	"github.com/streamscale/streamscale/pkg/analytics"
	"github.com/streamscale/streamscale/pkg/event"
)

type accumulator struct {
	eventCount          int64
	purchaseCount       int64
	revenue             *big.Rat
	paymentSuccessCount int64
	paymentFailureCount int64
	users               map[string]struct{}
	byRegion            map[string]int64
	byEventType         map[string]int64
}

func newAccumulator() *accumulator {
	return &accumulator{
		revenue:     big.NewRat(0, 1),
		users:       map[string]struct{}{},
		byRegion:    map[string]int64{},
		byEventType: map[string]int64{},
	}
}

func (a *accumulator) add(e event.Event) {
	a.eventCount++
	a.users[e.UserID] = struct{}{}
	a.byRegion[e.Region]++
	a.byEventType[string(e.EventType)]++

	switch e.EventType {
	case event.TypePurchase:
		a.purchaseCount++
		if amt := parseAmount(e.Amount); amt != nil {
			a.revenue.Add(a.revenue, amt)
		}
	case event.TypePaymentSuccess:
		a.paymentSuccessCount++
	case event.TypePaymentFailure:
		a.paymentFailureCount++
	}
}

func (a *accumulator) metrics() analytics.Metrics {
	byRegion := make(map[string]int64, len(a.byRegion))
	for k, v := range a.byRegion {
		byRegion[k] = v
	}
	byType := make(map[string]int64, len(a.byEventType))
	for k, v := range a.byEventType {
		byType[k] = v
	}
	return analytics.Metrics{
		EventCount:          a.eventCount,
		PurchaseCount:       a.purchaseCount,
		Revenue:             formatRat(a.revenue),
		PaymentSuccessCount: a.paymentSuccessCount,
		PaymentFailureCount: a.paymentFailureCount,
		ActiveUsers:         int64(len(a.users)),
		ByRegion:            byRegion,
		ByEventType:         byType,
	}
}

func parseAmount(s string) *big.Rat {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	r := new(big.Rat)
	if _, ok := r.SetString(s); !ok {
		return nil
	}
	return r
}

func formatRat(r *big.Rat) string {
	if r == nil {
		return "0"
	}
	return r.FloatString(2)
}
