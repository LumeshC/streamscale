package generate

import (
	"testing"

	"github.com/LumeshC/streamscale/pkg/event"
)

func TestNextAlwaysValid(t *testing.T) {
	g := New(42, 20)
	seen := map[event.Type]int{}
	for i := 0; i < 500; i++ {
		e := g.Next()
		if err := e.Validate(); err != nil {
			t.Fatalf("invalid event: %v %+v", err, e)
		}
		if e.EventID == "" || e.UserID == "" {
			t.Fatal("missing ids")
		}
		seen[e.EventType]++
	}
	for _, typ := range event.AllTypes {
		if seen[typ] == 0 {
			t.Fatalf("did not generate %s in 500 samples (seed 42)", typ)
		}
	}
}

func TestCommerceEventsHaveAmount(t *testing.T) {
	g := New(1, 5)
	found := false
	for i := 0; i < 400; i++ {
		e := g.Next()
		if e.EventType.RequiresCommerce() {
			found = true
			if e.ProductID == "" || e.Amount == "" || e.Currency != "USD" {
				t.Fatalf("commerce fields incomplete: %+v", e)
			}
		}
	}
	if !found {
		t.Fatal("no commerce events generated")
	}
}
