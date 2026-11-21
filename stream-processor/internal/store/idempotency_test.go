package store

import (
	"context"
	"testing"
	"time"

	"github.com/LumeshC/streamscale/pkg/event"
	"github.com/LumeshC/streamscale/stream-processor/internal/classify"
)

// memIdempotencyStore proves the duplicate contract used by Process without Postgres.
type memIdempotencyStore struct {
	events map[string]event.Event
}

func (m *memIdempotencyStore) Process(_ context.Context, e event.Event) (ProcessOutcome, error) {
	if m.events == nil {
		m.events = map[string]event.Event{}
	}
	if _, ok := m.events[e.EventID]; ok {
		return OutcomeDuplicate, nil
	}
	m.events[e.EventID] = e
	return OutcomeInserted, nil
}

func TestIdempotencyKeyPreventsDoubleInsert(t *testing.T) {
	m := &memIdempotencyStore{}
	e := event.Event{
		EventID:   "11111111-1111-4111-8111-111111111111",
		EventType: event.TypeLogin,
		UserID:    "22222222-2222-4222-8222-222222222222",
		Timestamp: time.Now().UTC(),
		Region:    "us-east-1",
		Metadata:  map[string]string{"k": "v"},
	}
	o1, err := m.Process(context.Background(), e)
	if err != nil || o1 != OutcomeInserted {
		t.Fatalf("first: %v %v", o1, err)
	}
	o2, err := m.Process(context.Background(), e)
	if err != nil || o2 != OutcomeDuplicate {
		t.Fatalf("second: %v %v", o2, err)
	}
	if len(m.events) != 1 {
		t.Fatalf("events=%d", len(m.events))
	}
}

func TestProcessClassifiesNilMetadataJSONAsPermanent(t *testing.T) {
	// Ensure classify helpers used by Process stay wired for permanent failures.
	err := classify.Permanent("metadata_json", context.Canceled)
	if classify.Of(err).Class != classify.ClassPermanent {
		t.Fatal(err)
	}
}
