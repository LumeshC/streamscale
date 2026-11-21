package consume

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/LumeshC/streamscale/pkg/analytics"
	"github.com/LumeshC/streamscale/pkg/event"
	"github.com/LumeshC/streamscale/stream-processor/internal/classify"
	"github.com/LumeshC/streamscale/stream-processor/internal/dlq"
	"github.com/LumeshC/streamscale/stream-processor/internal/retry"
	"github.com/LumeshC/streamscale/stream-processor/internal/store"
	"github.com/segmentio/kafka-go"
)

type memStore struct {
	mu        sync.Mutex
	events    map[string]event.Event
	statuses  []store.StatusRecord
	processFn func(e event.Event) (store.ProcessOutcome, error)
	calls     int
}

func newMemStore() *memStore {
	return &memStore{events: map[string]event.Event{}}
}

func (m *memStore) Process(ctx context.Context, e event.Event) (store.ProcessOutcome, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.processFn != nil {
		return m.processFn(e)
	}
	if _, ok := m.events[e.EventID]; ok {
		return store.OutcomeDuplicate, nil
	}
	m.events[e.EventID] = e
	return store.OutcomeInserted, nil
}

func (m *memStore) RecordStatus(ctx context.Context, rec store.StatusRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statuses = append(m.statuses, rec)
	return nil
}

func (m *memStore) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.events)
}

func (m *memStore) statusCount(s store.Status) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, st := range m.statuses {
		if st.Status == s {
			n++
		}
	}
	return n
}

type memDLQ struct {
	mu    sync.Mutex
	msgs  []dlq.Meta
	raw   [][]byte
	err   error
	calls int
}

func (d *memDLQ) Publish(ctx context.Context, payload []byte, meta dlq.Meta) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	if d.err != nil {
		return d.err
	}
	d.msgs = append(d.msgs, meta)
	d.raw = append(d.raw, append([]byte(nil), payload...))
	return nil
}

func (d *memDLQ) Close() error { return nil }

func testProc(st EventStore, d dlq.Publisher) *Processor {
	return New(Options{
		Brokers: []string{"127.0.0.1:1"},
		Topic:   "events",
		GroupID: "test",
		Store:   st,
		DLQ:     d,
		Log:     slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		Retry:   retry.Config{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
	})
}

func validPayload(id string) []byte {
	e := event.Event{
		EventID:   id,
		EventType: event.TypeLogin,
		UserID:    "22222222-2222-4222-8222-222222222222",
		Timestamp: time.Now().UTC(),
		Region:    "us-east-1",
		Metadata:  map[string]string{"source": "test"},
	}
	b, err := event.Marshal(e)
	if err != nil {
		panic(err)
	}
	return b
}

func TestHandleMessageInsertsOnceForDuplicates(t *testing.T) {
	st := newMemStore()
	d := &memDLQ{}
	p := testProc(st, d)
	msg := kafka.Message{Topic: "events", Partition: 0, Offset: 1, Value: validPayload("11111111-1111-4111-8111-111111111111")}

	if err := p.HandleMessage(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	msg.Offset = 2
	if err := p.HandleMessage(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if st.count() != 1 {
		t.Fatalf("expected 1 persisted event, got %d", st.count())
	}
	if st.statusCount(store.StatusProcessed) != 1 {
		t.Fatalf("processed statuses=%d", st.statusCount(store.StatusProcessed))
	}
	if st.statusCount(store.StatusDuplicate) != 1 {
		t.Fatalf("duplicate statuses=%d", st.statusCount(store.StatusDuplicate))
	}
	if d.calls != 0 {
		t.Fatalf("dlq calls=%d", d.calls)
	}
}

func TestHandleMessageMalformedGoesToDLQ(t *testing.T) {
	st := newMemStore()
	d := &memDLQ{}
	p := testProc(st, d)
	msg := kafka.Message{Partition: 1, Offset: 9, Value: []byte(`{not-json`)}
	if err := p.HandleMessage(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if st.count() != 0 {
		t.Fatal("should not persist")
	}
	if d.calls != 1 || d.msgs[0].FailureClass != classify.ClassMalformed {
		t.Fatalf("dlq=%+v", d.msgs)
	}
	if st.statusCount(store.StatusDLQ) != 1 {
		t.Fatal("expected dlq status")
	}
}

func TestHandleMessageValidationFailureGoesToDLQ(t *testing.T) {
	st := newMemStore()
	d := &memDLQ{}
	p := testProc(st, d)
	payload := []byte(`{
		"event_id":"11111111-1111-4111-8111-111111111111",
		"event_type":"PURCHASE",
		"user_id":"22222222-2222-4222-8222-222222222222",
		"timestamp":"2026-09-18T00:00:00Z",
		"region":"us-east-1",
		"metadata":{},
		"product_id":"sku",
		"amount":"-5.00",
		"currency":"USD"
	}`)
	msg := kafka.Message{Partition: 0, Offset: 3, Value: payload}
	if err := p.HandleMessage(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if d.calls != 1 || d.msgs[0].Reason != "validation_failed" {
		t.Fatalf("%+v", d.msgs)
	}
}

func TestHandleMessageRetriesTransientThenSucceeds(t *testing.T) {
	st := newMemStore()
	st.processFn = func(e event.Event) (store.ProcessOutcome, error) {
		if st.calls < 3 {
			return "", classify.Transient("db", errors.New("connection reset"))
		}
		st.events[e.EventID] = e
		return store.OutcomeInserted, nil
	}
	d := &memDLQ{}
	p := testProc(st, d)
	msg := kafka.Message{Offset: 1, Value: validPayload("11111111-1111-4111-8111-111111111111")}
	if err := p.HandleMessage(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if st.count() != 1 {
		t.Fatalf("count=%d calls=%d", st.count(), st.calls)
	}
	if d.calls != 0 {
		t.Fatal("should not dlq")
	}
}

func TestHandleMessageExhaustedRetriesGoToDLQ(t *testing.T) {
	st := newMemStore()
	st.processFn = func(e event.Event) (store.ProcessOutcome, error) {
		return "", classify.Transient("db", errors.New("down"))
	}
	d := &memDLQ{}
	p := testProc(st, d)
	msg := kafka.Message{Offset: 1, Value: validPayload("11111111-1111-4111-8111-111111111111")}
	if err := p.HandleMessage(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if st.count() != 0 {
		t.Fatal("should not persist")
	}
	if d.calls != 1 || d.msgs[0].FailureClass != classify.ClassTransient {
		t.Fatalf("%+v", d.msgs)
	}
	if d.msgs[0].Attempts != 3 {
		t.Fatalf("attempts=%d", d.msgs[0].Attempts)
	}
}

func TestHandleMessagePermanentDBFailureGoesToDLQ(t *testing.T) {
	st := newMemStore()
	st.processFn = func(e event.Event) (store.ProcessOutcome, error) {
		return "", classify.Permanent("postgres_check_violation", errors.New("check"))
	}
	d := &memDLQ{}
	p := testProc(st, d)
	msg := kafka.Message{Offset: 1, Value: validPayload("11111111-1111-4111-8111-111111111111")}
	if err := p.HandleMessage(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if d.calls != 1 || d.msgs[0].FailureClass != classify.ClassPermanent {
		t.Fatalf("%+v", d.msgs)
	}
	if st.calls != 1 {
		t.Fatalf("should not retry permanent, calls=%d", st.calls)
	}
}

func TestHandleMessageKafkaDLQFailureDoesNotCommit(t *testing.T) {
	st := newMemStore()
	d := &memDLQ{err: errors.New("broker not available")}
	p := testProc(st, d)
	msg := kafka.Message{Offset: 1, Value: []byte(`{`)}
	err := p.HandleMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error so offset is not committed")
	}
	if classify.Of(err).Class != classify.ClassTransient {
		t.Fatalf("%v", err)
	}
}

func TestDecodeAndValidate(t *testing.T) {
	_, err := decodeAndValidate([]byte(`{"event_id":"x"}`))
	if classify.Of(err).Class != classify.ClassMalformed {
		t.Fatalf("%v", err)
	}
}

type countingAnalytics struct {
	calls int
}

func (c *countingAnalytics) OnEvent(ctx context.Context, e event.Event) ([]analytics.ClosedWindow, error) {
	c.calls++
	return nil, nil
}
func (c *countingAnalytics) OpenSnapshots() []analytics.Snapshot { return nil }
func (c *countingAnalytics) LateDropCount() int64                { return 0 }

func TestAnalyticsOnlyOnInsertedNotDuplicate(t *testing.T) {
	st := newMemStore()
	d := &memDLQ{}
	an := &countingAnalytics{}
	p := New(Options{
		Brokers:   []string{"127.0.0.1:1"},
		Topic:     "events",
		GroupID:   "test",
		Store:     st,
		DLQ:       d,
		Analytics: an,
		Log:       slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		Retry:     retry.Config{MaxAttempts: 1, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
	})
	msg := kafka.Message{Offset: 1, Value: validPayload("11111111-1111-4111-8111-111111111111")}
	if err := p.HandleMessage(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	msg.Offset = 2
	if err := p.HandleMessage(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if an.calls != 1 {
		t.Fatalf("analytics calls=%d want 1 (duplicates must not double-count)", an.calls)
	}
}

type memRealtime struct {
	mu     sync.Mutex
	events []event.Event
}

func (m *memRealtime) Observe(_ context.Context, e event.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, e)
}

func TestRealtimeOnlyOnInsertedNotDuplicateOrDLQ(t *testing.T) {
	st := newMemStore()
	rt := &memRealtime{}
	p := New(Options{
		Brokers:  []string{"127.0.0.1:1"},
		Topic:    "events",
		GroupID:  "test",
		Store:    st,
		DLQ:      &memDLQ{},
		Realtime: rt,
		Log:      slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		Retry:    retry.Config{MaxAttempts: 1, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
	})
	id := "33333333-3333-4333-8333-333333333333"
	for i := 0; i < 3; i++ {
		if err := p.HandleMessage(context.Background(), kafka.Message{Offset: int64(i), Value: validPayload(id)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.HandleMessage(context.Background(), kafka.Message{Offset: 9, Value: []byte(`{bad`)}); err != nil {
		t.Fatal(err)
	}
	if len(rt.events) != 1 || rt.events[0].EventID != id {
		t.Fatalf("expected exactly one realtime observation, got %d", len(rt.events))
	}
}
