package store

import (
	"testing"
	"time"

	"github.com/streamscale/streamscale/pkg/event"
)

func TestInsertSQLShape(t *testing.T) {
	e := event.Event{
		EventID:   "11111111-1111-4111-8111-111111111111",
		EventType: event.TypeLogin,
		UserID:    "22222222-2222-4222-8222-222222222222",
		Timestamp: time.Now().UTC(),
		Region:    "us-east-1",
		Metadata:  map[string]string{"k": "v"},
	}
	if err := e.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestVersionOf(t *testing.T) {
	v, err := versionOf("0001_events.sql")
	if err != nil || v != 1 {
		t.Fatalf("v=%d err=%v", v, err)
	}
	v2, err := versionOf("0002_reliability.sql")
	if err != nil || v2 != 2 {
		t.Fatalf("v2=%d err=%v", v2, err)
	}
	v3, err := versionOf("0003_metrics_windows.sql")
	if err != nil || v3 != 3 {
		t.Fatalf("v3=%d err=%v", v3, err)
	}
	if _, err := versionOf("bad.sql"); err == nil {
		t.Fatal("expected error")
	}
}
