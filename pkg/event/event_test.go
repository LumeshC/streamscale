package event

import (
	"encoding/json"
	"testing"
	"time"
)

func TestMarshalRoundTripPurchase(t *testing.T) {
	e := Event{
		EventID:   "11111111-1111-1111-1111-111111111111",
		EventType: TypePurchase,
		UserID:    "22222222-2222-2222-2222-222222222222",
		Timestamp: time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC),
		Region:    "us-east-1",
		Metadata:  map[string]string{"path": "/checkout"},
		ProductID: "sku_1",
		Amount:    "19.99",
		Currency:  "USD",
	}
	b, err := Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Unmarshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.EventType != TypePurchase || got.Amount != "19.99" || got.ProductID != "sku_1" {
		t.Fatalf("unexpected event: %+v", got)
	}
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"event_id", "event_type", "user_id", "timestamp", "region", "metadata", "product_id", "amount", "currency"} {
		if _, ok := raw[key]; !ok {
			t.Fatalf("missing json key %s", key)
		}
	}
}

func TestUnmarshalRejectsUnknownType(t *testing.T) {
	_, err := Unmarshal([]byte(`{
		"event_id":"a","event_type":"CLICK","user_id":"u",
		"timestamp":"2026-09-18T00:00:00Z","region":"us-east-1","metadata":{}
	}`))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestValidateRequiresCommerceFields(t *testing.T) {
	e := Event{
		EventID:   "a",
		EventType: TypePaymentFailure,
		UserID:    "u",
		Timestamp: time.Now().UTC(),
		Region:    "eu-west-1",
		Metadata:  map[string]string{},
	}
	if err := e.Validate(); err == nil {
		t.Fatal("expected commerce field error")
	}
}

func TestParseType(t *testing.T) {
	if _, err := ParseType("page_view"); err != nil {
		t.Fatal(err)
	}
	if _, err := ParseType("nope"); err == nil {
		t.Fatal("expected error")
	}
}
