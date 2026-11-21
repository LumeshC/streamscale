package event

import (
	"testing"
	"time"
)

func TestValidateRejectsEmptyEventID(t *testing.T) {
	e := validLogin()
	e.EventID = ""
	if err := e.Validate(); err == nil {
		t.Fatal("expected error")
	}
}

func TestValidateRejectsEmptyUserID(t *testing.T) {
	e := validLogin()
	e.UserID = "  "
	if err := e.Validate(); err == nil {
		t.Fatal("expected error")
	}
}

func TestValidateRejectsZeroTimestamp(t *testing.T) {
	e := validLogin()
	e.Timestamp = time.Time{}
	if err := e.Validate(); err == nil {
		t.Fatal("expected error")
	}
}

func TestValidateRejectsNegativeAmount(t *testing.T) {
	e := validPurchase()
	e.Amount = "-1.00"
	if err := e.Validate(); err == nil {
		t.Fatal("expected error")
	}
}

func TestValidateRejectsInvalidCurrency(t *testing.T) {
	e := validPurchase()
	e.Currency = "XXX"
	if err := e.Validate(); err == nil {
		t.Fatal("expected error")
	}
}

func TestValidateAcceptsZeroAmount(t *testing.T) {
	e := validPurchase()
	e.Amount = "0"
	if err := e.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestDecodeDoesNotValidate(t *testing.T) {
	raw := []byte(`{"event_id":"","event_type":"LOGIN","user_id":"u","timestamp":"2026-09-18T00:00:00Z","region":"us-east-1","metadata":{}}`)
	e, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
}

func validLogin() Event {
	return Event{
		EventID:   "11111111-1111-4111-8111-111111111111",
		EventType: TypeLogin,
		UserID:    "22222222-2222-4222-8222-222222222222",
		Timestamp: time.Now().UTC(),
		Region:    "us-east-1",
		Metadata:  map[string]string{"source": "test"},
	}
}

func validPurchase() Event {
	e := validLogin()
	e.EventType = TypePurchase
	e.ProductID = "sku_1"
	e.Amount = "19.99"
	e.Currency = "USD"
	return e
}
