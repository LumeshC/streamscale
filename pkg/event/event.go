package event

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const SchemaVersion = 1

type Type string

const (
	TypeLogin          Type = "LOGIN"
	TypeLogout         Type = "LOGOUT"
	TypePageView       Type = "PAGE_VIEW"
	TypeSearch         Type = "SEARCH"
	TypePurchase       Type = "PURCHASE"
	TypePaymentSuccess Type = "PAYMENT_SUCCESS"
	TypePaymentFailure Type = "PAYMENT_FAILURE"
)

var AllTypes = []Type{
	TypeLogin,
	TypeLogout,
	TypePageView,
	TypeSearch,
	TypePurchase,
	TypePaymentSuccess,
	TypePaymentFailure,
}

func (t Type) RequiresCommerce() bool {
	switch t {
	case TypePurchase, TypePaymentSuccess, TypePaymentFailure:
		return true
	default:
		return false
	}
}

func ParseType(s string) (Type, error) {
	t := Type(strings.ToUpper(strings.TrimSpace(s)))
	for _, known := range AllTypes {
		if t == known {
			return t, nil
		}
	}
	return "", fmt.Errorf("unknown event_type %q", s)
}

// Event is the v1 JSON envelope produced to Kafka topic "events".
type Event struct {
	EventID   string            `json:"event_id"`
	EventType Type              `json:"event_type"`
	UserID    string            `json:"user_id"`
	Timestamp time.Time         `json:"timestamp"`
	Region    string            `json:"region"`
	Metadata  map[string]string `json:"metadata"`
	ProductID string            `json:"product_id,omitempty"`
	Amount    string            `json:"amount,omitempty"`
	Currency  string            `json:"currency,omitempty"`
}

func Unmarshal(b []byte) (Event, error) {
	e, err := Decode(b)
	if err != nil {
		return Event{}, err
	}
	if err := e.Validate(); err != nil {
		return Event{}, err
	}
	return e, nil
}

func Marshal(e Event) ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(e)
}
