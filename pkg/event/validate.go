package event

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// Common ISO 4217 currencies accepted for financial events in Phase 2.
var allowedCurrencies = map[string]struct{}{
	"USD": {}, "EUR": {}, "GBP": {}, "INR": {}, "JPY": {},
	"CAD": {}, "AUD": {}, "CHF": {}, "CNY": {}, "SGD": {},
	"BRL": {}, "MXN": {}, "KRW": {}, "HKD": {}, "NZD": {},
	"SEK": {}, "NOK": {}, "DKK": {}, "ZAR": {}, "AED": {},
}

// MaxFutureSkew is how far ahead of wall clock a timestamp may be.
const MaxFutureSkew = 24 * time.Hour

func (e Event) Validate() error {
	if strings.TrimSpace(e.EventID) == "" {
		return fmt.Errorf("event_id cannot be empty")
	}
	if _, err := ParseType(string(e.EventType)); err != nil {
		return err
	}
	if strings.TrimSpace(e.UserID) == "" {
		return fmt.Errorf("user_id cannot be empty")
	}
	if err := validateTimestamp(e.Timestamp); err != nil {
		return err
	}
	if strings.TrimSpace(e.Region) == "" {
		return fmt.Errorf("region is required")
	}
	if e.Metadata == nil {
		return fmt.Errorf("metadata is required")
	}
	if e.EventType.RequiresCommerce() {
		if strings.TrimSpace(e.ProductID) == "" {
			return fmt.Errorf("product_id is required for %s", e.EventType)
		}
		if err := validateAmount(e.Amount); err != nil {
			return err
		}
		if err := validateCurrency(e.Currency); err != nil {
			return err
		}
	}
	return nil
}

func validateTimestamp(ts time.Time) error {
	if ts.IsZero() {
		return fmt.Errorf("timestamp must be valid")
	}
	if ts.After(time.Now().UTC().Add(MaxFutureSkew)) {
		return fmt.Errorf("timestamp is too far in the future")
	}
	return nil
}

func validateAmount(amount string) error {
	amount = strings.TrimSpace(amount)
	if amount == "" {
		return fmt.Errorf("amount is required")
	}
	r := new(big.Rat)
	if _, ok := r.SetString(amount); !ok {
		return fmt.Errorf("amount must be a valid decimal number")
	}
	if r.Sign() < 0 {
		return fmt.Errorf("amount must be non-negative")
	}
	return nil
}

func validateCurrency(currency string) error {
	c := strings.ToUpper(strings.TrimSpace(currency))
	if c == "" {
		return fmt.Errorf("currency is required for financial events")
	}
	if len(c) != 3 {
		return fmt.Errorf("currency must be a 3-letter ISO 4217 code")
	}
	if _, ok := allowedCurrencies[c]; !ok {
		return fmt.Errorf("currency %q is not supported", currency)
	}
	return nil
}

// Decode parses JSON without validation. Use Validate separately.
func Decode(b []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(b, &e); err != nil {
		return Event{}, fmt.Errorf("decode event json: %w", err)
	}
	if e.Metadata == nil {
		e.Metadata = map[string]string{}
	}
	return e, nil
}
