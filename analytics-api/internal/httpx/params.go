package httpx

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Query validates query parameters, collecting every problem so the client
// gets all errors in one response instead of fixing them one at a time.
type Query struct {
	values map[string][]string
	errs   []FieldError
}

// ParseQuery rejects parameters not in allowed (catches typos such as
// ?form=... silently falling back to defaults) and repeated parameters.
func ParseQuery(r *http.Request, allowed ...string) *Query {
	q := &Query{values: r.URL.Query()}
	ok := map[string]bool{}
	for _, a := range allowed {
		ok[a] = true
	}
	names := make([]string, 0, len(q.values))
	for k := range q.values {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		switch {
		case !ok[k]:
			q.add(k, "unknown parameter; allowed: "+strings.Join(allowed, ", "))
		case len(q.values[k]) > 1:
			q.add(k, "must not be repeated")
		}
	}
	return q
}

func (q *Query) add(field, reason string) {
	q.errs = append(q.errs, FieldError{Field: field, Reason: reason})
}

func (q *Query) raw(name string) (string, bool) {
	v, ok := q.values[name]
	if !ok || len(v) == 0 {
		return "", false
	}
	return strings.TrimSpace(v[0]), true
}

// Time parses an RFC 3339 timestamp; ok=false when absent or invalid.
func (q *Query) Time(name string) (time.Time, bool) {
	s, present := q.raw(name)
	if !present {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		q.add(name, "must be an RFC 3339 timestamp, e.g. 2026-01-02T15:04:05Z")
		return time.Time{}, false
	}
	return t.UTC(), true
}

// Int parses an integer within [min, max], defaulting to def when absent.
func (q *Query) Int(name string, def, min, max int) int {
	s, present := q.raw(name)
	if !present {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < min || n > max {
		q.add(name, fmt.Sprintf("must be an integer between %d and %d", min, max))
		return def
	}
	return n
}

// Float parses a number within [min, max], defaulting to def when absent.
func (q *Query) Float(name string, def, min, max float64) float64 {
	s, present := q.raw(name)
	if !present {
		return def
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || f < min || f > max {
		q.add(name, fmt.Sprintf("must be a number between %g and %g", min, max))
		return def
	}
	return f
}

// Enum returns the value if it is one of allowed, def when absent.
func (q *Query) Enum(name, def string, allowed ...string) string {
	s, present := q.raw(name)
	if !present {
		return def
	}
	for _, a := range allowed {
		if s == a {
			return s
		}
	}
	q.add(name, "must be one of: "+strings.Join(allowed, ", "))
	return def
}

// String returns the raw value or def.
func (q *Query) String(name, def string) string {
	if s, ok := q.raw(name); ok {
		return s
	}
	return def
}

// Fail records a custom validation error.
func (q *Query) Fail(field, reason string) { q.add(field, reason) }

// Err returns an invalid_parameter APIError if anything failed.
func (q *Query) Err() error {
	if len(q.errs) == 0 {
		return nil
	}
	return InvalidParams(q.errs...)
}

// TimeRange is a validated half-open [From, To) interval.
type TimeRange struct {
	From time.Time
	To   time.Time
}

// Range reads ?from and ?to. Defaults: to=now, from=to-def. Enforces from<to
// and to-from<=max so one request cannot scan unbounded history.
func (q *Query) Range(now time.Time, def, max time.Duration) TimeRange {
	to, hasTo := q.Time("to")
	if !hasTo {
		to = now.UTC()
	}
	from, hasFrom := q.Time("from")
	if !hasFrom {
		from = to.Add(-def)
	}
	if hasErr(q.errs, "from") || hasErr(q.errs, "to") {
		return TimeRange{From: from, To: to}
	}
	if !from.Before(to) {
		q.add("from", "must be earlier than to")
	} else if to.Sub(from) > max {
		q.add("from", fmt.Sprintf("range must not exceed %s", max))
	}
	return TimeRange{From: from, To: to}
}

func hasErr(errs []FieldError, field string) bool {
	for _, e := range errs {
		if e.Field == field {
			return true
		}
	}
	return false
}

// Cursor reads an opaque pagination cursor into v; empty cursor leaves v untouched.
func (q *Query) Cursor(v any) bool {
	s, present := q.raw("cursor")
	if !present || s == "" {
		return false
	}
	if err := DecodeCursor(s, v); err != nil {
		q.add("cursor", "invalid cursor; use next_cursor from a previous response")
		return false
	}
	return true
}

// EncodeCursor makes an opaque, URL-safe cursor. Clients must not parse it.
func EncodeCursor(v any) string {
	b, _ := json.Marshal(v)
	return base64.RawURLEncoding.EncodeToString(b)
}

func DecodeCursor(s string, v any) error {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// Page is the pagination block of list responses.
type Page struct {
	Limit      int    `json:"limit"`
	NextCursor string `json:"next_cursor,omitempty"`
}
