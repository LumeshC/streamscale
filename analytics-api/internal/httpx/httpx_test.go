package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func req(target string) *http.Request { return httptest.NewRequest(http.MethodGet, target, nil) }

func fieldsOf(err error) map[string]string {
	out := map[string]string{}
	var ae *APIError
	if errors.As(err, &ae) {
		for _, d := range ae.Details {
			out[d.Field] = d.Reason
		}
	}
	return out
}

func TestParseQueryRejectsUnknownAndRepeated(t *testing.T) {
	q := ParseQuery(req("/x?limit=1&limit=2&form=x"), "limit", "from")
	f := fieldsOf(q.Err())
	if !strings.Contains(f["form"], "unknown parameter") || !strings.Contains(f["limit"], "repeated") {
		t.Fatalf("errors %v", f)
	}
}

func TestQueryCollectsAllErrors(t *testing.T) {
	q := ParseQuery(req("/x?limit=0&interval=2h&from=yesterday&min_z=abc"), "limit", "interval", "from", "min_z")
	q.Int("limit", 100, 1, 1000)
	q.Enum("interval", "1m", "1s", "1m", "5m")
	q.Time("from")
	q.Float("min_z", 3, 2, 10)
	f := fieldsOf(q.Err())
	for _, k := range []string{"limit", "interval", "from", "min_z"} {
		if f[k] == "" {
			t.Fatalf("missing error for %s: %v", k, f)
		}
	}
	var ae *APIError
	if !errors.As(q.Err(), &ae) || ae.Status != http.StatusBadRequest || ae.Code != CodeInvalidParameter {
		t.Fatalf("want 400 invalid_parameter, got %+v", ae)
	}
}

func TestRangeDefaultsAndValidation(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	q := ParseQuery(req("/x"), "from", "to")
	tr := q.Range(now, time.Hour, 24*time.Hour)
	if q.Err() != nil || !tr.To.Equal(now) || !tr.From.Equal(now.Add(-time.Hour)) {
		t.Fatalf("defaults: %+v %v", tr, q.Err())
	}

	q = ParseQuery(req("/x?from=2026-01-01T12:00:00Z&to=2026-01-01T11:00:00Z"), "from", "to")
	q.Range(now, time.Hour, 24*time.Hour)
	if !strings.Contains(fieldsOf(q.Err())["from"], "earlier than to") {
		t.Fatalf("from>=to: %v", q.Err())
	}

	q = ParseQuery(req("/x?from=2025-01-01T00:00:00Z"), "from", "to")
	q.Range(now, time.Hour, 24*time.Hour)
	if !strings.Contains(fieldsOf(q.Err())["from"], "must not exceed") {
		t.Fatalf("max range: %v", q.Err())
	}

	// Offsets are accepted and normalized to UTC.
	q = ParseQuery(req("/x?from=2026-01-01T13:00:00%2B02:00&to=2026-01-01T12:00:00Z"), "from", "to")
	tr = q.Range(now, time.Hour, 24*time.Hour)
	if q.Err() != nil || tr.From.Location() != time.UTC || tr.From.Hour() != 11 {
		t.Fatalf("offset: %+v %v", tr, q.Err())
	}
}

func TestCursorRoundTripAndTamper(t *testing.T) {
	type c struct {
		B time.Time `json:"b"`
	}
	in := c{B: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	enc := EncodeCursor(in)
	var out c
	q := ParseQuery(req("/x?cursor="+enc), "cursor")
	if !q.Cursor(&out) || q.Err() != nil || !out.B.Equal(in.B) {
		t.Fatalf("round trip: %+v %v", out, q.Err())
	}
	for _, bad := range []string{"!!!", EncodeCursor(map[string]int{"zzz": 1})} {
		q = ParseQuery(req("/x?cursor="+bad), "cursor")
		q.Cursor(&out)
		if fieldsOf(q.Err())["cursor"] == "" {
			t.Fatalf("cursor %q should be rejected", bad)
		}
	}
}

func TestFromErrorMapping(t *testing.T) {
	cases := []struct {
		err    error
		status int
		code   string
	}{
		{context.DeadlineExceeded, http.StatusGatewayTimeout, CodeTimeout},
		{errors.New("pg: boom"), http.StatusInternalServerError, CodeInternal},
		{NotFound("x"), http.StatusNotFound, CodeNotFound},
	}
	for _, c := range cases {
		ae := FromError(c.err)
		if ae.Status != c.status || ae.Code != c.code {
			t.Errorf("%v → %d %s", c.err, ae.Status, ae.Code)
		}
	}
	if FromError(errors.New("secret dsn")).Message != "internal error" {
		t.Fatal("internal errors must not leak causes")
	}
}

func TestMiddlewareRequestIDPanicAndTimeout(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	mux.HandleFunc("/panic", func(http.ResponseWriter, *http.Request) { panic("kaboom") })
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		WriteError(w, r, r.Context().Err())
	})
	mux.HandleFunc("/ok", func(w http.ResponseWriter, r *http.Request) {
		WriteJSON(w, 200, map[string]string{"id": RequestID(r.Context())})
	})
	h := Chain(mux, log, 50*time.Millisecond)

	// Valid client ID is echoed; invalid one is replaced.
	r := req("/ok")
	r.Header.Set(HeaderRequestID, "client-123")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Header().Get(HeaderRequestID) != "client-123" {
		t.Fatalf("id not echoed: %q", rec.Header().Get(HeaderRequestID))
	}
	r = req("/ok")
	r.Header.Set(HeaderRequestID, "bad id with spaces\n")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if id := rec.Header().Get(HeaderRequestID); id == "" || strings.Contains(id, " ") {
		t.Fatalf("invalid id not replaced: %q", id)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req("/panic"))
	var body ErrorBody
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if rec.Code != 500 || body.Error == nil || body.Error.Code != CodeInternal || body.RequestID == "" {
		t.Fatalf("panic: %d %s", rec.Code, rec.Body.String())
	}

	start := time.Now()
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req("/slow"))
	if rec.Code != http.StatusGatewayTimeout || time.Since(start) > time.Second {
		t.Fatalf("timeout: %d after %s", rec.Code, time.Since(start))
	}
}
