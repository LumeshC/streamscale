package health

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthz(t *testing.T) {
	srv := Server(":0", nil)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestReadyzError(t *testing.T) {
	srv := Server(":0", func(ctx context.Context) error {
		return context.DeadlineExceeded
	})
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "not_ready" {
		t.Fatalf("body %+v", body)
	}
}

func TestReadyzInfoCheckIsNonFatal(t *testing.T) {
	srv := Server(":0", func(context.Context) error { return nil },
		WithInfo("redis", func(context.Context) error { return context.DeadlineExceeded }),
		WithRoutes(func(m *http.ServeMux) {
			m.HandleFunc("/extra", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
		}),
	)
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("optional dependency failure must not fail readiness: %d", rec.Code)
	}
	var body struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Status != "ready" || body.Checks["redis"] == "ok" || body.Checks["redis"] == "" {
		t.Fatalf("body %+v", body)
	}
	rec = httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/extra", nil))
	if rec.Code != http.StatusTeapot {
		t.Fatalf("extra route not registered: %d", rec.Code)
	}
}
