package handler

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/streamscale/streamscale/analytics-api/internal/httpx"
)

// Route is one public endpoint (also used to keep OpenAPI in sync in tests).
type Route struct {
	Path    string
	Handler http.HandlerFunc
}

// Routes lists every public endpoint. All are GET (read-only API).
func (a *API) Routes() []Route {
	return []Route{
		{"/health", a.Health},
		{"/metrics/overview", a.Overview},
		{"/metrics/events", a.Events},
		{"/metrics/revenue", a.Revenue},
		{"/metrics/payments", a.Payments},
		{"/metrics/regions", a.Regions},
		{"/metrics/event-types", a.EventTypes},
		{"/metrics/consumer-lag", a.ConsumerLag},
		{"/metrics/latency", a.Latency},
		{"/anomalies", a.Anomalies},
		{"/errors", a.Errors},
		{"/openapi.yaml", a.OpenAPI},
	}
}

// Router builds the full handler: routes + JSON 404/405 + middleware.
func (a *API) Router(log *slog.Logger, timeout time.Duration) http.Handler {
	mux := http.NewServeMux()
	for _, rt := range a.Routes() {
		mux.Handle(rt.Path, getOnly(rt.Handler))
	}
	// Exact-match fallback so unknown paths get the JSON error envelope
	// instead of net/http's plain-text 404.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteError(w, r, httpx.NotFound("no route for "+r.URL.Path))
	})
	return httpx.Chain(mux, log, timeout)
}

func getOnly(h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			httpx.WriteError(w, r, &httpx.APIError{
				Status: http.StatusMethodNotAllowed, Code: httpx.CodeMethodNotAllowed,
				Message: r.Method + " is not allowed; this API is read-only",
			})
			return
		}
		h(w, r)
	})
}
