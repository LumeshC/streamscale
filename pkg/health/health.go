package health

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

type Checker func(ctx context.Context) error

// Option customizes the health server.
type Option func(*options)

type options struct {
	info   map[string]Checker
	routes []func(*http.ServeMux)
}

// WithInfo adds a non-fatal dependency check. Its result is reported under
// "checks" in /readyz but never makes the service not-ready (used for optional
// dependencies such as Redis that the service can run without).
func WithInfo(name string, c Checker) Option {
	return func(o *options) {
		if o.info == nil {
			o.info = map[string]Checker{}
		}
		o.info[name] = c
	}
}

// WithRoutes registers extra handlers on the same server.
func WithRoutes(register func(*http.ServeMux)) Option {
	return func(o *options) { o.routes = append(o.routes, register) }
}

func Server(addr string, ready Checker, opts ...Option) *http.Server {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		body := map[string]any{"status": "ready"}
		if len(o.info) > 0 {
			checks := map[string]string{}
			for name, c := range o.info {
				if err := c(ctx); err != nil {
					checks[name] = "degraded: " + err.Error()
				} else {
					checks[name] = "ok"
				}
			}
			body["checks"] = checks
		}
		if ready != nil {
			if err := ready(ctx); err != nil {
				body["status"] = "not_ready"
				body["error"] = err.Error()
				writeJSON(w, http.StatusServiceUnavailable, body)
				return
			}
		}
		writeJSON(w, http.StatusOK, body)
	})
	for _, reg := range o.routes {
		reg(mux)
	}
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
