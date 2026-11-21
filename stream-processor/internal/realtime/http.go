package realtime

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

// Routes registers read-only real-time endpoints:
//
//	GET /v1/realtime?window=60     rolling snapshot over the last N seconds
//	GET /v1/realtime/users/{id}    live session state for one user
//	GET /v1/realtime/stats         tracker counters + circuit state
//
// When Redis is unavailable these return 503 with {"status":"degraded"}; they
// never fall back to PostgreSQL (closed windows are served from
// metrics_windows by design, not by this API).
func (t *Tracker) Routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/realtime", func(w http.ResponseWriter, r *http.Request) {
		window := 60 * time.Second
		if v := r.URL.Query().Get("window"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "window must be a positive integer (seconds)"})
				return
			}
			window = time.Duration(n) * time.Second
		}
		snap, err := t.Snapshot(r.Context(), window)
		if err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "degraded", "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, snap)
	})
	mux.HandleFunc("GET /v1/realtime/users/{id}", func(w http.ResponseWriter, r *http.Request) {
		st, found, err := t.UserState(r.Context(), r.PathValue("id"))
		if err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "degraded", "error": err.Error()})
			return
		}
		if !found {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "no recent activity"})
			return
		}
		writeJSON(w, http.StatusOK, st)
	})
	mux.HandleFunc("GET /v1/realtime/stats", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, t.Stats())
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
