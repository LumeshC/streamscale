package httpx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"runtime/debug"
	"time"
)

type ctxKey int

const requestIDKey ctxKey = 1

// HeaderRequestID is accepted from clients (if well-formed) and always echoed.
const HeaderRequestID = "X-Request-ID"

var validRequestID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// RequestID returns the request's ID, or "" outside a request.
func RequestID(ctx context.Context) string {
	v, _ := ctx.Value(requestIDKey).(string)
	return v
}

// Chain wraps h with the standard middleware, outermost first:
// request ID → access log → panic recovery → timeout.
func Chain(h http.Handler, log *slog.Logger, timeout time.Duration) http.Handler {
	return withRequestID(withAccessLog(withRecover(withTimeout(h, timeout), log), log))
}

func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(HeaderRequestID)
		if !validRequestID.MatchString(id) {
			id = newID()
		}
		w.Header().Set(HeaderRequestID, id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, id)))
	})
}

// recorder captures status/bytes and the APIError (if any) for the access log.
type recorder struct {
	http.ResponseWriter
	status int
	bytes  int
	err    *APIError
}

func (r *recorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

func withAccessLog(next http.Handler, log *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &recorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		attrs := []any{
			"request_id", RequestID(r.Context()),
			"method", r.Method,
			"path", r.URL.Path,
			"query", r.URL.RawQuery,
			"status", rec.status,
			"bytes", rec.bytes,
			"duration_ms", float64(time.Since(start).Microseconds()) / 1000,
		}
		level := slog.LevelInfo
		if rec.err != nil {
			attrs = append(attrs, "error_code", rec.err.Code)
			if rec.err.cause != nil {
				attrs = append(attrs, "err", rec.err.cause.Error())
			}
			if rec.status >= 500 {
				level = slog.LevelError
			} else {
				level = slog.LevelWarn
			}
		}
		log.Log(r.Context(), level, "request", attrs...)
	})
}

func withRecover(next http.Handler, log *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if p := recover(); p != nil {
				log.Error("panic", "request_id", RequestID(r.Context()), "panic", fmt.Sprint(p), "stack", string(debug.Stack()))
				WriteError(w, r, fmt.Errorf("panic: %v", p))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// withTimeout gives every request a context deadline. Handlers pass the
// context to PostgreSQL/Redis, so a slow query is canceled server-side and
// surfaces as a 504 envelope (see FromError) rather than a hung connection.
func withTimeout(next http.Handler, d time.Duration) http.Handler {
	if d <= 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), d)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func newID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
