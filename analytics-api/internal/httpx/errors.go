// Package httpx holds the API's HTTP plumbing: the error envelope, middleware
// (request IDs, access logs, panics, timeouts) and query validation.
package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
)

// Error codes are stable, machine-readable identifiers (documented in OpenAPI).
const (
	CodeInvalidParameter = "invalid_parameter"
	CodeNotFound         = "not_found"
	CodeMethodNotAllowed = "method_not_allowed"
	CodeTimeout          = "timeout"
	CodeUnavailable      = "unavailable"
	CodeInternal         = "internal"
)

// FieldError describes one invalid input.
type FieldError struct {
	Field  string `json:"field"`
	Reason string `json:"reason"`
}

// APIError is returned by handlers and rendered as the error envelope.
type APIError struct {
	Status  int          `json:"-"`
	Code    string       `json:"code"`
	Message string       `json:"message"`
	Details []FieldError `json:"details,omitempty"`
	cause   error
}

func (e *APIError) Error() string { return e.Code + ": " + e.Message }
func (e *APIError) Unwrap() error { return e.cause }

// ErrorBody is the JSON envelope for every non-2xx response:
//
//	{"error": {"code": "...", "message": "...", "details": [...]}, "request_id": "..."}
type ErrorBody struct {
	Error     *APIError `json:"error"`
	RequestID string    `json:"request_id,omitempty"`
}

func InvalidParams(details ...FieldError) *APIError {
	return &APIError{Status: http.StatusBadRequest, Code: CodeInvalidParameter,
		Message: "one or more query parameters are invalid", Details: details}
}

func NotFound(msg string) *APIError {
	return &APIError{Status: http.StatusNotFound, Code: CodeNotFound, Message: msg}
}

func Unavailable(msg string, cause error) *APIError {
	return &APIError{Status: http.StatusServiceUnavailable, Code: CodeUnavailable, Message: msg, cause: cause}
}

// FromError maps any error to an APIError. Deadline errors become 504 so
// clients can tell "too slow" from "broken"; anything unknown is a 500 whose
// message never leaks internals (the cause is logged instead).
func FromError(err error) *APIError {
	var ae *APIError
	if errors.As(err, &ae) {
		return ae
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &APIError{Status: http.StatusGatewayTimeout, Code: CodeTimeout,
			Message: "request exceeded its time budget", cause: err}
	}
	if errors.Is(err, context.Canceled) {
		// Client went away; status is mostly for the access log.
		return &APIError{Status: 499, Code: CodeTimeout, Message: "request canceled", cause: err}
	}
	return &APIError{Status: http.StatusInternalServerError, Code: CodeInternal,
		Message: "internal error", cause: err}
}

// WriteError renders err as the error envelope and records it for the access log.
func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	ae := FromError(err)
	if rec, ok := w.(*recorder); ok {
		rec.err = ae
	}
	status := ae.Status
	if status == 499 {
		status = http.StatusServiceUnavailable // never sent to a real client
	}
	WriteJSON(w, status, ErrorBody{Error: ae, RequestID: RequestID(r.Context())})
}

// WriteJSON writes v as JSON with the given status.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
