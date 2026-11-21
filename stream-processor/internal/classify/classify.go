package classify

import (
	"context"
	"errors"
	"net"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/segmentio/kafka-go"
)

// Class distinguishes malformed, permanent, and transient failures.
type Class string

const (
	ClassMalformed Class = "malformed"
	ClassPermanent Class = "permanent"
	ClassTransient Class = "transient"
)

// Error is a classified processing error with a stable reason code.
type Error struct {
	Class  Class
	Reason string
	Err    error
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Err == nil {
		return string(e.Class) + ": " + e.Reason
	}
	return string(e.Class) + ": " + e.Reason + ": " + e.Err.Error()
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func Malformed(reason string, err error) *Error {
	return &Error{Class: ClassMalformed, Reason: reason, Err: err}
}

func Permanent(reason string, err error) *Error {
	return &Error{Class: ClassPermanent, Reason: reason, Err: err}
}

func Transient(reason string, err error) *Error {
	return &Error{Class: ClassTransient, Reason: reason, Err: err}
}

// Of returns a classified error. Unclassified errors default to transient.
func Of(err error) *Error {
	if err == nil {
		return nil
	}
	var ce *Error
	if errors.As(err, &ce) {
		return ce
	}
	return FromStore(err)
}

// FromStore maps database / infrastructure errors to transient or permanent.
func FromStore(err error) *Error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return Transient("context_deadline", err)
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return Transient("network", err)
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			return Permanent("unique_violation", err)
		case "23514":
			return Permanent("check_violation", err)
		case "23502":
			return Permanent("not_null_violation", err)
		case "22P02":
			return Permanent("invalid_input", err)
		case "40001", "40P01":
			return Transient("serialization", err)
		case "57P01", "57P03", "08000", "08003", "08006", "53300":
			return Transient("postgres_unavailable", err)
		default:
			if strings.HasPrefix(pgErr.Code, "08") || strings.HasPrefix(pgErr.Code, "53") {
				return Transient("postgres_unavailable", err)
			}
			return Permanent("postgres_"+pgErr.Code, err)
		}
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "connection reset"),
		strings.Contains(msg, "broken pipe"),
		strings.Contains(msg, "i/o timeout"),
		strings.Contains(msg, "timeout"),
		strings.Contains(msg, "temporarily unavailable"),
		strings.Contains(msg, "too many clients"),
		strings.Contains(msg, "begin tx"),
		strings.Contains(msg, "insert event"),
		strings.Contains(msg, "commit"):
		return Transient("connectivity", err)
	default:
		return Transient("store_error", err)
	}
}

// FromKafka maps Kafka client errors to transient failures.
func FromKafka(err error) *Error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return Transient("context_deadline", err)
	}
	if errors.Is(err, kafka.LeaderNotAvailable) || errors.Is(err, kafka.NotLeaderForPartition) {
		return Transient("kafka_leader", err)
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "i/o timeout"),
		strings.Contains(msg, "timeout"),
		strings.Contains(msg, "broken pipe"),
		strings.Contains(msg, "eof"):
		return Transient("kafka_connectivity", err)
	default:
		return Transient("kafka_error", err)
	}
}
