package classify

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestFromStoreTransientNetwork(t *testing.T) {
	err := FromStore(&net.OpError{Op: "dial", Err: errors.New("connection refused")})
	if err.Class != ClassTransient {
		t.Fatalf("got %s", err.Class)
	}
}

func TestFromStorePermanentCheck(t *testing.T) {
	err := FromStore(&pgconn.PgError{Code: "23514", Message: "check"})
	if err.Class != ClassPermanent {
		t.Fatalf("got %s", err.Class)
	}
}

func TestFromStoreUnique(t *testing.T) {
	err := FromStore(&pgconn.PgError{Code: "23505", Message: "unique"})
	if err.Class != ClassPermanent || err.Reason != "unique_violation" {
		t.Fatalf("%+v", err)
	}
}

func TestFromStoreContext(t *testing.T) {
	err := FromStore(context.DeadlineExceeded)
	if err.Class != ClassTransient {
		t.Fatalf("got %s", err.Class)
	}
}

func TestOf(t *testing.T) {
	err := Malformed("bad_json", errors.New("x"))
	got := Of(err)
	if got.Class != ClassMalformed {
		t.Fatalf("%+v", got)
	}
	if Of(errors.New("connection refused")).Class != ClassTransient {
		t.Fatal("unclassified connectivity should be transient")
	}
}
