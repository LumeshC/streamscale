package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/LumeshC/streamscale/stream-processor/internal/classify"
)

func TestDoSucceedsAfterTransient(t *testing.T) {
	n := 0
	attempts, err := Do(context.Background(), Config{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond}, func(attempt int) error {
		n++
		if attempt < 3 {
			return classify.Transient("flaky", errors.New("boom"))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 3 || n != 3 {
		t.Fatalf("attempts=%d n=%d", attempts, n)
	}
}

func TestDoDoesNotRetryPermanent(t *testing.T) {
	n := 0
	attempts, err := Do(context.Background(), Config{MaxAttempts: 5, BaseDelay: time.Millisecond}, func(attempt int) error {
		n++
		return classify.Permanent("bad", errors.New("nope"))
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if attempts != 1 || n != 1 {
		t.Fatalf("attempts=%d n=%d", attempts, n)
	}
	if classify.Of(err).Class != classify.ClassPermanent {
		t.Fatalf("%v", err)
	}
}

func TestDoExhaustsTransient(t *testing.T) {
	attempts, err := Do(context.Background(), Config{MaxAttempts: 2, BaseDelay: time.Millisecond}, func(int) error {
		return classify.Transient("db", errors.New("down"))
	})
	if err == nil || attempts != 2 {
		t.Fatalf("attempts=%d err=%v", attempts, err)
	}
	c := classify.Of(err)
	if c.Class != classify.ClassTransient || c.Reason != "retries_exhausted" {
		t.Fatalf("%+v", c)
	}
}
