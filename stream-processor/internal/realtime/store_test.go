package realtime

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/streamscale/streamscale/pkg/event"
)

func TestAmountCents(t *testing.T) {
	cases := map[string]int64{
		"42.00": 4200, "0.01": 1, "19.999": 2000, "7": 700, "0.004": 0, "bad": 0, "-1.00": 0,
	}
	for in, want := range cases {
		if got := amountCents(in); got != want {
			t.Errorf("amountCents(%q)=%d want %d", in, got, want)
		}
	}
}

func TestConfigDefaultsFillZeroValues(t *testing.T) {
	c := Config{}.withDefaults()
	d := DefaultConfig()
	if c.Addr != d.Addr || c.OpTimeout != d.OpTimeout || c.BucketRetention != d.BucketRetention || c.RateWindow != time.Second {
		t.Fatalf("defaults not applied: %+v", c)
	}
}

// unusedAddr returns a localhost address with nothing listening.
func unusedAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// With Redis unreachable, Observe must return quickly, never panic, open the
// circuit, and then skip Redis entirely.
func TestTrackerDegradesWhenRedisDown(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Addr = unusedAddr(t)
	cfg.BreakerThreshold = 2
	cfg.BreakerCooldown = time.Hour
	tr := NewTracker(NewClient(cfg), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer tr.Close()

	e := event.Event{
		EventID: "e1", EventType: event.TypeLogin, UserID: "u1",
		Timestamp: time.Now(), Region: "us-east-1", Metadata: map[string]string{},
	}
	start := time.Now()
	for i := 0; i < 100; i++ {
		e.EventID = "e" + string(rune('a'+i%26))
		tr.Observe(context.Background(), e)
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("degraded path too slow: %s", el)
	}
	s := tr.Stats()
	if s.State != StateOpen {
		t.Fatalf("circuit should be open, got %s", s.State)
	}
	if s.Failures != 2 || s.Skipped != 98 {
		t.Fatalf("stats %+v", s)
	}
	if _, err := tr.Snapshot(context.Background(), time.Minute); err != ErrUnavailable {
		t.Fatalf("snapshot err=%v want ErrUnavailable", err)
	}
	if err := tr.Check(context.Background()); err == nil {
		t.Fatal("check must report degraded")
	}
}
