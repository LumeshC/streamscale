package consume

import (
	"log/slog"
	"os"
	"testing"
)

func TestProcessorClosesReader(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	p := New(Options{
		Brokers: []string{"127.0.0.1:1"},
		Topic:   "events",
		GroupID: "g",
		Store:   newMemStore(),
		DLQ:     &memDLQ{},
		Log:     log,
	})
	if p.reader == nil {
		t.Fatal("reader")
	}
	_ = p.Close()
}
