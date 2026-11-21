package analytics

import (
	"context"

	"github.com/LumeshC/streamscale/pkg/event"
)

// Engine is the stream analytics processing contract.
// A future Flink (or other) implementation can replace the in-process Go engine
// behind this interface while keeping the same closed-window sink contract.
type Engine interface {
	// OnEvent applies an event using event-time semantics and may close windows.
	OnEvent(ctx context.Context, e event.Event) ([]ClosedWindow, error)
	// OpenSnapshots returns current open-window metrics (for tests / debugging).
	OpenSnapshots() []Snapshot
	// LateDropCount returns how many events were dropped as too late.
	LateDropCount() int64
}

// Snapshot is an open (not yet closed) window view.
type Snapshot struct {
	Spec    Spec     `json:"spec"`
	Window  Interval `json:"window"`
	Metrics Metrics  `json:"metrics"`
	Rates   Rates    `json:"rates"`
}

// Sink persists closed windows. Implementations must be idempotent on
// (spec name, window start, window end).
type Sink interface {
	PersistClosed(ctx context.Context, windows []ClosedWindow) error
}

// NopSink discards closed windows (tests / disabled persistence).
type NopSink struct{}

func (NopSink) PersistClosed(context.Context, []ClosedWindow) error { return nil }
