package analytics

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/LumeshC/streamscale/pkg/analytics"
	"github.com/LumeshC/streamscale/pkg/event"
)

// Config controls the in-process Go window engine.
type Config struct {
	Specs           []analytics.Spec
	AllowedLateness time.Duration
	Sink            analytics.Sink
}

// MemoryEngine is a Go event-time window engine with tumbling + sliding support.
// It sits behind analytics.Engine so Flink (or another system) can replace it
// later while writing the same ClosedWindow shapes to a Sink.
type MemoryEngine struct {
	mu              sync.Mutex
	specs           []analytics.Spec
	allowedLateness time.Duration
	sink            analytics.Sink
	maxEventTime    time.Time
	watermark       time.Time
	windows         map[string]*windowState
	lateDrops       int64
}

type windowState struct {
	spec analytics.Spec
	win  analytics.Interval
	acc  *accumulator
}

func NewMemoryEngine(cfg Config) *MemoryEngine {
	if len(cfg.Specs) == 0 {
		cfg.Specs = analytics.DefaultSpecs()
	}
	if cfg.AllowedLateness < 0 {
		cfg.AllowedLateness = 0
	}
	if cfg.Sink == nil {
		cfg.Sink = analytics.NopSink{}
	}
	return &MemoryEngine{
		specs:           cfg.Specs,
		allowedLateness: cfg.AllowedLateness,
		sink:            cfg.Sink,
		windows:         map[string]*windowState{},
	}
}

func (e *MemoryEngine) LateDropCount() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lateDrops
}

func (e *MemoryEngine) Watermark() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.watermark
}

func (e *MemoryEngine) OpenSnapshots() []analytics.Snapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]analytics.Snapshot, 0, len(e.windows))
	for _, st := range e.windows {
		m := st.acc.metrics()
		out = append(out, analytics.Snapshot{
			Spec:    st.spec,
			Window:  st.win,
			Metrics: m,
			Rates:   m.Rates(st.win.Duration()),
		})
	}
	return out
}

func (e *MemoryEngine) OnEvent(ctx context.Context, ev event.Event) ([]analytics.ClosedWindow, error) {
	e.mu.Lock()
	ts := ev.Timestamp.UTC()

	// Watermark from previously observed event times (not including this event yet).
	var wm time.Time
	if !e.maxEventTime.IsZero() {
		wm = e.maxEventTime.Add(-e.allowedLateness)
	}

	if !wm.IsZero() && ts.Before(wm) {
		e.lateDrops++
		e.watermark = wm
		closed := e.closeReadyLocked(wm)
		e.mu.Unlock()
		return e.persist(ctx, closed)
	}

	if e.maxEventTime.IsZero() || ts.After(e.maxEventTime) {
		e.maxEventTime = ts
	}
	wm = e.maxEventTime.Add(-e.allowedLateness)
	e.watermark = wm

	for _, spec := range e.specs {
		for _, iv := range assign(spec, ts) {
			// Do not reopen windows already past the watermark.
			if !wm.IsZero() && !iv.End.After(wm) {
				continue
			}
			key := windowKey(spec, iv)
			st, ok := e.windows[key]
			if !ok {
				st = &windowState{spec: spec, win: iv, acc: newAccumulator()}
				e.windows[key] = st
			}
			st.acc.add(ev)
		}
	}

	closed := e.closeReadyLocked(wm)
	e.mu.Unlock()
	return e.persist(ctx, closed)
}

func (e *MemoryEngine) persist(ctx context.Context, closed []analytics.ClosedWindow) ([]analytics.ClosedWindow, error) {
	if len(closed) == 0 {
		return nil, nil
	}
	if err := e.sink.PersistClosed(ctx, closed); err != nil {
		return nil, fmt.Errorf("persist closed windows: %w", err)
	}
	return closed, nil
}

func (e *MemoryEngine) closeReadyLocked(wm time.Time) []analytics.ClosedWindow {
	if wm.IsZero() {
		return nil
	}
	var closed []analytics.ClosedWindow
	now := time.Now().UTC()
	for key, st := range e.windows {
		if st.win.End.After(wm) {
			continue
		}
		m := st.acc.metrics()
		closed = append(closed, analytics.ClosedWindow{
			Spec:     st.spec,
			Window:   st.win,
			Metrics:  m,
			Rates:    m.Rates(st.win.Duration()),
			ClosedAt: now,
		})
		delete(e.windows, key)
	}
	return closed
}

func assign(spec analytics.Spec, ts time.Time) []analytics.Interval {
	switch spec.Kind {
	case analytics.KindSliding:
		slide := spec.Slide
		if slide <= 0 {
			slide = spec.Size
		}
		return analytics.AssignSliding(ts, spec.Size, slide)
	default:
		return []analytics.Interval{analytics.AssignTumbling(ts, spec.Size)}
	}
}

func windowKey(spec analytics.Spec, iv analytics.Interval) string {
	return fmt.Sprintf("%s|%s|%d|%d", spec.Name, spec.Kind, iv.Start.UnixNano(), iv.End.UnixNano())
}

// Ensure MemoryEngine implements analytics.Engine.
var _ analytics.Engine = (*MemoryEngine)(nil)
