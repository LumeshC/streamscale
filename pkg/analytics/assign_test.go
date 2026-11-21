package analytics

import (
	"testing"
	"time"
)

func TestAssignTumblingBoundary(t *testing.T) {
	size := time.Minute
	// Exactly on boundary belongs to the window starting at that instant.
	ts := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	w := AssignTumbling(ts, size)
	if !w.Start.Equal(ts) || !w.End.Equal(ts.Add(size)) {
		t.Fatalf("got %+v", w)
	}
	// One nanosecond before next minute stays in previous window.
	ts2 := time.Date(2026, 9, 18, 12, 0, 59, 999999999, time.UTC)
	w2 := AssignTumbling(ts2, size)
	if !w2.Start.Equal(ts) {
		t.Fatalf("got start %v want %v", w2.Start, ts)
	}
	// Next second rolls to next tumbling window.
	ts3 := time.Date(2026, 9, 18, 12, 1, 0, 0, time.UTC)
	w3 := AssignTumbling(ts3, size)
	if !w3.Start.Equal(ts3) {
		t.Fatalf("got %v", w3.Start)
	}
}

func TestAssignTumblingOneSecond(t *testing.T) {
	ts := time.Date(2026, 9, 18, 12, 0, 0, 500, time.UTC)
	w := AssignTumbling(ts, time.Second)
	wantStart := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	if !w.Start.Equal(wantStart) || !w.End.Equal(wantStart.Add(time.Second)) {
		t.Fatalf("%+v", w)
	}
}

func TestAssignSlidingCoverage(t *testing.T) {
	size := 5 * time.Minute
	slide := time.Minute
	ts := time.Date(2026, 9, 18, 12, 3, 30, 0, time.UTC)
	windows := AssignSliding(ts, size, slide)
	// Windows starting at 11:59, 12:00, 12:01, 12:02, 12:03 (5 windows)
	if len(windows) != 5 {
		t.Fatalf("len=%d windows=%v", len(windows), windows)
	}
	for _, w := range windows {
		if !w.Contains(ts) {
			t.Fatalf("window %+v should contain %v", w, ts)
		}
		if w.Duration() != size {
			t.Fatalf("duration %v", w.Duration())
		}
	}
}

func TestAssignSlidingBoundaryExclusiveEnd(t *testing.T) {
	size := 5 * time.Minute
	slide := time.Minute
	// Event at exact end of a window is not in that window (half-open).
	start := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	end := start.Add(size)
	windows := AssignSliding(end, size, slide)
	for _, w := range windows {
		if w.Start.Equal(start) && w.End.Equal(end) {
			t.Fatalf("event at end should not be in [%v, %v)", start, end)
		}
	}
}
