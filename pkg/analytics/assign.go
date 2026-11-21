package analytics

import (
	"time"
)

// AssignTumbling returns the single tumbling window containing ts: [start, start+size).
func AssignTumbling(ts time.Time, size time.Duration) Interval {
	if size <= 0 {
		size = time.Second
	}
	ts = ts.UTC()
	startUnix := ts.UnixNano() - (ts.UnixNano() % int64(size))
	start := time.Unix(0, startUnix).UTC()
	return Interval{Start: start, End: start.Add(size)}
}

// AssignSliding returns all sliding windows of (size, slide) that contain ts.
// Window starts are aligned to slide boundaries.
func AssignSliding(ts time.Time, size, slide time.Duration) []Interval {
	if size <= 0 || slide <= 0 {
		return nil
	}
	ts = ts.UTC()
	lastStart := alignFloor(ts, slide)
	// Earliest start that can still contain ts: start > ts-size  ⇒ start >= alignFloor(ts-size+ε)
	earliest := ts.Add(-size).Add(time.Nanosecond)
	firstStart := alignFloor(earliest, slide)
	if firstStart.Before(earliest) {
		firstStart = firstStart.Add(slide)
	}

	out := make([]Interval, 0, int(size/slide)+1)
	for start := firstStart; !start.After(lastStart); start = start.Add(slide) {
		end := start.Add(size)
		if !ts.Before(start) && ts.Before(end) {
			out = append(out, Interval{Start: start, End: end})
		}
	}
	return out
}

func alignFloor(t time.Time, d time.Duration) time.Time {
	t = t.UTC()
	ns := t.UnixNano()
	return time.Unix(0, ns-(ns%int64(d))).UTC()
}

// DefaultSpecs returns the Phase 3 window definitions.
func DefaultSpecs() []Spec {
	return []Spec{
		{Name: "eps_1s", Kind: KindTumbling, Size: time.Second, Slide: time.Second},
		{Name: "metrics_1m", Kind: KindTumbling, Size: time.Minute, Slide: time.Minute},
		{Name: "metrics_5m_slide_1m", Kind: KindSliding, Size: 5 * time.Minute, Slide: time.Minute},
	}
}
