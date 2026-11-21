package handler

import (
	"errors"
	"net/http"
	"time"

	"github.com/LumeshC/streamscale/analytics-api/internal/httpx"
	"github.com/LumeshC/streamscale/analytics-api/internal/kafkalag"
	"github.com/LumeshC/streamscale/analytics-api/internal/repo"
)

// Operational endpoints (Phase 6): consumer lag, processing latency, errors.

// ---- GET /metrics/consumer-lag ----

// ConsumerLag is a point-in-time read of the processor group's lag. Kafka
// being unreachable is a 503 `unavailable` (the dashboard shows it as such).
func (a *API) ConsumerLag(w http.ResponseWriter, r *http.Request) {
	if err := httpx.ParseQuery(r).Err(); err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	rep, err := a.lag.Lag(r.Context())
	if err != nil {
		if errors.Is(err, kafkalag.ErrUnavailable) && r.Context().Err() == nil {
			err = httpx.Unavailable("kafka is unavailable", err)
		}
		a.fail(w, r, err)
		return
	}
	if rep.Partitions == nil {
		rep.Partitions = []kafkalag.PartitionLag{}
	}
	httpx.WriteJSON(w, http.StatusOK, rep)
}

// ---- GET /metrics/latency ----

type latencyStatsJSON struct {
	Count int64   `json:"count"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
}

type latencyPointJSON struct {
	Minute time.Time `json:"minute"`
	latencyStatsJSON
}

type latencyJSON struct {
	Range      rangeJSON          `json:"range"`
	Definition string             `json:"definition"`
	Summary    latencyStatsJSON   `json:"summary"`
	Data       []latencyPointJSON `json:"data"`
}

func latStats(s repo.LatencyStats) latencyStatsJSON {
	return latencyStatsJSON{Count: s.Count, P50: round(s.P50, 1), P95: round(s.P95, 1), P99: round(s.P99, 1), Max: round(s.Max, 1)}
}

// Latency reports end-to-end ingest latency percentiles. Percentiles need the
// raw rows, so the range is capped lower than for window-based endpoints.
func (a *API) Latency(w http.ResponseWriter, r *http.Request) {
	q := httpx.ParseQuery(r, "from", "to")
	tr := q.Range(a.now(), 15*time.Minute, a.latencyMax)
	if err := q.Err(); err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	sum, pts, err := a.repo.Latency(r.Context(), tr.From, tr.To)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	data := make([]latencyPointJSON, 0, len(pts))
	for _, p := range pts {
		data = append(data, latencyPointJSON{Minute: p.Minute, latencyStatsJSON: latStats(p.LatencyStats)})
	}
	httpx.WriteJSON(w, http.StatusOK, latencyJSON{
		Range:      rangeOf(tr),
		Definition: "ingested_at - event timestamp: generator -> Kafka -> processor -> PostgreSQL commit",
		Summary:    latStats(sum),
		Data:       data,
	})
}

// ---- GET /errors ----

type errorCursor struct {
	At time.Time `json:"t"`
	ID int64     `json:"i"`
}

type errorJSON struct {
	ID            int64     `json:"id"`
	EventID       *string   `json:"event_id"`
	Topic         string    `json:"topic"`
	Partition     int32     `json:"partition"`
	Offset        int64     `json:"offset"`
	FailureClass  string    `json:"failure_class"`
	FailureReason string    `json:"failure_reason"`
	Attempts      int32     `json:"attempts"`
	OccurredAt    time.Time `json:"occurred_at"`
}

type reasonJSON struct {
	FailureClass  string `json:"failure_class"`
	FailureReason string `json:"failure_reason"`
	Count         int64  `json:"count"`
}

type statusTotalsJSON struct {
	Processed int64   `json:"processed"`
	Duplicate int64   `json:"duplicate"`
	DLQ       int64   `json:"dlq"`
	ErrorRate float64 `json:"error_rate"`
}

type errorsJSON struct {
	Range    rangeJSON        `json:"range"`
	Totals   statusTotalsJSON `json:"totals"`
	ByReason []reasonJSON     `json:"by_reason"`
	Data     []errorJSON      `json:"data"`
	Page     httpx.Page       `json:"page"`
}

func deref(s *string, def string) string {
	if s == nil || *s == "" {
		return def
	}
	return *s
}

// Errors lists dead-lettered messages newest first, with volume by reason and
// the error rate (dlq / all outcomes) over the range.
func (a *API) Errors(w http.ResponseWriter, r *http.Request) {
	q := httpx.ParseQuery(r, "from", "to", "class", "limit", "cursor")
	tr := q.Range(a.now(), 24*time.Hour, a.maxRange)
	class := q.Enum("class", "all", "all", "malformed", "permanent", "transient")
	limit := q.Int("limit", 50, 1, 500)
	var cur errorCursor
	hasCur := q.Cursor(&cur)
	if err := q.Err(); err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	if class == "all" {
		class = ""
	}
	eq := repo.ErrorQuery{From: tr.From, To: tr.To, Class: class, Limit: limit}
	if hasCur {
		at, id := cur.At, cur.ID
		eq.BeforeAt, eq.BeforeID = &at, &id
	}
	recs, err := a.repo.Errors(r.Context(), eq)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	reasons, err := a.repo.ErrorReasons(r.Context(), tr.From, tr.To, class)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	counts, err := a.repo.StatusCounts(r.Context(), tr.From, tr.To)
	if err != nil {
		a.fail(w, r, err)
		return
	}

	page := httpx.Page{Limit: limit}
	if len(recs) > limit {
		recs = recs[:limit]
		last := recs[len(recs)-1]
		page.NextCursor = httpx.EncodeCursor(errorCursor{At: last.OccurredAt, ID: last.ID})
	}
	data := make([]errorJSON, 0, len(recs))
	for _, e := range recs {
		data = append(data, errorJSON{
			ID: e.ID, EventID: e.EventID, Topic: e.Topic, Partition: e.Partition, Offset: e.Offset,
			FailureClass: deref(e.FailureClass, "unknown"), FailureReason: deref(e.FailureReason, "unknown"),
			Attempts: e.Attempts, OccurredAt: e.OccurredAt,
		})
	}
	by := make([]reasonJSON, 0, len(reasons))
	for _, rc := range reasons {
		by = append(by, reasonJSON{FailureClass: rc.Class, FailureReason: rc.Reason, Count: rc.Count})
	}
	total := counts.Processed + counts.Duplicate + counts.DLQ
	rate := 0.0
	if total > 0 {
		rate = round(float64(counts.DLQ)/float64(total), 6)
	}
	httpx.WriteJSON(w, http.StatusOK, errorsJSON{
		Range:    rangeOf(tr),
		Totals:   statusTotalsJSON{Processed: counts.Processed, Duplicate: counts.Duplicate, DLQ: counts.DLQ, ErrorRate: rate},
		ByReason: by,
		Data:     data,
		Page:     page,
	})
}
