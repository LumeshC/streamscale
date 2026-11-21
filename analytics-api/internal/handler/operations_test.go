package handler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"testing"
	"time"

	"github.com/streamscale/streamscale/analytics-api/internal/kafkalag"
	"github.com/streamscale/streamscale/analytics-api/internal/repo"
)

// ---- fakeRepo: Phase 6 methods ----

func (f *fakeRepo) Latency(ctx context.Context, from, to time.Time) (repo.LatencyStats, []repo.LatencyPoint, error) {
	if err := f.wait(ctx); err != nil {
		return repo.LatencyStats{}, nil, err
	}
	f.lastLatency = [2]time.Time{from, to}
	return f.latency, f.latencyPts, nil
}

func (f *fakeRepo) Errors(ctx context.Context, q repo.ErrorQuery) ([]repo.ErrorRecord, error) {
	if err := f.wait(ctx); err != nil {
		return nil, err
	}
	recs := append([]repo.ErrorRecord(nil), f.errs...)
	sort.Slice(recs, func(i, j int) bool {
		if !recs[i].OccurredAt.Equal(recs[j].OccurredAt) {
			return recs[i].OccurredAt.After(recs[j].OccurredAt)
		}
		return recs[i].ID > recs[j].ID
	})
	var out []repo.ErrorRecord
	for _, e := range recs {
		if q.Class != "" && (e.FailureClass == nil || *e.FailureClass != q.Class) {
			continue
		}
		if q.BeforeAt != nil {
			if e.OccurredAt.After(*q.BeforeAt) || (e.OccurredAt.Equal(*q.BeforeAt) && e.ID >= *q.BeforeID) {
				continue
			}
		}
		out = append(out, e)
		if len(out) == q.Limit+1 {
			break
		}
	}
	return out, nil
}

func (f *fakeRepo) ErrorReasons(ctx context.Context, from, to time.Time, class string) ([]repo.ReasonCount, error) {
	if err := f.wait(ctx); err != nil {
		return nil, err
	}
	m := map[[2]string]int64{}
	for _, e := range f.errs {
		c := *e.FailureClass
		if class != "" && c != class {
			continue
		}
		m[[2]string{c, *e.FailureReason}]++
	}
	var out []repo.ReasonCount
	for k, v := range m {
		out = append(out, repo.ReasonCount{Class: k[0], Reason: k[1], Count: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Count > out[j].Count })
	return out, nil
}

func (f *fakeRepo) StatusCounts(ctx context.Context, from, to time.Time) (repo.StatusCounts, error) {
	if err := f.wait(ctx); err != nil {
		return repo.StatusCounts{}, err
	}
	return repo.StatusCounts{Processed: 990, Duplicate: 5, DLQ: int64(len(f.errs))}, nil
}

type fakeLag struct {
	rep kafkalag.Report
	err error
}

func (l fakeLag) Lag(context.Context) (kafkalag.Report, error) { return l.rep, l.err }
func (l fakeLag) Ping(context.Context) error                   { return l.err }

func newAPIWithLag(r repo.Repository, lag kafkalag.Source) http.Handler {
	a := New(Options{
		Repo: r, Live: fakeLive{}, Lag: lag, Now: func() time.Time { return now },
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	return a.Router(slog.New(slog.NewTextHandler(io.Discard, nil)), 200*time.Millisecond)
}

func strp(s string) *string { return &s }

// ---- tests ----

func TestConsumerLag(t *testing.T) {
	rep := kafkalag.Report{Group: "g", Topic: "events", AsOf: now, TotalLag: 42,
		Partitions: []kafkalag.PartitionLag{{Partition: 0, EndOffset: 100, CommittedOffset: 58, Lag: 42}}}
	rec, body := get(t, newAPIWithLag(&fakeRepo{}, fakeLag{rep: rep}), "/metrics/consumer-lag")
	if rec.Code != 200 || body["total_lag"].(float64) != 42 || len(body["partitions"].([]any)) != 1 {
		t.Fatalf("%d %v", rec.Code, body)
	}

	down := fakeLag{err: fmt.Errorf("%w: dial tcp: refused", kafkalag.ErrUnavailable)}
	rec, body = get(t, newAPIWithLag(&fakeRepo{}, down), "/metrics/consumer-lag")
	if rec.Code != 503 || errCode(body) != "unavailable" {
		t.Fatalf("kafka down: %d %v", rec.Code, body)
	}
	rec, body = get(t, newAPIWithLag(&fakeRepo{}, nil), "/metrics/consumer-lag")
	if rec.Code != 503 || errCode(body) != "unavailable" {
		t.Fatalf("disabled: %d %v", rec.Code, body)
	}
	if rec, _ := get(t, newAPIWithLag(&fakeRepo{}, fakeLag{rep: rep}), "/metrics/consumer-lag?x=1"); rec.Code != 400 {
		t.Fatalf("unknown param: %d", rec.Code)
	}
}

func TestHealthReportsKafka(t *testing.T) {
	_, body := get(t, newAPIWithLag(&fakeRepo{}, fakeLag{err: errors.New("down")}), "/health")
	if body["status"] != "degraded" || body["checks"].(map[string]any)["kafka"].(map[string]any)["status"] != "down" {
		t.Fatalf("%v", body)
	}
	_, body = get(t, newAPIWithLag(&fakeRepo{}, nil), "/health")
	if body["status"] != "ok" || body["checks"].(map[string]any)["kafka"].(map[string]any)["status"] != "disabled" {
		t.Fatalf("%v", body)
	}
}

func TestLatency(t *testing.T) {
	r := &fakeRepo{
		latency: repo.LatencyStats{Count: 1000, P50: 12.34, P95: 48.06, P99: 95.551, Max: 210},
		latencyPts: []repo.LatencyPoint{{Minute: now.Add(-time.Minute),
			LatencyStats: repo.LatencyStats{Count: 60, P50: 10, P95: 40, P99: 90, Max: 100}}},
	}
	h := newAPIWithLag(r, nil)
	rec, body := get(t, h, "/metrics/latency")
	s := body["summary"].(map[string]any)
	if rec.Code != 200 || s["p50_ms"].(float64) != 12.3 || s["p99_ms"].(float64) != 95.6 || len(body["data"].([]any)) != 1 {
		t.Fatalf("%d %v", rec.Code, body)
	}
	if got := r.lastLatency[1].Sub(r.lastLatency[0]); got != 15*time.Minute {
		t.Fatalf("default latency range %s, want 15m", got)
	}
	rec, body = get(t, h, "/metrics/latency?from="+now.Add(-4*time.Hour).Format(time.RFC3339))
	if rec.Code != 400 || errFields(body)[0] != "from" {
		t.Fatalf("range over 3h cap must be rejected: %d %v", rec.Code, body)
	}
}

func TestErrorsPaginationFilterAndRate(t *testing.T) {
	var errs []repo.ErrorRecord
	for i := 0; i < 7; i++ {
		class, reason := "malformed", "invalid_json"
		if i%3 == 0 {
			class, reason = "permanent", "check_violation"
		}
		errs = append(errs, repo.ErrorRecord{
			ID: int64(i + 1), Topic: "events", Partition: 1, Offset: int64(100 + i),
			FailureClass: strp(class), FailureReason: strp(reason), Attempts: 1,
			// Two records share a timestamp to exercise the (time, id) tiebreak.
			OccurredAt: now.Add(-time.Duration(i/2) * time.Minute),
		})
	}
	h := newAPIWithLag(&fakeRepo{errs: errs}, nil)

	var ids []float64
	target := "/errors?limit=3"
	for i := 0; i < 5; i++ {
		rec, body := get(t, h, target)
		if rec.Code != 200 {
			t.Fatalf("%d %v", rec.Code, body)
		}
		if i == 0 {
			tot := body["totals"].(map[string]any)
			if tot["dlq"].(float64) != 7 || tot["error_rate"].(float64) != 0.006986 {
				t.Fatalf("totals %v", tot)
			}
			if br := body["by_reason"].([]any); len(br) != 2 || br[0].(map[string]any)["count"].(float64) != 4 {
				t.Fatalf("by_reason %v", br)
			}
		}
		for _, d := range body["data"].([]any) {
			ids = append(ids, d.(map[string]any)["id"].(float64))
		}
		next, ok := body["page"].(map[string]any)["next_cursor"].(string)
		if !ok {
			break
		}
		target = "/errors?limit=3&cursor=" + url.QueryEscape(next)
	}
	if fmt.Sprint(ids) != "[2 1 4 3 6 5 7]" {
		t.Fatalf("page order %v", ids)
	}

	_, body := get(t, h, "/errors?class=permanent")
	if n := len(body["data"].([]any)); n != 3 {
		t.Fatalf("class filter: %d", n)
	}
	rec, body := get(t, h, "/errors?class=fatal&limit=501")
	if rec.Code != 400 || len(errFields(body)) != 2 {
		t.Fatalf("%d %v", rec.Code, body)
	}
}
