package realtime

import (
	"context"
	_ "embed"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/streamscale/streamscale/pkg/event"
	"github.com/streamscale/streamscale/pkg/rtstate"
)

//go:embed apply.lua
var applyLua string

var applyScript = redis.NewScript(applyLua)

// maxBucketFutureSkew: events stamped further ahead than this are not written
// to second buckets (they would sit invisible in the future and skew "now").
const maxBucketFutureSkew = time.Minute

// Store reads and writes real-time state in Redis. It has no fallback logic of
// its own; Tracker adds timeouts, the circuit breaker and degradation.
type Store struct {
	rdb *redis.Client
	cfg Config
	now func() time.Time
}

func NewStore(rdb *redis.Client, cfg Config) *Store {
	return &Store{rdb: rdb, cfg: cfg.withDefaults(), now: time.Now}
}

// ApplyResult reports what the apply script did.
type ApplyResult struct {
	Applied     bool // false when this event_id was already applied (retry)
	RateLimited bool
}

// Apply records one newly persisted event. Idempotent per event_id for DedupTTL.
func (s *Store) Apply(ctx context.Context, e event.Event) (ApplyResult, error) {
	now := s.now().UTC()
	nowMs := now.UnixMilli()

	ts := e.Timestamp.UTC()
	tsMs := ts.UnixMilli()
	if tsMs > nowMs {
		// Clock skew: never let a future-stamped event look "more recent than now".
		tsMs = nowMs
	}

	sec := ts.Unix()
	var expireAt int64
	if !ts.After(now.Add(maxBucketFutureSkew)) {
		expireAt = sec + int64(s.cfg.BucketRetention/time.Second)
		if expireAt <= now.Unix() {
			expireAt = 0 // bucket already past retention: durable path only
		}
	}

	var cents int64
	if e.EventType == event.TypePurchase {
		cents = amountCents(e.Amount)
	}

	keys := []string{
		rtstate.AppliedKey(e.EventID),
		rtstate.SecondBucketKey(sec),
		rtstate.ActiveUsersKey(),
		rtstate.UserStateKey(e.UserID),
		rtstate.UserRateKey(e.UserID, now.Unix()/int64(s.cfg.RateWindow/time.Second)),
	}
	args := []any{
		int64(s.cfg.DedupTTL / time.Second),
		expireAt,
		string(e.EventType),
		e.Region,
		cents,
		tsMs,
		e.UserID,
		s.cfg.ActiveWindow.Milliseconds(),
		nowMs,
		int64(s.cfg.UserStateTTL / time.Second),
		s.cfg.UserRateLimit,
		int64(s.cfg.RateWindow/time.Second) + 1,
		int64(2 * s.cfg.ActiveWindow / time.Second),
	}
	res, err := applyScript.Run(ctx, s.rdb, keys, args...).Int64Slice()
	if err != nil {
		return ApplyResult{}, err
	}
	if len(res) != 2 {
		return ApplyResult{}, fmt.Errorf("apply script: unexpected reply %v", res)
	}
	return ApplyResult{Applied: res[0] == 1, RateLimited: res[1] == 1}, nil
}

// Snapshot is the rolling real-time view (shared with analytics-api).
type Snapshot = rtstate.Snapshot

// SecondPoint is one event-time second of the rolling series.
type SecondPoint = rtstate.SecondPoint

// Snapshot sums the last `window` seconds of buckets and counts active users.
func (s *Store) Snapshot(ctx context.Context, window time.Duration) (Snapshot, error) {
	return rtstate.ReadSnapshot(ctx, s.rdb, s.now().UTC(), window, rtstate.ReadOptions{
		ActiveWindow: s.cfg.ActiveWindow,
		MaxWindow:    s.cfg.BucketRetention,
	})
}

// UserState is the live per-user session view.
type UserState struct {
	UserID         string `json:"user_id"`
	LoggedIn       bool   `json:"logged_in"`
	LastSeenMs     int64  `json:"last_seen_ms"`
	LastEventType  string `json:"last_event_type"`
	Region         string `json:"region"`
	SessionStartMs int64  `json:"session_start_ms,omitempty"`
	SessionEvents  int64  `json:"session_events"`
	RateLimited    int64  `json:"rate_limited"`
	TTLSeconds     int64  `json:"ttl_seconds"`
}

// UserState returns the user's live state; found=false when the user has no
// recent activity (key expired or never seen).
func (s *Store) UserState(ctx context.Context, userID string) (UserState, bool, error) {
	pipe := s.rdb.Pipeline()
	h := pipe.HGetAll(ctx, rtstate.UserStateKey(userID))
	ttl := pipe.TTL(ctx, rtstate.UserStateKey(userID))
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return UserState{}, false, err
	}
	m := h.Val()
	if len(m) == 0 {
		return UserState{}, false, nil
	}
	i := func(k string) int64 { v, _ := strconv.ParseInt(m[k], 10, 64); return v }
	return UserState{
		UserID:         userID,
		LoggedIn:       m["logged_in"] == "1",
		LastSeenMs:     i("last_seen_ms"),
		LastEventType:  m["last_event_type"],
		Region:         m["region"],
		SessionStartMs: i("session_start_ms"),
		SessionEvents:  i("session_events"),
		RateLimited:    i("rate_limited"),
		TTLSeconds:     int64(ttl.Val() / time.Second),
	}, true, nil
}

func (s *Store) Ping(ctx context.Context) error {
	return s.rdb.Ping(ctx).Err()
}

// amountCents converts a validated decimal amount to integer cents (half-up).
// Like Phase 3 revenue, amounts are summed regardless of currency.
func amountCents(amount string) int64 {
	r := new(big.Rat)
	if _, ok := r.SetString(strings.TrimSpace(amount)); !ok || r.Sign() < 0 {
		return 0
	}
	r.Mul(r, big.NewRat(100, 1))
	r.Add(r, big.NewRat(1, 2))
	q := new(big.Int).Quo(r.Num(), r.Denom())
	if !q.IsInt64() {
		return 0
	}
	return q.Int64()
}
