// Package rtstate defines the Redis real-time state layout shared by the
// stream-processor (writer) and analytics-api (reader). Key names are a
// cross-service contract: change them here only, never inline.
package rtstate

import "strconv"

// Key namespace. Every key the processor writes lives under "ss:" so an operator
// can SCAN/flush this service's state without touching anything else.
//
// Redis holds only derived, short-lived, real-time state. PostgreSQL remains the
// system of record for events (events), processing outcomes (processing_status)
// and closed analytics windows (metrics_windows). Nothing here is needed to
// rebuild those tables, and losing all of Redis only loses "the last few minutes"
// views until new traffic refills them.
//
// Every key carries a TTL, so memory is bounded by traffic over the TTL horizon,
// not by total history.
const prefix = "ss:"

// SecondBucketKey → HASH  ss:rt:sec:{unix_second}
//
// Why: rolling metrics ("events in the last 60s", EPS, purchases/min, revenue,
// payment success rate, by type, by region). One hash per event-time second
// with integer counters (HINCRBY). A rolling window of N seconds is the sum of
// N hashes, read in one pipeline.
//
// Why not PostgreSQL: answering "last 60s" from the events table means a range
// scan on every dashboard poll, and metrics_windows only has *closed* windows
// (watermark-delayed by allowed lateness, 30s by default). Redis serves the
// still-open, sub-second-fresh view.
//
// TTL: EXPIREAT bucket_second + BucketRetention (default 15m). Events whose
// bucket would already be expired are not written (replays/backfills do not
// resurrect old buckets).
func SecondBucketKey(sec int64) string {
	return prefix + "rt:sec:" + strconv.FormatInt(sec, 10)
}

// ActiveUsersKey → ZSET  ss:rt:active_users   member=user_id score=last_seen_ms
//
// Why: "how many users are active right now" (seen within ActiveWindow, default
// 5m) and "who". ZCOUNT over a score range is O(log N); stale members are
// trimmed on every write with ZREMRANGEBYSCORE, so the set only ever holds
// users from the last ActiveWindow — not every user ever seen.
//
// A LOGOUT removes the user immediately (if no newer activity is recorded).
//
// TTL: the whole key expires after ActiveWindow*2 of no traffic.
func ActiveUsersKey() string {
	return prefix + "rt:active_users"
}

// UserStateKey → HASH  ss:rt:user:{user_id}
//
// Why: per-user live session state — logged_in, session_start_ms, last_seen_ms,
// last_event_type, region, session_events, rate_limited. This is a *summary*
// that PostgreSQL does not store as a row (it would need an aggregate over the
// user's events on every lookup). Only users active within UserStateTTL have a
// key.
//
// TTL: sliding; refreshed on every event, default 30m (an idle-session timeout).
func UserStateKey(userID string) string {
	return prefix + "rt:user:" + userID
}

// AppliedKey → STRING  ss:rt:applied:{event_id}
//
// Why: makes the Redis write idempotent *within Redis*. PostgreSQL already
// guarantees we only call Redis once per newly inserted event, but a Redis
// call that times out may still have executed server-side; go-redis then
// retries. The Lua script does SET NX on this key first and no-ops if it exists,
// so client retries never double-count.
//
// TTL: DedupTTL (default 10m) — far longer than any retry loop. After it
// expires, PostgreSQL's idempotency still prevents re-application.
func AppliedKey(eventID string) string {
	return prefix + "rt:applied:" + eventID
}

// UserRateKey → STRING (counter)  ss:rl:user:{user_id}:{window_index}
// window_index = unix_seconds / RateWindow (equal to the unix second for the
// default 1s window).
//
// Why: per-user rate limiting (fixed window, processing time).
// Shared across every processor instance, which an in-process limiter cannot
// do because a user's events are spread over partitions → pods only by key
// hashing, which can change on rebalance.
//
// What it limits: events are *facts* already durably stored in PostgreSQL, so
// they are never dropped. A user over the limit is counted in metrics as
// rate_limited and flagged in their user state, and their per-user state
// writes are skipped (hot-key protection for Redis). Global rolling metrics
// still count the event.
//
// TTL: RateWindow + 1s.
func UserRateKey(userID string, sec int64) string {
	return prefix + "rl:user:" + userID + ":" + strconv.FormatInt(sec, 10)
}
