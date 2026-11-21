# Phase 4 — Redis real-time state

## Architecture

```
Kafka (events)
   ↓
Go stream-processor
   ├── PostgreSQL  → durable storage (system of record)
   │      events, processing_status, metrics_windows (closed windows)
   └── Redis       → real-time state (derived, best-effort, TTL-bounded)
          rolling per-second metrics, active users, live user sessions,
          per-user rate limiting, idempotency markers for Redis retries
```

Per message, the processor does:

1. Decode + validate (malformed → DLQ).
2. `store.Process` in PostgreSQL (idempotent insert, retried; failure → DLQ).
3. **If the event was newly inserted**: `realtime.Tracker.Observe` → one Lua
   script in Redis (≤ `REDIS_OP_TIMEOUT_MS`, never fails the message).
4. Phase 3 analytics engine (closed windows → PostgreSQL).
5. Commit the Kafka offset.

Code: `stream-processor/internal/realtime/` (`keys.go`, `apply.lua`, `store.go`,
`tracker.go`, `breaker.go`, `http.go`).

## What lives in Redis — and why

Redis holds only state that is (a) needed with sub-second freshness, (b) cheap to
lose, and (c) awkward or expensive to compute from PostgreSQL on every read.
It is **not** a copy of the events table. Every key has a TTL, so memory is
bounded by recent traffic, not by history.

| Key | Type | TTL | Why it exists | Why not PostgreSQL |
|---|---|---|---|---|
| `ss:rt:sec:{unix_sec}` | HASH of counters (`events`, `purchases`, `revenue_cents`, `pay_ok`, `pay_fail`, `rate_limited`, `type:*`, `region:*`) | `EXPIREAT sec + 15m` | Rolling metrics: last-N-seconds EPS, purchases, revenue, payment success rate, by type/region. Rolling window = sum of N buckets, one pipeline. | `metrics_windows` only contains windows **closed** by the watermark (≥ 30 s behind). A live "last 60 s" query over `events` would range-scan on every poll. |
| `ss:rt:active_users` | ZSET `user_id → last_seen_ms` | Trimmed to 5 m on every write; key expires after 10 m idle | "Active users right now" (`ZCOUNT`), including immediate removal on `LOGOUT`. | `COUNT(DISTINCT user_id)` over a moving range per request; Phase 3 active users are per closed window only. |
| `ss:rt:user:{user_id}` | HASH (`logged_in`, `session_start_ms`, `last_seen_ms`, `last_event_type`, `region`, `session_events`, `rate_limited`) | Sliding 30 m (idle-session timeout) | Live per-user session state for lookups. | PostgreSQL has the raw events but no per-user session row; deriving it needs an aggregate per lookup. Only users active in the last 30 m have a key. |
| `ss:rt:applied:{event_id}` | STRING marker | 10 m | Short-lived aggregation guard: makes the Redis write idempotent across **client retries** of an ambiguous timeout. | PostgreSQL already de-duplicates Kafka redeliveries; this covers the Redis hop itself. |
| `ss:rl:user:{user_id}:{window}` | STRING counter | rate window + 1 s | Per-user fixed-window rate limiting shared by all processor instances. | Hot counter with sub-second lifetime; a DB write per event for it would be wasteful. |

Explicitly **not** in Redis: raw events, processing status, DLQ data, closed
analytics windows, anything needed to recover after a crash.

### Rate limiting: where it is (and isn't) appropriate

Events on Kafka are facts that have already been accepted upstream; the
processor must not drop them. So the limiter does **not** gate persistence or
global metrics. When a user exceeds `REALTIME_USER_RATE_LIMIT` events per
`REALTIME_RATE_WINDOW_S`:

- the event is still in PostgreSQL and still counted in rolling metrics,
- `rate_limited` is incremented in the second bucket (visible abuse signal),
- that user's session hash gets `rate_limited++` instead of full updates
  (hot-key protection for Redis).

The read API is not rate-limited here; it belongs behind the future API/dashboard
gateway.

### Atomicity

All writes for one event happen in a single Lua script (`apply.lua`): one round
trip, executed atomically by Redis, so concurrent processor instances never see a
half-applied event. The script first does `SET NX` on the applied marker and
exits if it already exists.

## Connection pooling, timeouts, retries

| Setting | Default | Purpose |
|---|---|---|
| `REDIS_POOL_SIZE` | 20 | Max connections per process (go-redis pool). |
| `REDIS_MIN_IDLE_CONNS` | 2 | Pre-dialed connections; avoids dial latency after idle. |
| `REDIS_POOL_TIMEOUT_MS` | 300 | Max wait for a free pooled connection. |
| `REDIS_DIAL_TIMEOUT_MS` | 500 | TCP connect timeout. |
| `REDIS_READ_TIMEOUT_MS` / `REDIS_WRITE_TIMEOUT_MS` | 200 | Socket I/O timeouts. |
| `REDIS_OP_TIMEOUT_MS` | 300 | **Total** per-event budget (context deadline, incl. retries). The Kafka loop never waits longer on Redis. |
| `REDIS_MAX_RETRIES` | 2 | go-redis retries on network errors/timeouts with 8–64 ms backoff. Safe because the script is idempotent per `event_id`. |
| `REDIS_BREAKER_THRESHOLD` | 3 | Consecutive failures that open the circuit. |
| `REDIS_BREAKER_COOLDOWN_MS` | 5000 | How long to skip Redis before one probe. |

The client is created lazily (no dial at construction), so the processor starts
even if Redis is down.

## Graceful degradation

```
closed ──(N consecutive failures)──▶ open ──(cooldown)──▶ half_open
  ▲                                   ▲                      │
  └────────────(probe succeeds)───────┼──────────────────────┘
                                      └──(probe fails)───────┘
```

- **Closed**: every inserted event is applied to Redis.
- **Open**: Redis is skipped entirely (no dial, no timeout wait), so a dead Redis
  costs nothing per event. Without this, 1 k events/s × 300 ms timeouts would stall
  the consumer.
- **Half-open**: one event acts as a probe; success closes, failure reopens.

What stays correct while Redis is down:

| Concern | Behavior |
|---|---|
| Event persistence | Unaffected. |
| Kafka offsets | Committed normally — a Redis failure never causes redelivery or DLQ. |
| Phase 3 closed windows | Unaffected (in-process engine + PostgreSQL). |
| `/readyz` | Stays `200`; reports `"checks": {"redis": "degraded: …"}`. Redis is optional, so it must not pull pods out of rotation. |
| `/v1/realtime*` | `503 {"status":"degraded"}`. No fallback to PostgreSQL — callers wanting authoritative numbers read `metrics_windows`. |
| Logs | One `WARN` on circuit open and one `INFO` on close — not one log per event. |
| Counters | `GET /v1/realtime/stats` → `applied`, `duplicates`, `failures`, `skipped_circuit_open`, `circuit`. |

## Consistency model

**PostgreSQL is the source of truth. Redis is a derived, eventually-consistent,
best-effort projection that may under-count but does not over-count.**

1. **Gated by PostgreSQL idempotency.** Redis is updated only for
   `OutcomeInserted`. Kafka redeliveries come back as duplicates and skip Redis,
   so redelivery never double-counts.
2. **At-most-once relative to PostgreSQL.** Gaps (under-counting) happen when:
   - Redis is down / circuit open (events during the outage are not backfilled),
   - the process crashes after the PostgreSQL insert but before the Redis script,
     because the redelivered message is a duplicate.
   Those events are still in `events` and in `metrics_windows`.
3. **Retries do not double count.** A timed-out script may still have executed
   server-side; go-redis retries it, and the `applied` marker turns the retry into
   a no-op. Integration-tested with `CLIENT PAUSE`.
   Residual risk: if Redis evicts the marker under memory pressure *during* a
   retry loop (milliseconds), that one event can be counted twice.
4. **Ordering.** Counters are commutative, so partition interleaving across
   processors does not matter. User state and active-user scores use
   "only move forward in event time" (`ts >= last_seen`, `ZADD GT`), and a late
   event older than a `LOGOUT` does not re-activate the user.
5. **Time semantics.**
   - Metric buckets use **event time** (like Phase 3), so they line up with
     `metrics_windows` once those windows close.
   - "Active", retention and rate windows use **processor wall-clock** time.
   - Events older than bucket retention (replays/backfills) are not written to
     buckets; future-stamped events (> 1 min) are not bucketed and active-user
     scores are clamped to now.
6. **Redis vs. closed windows.** The rolling view includes events not yet past the
   watermark and has no allowed-lateness logic, so it can differ slightly from the
   eventually closed `metrics_windows` row. The closed window is authoritative.
7. **Revenue** is summed in integer cents regardless of currency, matching the
   Phase 3 window revenue semantics.

## Failure behavior summary

| Failure | Effect | Recovery |
|---|---|---|
| Redis down at startup | Warn log, processor starts, circuit opens on first events. | Automatic on the first successful probe. |
| Redis down mid-stream | ≤ threshold × `OpTimeout` latency once, then events skip Redis. | Probe every cooldown; closes on success. |
| Redis slow (latency spikes) | Op exceeds 300 ms → counts as failure; repeated → open. | Same as above. |
| Redis restart (data lost) | Rolling metrics/active users empty until traffic refills them (≤ 15 m for full history). | Automatic; no rebuild needed. |
| Memory pressure | `volatile-lru` evicts least-recently-used TTL'd keys (all processor keys have TTLs). | Graceful: views slightly incomplete. |
| Processor crash after PG insert, before Redis | That event is missing from Redis. | None needed; PG + closed windows are correct. |

## Deployment notes

- Compose runs `redis:7-alpine` as a pure cache: no RDB/AOF, `maxmemory 256mb`,
  `maxmemory-policy volatile-lru`. Requires Redis ≥ 6.2 (`ZADD GT`).
- The processor depends on Redis with `service_started`, not `service_healthy`.
- **Redis Cluster is not supported as-is**: the apply script touches global keys
  and per-user keys, which hash to different slots (CROSSSLOT). Single node or
  Sentinel is assumed. Cluster would require splitting the script (losing
  per-event atomicity across global and user state) or hash-tagging.

## Read API

Served on the processor's health port (`:8081`):

```bash
curl -s "http://127.0.0.1:8081/v1/realtime?window=60"   # rolling snapshot (1 s … 15 m)
curl -s http://127.0.0.1:8081/v1/realtime/users/<user_id>
curl -s http://127.0.0.1:8081/v1/realtime/stats
curl -s http://127.0.0.1:8081/readyz                    # includes checks.redis
```

## Configuration

| Variable | Default |
|---|---|
| `REDIS_ENABLED` | `true` |
| `REDIS_ADDR` | `localhost:6379` |
| `REDIS_PASSWORD` / `REDIS_DB` | empty / `0` |
| `REDIS_POOL_SIZE` / `REDIS_MIN_IDLE_CONNS` | `20` / `2` |
| `REDIS_POOL_TIMEOUT_MS` / `REDIS_DIAL_TIMEOUT_MS` | `300` / `500` |
| `REDIS_READ_TIMEOUT_MS` / `REDIS_WRITE_TIMEOUT_MS` | `200` / `200` |
| `REDIS_OP_TIMEOUT_MS` | `300` |
| `REDIS_MAX_RETRIES` | `2` |
| `REDIS_BREAKER_THRESHOLD` / `REDIS_BREAKER_COOLDOWN_MS` | `3` / `5000` |
| `REALTIME_BUCKET_RETENTION_S` | `900` |
| `REALTIME_ACTIVE_WINDOW_S` | `300` |
| `REALTIME_USER_STATE_TTL_S` | `1800` |
| `REALTIME_DEDUP_TTL_S` | `600` |
| `REALTIME_USER_RATE_LIMIT` / `REALTIME_RATE_WINDOW_S` | `50` / `1` (0 disables the limiter) |

## Tests

Unit (no Docker): breaker state machine, config defaults, cents conversion,
processor only calls Redis for inserted events, tracker degrades fast against an
unreachable Redis, `/readyz` treats Redis as non-fatal.

Integration (`stream-processor/e2e/phase4_test.go`, testcontainers):

- `TestPhase4RedisState` (Redis only)
  - rolling metrics, revenue, payment rate, breakdowns; retry of the same event is a no-op
  - every `ss:*` key has a TTL within its configured bound
  - stale (2 h old) events create no real-time keys
  - login/logout drive active users and session state; out-of-order events don't re-activate
  - rate limit shared across two trackers ("pods"); events still counted
  - `CLIENT PAUSE` → timeouts → circuit opens fast → recovers → no double counting
- `TestPhase4PipelineWithRedis` (Kafka + PostgreSQL + Redis)
  - duplicates in Kafka are counted once in Redis
  - Redis frozen mid-stream: PostgreSQL still receives every event; Redis
    under-counts within bounds; real-time state recovers afterwards

```bash
make test-unit
make test-e2e
```
