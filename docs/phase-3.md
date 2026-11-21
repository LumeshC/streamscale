# Phase 3 — Real-time stream analytics (windowing)

## Architecture

```
Kafka events
    → stream-processor (validate, idempotent persist)
        → analytics.Engine.OnEvent   ← swappable interface
              ├── tumbling 1s   (events/sec)
              ├── tumbling 1m   (per-minute metrics)
              └── sliding 5m/1m (smoothed rates)
        → Sink.PersistClosed → PostgreSQL metrics_windows
```

The processing contract lives in `pkg/analytics`:

- `Engine` — ingest event-time events, emit closed windows
- `Sink` — persist closed windows (idempotent upsert)

The current implementation is `stream-processor/internal/analytics.MemoryEngine` (pure Go).
A future Flink job can implement the same `Engine`/`Sink` contracts (or write the same
`metrics_windows` rows) without changing the analytics API or dashboard consumers.

## Metrics

| Metric | Source |
|---|---|
| Events per second | `event_count / window_seconds` (1s tumbling) |
| Events per minute | `event_count / window_minutes` |
| Purchases per minute | `purchase_count / window_minutes` |
| Revenue per minute | window purchase sum / window minutes |
| Payment success rate | successes / (successes + failures) |
| Payment failure rate | failures / (successes + failures) |
| Active users | distinct `user_id` in the window |
| Events by region | histogram map |
| Events by event type | histogram map |

Raw counts are stored; rates are derived at close time.

## Windowing strategy

### Tumbling

Half-open intervals `[start, end)` with `end = start + size`, starts aligned to size.

Defaults:

- `eps_1s` — size 1s
- `metrics_1m` — size 1m

### Sliding

Size S, slide D. An event at time `t` updates every window whose start is aligned to D
and whose interval contains `t`.

Default:

- `metrics_5m_slide_1m` — size 5m, slide 1m (up to 5 overlapping windows)

## Time semantics

| Concept | Choice |
|---|---|
| Time domain | **Event time** (`event.timestamp`) |
| Watermark | `max_event_time_seen - allowed_lateness` |
| Lateness default | 30s (`ANALYTICS_ALLOWED_LATENESS_MS`) |
| Late events | Dropped (counted in `LateDropCount`); do not reopen closed windows |
| Window close | When `watermark >= window.end` |

Processing time is not used for assignment (avoids wall-clock skew across pods).

## State management

| State | Where | Lifetime |
|---|---|---|
| Open windows | In-memory map in `MemoryEngine` | Until watermark closes them |
| Closed windows | PostgreSQL `metrics_windows` | Durable, upsert by `(spec_name, start, end)` |
| Active users | In-memory `map[user_id]struct{}` per open window | Dropped when window closes |

### Idempotency interaction

Analytics runs **only** when the event was newly inserted (`OutcomeInserted`).
Duplicate Kafka redeliveries do not double-count metrics.

## Tradeoffs

| Decision | Benefit | Cost |
|---|---|---|
| Go in-process engine | Simple ops, no Flink cluster | Open-window state lost on restart |
| Event-time + watermark | Correct out-of-order handling | Late events after watermark are dropped |
| Exact active-user sets | Precise counts | Memory grows with unique users per open window |
| Upsert closed windows | Safe sink retries | Last writer wins (fine for single engine) |
| Analytics after DB insert | Keeps facts durable first | If analytics fails after insert and Kafka retries as duplicate, that event may be missing from open windows (documented edge case) |
| Interface-first design | Flink-ready later | Slight indirection |

## Configuration

| Variable | Default |
|---|---|
| `ANALYTICS_ENABLED` | `true` |
| `ANALYTICS_ALLOWED_LATENESS_MS` | `30000` |

## Benchmarks

Measure pure engine overhead (no Kafka/Postgres):

```bash
go test ./stream-processor/internal/analytics -bench=BenchmarkOnEvent -benchmem -count=3
```

Record results under `docs/benchmarks/` when publishing numbers. Do not claim
system-wide throughput from this microbenchmark alone.

## Out of scope

Redis hot state, Flink, analytics HTTP API, dashboard, Kubernetes, AWS.
