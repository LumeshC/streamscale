# Phase 5 — analytics-api (REST)

## Architecture

```
Kafka → stream-processor ─┬→ PostgreSQL  metrics_windows (closed windows) ─┐
                          └→ Redis       ss:rt:* (live, best-effort) ──────┤
                                                                           ↓
                                                         analytics-api (Go, :8082, read-only)
```

The API is **read-only**. It never writes, never runs migrations (the processor
owns the schema), and never scans the raw `events` table — every endpoint reads
the pre-aggregated `metrics_windows` rows (and, for the live block, Redis).
The pool sets `default_transaction_read_only=on`.

OpenAPI 3.0 spec: [`analytics-api/api/openapi.yaml`](../analytics-api/api/openapi.yaml),
also served by the running API at `GET /openapi.yaml` (embedded at build time).

## Endpoints

| Endpoint | Source | Paginated | Notes |
|---|---|---|---|
| `GET /health` | PG ping + Redis ping | — | `200 ok`, `200 degraded` (Redis down), `503 unavailable` (PG down) |
| `GET /metrics/overview` | 1m windows + Redis | — | Totals over range, `coverage`, `live` block (null if Redis down → `degraded: ["live"]`) |
| `GET /metrics/events` | windows at `interval` | ✓ | events, EPS, events/min, active users per window |
| `GET /metrics/revenue` | windows + 1m summary | ✓ | purchases, revenue, per-minute rates, average order value |
| `GET /metrics/payments` | windows + 1m summary | ✓ | success/failure counts and rates |
| `GET /metrics/regions` | 1m `by_region` | — | sorted desc, with `share` |
| `GET /metrics/event-types` | 1m `by_event_type` | — | all 7 types always listed; optional `?event_type=` |
| `GET /anomalies` | 1m windows | ✓ | rolling z-score; filter by `kind`, `severity`, `min_z` |
| `GET /openapi.yaml` | embedded | — | this API's spec |

### Query conventions

- `from`, `to`: RFC 3339, half-open `[from, to)`; a window belongs to the range
  its `window_start` falls in. Defaults: last 60 minutes. Max range 7 days.
- `interval` (series only): `1s` | `1m` (default) | `5m` (sliding, overlapping points).
- **Totals and summaries always use tumbling 1m windows**, regardless of
  `interval`. Summing overlapping 5m sliding windows would count each event
  up to 5 times; summing 1s windows would be 60× more rows for the same answer.
- Active users: distinct users are not additive across windows, so totals
  report `peak_active_users_per_minute` (max over 1m windows). The live block
  reports users active in the last 5 minutes from Redis.

### Request validation

- Every parameter is type/range checked; **all** problems are returned at once
  (`details[]`, one entry per field).
- Unknown parameters are rejected (`?form=` typo would otherwise silently use
  defaults); repeated parameters are rejected.

### Pagination

Keyset (cursor) pagination on `window_start` (unique per window spec), newest
first. Responses include `page.limit` and, unless it is the last page,
`page.next_cursor`; send it back as `cursor` with the same filters.

Why keyset, not offset: pages stay consistent while the processor keeps
inserting new windows (no skipped or repeated rows), and each page is an index
range scan (`metrics_windows_unique`) instead of `OFFSET n` scanning `n` rows.
The API fetches `limit + 1` rows to know if another page exists without a
`COUNT(*)`.

Regions and event types are not paginated (small, bounded cardinality).

### Error envelope

Every non-2xx response:

```json
{
  "error": {
    "code": "invalid_parameter",
    "message": "one or more query parameters are invalid",
    "details": [{"field": "limit", "reason": "must be an integer between 1 and 1000"}]
  },
  "request_id": "3f9c0a6d2b1e4c5f8a7b6c5d"
}
```

| Status | `code` | When |
|---|---|---|
| 400 | `invalid_parameter` | validation failure |
| 404 | `not_found` | unknown path |
| 405 | `method_not_allowed` | non-GET (with `Allow: GET, HEAD`) |
| 500 | `internal` | unexpected error (cause logged, never returned) |
| 503 | `unavailable` | PostgreSQL unreachable |
| 504 | `timeout` | request exceeded its time budget |

## Timeouts

| Layer | Default | Purpose |
|---|---|---|
| Request context | `API_REQUEST_TIMEOUT_MS=5000` | Passed to every PG/Redis call; exceeding it → 504 |
| PG `statement_timeout` | `API_DB_STATEMENT_TIMEOUT_MS=10000` | Server-side backstop if a client cancel is lost |
| Redis per read | `API_REDIS_TIMEOUT_MS=300` | Live block must not slow the overview |
| PG connect | 3 s | Fail fast when PG is down |
| HTTP server | read-header 3 s, read 5 s, write = request timeout + 5 s, idle 60 s | Slowloris protection; write timeout leaves room for the 504 envelope |
| Shutdown | `API_SHUTDOWN_TIMEOUT_MS=10000` | Drain in-flight requests on SIGTERM |

## Anomalies

`GET /anomalies` evaluates closed 1-minute windows on read (no stored state):

| Kind | Rule |
|---|---|
| `event_rate_spike` / `event_rate_drop` | z = (events − mean) / sd over the previous 30 windows (≥ 10 required); sd floored at `max(sqrt(mean), 1)` (Poisson noise) so a flat baseline does not flag tiny wiggles. Missing minutes count as 0 events, so outages show up as drops. |
| `payment_failure_spike` | window failure rate vs. pooled baseline rate, z using binomial standard error `sqrt(p(1−p)/n)` (floor 0.02); requires ≥ 20 payments in the window. |

`|z| ≥ min_z` (default 3) is reported; `|z| ≥ 5` is `critical`, else `warning`.

This is a lightweight detector so the endpoint is useful now. It lives behind
`anomaly.Detect` and the handler only depends on its output shape, so a later
streaming detection pipeline (with stored anomalies and alerting) can replace it
without changing the API contract.

## Logging

JSON (`log/slog`) with one access-log line per request: `request_id`, method,
path, query, status, bytes, `duration_ms`, and `error_code` / `err` (cause) on
failures. 4xx log at WARN, 5xx at ERROR. `X-Request-ID` is accepted from
clients when well-formed (`[A-Za-z0-9._-]{1,64}`), otherwise generated, and
always echoed in the header and the error envelope. Panics are recovered,
logged with a stack, and returned as a 500 envelope.

## Code layout

```
analytics-api/
  main.go                 wiring, HTTP server timeouts, graceful shutdown
  api/openapi.yaml        OpenAPI 3.0 spec (embedded via api/embed.go)
  internal/config         env config + validation
  internal/httpx          error envelope, middleware, query validation, cursors
  internal/repo           Repository interface + PostgreSQL implementation
  internal/live           Redis live source (shared key layout: pkg/rtstate)
  internal/anomaly        detector (pure functions)
  internal/handler        endpoints + router
  e2e/                    integration tests (testcontainers)
pkg/rtstate               Redis key layout + snapshot reader shared with the processor
```

## Tests

Unit (`make test-unit`, no Docker):

- `httpx`: unknown/repeated params, all errors collected, range defaults/limits,
  cursor round-trip and tampering, error mapping (no leaked causes), request-ID
  echo/replace, panic → 500 envelope, timeout → 504.
- `anomaly`: spike, drop, Poisson floor, gaps-as-zero, minimum baseline, range
  filter, payment failure spike, `min_z`.
- `handler` (fake repository): every endpoint, validation envelope, pagination
  walks all pages without duplicates, interval mapping, summaries, zero-filled
  event types, anomaly pagination, 504 on slow repository, 500 without leaking
  causes, 503 on DB outage, 404/405 envelopes, health states.
- **OpenAPI sync**: spec paths == router paths; every documented parameter is
  accepted; undocumented parameters are rejected.

Integration (`make test-e2e`, Docker): real router + PostgreSQL + Redis with
the processor's migrations applied and deterministic seed data:
totals, pagination across 9 pages, 1s/5m intervals, summary independence from
interval, payments, breakdowns summing to totals, seeded spike detected,
validation envelope, OpenAPI served, real slow query (table lock) → 504, Redis
down → `degraded`, PostgreSQL down → 503.

The spec also passes `openapi-spec-validator` (OpenAPI 3.0).

## Configuration

| Variable | Default |
|---|---|
| `API_HTTP_ADDR` | `:8082` |
| `DATABASE_URL` | local Compose URL |
| `API_DB_MAX_CONNS` | `10` |
| `API_DB_STATEMENT_TIMEOUT_MS` | `10000` |
| `API_REQUEST_TIMEOUT_MS` | `5000` |
| `API_SHUTDOWN_TIMEOUT_MS` | `10000` |
| `REDIS_ENABLED` / `REDIS_ADDR` | `true` / `localhost:6379` |
| `API_REDIS_TIMEOUT_MS` | `300` |
| `API_DEFAULT_RANGE_MINUTES` | `60` |
| `API_MAX_RANGE_HOURS` | `168` |

## Known limitations

- No authentication, authorization or rate limiting on the API yet (local/internal use).
- Totals cover **closed** windows only (~1–2 min behind); use the `live` block for "now".
- Revenue sums amounts across currencies (as the processor stores it).
- Anomaly detection is on-read and stateless: no history of acknowledged anomalies, no alerting.
- `peak_active_users_per_minute` is not distinct users over the whole range.
