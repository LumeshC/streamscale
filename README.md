# StreamScale — Phases 1–6

Distributed real-time event processing platform with a live analytics dashboard.

```
Event Generator → Kafka (events) → Stream Processor ─┬→ PostgreSQL (durable: events, status, metrics_windows)
                                      ↘ events.dlq    └→ Redis (real-time: rolling metrics, active users, sessions)
                                      ↘ analytics.Engine (tumbling + sliding windows)
                                                          ↓
              Kafka (lag only) → analytics-api (REST, :8082) ← PostgreSQL + Redis (read-only)
                                                          ↓  /api
                                        dashboard (React + TypeScript, :3000)
```

| Phase | Scope |
|---|---|
| 1 | Generator → Kafka → processor → PostgreSQL |
| 2 | Validation, DLQ, idempotency, retries, processing status |
| 3 | Windowed stream analytics (Go engine behind swappable interfaces) |
| 4 | Redis real-time state with TTLs, pooling, timeouts, circuit breaker |
| 5 | analytics-api: read-only REST API with OpenAPI spec |
| 6 | React + TypeScript dashboard (+ consumer-lag, latency and errors endpoints) |

Not yet: Kubernetes, Prometheus/Grafana, autoscaling, AWS, Flink, authentication.

## Prerequisites

- Docker and Docker Compose — **this is all you need to run everything**
- Go 1.23+ — only to run Go tests or services outside Docker (`export PATH="$HOME/sdk/go/bin:$PATH"` if needed)
- Node.js 20+ — optional; only for running the dashboard dev server without Docker
- `curl` (optional)

## Run frontend + backend

### Option A — everything in Docker (recommended)

```bash
docker compose -f infrastructure/docker/docker-compose.yml up --build -d
```

or `make up`. This starts Kafka, PostgreSQL, Redis, the event generator (~100 events/s),
the stream processor, analytics-api and the dashboard. When `docker compose ps` shows
everything healthy (about a minute on first build):

| What | URL |
|---|---|
| **Dashboard** | http://localhost:3000 |
| analytics-api | http://localhost:8082 (`/health`, `/openapi.yaml`) |
| stream-processor health | http://localhost:8081/readyz |
| event-generator health | http://localhost:8080/healthz |

The dashboard only talks to analytics-api: its nginx proxies `/api/*` →
`analytics-api:8082` (same origin, no CORS). 1-minute charts fill in as windows close
(the first closes about a minute after events start flowing).

Stop: `docker compose -f infrastructure/docker/docker-compose.yml down` (keeps the
PostgreSQL volume) or `make down` (also deletes data).

### Option B — dashboard dev server (hot reload) against the Docker backend

Start the backend (Option A), then run Vite on port 5173. It proxies `/api` to
`http://localhost:8082`.

With Node.js 20+ installed:

```bash
cd dashboard
npm ci
npm run dev
```

Without Node.js (runs Node in Docker):

```bash
make dashboard-install
make dashboard-dev
```

Open http://localhost:5173. To point the dev server at another API, set
`API_TARGET` (e.g. `API_TARGET=http://staging-host:8082 npm run dev`).

### Option C — analytics-api from source

With the Compose stack running (it provides Kafka/PostgreSQL/Redis on localhost),
stop the containerized API and run it with Go:

```bash
docker compose -f infrastructure/docker/docker-compose.yml stop analytics-api
go run ./analytics-api
```

Defaults point at `localhost` (PostgreSQL 5432, Redis 6379, Kafka 9092). The Kafka
broker advertises `kafka:9092`, so for consumer lag from a host-run API add
`127.0.0.1 kafka` to `/etc/hosts` or set `API_KAFKA_ENABLED=false`.

## Dashboard

Ten sections, all from analytics-api, all following one time range (15m / 1h / 6h / 24h)
and one auto-refresh cadence (5s–1m or off):

| Section | Endpoint |
|---|---|
| System overview | `/health`, `/metrics/overview` |
| Event throughput | `/metrics/events` (1m, or 1s for the last 15 min) |
| Kafka consumer lag | `/metrics/consumer-lag` |
| Processing latency | `/metrics/latency` (p50/p95/p99, max 3 h) |
| Revenue | `/metrics/revenue` |
| Payment success/failure | `/metrics/payments` |
| Active users | `/metrics/events` (per minute) + `/metrics/overview` (live) |
| Regional distribution | `/metrics/regions` |
| Event type distribution | `/metrics/event-types` |
| Recent errors | `/errors` |

Every section has loading, error (with retry and request ID), empty, and "showing last
good data" states; charts have a table view; light and dark themes; responsive down to
phone width. Details: [docs/phase-6.md](docs/phase-6.md).

## Inspecting the backend directly

```bash
make api        # /health + /metrics/overview
make windows    # closed windows in PostgreSQL
make realtime   # Redis live view via the processor
make status     # processing outcomes
make logs       # follow processor, generator and API logs
```

```bash
curl -s http://127.0.0.1:8082/metrics/consumer-lag
curl -s http://127.0.0.1:8082/metrics/latency
curl -s "http://127.0.0.1:8082/errors?limit=5"
curl -s "http://127.0.0.1:8082/metrics/events?interval=1m&limit=10"
```

## Phase summaries

**Phase 3 — analytics.** Tumbling 1s (`eps_1s`) and 1m (`metrics_1m`), sliding 5m/1m
(`metrics_5m_slide_1m`); event-time watermarks; engine behind `pkg/analytics.Engine`.
[docs/phase-3.md](docs/phase-3.md)

**Phase 4 — Redis.** Rolling per-second buckets, active users, sessions, per-user rate
limiting; one atomic Lua script per inserted event; every key has a TTL; circuit
breaker so Redis outages never block processing. [docs/phase-4.md](docs/phase-4.md)

**Phase 5 — analytics-api.** Read-only REST over closed windows and live state; strict
validation, one error envelope, keyset pagination, per-request timeouts, OpenAPI 3.0
([analytics-api/api/openapi.yaml](analytics-api/api/openapi.yaml)). [docs/phase-5.md](docs/phase-5.md)

**Phase 6 — dashboard.** React + TypeScript (Vite, Recharts), plus API endpoints for
consumer lag, processing latency and recent errors. [docs/phase-6.md](docs/phase-6.md)

## Configuration

| Variable | Default |
|---|---|
| `KAFKA_BROKERS` | `localhost:9092` |
| `KAFKA_TOPIC` / `KAFKA_DLQ_TOPIC` | `events` / `events.dlq` |
| `ANALYTICS_ALLOWED_LATENESS_MS` | `30000` |
| `PROCESSOR_MAX_RETRIES` | `3` |
| `REDIS_ENABLED` / `REDIS_ADDR` | `true` / `localhost:6379` |
| `API_HTTP_ADDR` | `:8082` |
| `API_REQUEST_TIMEOUT_MS` | `5000` |
| `API_KAFKA_ENABLED` | `true` |
| `API_LATENCY_MAX_RANGE_MINUTES` | `180` |

Full lists: `.env.example`, [docs/phase-4.md](docs/phase-4.md), [docs/phase-5.md](docs/phase-5.md).

## Tests

```bash
make test-unit        # Go unit tests (no Docker)
make test-e2e         # Go integration tests with testcontainers (needs Docker)
make dashboard-test   # dashboard typecheck + Vitest (Node in Docker)
make bench            # analytics engine microbenchmark
```

With Node.js installed: `cd dashboard && npm ci && npm run typecheck && npm test`.

## Docs

- `docs/phase-1.md` — vertical slice
- `docs/phase-2.md` — reliable processing
- `docs/phase-3.md` — windowing strategy, state, time semantics, tradeoffs
- `docs/phase-4.md` — Redis keys, TTLs, consistency and failure behavior
- `docs/phase-5.md` — analytics API: endpoints, validation, pagination, errors, timeouts, anomalies
- `docs/phase-6.md` — dashboard and the operational endpoints

## Known limitations

- No authentication or rate limiting on analytics-api or the dashboard (local/internal use).
- Totals and charts use closed windows, so they lag ~1–2 minutes; the live tiles come from Redis.
- Consumer-lag history in the dashboard is session-local (the API reports point-in-time lag).
- Latency percentiles read raw rows and are capped at 3 hours per request.
- Open-window state is in-memory (lost on restart); late events past the watermark are dropped.
- Redis real-time state is best-effort (can under-count during outages); single-node/Sentinel only.
- Analytics microbenchmarks are not end-to-end throughput claims.
