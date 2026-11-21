# Phase 6 — Dashboard

## Architecture

```
browser ──/api/*──▶ dashboard (nginx :3000 → static React bundle)
                         │ proxy_pass (same origin, no CORS)
                         ▼
                    analytics-api :8082 ──▶ PostgreSQL (closed windows, events, processing_status)
                                       ├──▶ Redis (live view)
                                       └──▶ Kafka (consumer-group offsets, read-only)
```

The dashboard connects **only** to analytics-api. It uses no mock data at
runtime; test fixtures exist only in `*.test.tsx`.

## Backend additions (analytics-api)

Three sections needed data the Phase 5 API did not expose:

| Endpoint | Source | Notes |
|---|---|---|
| `GET /metrics/consumer-lag` | Kafka admin API (metadata, list offsets, offset fetch) | Lag per partition = log-end − committed offset, as `kafka-consumer-groups.sh` computes it. Never joins the group. `503 unavailable` when Kafka is down. |
| `GET /metrics/latency` | `events` (`ingested_at − ts`) | Exact p50/p95/p99/max overall and per minute. Default last 15 min, max 3 h (percentiles read raw rows). |
| `GET /errors` | `processing_status` where `status='dlq'` | Newest first with keyset pagination on `(updated_at, id)`, counts by reason, error rate `dlq / all outcomes`. Filter by `class`. |

`/health` now also reports `kafka` (optional, like Redis: down = `degraded`).

Migration `0004_dashboard_indexes.sql` (applied by the processor, which owns the
schema) adds `events(ingested_at)` for latency and a partial index
`processing_status(updated_at DESC, id DESC) WHERE status='dlq'` for errors.

All three are in the OpenAPI spec, covered by unit tests, and by integration
tests against real PostgreSQL and a real Kafka broker with a consumer group
that has committed part of the topic.

### Latency definition

`ingested_at − event timestamp` per event: the time from the generator creating
the event, through Kafka and the processor, to the PostgreSQL commit. It
assumes generator and database clocks agree (same host in Compose); negative
values from skew are clamped to 0. Replayed or backfilled events would show
their age, not processing time.

## Frontend

React 18 + TypeScript (strict), Vite 5, Recharts 2. No CSS framework: one
stylesheet of design tokens.

```
dashboard/src/
  api/          client.ts (fetch, timeout, error envelope, pagination), endpoints.ts, types.ts
  hooks/        useDashboard (range + refresh context), usePolling (auto-refresh)
  components/   Card + AsyncBody (state machine), StatTile, StatusBadge, DataTable,
                ViewToggle, Toolbar, States, Icons
  components/charts/  TimeSeriesChart, ColumnChart (+ bucketize), BarList, Sparkline,
                      Legend, ChartTooltip
  sections/     one file per dashboard section
  lib/          format.ts, series.ts (gap filling)
```

### Data flow and refresh

- **One time range and one refresh cadence** (toolbar) scope every section, so
  all numbers describe the same slice of time. Both are remembered in
  `localStorage` (a convenience only).
- `usePolling` fetches on mount, on range change, on the refresh interval, on
  "Refresh", and when the tab becomes visible. It **pauses while the tab is
  hidden** and aborts superseded or unmounted requests.
- **Stale-while-revalidate**: during a refetch the previous render stays (at
  reduced opacity, no skeleton flash). If a refresh fails, the last good data
  stays with a "showing last good data" note. Only a first-load failure shows
  the error state.
- Long ranges follow `page.next_cursor` (up to 5 pages). Revenue and payment
  columns are summed into at most 60 buckets.
- Every request has an 8 s client timeout (the API's own budget is 5 s).

### States

Every section uses the same `AsyncBody` state machine:

| State | Shown |
|---|---|
| first load | skeleton |
| error, no data | message + HTTP status + request ID + Retry |
| error, stale data | data + non-blocking note |
| empty | explanation (e.g. "No closed windows in this range") |
| refreshing | data at 65% opacity, `aria-busy` |

One section failing (for example consumer lag while Kafka is down) does not
affect the others. If analytics-api itself is unreachable, nginx returns 502/504
and each section says "Cannot reach the analytics API".

### Charts

These follow a validated palette and mark spec:

- **Color by role.** Single-series charts use one hue (slot 1 blue). Payments
  stack succeeded (blue) and failed (orange). Latency p50/p95/p99 use an
  ordered single-hue blue ramp. The palettes were checked with a
  colorblind/contrast validator in light and dark mode, and all checks pass.
  Status colors appear only on status badges, always with an icon and a label.
- **Marks.** 2px lines, a 10% area wash, ≤24px columns with 4px rounded tops
  and a 2px surface gap between stacked segments, solid hairline grid.
- **Gaps stay gaps.** Missing minutes (no closed window, e.g. while the
  processor was down) are inserted as empty points, so lines break and columns
  leave a hole instead of drawing a line across a period with no data.
  Isolated points render as dots.
- **Interaction.** Crosshair tooltip on line charts listing every series;
  per-bar tooltip on columns; hover/focus titles on ranked bars.
- **Accessibility.** A legend for 2+ series; a chart/table toggle on every
  chart; ARIA labels on charts; keyboard focus styles; `prefers-reduced-motion`
  respected.
- **Dark mode** uses its own validated color steps (not an inverted palette).
  It follows the OS setting, with a toggle in the toolbar (system → dark → light).

### Responsive layout

12-column grid: wide screens show 2–3 cards per row; under 1100px cards go
full width; under 720px tiles go two per row with the hero figure on its own
row. Checked at 375px with no horizontal scroll.

## Consumer lag thresholds

| Total lag | Badge |
|---|---|
| ≤ 1,000 messages | Keeping up |
| ≤ 10,000 | Elevated lag |
| > 10,000 | Falling behind |

At the default ~100 events/s, 1,000 messages is about 10 s behind. The sparkline
shows lag samples collected during the current browser session only (real
samples, not stored or interpolated).

## Tests

- **Dashboard (Vitest + Testing Library, 24 tests):**
  - formatting and gap filling
  - API client: error envelope, proxy 502, network failure, caller abort, pagination and truncation
  - polling hook: stale data on failure, interval refresh, pause/resume on tab visibility, abort on key change
  - `AsyncBody` states
  - full-dashboard render: all ten sections, a failing section doesn't affect the others, and only `/api` paths are called with documented query parameters
- **analytics-api:** unit tests for the new handlers (lag, latency range cap, error pagination with time ties, class filter, Kafka in `/health`); the OpenAPI sync test covers the new routes; integration tests against real PostgreSQL + Kafka check exact percentiles, committed-offset lag, a group with no commits, DLQ paging, and Kafka down → 503.
- **Manual check in a browser** against the live Compose stack: all ten sections with real data, light and dark mode, 375px width, API stopped (stale-data notes, then error states), API restarted (recovers by itself). This found and fixed three issues:
  - an extra query parameter that the API's strict validation rejected (now guarded by a test)
  - lines drawn across periods with no data
  - stale health badges shown while the API was down

## Running

See the README section "Run frontend + backend".

## Known limitations

- No authentication; intended for local or internal networks.
- Lag history is session-local; there is no stored lag time series (a metrics
  system such as Prometheus is out of scope for this phase).
- Latency is limited to 3-hour ranges.
- Charts show closed windows, so they run ~1–2 minutes behind; the overview's
  live tiles cover the most recent 60 seconds.
