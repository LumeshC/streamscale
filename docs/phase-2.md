# Phase 2 — Reliable event processing

## Architecture

```
events (Kafka)
    |
    v
stream-processor
    |-- validate / classify
    |-- retry (transient only, bounded)
    |-- idempotent persist (event_id)
    |-- processing_status
    |
    +--> PostgreSQL (events + processing_status)
    |
    +--> events.dlq  (malformed / permanent / retries_exhausted)
```

## Validation

Rejected before persistence (sent to DLQ, consumer does not crash):

- empty `event_id` / `user_id`
- unsupported `event_type`
- invalid / zero / absurd future `timestamp`
- commerce events: missing product, non-negative `amount`, supported ISO 4217 `currency`

## Error classes

| Class | Behavior |
|---|---|
| `malformed` | DLQ immediately, commit offset |
| `permanent` | DLQ immediately, commit offset |
| `transient` | Bounded in-process retries with backoff; on exhaustion → DLQ |

DLQ messages carry headers: `dlq.reason`, `dlq.kind` / `dlq.failure_class`, `dlq.original_topic`, `dlq.partition`, `dlq.offset`, `dlq.attempts`, `dlq.event_id`, `dlq.error`.

## Idempotency

`events.event_id` is the idempotency key (`PRIMARY KEY` + unique index).

`INSERT ... ON CONFLICT (event_id) DO NOTHING` ensures a redelivered event never inserts twice. Outcome is recorded as `processed` or `duplicate` in `processing_status`.

## Processing status

Table `processing_status` tracks per Kafka offset:

- `processed`
- `duplicate`
- `dlq`

## Configuration

| Variable | Default |
|---|---|
| `KAFKA_DLQ_TOPIC` | `events.dlq` |
| `KAFKA_DLQ_PARTITIONS` | `3` |
| `PROCESSOR_MAX_RETRIES` | `3` |
| `PROCESSOR_RETRY_BASE_MS` | `100` |
| `PROCESSOR_RETRY_MAX_MS` | `2000` |

## Tests

```bash
go test ./pkg/... ./event-generator/... ./stream-processor/internal/...
go test ./stream-processor/e2e -count=1 -timeout 12m   # needs Docker
```

Unit coverage: duplicates, malformed → DLQ, validation → DLQ, transient retries, permanent failures, DLQ publish failure (offset not committed).

E2E coverage: triple-produce same `event_id` → one row; malformed + negative amount → `events.dlq` with reason headers.

## Out of scope

Redis, windowing, analytics API, dashboard, Kubernetes, Prometheus, AWS.
