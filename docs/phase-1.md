# Phase 1 — vertical slice

## Architecture implemented

```
event-generator  --JSON-->  Kafka topic "events" (3 partitions, key=user_id)
                                |
                                v
                         stream-processor
                         (consumer group streamscale-processor)
                                |
                                v
                            PostgreSQL table events
```

## Behavior

1. Generator creates realistic LOGIN / LOGOUT / PAGE_VIEW / SEARCH / PURCHASE / PAYMENT_* events.
2. Each event includes event_id, event_type, user_id, timestamp, region, metadata.
3. Purchase and payment events also include product_id, amount (decimal string), currency.
4. Processor deserializes JSON, logs each persist, inserts into PostgreSQL.
5. Both processes honor SIGINT/SIGTERM, structured JSON logs, and HTTP health endpoints.
6. SQL is applied from embedded migrations on processor startup.
7. Topic `events` is created with 3 partitions (kafka-init + application Ensure).

## Superseded in Phase 2

- Invalid payloads are no longer silently skipped; they go to `events.dlq`.
- Idempotent processing and retries are documented in `docs/phase-2.md`.

## Integration test

`stream-processor/e2e/phase1_test.go` → `TestPhase1GeneratorToPostgres` exercises the
real vertical slice with testcontainers (Kafka + PostgreSQL):

1. Builds the actual `./event-generator` binary and runs it with
   `--rate=500 --count=1000` against the Kafka container.
2. Checks Kafka received exactly 1,000 messages on `events`, spread over all 3 partitions.
3. Runs the stream processor and waits until PostgreSQL holds exactly 1,000 events.
4. Checks all 7 event types are present, commerce events carry
   `product_id`/`amount`/`currency`, and every event has a `processed` status row.

```bash
make test-e2e   # needs Docker
```
