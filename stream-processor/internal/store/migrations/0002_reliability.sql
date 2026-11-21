-- Phase 2: constraints, processing status, supporting indexes.

ALTER TABLE events
    ADD CONSTRAINT events_amount_non_negative
    CHECK (amount IS NULL OR amount >= 0);

ALTER TABLE events
    ADD CONSTRAINT events_currency_len
    CHECK (currency IS NULL OR char_length(currency) = 3);

CREATE TABLE IF NOT EXISTS processing_status (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID,
    kafka_topic TEXT NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset BIGINT NOT NULL,
    status TEXT NOT NULL,
    failure_class TEXT,
    failure_reason TEXT,
    attempts INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT processing_status_unique_offset UNIQUE (kafka_topic, kafka_partition, kafka_offset),
    CONSTRAINT processing_status_status_check CHECK (
        status IN ('processed', 'duplicate', 'dlq')
    )
);

CREATE INDEX IF NOT EXISTS processing_status_event_id_idx
    ON processing_status (event_id)
    WHERE event_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS processing_status_status_idx
    ON processing_status (status);

CREATE INDEX IF NOT EXISTS processing_status_updated_at_idx
    ON processing_status (updated_at DESC);

-- Idempotency is enforced by events.event_id PRIMARY KEY.
-- Explicit unique index documents the Phase 2 contract.
CREATE UNIQUE INDEX IF NOT EXISTS events_idempotency_event_id_uidx ON events (event_id);
