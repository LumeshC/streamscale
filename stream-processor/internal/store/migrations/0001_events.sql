-- StreamScale Phase 1 events fact table.
CREATE TABLE IF NOT EXISTS events (
    event_id UUID PRIMARY KEY,
    event_type TEXT NOT NULL,
    user_id UUID NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    region TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    product_id TEXT,
    amount NUMERIC(12, 2),
    currency CHAR(3),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS events_user_ts_idx ON events (user_id, ts DESC);
CREATE INDEX IF NOT EXISTS events_type_ts_idx ON events (event_type, ts DESC);
