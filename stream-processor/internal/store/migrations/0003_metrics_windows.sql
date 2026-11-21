-- Phase 3: durable closed-window analytics snapshots.

CREATE TABLE IF NOT EXISTS metrics_windows (
    id BIGSERIAL PRIMARY KEY,
    spec_name TEXT NOT NULL,
    window_kind TEXT NOT NULL,
    window_size_ms BIGINT NOT NULL,
    window_slide_ms BIGINT NOT NULL DEFAULT 0,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    event_count BIGINT NOT NULL DEFAULT 0,
    purchase_count BIGINT NOT NULL DEFAULT 0,
    revenue NUMERIC(20, 2) NOT NULL DEFAULT 0,
    payment_success_count BIGINT NOT NULL DEFAULT 0,
    payment_failure_count BIGINT NOT NULL DEFAULT 0,
    active_users BIGINT NOT NULL DEFAULT 0,
    events_per_second DOUBLE PRECISION NOT NULL DEFAULT 0,
    events_per_minute DOUBLE PRECISION NOT NULL DEFAULT 0,
    purchases_per_minute DOUBLE PRECISION NOT NULL DEFAULT 0,
    revenue_per_minute NUMERIC(20, 2) NOT NULL DEFAULT 0,
    payment_success_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
    payment_failure_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
    by_region JSONB NOT NULL DEFAULT '{}'::jsonb,
    by_event_type JSONB NOT NULL DEFAULT '{}'::jsonb,
    closed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT metrics_windows_unique UNIQUE (spec_name, window_start, window_end)
);

CREATE INDEX IF NOT EXISTS metrics_windows_start_idx ON metrics_windows (window_start DESC);
CREATE INDEX IF NOT EXISTS metrics_windows_kind_idx ON metrics_windows (window_kind, spec_name);
CREATE INDEX IF NOT EXISTS metrics_windows_closed_at_idx ON metrics_windows (closed_at DESC);
