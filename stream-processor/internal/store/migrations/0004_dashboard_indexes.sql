-- Phase 6: indexes for analytics-api dashboard reads.

-- Processing latency (ingested_at - ts) is computed over recently ingested
-- events; without this index every latency query would scan the events table.
CREATE INDEX IF NOT EXISTS events_ingested_at_idx ON events (ingested_at);

-- Recent errors: keyset pagination over DLQ rows, newest first.
CREATE INDEX IF NOT EXISTS processing_status_dlq_recent_idx
    ON processing_status (updated_at DESC, id DESC)
    WHERE status = 'dlq';
