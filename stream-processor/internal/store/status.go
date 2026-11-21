package store

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// Status is a processing_status row outcome.
type Status string

const (
	StatusProcessed Status = "processed"
	StatusDuplicate Status = "duplicate"
	StatusDLQ       Status = "dlq"
)

// StatusRecord tracks per-message processing outcome for ops / debugging.
type StatusRecord struct {
	EventID       *uuid.UUID
	Topic         string
	Partition     int
	Offset        int64
	Status        Status
	FailureClass  string
	FailureReason string
	Attempts      int
}

func (p *Postgres) RecordStatus(ctx context.Context, rec StatusRecord) error {
	var eventID any
	if rec.EventID != nil {
		eventID = *rec.EventID
	}
	_, err := p.pool.Exec(ctx, `
		INSERT INTO processing_status (
			event_id, kafka_topic, kafka_partition, kafka_offset,
			status, failure_class, failure_reason, attempts, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
		ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO UPDATE SET
			event_id = EXCLUDED.event_id,
			status = EXCLUDED.status,
			failure_class = EXCLUDED.failure_class,
			failure_reason = EXCLUDED.failure_reason,
			attempts = EXCLUDED.attempts,
			updated_at = now()
	`, eventID, rec.Topic, rec.Partition, rec.Offset,
		string(rec.Status), nullIfEmpty(rec.FailureClass), nullIfEmpty(rec.FailureReason), rec.Attempts)
	if err != nil {
		return fmt.Errorf("record processing status: %w", err)
	}
	return nil
}

func (p *Postgres) CountByStatus(ctx context.Context, status Status) (int64, error) {
	var n int64
	if err := p.pool.QueryRow(ctx, `SELECT COUNT(*) FROM processing_status WHERE status = $1`, string(status)).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
