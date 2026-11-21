package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/LumeshC/streamscale/pkg/event"
	"github.com/LumeshC/streamscale/stream-processor/internal/classify"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
}

func (p *Postgres) Pool() *pgxpool.Pool { return p.pool }

func (p *Postgres) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
}

// Process inserts the event if event_id is new. Duplicate event_id returns
// OutcomeDuplicate without mutating durable event facts again.
func (p *Postgres) Process(ctx context.Context, e event.Event) (ProcessOutcome, error) {
	meta, err := json.Marshal(e.Metadata)
	if err != nil {
		return "", classify.Permanent("metadata_json", err)
	}
	var product any
	var amount any
	var currency any
	if e.ProductID != "" {
		product = e.ProductID
	}
	if e.Amount != "" {
		amount = e.Amount
	}
	if e.Currency != "" {
		currency = e.Currency
	}

	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return "", classify.FromStore(fmt.Errorf("begin tx: %w", err))
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var insertedID string
	err = tx.QueryRow(ctx, `
		INSERT INTO events (event_id, event_type, user_id, ts, region, metadata, product_id, amount, currency)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9)
		ON CONFLICT (event_id) DO NOTHING
		RETURNING event_id
	`, e.EventID, string(e.EventType), e.UserID, e.Timestamp, e.Region, meta, product, amount, currency).Scan(&insertedID)

	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return "", classify.FromStore(fmt.Errorf("commit duplicate: %w", err))
		}
		return OutcomeDuplicate, nil
	}
	if err != nil {
		return "", classify.FromStore(fmt.Errorf("insert event: %w", err))
	}
	if err := tx.Commit(ctx); err != nil {
		return "", classify.FromStore(fmt.Errorf("commit insert: %w", err))
	}
	return OutcomeInserted, nil
}

// Insert is retained for Phase 1 call sites; prefer Process.
func (p *Postgres) Insert(ctx context.Context, e event.Event) error {
	_, err := p.Process(ctx, e)
	return err
}

func (p *Postgres) Count(ctx context.Context) (int64, error) {
	var n int64
	if err := p.pool.QueryRow(ctx, `SELECT COUNT(*) FROM events`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func (p *Postgres) Close() {
	p.pool.Close()
}
