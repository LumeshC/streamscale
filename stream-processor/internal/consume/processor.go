package consume

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/streamscale/streamscale/pkg/analytics"
	"github.com/streamscale/streamscale/pkg/event"
	"github.com/streamscale/streamscale/stream-processor/internal/classify"
	"github.com/streamscale/streamscale/stream-processor/internal/dlq"
	"github.com/streamscale/streamscale/stream-processor/internal/retry"
	"github.com/streamscale/streamscale/stream-processor/internal/store"
)

// EventStore is the durable write path used by the processor.
type EventStore interface {
	Process(ctx context.Context, e event.Event) (store.ProcessOutcome, error)
	RecordStatus(ctx context.Context, rec store.StatusRecord) error
}

// RealtimeSink receives newly inserted events for best-effort real-time state
// (Phase 4, Redis). Implementations must not block beyond their own timeout
// and must not fail the event: the durable write has already succeeded.
type RealtimeSink interface {
	Observe(ctx context.Context, e event.Event)
}

type Options struct {
	Brokers      []string
	Topic        string
	GroupID      string
	Retry        retry.Config
	Store        EventStore
	DLQ          dlq.Publisher
	Analytics    analytics.Engine // optional; Phase 3 windowed analytics
	Realtime     RealtimeSink     // optional; Phase 4 Redis real-time state
	Log          *slog.Logger
	WriteTimeout time.Duration
}

type Processor struct {
	reader       *kafka.Reader
	store        EventStore
	dlq          dlq.Publisher
	analytics    analytics.Engine
	realtime     RealtimeSink
	log          *slog.Logger
	retry        retry.Config
	topic        string
	writeTimeout time.Duration
}

func New(opts Options) *Processor {
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.Retry.MaxAttempts == 0 {
		opts.Retry = retry.DefaultConfig()
	}
	if opts.WriteTimeout <= 0 {
		opts.WriteTimeout = 5 * time.Second
	}
	return &Processor{
		store:        opts.Store,
		dlq:          opts.DLQ,
		analytics:    opts.Analytics,
		realtime:     opts.Realtime,
		log:          opts.Log,
		retry:        opts.Retry,
		topic:        opts.Topic,
		writeTimeout: opts.WriteTimeout,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        opts.Brokers,
			GroupID:        opts.GroupID,
			Topic:          opts.Topic,
			MinBytes:       1,
			MaxBytes:       10e6,
			MaxWait:        500 * time.Millisecond,
			CommitInterval: 0,
			StartOffset:    kafka.FirstOffset,
		}),
	}
}

func (p *Processor) Close() error {
	var err error
	if p.reader != nil {
		err = p.reader.Close()
	}
	if p.dlq != nil {
		if cerr := p.dlq.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

func (p *Processor) Run(ctx context.Context) error {
	p.log.Info("consumer started", "topic", p.topic, "group", p.reader.Config().GroupID)
	for {
		msg, err := p.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}
		if err := p.HandleMessage(ctx, msg); err != nil {
			p.log.Error("message handling failed; not committing",
				"partition", msg.Partition,
				"offset", msg.Offset,
				"err", err,
			)
			return err
		}
		if err := p.reader.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

// HandleMessage processes a single Kafka message. Exported for unit tests.
func (p *Processor) HandleMessage(ctx context.Context, msg kafka.Message) error {
	e, parseErr := decodeAndValidate(msg.Value)
	if parseErr != nil {
		return p.toDLQ(ctx, msg, "", 0, parseErr)
	}

	var outcome store.ProcessOutcome
	attempts, err := retry.Do(ctx, p.retry, func(attempt int) error {
		writeCtx, cancel := context.WithTimeout(ctx, p.writeTimeout)
		defer cancel()
		var processErr error
		outcome, processErr = p.store.Process(writeCtx, e)
		return processErr
	})
	if err != nil {
		c := classify.Of(err)
		if errors.Is(err, context.Canceled) ||
			(c != nil && (c.Reason == "context_done" || c.Reason == "context_deadline")) {
			return err
		}
		return p.toDLQ(ctx, msg, e.EventID, attempts, err)
	}

	status := store.StatusProcessed
	if outcome == store.OutcomeDuplicate {
		status = store.StatusDuplicate
	}
	if err := p.recordStatus(ctx, msg, e.EventID, status, "", "", attempts); err != nil {
		p.log.Error("failed to record processing status",
			"event_id", e.EventID,
			"status", status,
			"err", err,
		)
	}

	// Real-time state only for newly inserted events: PostgreSQL's idempotency
	// gates Redis, so Kafka redeliveries never double-count. Best-effort.
	if outcome == store.OutcomeInserted && p.realtime != nil {
		p.realtime.Observe(ctx, e)
	}

	// Analytics only for newly inserted events so duplicates never double-count.
	if outcome == store.OutcomeInserted && p.analytics != nil {
		closed, aerr := p.analytics.OnEvent(ctx, e)
		if aerr != nil {
			p.log.Error("analytics engine failed", "event_id", e.EventID, "err", aerr)
			return classify.Of(aerr)
		}
		if len(closed) > 0 {
			p.log.Info("windows closed",
				"count", len(closed),
				"event_id", e.EventID,
			)
		}
	}

	p.log.Info("event processed",
		"event_id", e.EventID,
		"event_type", e.EventType,
		"user_id", e.UserID,
		"outcome", outcome,
		"attempts", attempts,
		"partition", msg.Partition,
		"offset", msg.Offset,
	)
	return nil
}

func decodeAndValidate(payload []byte) (event.Event, error) {
	e, err := event.Decode(payload)
	if err != nil {
		return event.Event{}, classify.Malformed("invalid_json", err)
	}
	if err := e.Validate(); err != nil {
		return event.Event{}, classify.Malformed("validation_failed", err)
	}
	return e, nil
}

func (p *Processor) toDLQ(ctx context.Context, msg kafka.Message, eventID string, attempts int, err error) error {
	c := classify.Of(err)
	p.log.Error("sending event to dlq",
		"event_id", eventID,
		"failure_class", c.Class,
		"failure_reason", c.Reason,
		"attempts", attempts,
		"partition", msg.Partition,
		"offset", msg.Offset,
		"err", c.Err,
	)

	if p.dlq == nil {
		return classify.Permanent("dlq_unavailable", errors.New("dlq publisher not configured"))
	}

	meta := dlq.Meta{
		OriginalTopic: p.topic,
		Partition:     msg.Partition,
		Offset:        msg.Offset,
		EventID:       eventID,
		FailureClass:  c.Class,
		Reason:        c.Reason,
		Attempts:      attempts,
	}
	if c.Err != nil {
		meta.Error = c.Err.Error()
	} else {
		meta.Error = c.Error()
	}

	publishErr := p.dlq.Publish(ctx, msg.Value, meta)
	if publishErr != nil {
		return classify.Transient("dlq_publish_failed", publishErr)
	}

	if recErr := p.recordStatus(ctx, msg, eventID, store.StatusDLQ, string(c.Class), c.Reason, attempts); recErr != nil {
		p.log.Error("failed to record dlq status", "err", recErr, "event_id", eventID)
	}
	return nil
}

func (p *Processor) recordStatus(ctx context.Context, msg kafka.Message, eventID string, status store.Status, failureClass, failureReason string, attempts int) error {
	rec := store.StatusRecord{
		Topic:         p.topic,
		Partition:     msg.Partition,
		Offset:        msg.Offset,
		Status:        status,
		FailureClass:  failureClass,
		FailureReason: failureReason,
		Attempts:      attempts,
	}
	if eventID != "" {
		if id, err := uuid.Parse(eventID); err == nil {
			rec.EventID = &id
		}
	}
	writeCtx, cancel := context.WithTimeout(ctx, p.writeTimeout)
	defer cancel()
	return p.store.RecordStatus(writeCtx, rec)
}
