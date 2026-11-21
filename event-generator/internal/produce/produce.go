package produce

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/streamscale/streamscale/pkg/event"
)

type Writer struct {
	w   *kafka.Writer
	log *slog.Logger
}

func New(brokers []string, topic string, log *slog.Logger) *Writer {
	return &Writer{
		log: log,
		w: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireOne,
			BatchTimeout: 10 * time.Millisecond,
			Async:        false,
		},
	}
}

func (p *Writer) Write(ctx context.Context, e event.Event) error {
	payload, err := event.Marshal(e)
	if err != nil {
		return err
	}
	msg := kafka.Message{
		Key:   []byte(e.UserID),
		Value: payload,
		Time:  e.Timestamp,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(e.EventType)},
		},
	}
	if err := p.w.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka produce: %w", err)
	}
	return nil
}

func (p *Writer) Close() error {
	return p.w.Close()
}
