package dlq

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/LumeshC/streamscale/stream-processor/internal/classify"
	"github.com/segmentio/kafka-go"
)

// Meta is failure metadata attached as Kafka headers on the DLQ message.
type Meta struct {
	OriginalTopic string
	Partition     int
	Offset        int64
	EventID       string
	FailureClass  classify.Class
	Reason        string
	Error         string
	Attempts      int
}

// Publisher publishes poison / permanently failed messages to the DLQ topic.
type Publisher interface {
	Publish(ctx context.Context, payload []byte, meta Meta) error
	Close() error
}

// KafkaPublisher implements Publisher using kafka-go.
type KafkaPublisher struct {
	w     *kafka.Writer
	topic string
	log   *slog.Logger
}

func New(brokers []string, topic string, log *slog.Logger) *KafkaPublisher {
	if log == nil {
		log = slog.Default()
	}
	return &KafkaPublisher{
		topic: topic,
		log:   log,
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

// NewKafka is a convenience constructor used by e2e tests.
func NewKafka(brokers []string, topic string) *KafkaPublisher {
	return New(brokers, topic, slog.Default())
}

func (p *KafkaPublisher) Close() error {
	if p == nil || p.w == nil {
		return nil
	}
	return p.w.Close()
}

func (p *KafkaPublisher) Publish(ctx context.Context, payload []byte, meta Meta) error {
	key := []byte(meta.EventID)
	if len(key) == 0 {
		key = []byte(fmt.Sprintf("%s-%d-%d", meta.OriginalTopic, meta.Partition, meta.Offset))
	}
	headers := []kafka.Header{
		{Key: "dlq.reason", Value: []byte(meta.Reason)},
		{Key: "dlq.kind", Value: []byte(meta.FailureClass)},
		{Key: "dlq.failure_class", Value: []byte(meta.FailureClass)},
		{Key: "dlq.original_topic", Value: []byte(meta.OriginalTopic)},
		{Key: "dlq.partition", Value: []byte(strconv.Itoa(meta.Partition))},
		{Key: "dlq.offset", Value: []byte(strconv.FormatInt(meta.Offset, 10))},
		{Key: "dlq.attempts", Value: []byte(strconv.Itoa(meta.Attempts))},
		{Key: "dlq.error", Value: []byte(meta.Error)},
	}
	if meta.EventID != "" {
		headers = append(headers, kafka.Header{Key: "dlq.event_id", Value: []byte(meta.EventID)})
	}
	msg := kafka.Message{
		Key:     key,
		Value:   payload,
		Headers: headers,
		Time:    time.Now().UTC(),
	}
	if err := p.w.WriteMessages(ctx, msg); err != nil {
		return classify.FromKafka(fmt.Errorf("dlq publish: %w", err))
	}
	p.log.Warn("published to dlq",
		"dlq_topic", p.topic,
		"failure_class", meta.FailureClass,
		"reason", meta.Reason,
		"event_id", meta.EventID,
		"original_topic", meta.OriginalTopic,
		"partition", meta.Partition,
		"offset", meta.Offset,
		"attempts", meta.Attempts,
	)
	return nil
}

// HeaderMap extracts DLQ metadata headers for tests / ops.
func HeaderMap(headers []kafka.Header) map[string]string {
	out := make(map[string]string, len(headers))
	for _, h := range headers {
		out[h.Key] = string(h.Value)
	}
	return out
}
