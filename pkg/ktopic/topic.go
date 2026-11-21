package ktopic

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func Ensure(ctx context.Context, brokers []string, topic string, partitions int, log *slog.Logger) error {
	if len(brokers) == 0 {
		return errors.New("no kafka brokers")
	}
	if partitions < 1 {
		partitions = DefaultPartitions
	}
	var last error
	for attempt := 1; attempt <= 30; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		last = create(ctx, brokers[0], topic, partitions)
		if last == nil {
			log.Info("kafka topic ready", "topic", topic, "partitions", partitions, "broker", brokers[0])
			return nil
		}
		log.Warn("waiting for kafka topic", "attempt", attempt, "err", last)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("ensure topic %s: %w", topic, last)
}

func EnsureMany(ctx context.Context, brokers []string, topics map[string]int, log *slog.Logger) error {
	for topic, parts := range topics {
		if err := Ensure(ctx, brokers, topic, parts, log); err != nil {
			return err
		}
	}
	return nil
}

func create(ctx context.Context, broker, topic string, partitions int) error {
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	cfg := kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}
	if err := conn.CreateTopics(cfg); err == nil || alreadyExists(err) {
		return nil
	}

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("controller: %w", err)
	}
	host := controller.Host
	if host == "" || host == "localhost" || host == "127.0.0.1" || host == "::1" {
		h, _, splitErr := net.SplitHostPort(broker)
		if splitErr == nil {
			host = h
		}
	}
	ctrlAddr := net.JoinHostPort(host, fmt.Sprintf("%d", controller.Port))
	admin, err := kafka.DialContext(ctx, "tcp", ctrlAddr)
	if err != nil {
		return fmt.Errorf("dial controller %s: %w", ctrlAddr, err)
	}
	defer admin.Close()
	_ = admin.SetDeadline(time.Now().Add(10 * time.Second))
	if err := admin.CreateTopics(cfg); err != nil && !alreadyExists(err) {
		return err
	}
	return nil
}

func alreadyExists(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, kafka.TopicAlreadyExists) {
		return true
	}
	if te, ok := err.(kafka.Error); ok && te == kafka.TopicAlreadyExists {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "already exists") ||
		strings.Contains(msg, "TopicAlreadyExists") ||
		strings.Contains(msg, "TOPIC_ALREADY_EXISTS")
}
