// Package kafkalag reports consumer-group lag for the processor's topic, the
// same way kafka-consumer-groups.sh does: per partition, lag = log-end offset
// minus the group's committed offset. It only reads cluster metadata and
// offsets; it never joins the group or consumes messages.
package kafkalag

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/segmentio/kafka-go"
)

// ErrUnavailable marks errors caused by Kafka being unreachable or the source
// being disabled.
var ErrUnavailable = errors.New("kafka unavailable")

// PartitionLag is one partition's position.
type PartitionLag struct {
	Partition       int   `json:"partition"`
	StartOffset     int64 `json:"start_offset"`
	EndOffset       int64 `json:"end_offset"`
	CommittedOffset int64 `json:"committed_offset"` // -1 when the group has not committed
	Lag             int64 `json:"lag"`
}

// Report is a point-in-time lag snapshot.
type Report struct {
	Group      string         `json:"group"`
	Topic      string         `json:"topic"`
	AsOf       time.Time      `json:"as_of"`
	TotalLag   int64          `json:"total_lag"`
	EndOffset  int64          `json:"end_offset_total"`
	Partitions []PartitionLag `json:"partitions"`
}

// Source provides lag reports.
type Source interface {
	Lag(ctx context.Context) (Report, error)
	Ping(ctx context.Context) error
}

// Kafka implements Source with kafka-go's admin client.
type Kafka struct {
	client  *kafka.Client
	group   string
	topic   string
	timeout time.Duration
	now     func() time.Time
}

// Options configure the lag reader.
type Options struct {
	Brokers []string
	GroupID string
	Topic   string
	Timeout time.Duration
}

func New(o Options) *Kafka {
	if o.Timeout <= 0 {
		o.Timeout = 2 * time.Second
	}
	return &Kafka{
		client:  &kafka.Client{Addr: kafka.TCP(o.Brokers...), Timeout: o.Timeout},
		group:   o.GroupID,
		topic:   o.Topic,
		timeout: o.Timeout,
		now:     time.Now,
	}
}

func (k *Kafka) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, k.timeout)
	defer cancel()
	if _, err := k.client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{k.topic}}); err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return nil
}

func (k *Kafka) Lag(ctx context.Context) (Report, error) {
	ctx, cancel := context.WithTimeout(ctx, k.timeout)
	defer cancel()

	md, err := k.client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{k.topic}})
	if err != nil {
		return Report{}, fmt.Errorf("%w: metadata: %w", ErrUnavailable, err)
	}
	var parts []int
	for _, t := range md.Topics {
		if t.Name != k.topic {
			continue
		}
		if t.Error != nil {
			return Report{}, fmt.Errorf("topic %s: %w", k.topic, t.Error)
		}
		for _, p := range t.Partitions {
			parts = append(parts, p.ID)
		}
	}
	if len(parts) == 0 {
		return Report{}, fmt.Errorf("topic %s not found", k.topic)
	}
	sort.Ints(parts)

	reqs := make([]kafka.OffsetRequest, 0, 2*len(parts))
	for _, p := range parts {
		reqs = append(reqs, kafka.FirstOffsetOf(p), kafka.LastOffsetOf(p))
	}
	offs, err := k.client.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{k.topic: reqs}})
	if err != nil {
		return Report{}, fmt.Errorf("%w: list offsets: %w", ErrUnavailable, err)
	}
	committed, err := k.client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{GroupID: k.group, Topics: map[string][]int{k.topic: parts}})
	if err != nil {
		return Report{}, fmt.Errorf("%w: offset fetch: %w", ErrUnavailable, err)
	}
	if committed.Error != nil {
		return Report{}, fmt.Errorf("offset fetch: %w", committed.Error)
	}

	byPart := map[int]*PartitionLag{}
	for _, p := range parts {
		byPart[p] = &PartitionLag{Partition: p, CommittedOffset: -1}
	}
	for _, po := range offs.Topics[k.topic] {
		if po.Error != nil {
			return Report{}, fmt.Errorf("partition %d offsets: %w", po.Partition, po.Error)
		}
		if pl, ok := byPart[po.Partition]; ok {
			pl.StartOffset, pl.EndOffset = po.FirstOffset, po.LastOffset
		}
	}
	for _, cp := range committed.Topics[k.topic] {
		if cp.Error != nil {
			return Report{}, fmt.Errorf("partition %d committed offset: %w", cp.Partition, cp.Error)
		}
		if pl, ok := byPart[cp.Partition]; ok {
			pl.CommittedOffset = cp.CommittedOffset
		}
	}

	r := Report{Group: k.group, Topic: k.topic, AsOf: k.now().UTC()}
	for _, p := range parts {
		pl := byPart[p]
		pl.Lag = Compute(pl.StartOffset, pl.EndOffset, pl.CommittedOffset)
		r.TotalLag += pl.Lag
		r.EndOffset += pl.EndOffset
		r.Partitions = append(r.Partitions, *pl)
	}
	return r, nil
}

// Compute returns a partition's lag. Without a commit, the group (which starts
// from the earliest offset) is behind by everything still retained.
func Compute(start, end, committed int64) int64 {
	pos := committed
	if pos < 0 {
		pos = start
	}
	if pos < start {
		pos = start // committed offset already deleted by retention
	}
	if lag := end - pos; lag > 0 {
		return lag
	}
	return 0
}

// Disabled is used when Kafka is not configured for the API.
type Disabled struct{}

func (Disabled) Lag(context.Context) (Report, error) {
	return Report{}, fmt.Errorf("%w: kafka lag source disabled", ErrUnavailable)
}
func (Disabled) Ping(context.Context) error { return fmt.Errorf("%w: disabled", ErrUnavailable) }
