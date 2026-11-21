package dlq

import (
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/streamscale/streamscale/stream-processor/internal/classify"
)

func TestHeaderMap(t *testing.T) {
	m := HeaderMap([]kafka.Header{
		{Key: "dlq.reason", Value: []byte("validation_failed")},
		{Key: "dlq.kind", Value: []byte(classify.ClassMalformed)},
	})
	if m["dlq.reason"] != "validation_failed" {
		t.Fatalf("%v", m)
	}
}
