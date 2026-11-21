package dlq

import (
	"testing"

	"github.com/LumeshC/streamscale/stream-processor/internal/classify"
	"github.com/segmentio/kafka-go"
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
