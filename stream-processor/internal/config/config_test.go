package config

import (
	"os"
	"testing"
	"time"

	"github.com/LumeshC/streamscale/pkg/ktopic"
)

func TestLoadDefaults(t *testing.T) {
	os.Unsetenv("KAFKA_BROKERS")
	os.Unsetenv("DATABASE_URL")
	os.Unsetenv("KAFKA_DLQ_TOPIC")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.Topic != ktopic.DefaultName || c.Partitions != 3 {
		t.Fatalf("%+v", c)
	}
	if c.DLQTopic != ktopic.DefaultDLQName || c.DLQPartitions != 3 {
		t.Fatalf("dlq %+v", c)
	}
	if c.GroupID != "streamscale-processor" {
		t.Fatalf("group %s", c.GroupID)
	}
	if c.MaxRetries != 3 {
		t.Fatalf("retries %d", c.MaxRetries)
	}
	if !c.AnalyticsEnabled {
		t.Fatal("analytics should be enabled by default")
	}
	if c.AllowedLateness != 30*time.Second {
		t.Fatalf("lateness %v", c.AllowedLateness)
	}
}
