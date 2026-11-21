package config

import (
	"testing"
	"time"
)

func TestDefaults(t *testing.T) {
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":8082" || c.RequestTimeout != 5*time.Second || c.MaxRange != 168*time.Hour ||
		c.DefaultRange != time.Hour || !c.RedisEnabled || c.DBMaxConns != 10 {
		t.Fatalf("%+v", c)
	}
}

func TestRejectsInvalid(t *testing.T) {
	t.Setenv("API_DEFAULT_RANGE_MINUTES", "100000")
	t.Setenv("API_MAX_RANGE_HOURS", "1")
	if _, err := Load(); err == nil {
		t.Fatal("default range larger than max must be rejected")
	}
}

func TestOverrides(t *testing.T) {
	t.Setenv("API_REQUEST_TIMEOUT_MS", "1500")
	t.Setenv("REDIS_ENABLED", "false")
	c, err := Load()
	if err != nil || c.RequestTimeout != 1500*time.Millisecond || c.RedisEnabled {
		t.Fatalf("%+v %v", c, err)
	}
}
