package config

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/streamscale/streamscale/pkg/envcfg"
	"github.com/streamscale/streamscale/pkg/ktopic"
)

type Config struct {
	Brokers    []string
	Topic      string
	Partitions int
	Rate       int
	Count      int
	UserPool   int
	Seed       int64
	HTTPAddr   string
}

func Parse() (Config, error) {
	c := Config{
		Brokers:    envcfg.CSV("KAFKA_BROKERS", "localhost:9092"),
		Topic:      envcfg.String("KAFKA_TOPIC", ktopic.DefaultName),
		Partitions: envcfg.Int("KAFKA_PARTITIONS", ktopic.DefaultPartitions),
		Rate:       envcfg.Int("EVENT_RATE", 20),
		Count:      envcfg.Int("EVENT_COUNT", 0),
		UserPool:   envcfg.Int("USER_POOL", 250),
		Seed:       int64(envcfg.Int("GENERATOR_SEED", 0)),
		HTTPAddr:   envcfg.String("HTTP_ADDR", ":8080"),
	}

	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	brokers := fs.String("brokers", join(c.Brokers), "comma-separated Kafka brokers")
	topic := fs.String("topic", c.Topic, "Kafka topic")
	partitions := fs.Int("partitions", c.Partitions, "topic partition count when creating")
	rate := fs.Int("rate", c.Rate, "approximate events per second")
	count := fs.Int("count", c.Count, "stop after N events (0 = run until signal)")
	pool := fs.Int("users", c.UserPool, "synthetic user pool size")
	seed := fs.Int64("seed", c.Seed, "RNG seed (0 = current time)")
	httpAddr := fs.String("http-addr", c.HTTPAddr, "health check listen address")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return Config{}, err
	}
	c.Brokers = splitCSV(*brokers)
	c.Topic = *topic
	c.Partitions = *partitions
	c.Rate = *rate
	c.Count = *count
	c.UserPool = *pool
	c.Seed = *seed
	c.HTTPAddr = *httpAddr
	if c.Rate < 1 {
		return Config{}, fmt.Errorf("rate must be >= 1")
	}
	if len(c.Brokers) == 0 {
		return Config{}, fmt.Errorf("at least one kafka broker is required")
	}
	return c, nil
}

func join(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += ","
		}
		out += p
	}
	return out
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
