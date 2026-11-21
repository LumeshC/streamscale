// Phase 1 end-to-end: event-generator → Kafka "events" → processor → PostgreSQL.
package e2e_test

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/streamscale/streamscale/pkg/event"
	"github.com/streamscale/streamscale/pkg/ktopic"
	"github.com/streamscale/streamscale/stream-processor/internal/consume"
	"github.com/streamscale/streamscale/stream-processor/internal/dlq"
	"github.com/streamscale/streamscale/stream-processor/internal/retry"
	"github.com/streamscale/streamscale/stream-processor/internal/store"
)

func requireDocker(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not installed")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skipf("docker daemon not available: %v", err)
	}
}

// TestPhase1GeneratorToPostgres runs the real event-generator binary (built
// from ./event-generator) against a Kafka container, consumes with the real
// processor, and checks every generated event lands in PostgreSQL:
//
//	event-generator → Kafka "events" (3 partitions) → processor → PostgreSQL
func TestPhase1GeneratorToPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	genBin := buildGenerator(t)

	pgURL, brokers, cleanup := startInfra(t, ctx)
	defer cleanup()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ensureTopics(t, ctx, brokers, log)

	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := store.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	st := store.New(pool)

	procCtx, procCancel := context.WithCancel(ctx)
	defer procCancel()
	proc := consume.New(consume.Options{
		Brokers: brokers,
		Topic:   "events",
		GroupID: "streamscale-processor-e2e",
		Store:   st,
		DLQ:     dlq.New(brokers, "events.dlq", log),
		Log:     log,
		Retry:   retry.Config{MaxAttempts: 3, BaseDelay: 50 * time.Millisecond, MaxDelay: 200 * time.Millisecond},
	})
	defer func() { _ = proc.Close() }()
	errCh := make(chan error, 1)
	go func() { errCh <- proc.Run(procCtx) }()

	// Run the generator exactly as Compose does, via flags.
	const n = 1000
	var genOut bytes.Buffer
	gen := exec.CommandContext(ctx, genBin,
		"--brokers="+strings.Join(brokers, ","),
		"--topic=events",
		"--partitions=3",
		"--rate=500",
		"--count="+strconv.Itoa(n),
		"--users=100",
		"--seed=42",
		"--http-addr=127.0.0.1:0",
	)
	gen.Stdout = &genOut
	gen.Stderr = &genOut
	if err := gen.Run(); err != nil {
		t.Fatalf("event-generator failed: %v\n%s", err, genOut.String())
	}
	if !strings.Contains(genOut.String(), `"msg":"count reached"`) ||
		!strings.Contains(genOut.String(), `"produced":1000`) {
		t.Fatalf("generator did not report producing %d events:\n%s", n, genOut.String())
	}

	// Kafka received every event, spread over the 3 partitions.
	perPartition := kafkaEndOffsets(t, brokers, "events", 3)
	var total int64
	for _, o := range perPartition {
		total += o
	}
	if total != n {
		t.Fatalf("kafka has %d messages on events (per partition %v), want %d", total, perPartition, n)
	}
	for p, o := range perPartition {
		if o == 0 {
			t.Fatalf("partition %d received no events (per partition %v)", p, perPartition)
		}
	}

	// The processor consumed them all into PostgreSQL.
	deadline := time.Now().Add(90 * time.Second)
	for {
		select {
		case err := <-errCh:
			if err != nil && procCtx.Err() == nil {
				t.Fatalf("processor: %v", err)
			}
		default:
		}
		got, err := st.Count(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got == n {
			break
		}
		if got > n || time.Now().After(deadline) {
			t.Fatalf("postgres events=%d want %d", got, n)
		}
		time.Sleep(300 * time.Millisecond)
	}

	// Content checks on what the generator produced.
	rows, err := pool.Query(ctx, `SELECT event_type, COUNT(*) FROM events GROUP BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	byType := map[string]int64{}
	for rows.Next() {
		var typ string
		var c int64
		if err := rows.Scan(&typ, &c); err != nil {
			t.Fatal(err)
		}
		byType[typ] = c
	}
	rows.Close()
	for _, typ := range event.AllTypes {
		if byType[string(typ)] == 0 {
			t.Fatalf("no %s events persisted: %v", typ, byType)
		}
	}
	var badCommerce, badRequired int64
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM events
		WHERE event_type IN ('PURCHASE','PAYMENT_SUCCESS','PAYMENT_FAILURE')
		  AND (product_id IS NULL OR amount IS NULL OR currency IS NULL)`).Scan(&badCommerce); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM events
		WHERE region = '' OR metadata IS NULL OR ts IS NULL`).Scan(&badRequired); err != nil {
		t.Fatal(err)
	}
	if badCommerce != 0 || badRequired != 0 {
		t.Fatalf("invalid rows: commerce=%d required=%d", badCommerce, badRequired)
	}
	statusProcessed, err := st.CountByStatus(ctx, store.StatusProcessed)
	if err != nil {
		t.Fatal(err)
	}
	if statusProcessed != n {
		t.Fatalf("processing_status processed=%d want %d", statusProcessed, n)
	}
	t.Logf("generator → kafka %v → postgres %d events, by type %v", perPartition, n, byType)
}

// buildGenerator compiles ./event-generator into a temp dir.
func buildGenerator(t *testing.T) string {
	t.Helper()
	goBin := filepath.Join(runtime.GOROOT(), "bin", "go")
	if _, err := os.Stat(goBin); err != nil {
		goBin = "go"
	}
	out := filepath.Join(t.TempDir(), "event-generator")
	cmd := exec.Command(goBin, "build", "-o", out, "./event-generator")
	cmd.Dir = filepath.Join("..", "..") // module root, from stream-processor/e2e
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build event-generator: %v\n%s", err, b)
	}
	return out
}

// kafkaEndOffsets returns the log-end offset of each partition.
func kafkaEndOffsets(t *testing.T, brokers []string, topic string, partitions int) []int64 {
	t.Helper()
	out := make([]int64, partitions)
	for p := 0; p < partitions; p++ {
		conn, err := kafka.DialLeader(context.Background(), "tcp", brokers[0], topic, p)
		if err != nil {
			t.Fatalf("dial partition %d: %v", p, err)
		}
		off, err := conn.ReadLastOffset()
		_ = conn.Close()
		if err != nil {
			t.Fatalf("last offset partition %d: %v", p, err)
		}
		out[p] = off
	}
	return out
}

func startInfra(t *testing.T, ctx context.Context) (pgURL string, brokers []string, cleanup func()) {
	t.Helper()
	pgC, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("streamscale"),
		tcpostgres.WithUsername("streamscale"),
		tcpostgres.WithPassword("streamscale"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("postgres container: %v", err)
	}
	kafkaC, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1")
	if err != nil {
		_ = pgC.Terminate(context.Background())
		t.Fatalf("kafka container: %v", err)
	}
	pgURL, err = pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	brokers, err = kafkaC.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cleanup = func() {
		_ = kafkaC.Terminate(context.Background())
		_ = pgC.Terminate(context.Background())
	}
	return pgURL, brokers, cleanup
}

func ensureTopics(t *testing.T, ctx context.Context, brokers []string, log *slog.Logger) {
	t.Helper()
	if err := ktopic.Ensure(ctx, brokers, "events", 3, log); err != nil {
		t.Fatalf("topic: %v", err)
	}
	if err := ktopic.Ensure(ctx, brokers, "events.dlq", 3, log); err != nil {
		t.Fatalf("dlq topic: %v", err)
	}
}
