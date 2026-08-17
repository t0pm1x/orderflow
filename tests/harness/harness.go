// Package harness starts a full set of test containers for the
// orderflow E2E, chaos, and load tests:
//
//   - three PostgreSQL 16-alpine instances (order, payment, inventory),
//     each migrated with its corresponding service's SQL files
//   - one Redis 7-alpine instance
//   - one Kafka (Confluent local, KRaft mode)
//   - optionally, one OTel Collector container
//
// Sub-stages 3.11.b–3.11.e will compose the four orderflow service
// binaries against this harness and drive them with real traffic.
//
// Tests that use New MUST NOT run with `-short`; the harness requires
// a reachable Docker daemon and pulls large images on first run.
package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Harness exposes connection details for every testcontainer started
// by New. Tests read these URLs to point the orderflow service binaries
// at the harness's dependencies.
type Harness struct {
	OrderURL     string
	PaymentURL   string
	InventoryURL string

	KafkaBrokers []string
	KafkaTopics  map[string]bool

	PostgresURLs map[string]string
	RedisURL     string

	OtelEndpoint string

	kafkaContainer testcontainers.Container

	t *testing.T
}

// Option mutates the harness configuration.
type Option func(*config)

type config struct {
	withOtel bool
}

// WithOtel starts an OpenTelemetry Collector container alongside the
// other dependencies and populates OtelEndpoint (host:port of the
// OTLP gRPC receiver, port 4317).
func WithOtel() Option { return func(c *config) { c.withOtel = true } }

// New starts a full harness, applies migrations, and registers a
// t.Cleanup that terminates every container on test exit.
//
// It calls t.Fatal on any setup error so callers do not need to
// check.
func New(t *testing.T, opts ...Option) *Harness {
	t.Helper()

	ctx := context.Background()
	cfg := &config{}
	for _, o := range opts {
		o(cfg)
	}

	pgOrder := mustPostgres(ctx, t, "order")
	pgPay := mustPostgres(ctx, t, "payment")
	pgInv := mustPostgres(ctx, t, "inventory")
	rd := mustRedis(ctx, t)
	kf := mustKafka(ctx, t)

	h := &Harness{
		OrderURL:     pgOrder.url,
		PaymentURL:   pgPay.url,
		InventoryURL: pgInv.url,
		KafkaBrokers: kf.brokers,
		KafkaTopics:  map[string]bool{},
		PostgresURLs: map[string]string{
			"order":     pgOrder.url,
			"payment":   pgPay.url,
			"inventory": pgInv.url,
		},
		RedisURL:       rd.url,
		kafkaContainer: kf.container,
		t:              t,
	}

	var oc *otelHandle
	if cfg.withOtel {
		oc = mustOtelCollector(ctx, t)
		h.OtelEndpoint = oc.endpoint
	}

	t.Cleanup(func() {
		terminate(ctx, pgOrder.container)
		terminate(ctx, pgPay.container)
		terminate(ctx, pgInv.container)
		terminate(ctx, rd.container)
		terminate(ctx, kf.container)
		if oc != nil {
			terminate(ctx, oc.container)
		}
	})

	return h
}

// StartService launches one of the orderflow service binaries as a
// child process. The harness takes care of env wiring (DATABASE_URL,
// KAFKA_BROKER, REDIS_URL where applicable, HTTP_ADDR). Returns a
// stop function that gracefully terminates the process.
//
// binName is the binary base name without `.exe` — the function picks
// the correct extension for the current OS via runtime.GOOS.
func (h *Harness) StartService(t *testing.T, name, binName string, env map[string]string) (stop func()) {
	t.Helper()
	bin := binName
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	root, err := findRepoRoot()
	if err != nil {
		t.Fatalf("harness: findRepoRoot: %v", err)
	}
	binPath := filepath.Join(root, "bin", bin)

	cmd := exec.Command(binPath)
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}
	// Default OTLP exporter to stdout so child processes don't try to
	// dial otel-collector:4317 (which is unreachable from this box).
	cmd.Env = append(cmd.Env, "OTEL_EXPORTER=stdout")
	logDir := filepath.Join(root, "tests", "logs")
	_ = os.MkdirAll(logDir, 0o755)
	logPath := filepath.Join(logDir, name+".log")
	logFile, ferr := os.Create(logPath)
	if ferr != nil {
		t.Fatalf("harness: create log file %s: %v", logPath, ferr)
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatalf("harness: start %s (%s): %v", name, binPath, err)
	}
	return func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_, _ = cmd.Process.Wait()
		_ = logFile.Close()
	}
}

// WaitForOrderState polls the order service until the order reaches
// the expected state or the timeout elapses. The order service must
// be running externally; sub-stages 3.11.b–3.11.e wire that up.
//
// Implementation deferred — the self-test does not exercise it.
func (h *Harness) WaitForOrderState(_ string, _ string, _ time.Duration) error {
	return errors.New("harness: WaitForOrderState not yet implemented (sub-stage 3.11.b)")
}

// KafkaContainer exposes the underlying Kafka testcontainer so chaos
// tests can terminate (and optionally restart) the broker. Returns
// nil if the harness failed to start Kafka.
func (h *Harness) KafkaContainer() testcontainers.Container {
	return h.kafkaContainer
}

// RestartKafka terminates the current Kafka container and starts a
// fresh one. Updates h.KafkaBrokers with the new address. Used by
// chaos tests to verify outbox retry recovery.
//
// Note: service binaries started via StartService capture
// KAFKA_BROKER in their environment at startup; after a restart the
// new broker lives at a different host:port that the already-running
// processes cannot reach. Callers that want full end-to-end recovery
// must also restart the dependent services.
func (h *Harness) RestartKafka(ctx context.Context) error {
	if h.kafkaContainer != nil {
		_ = h.kafkaContainer.Terminate(ctx)
		h.kafkaContainer = nil
	}
	kf := mustKafka(ctx, h.t)
	h.kafkaContainer = kf.container
	h.KafkaBrokers = kf.brokers
	return nil
}

// pgHandle bundles a running Postgres container with its connection
// string so cleanup and reporting have one value to pass around.
type pgHandle struct {
	container *tcpostgres.PostgresContainer
	url       string
}

func mustPostgres(ctx context.Context, t *testing.T, svcName string) *pgHandle {
	t.Helper()

	dbName := svcName + "_" + svcName
	c, err := tcpostgres.RunContainer(ctx,
		tcpostgres.WithDatabase(dbName),
		tcpostgres.WithUsername("orderflow"),
		tcpostgres.WithPassword("orderflow"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("harness: start postgres (%s): %v", svcName, err)
	}

	connStr, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("harness: postgres (%s) connection string: %v", svcName, err)
	}

	if err := applyMigrations(ctx, connStr, svcName); err != nil {
		t.Fatalf("harness: apply migrations (%s): %v", svcName, err)
	}

	return &pgHandle{container: c, url: connStr}
}

type redisHandle struct {
	container *tcredis.RedisContainer
	url       string
}

func mustRedis(ctx context.Context, t *testing.T) *redisHandle {
	t.Helper()
	c, err := tcredis.RunContainer(ctx)
	if err != nil {
		t.Fatalf("harness: start redis: %v", err)
	}
	url, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("harness: redis connection string: %v", err)
	}
	return &redisHandle{container: c, url: url}
}

type kafkaHandle struct {
	container *tckafka.KafkaContainer
	brokers   []string
}

func mustKafka(ctx context.Context, t *testing.T) *kafkaHandle {
	t.Helper()
	c, err := tckafka.RunContainer(ctx)
	if err != nil {
		t.Fatalf("harness: start kafka: %v", err)
	}
	brokers, err := c.Brokers(ctx)
	if err != nil {
		t.Fatalf("harness: kafka brokers: %v", err)
	}
	if len(brokers) == 0 {
		t.Fatal("harness: kafka returned no brokers")
	}
	return &kafkaHandle{container: c, brokers: brokers}
}

type otelHandle struct {
	container testcontainers.Container
	endpoint  string
}

func mustOtelCollector(ctx context.Context, t *testing.T) *otelHandle {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        "otel/opentelemetry-collector-contrib:0.108.0",
		ExposedPorts: []string{"4317/tcp", "4318/tcp"},
		WaitingFor:   wait.ForLog("Everything is ready").WithOccurrence(1).WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("harness: start otel-collector: %v", err)
	}
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("harness: otel host: %v", err)
	}
	port, err := c.MappedPort(ctx, "4317/tcp")
	if err != nil {
		t.Fatalf("harness: otel port: %v", err)
	}
	return &otelHandle{
		container: c,
		endpoint:  fmt.Sprintf("%s:%s", host, port.Port()),
	}
}

// applyMigrations opens a pgxpool against the freshly-started database,
// creates the extensions required by deploy/postgres/init-*.sh, then
// applies every *.sql file in services/<svcName>/migrations/ in lexical
// order.
func applyMigrations(ctx context.Context, url, svcName string) error {
	repoRoot, err := findRepoRoot()
	if err != nil {
		return err
	}
	migDir := filepath.Join(repoRoot, "services", svcName, "migrations")

	entries, err := os.ReadDir(migDir)
	if err != nil {
		return fmt.Errorf("read migrations dir %s: %w", migDir, err)
	}

	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	if len(files) == 0 {
		return fmt.Errorf("no .sql migrations found in %s", migDir)
	}

	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return fmt.Errorf("pgxpool: %w", err)
	}
	defer pool.Close()

	// Extensions required by every service schema (mirrors
	// deploy/postgres/init-{order,payment,inventory}.sh).
	if _, err := pool.Exec(ctx,
		`CREATE EXTENSION IF NOT EXISTS pgcrypto;`+
			`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`,
	); err != nil {
		return fmt.Errorf("create extensions: %w", err)
	}

	for _, f := range files {
		body, err := os.ReadFile(filepath.Join(migDir, f))
		if err != nil {
			return fmt.Errorf("read %s: %w", f, err)
		}
		if _, err := pool.Exec(ctx, string(body)); err != nil {
			return fmt.Errorf("exec %s: %w", f, err)
		}
	}
	return nil
}

// findRepoRoot walks up from this source file until it finds go.work.
// This makes the harness robust to the directory `go test` is invoked
// from — only the relative position of the repo matters.
func findRepoRoot() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("harness: cannot determine source file location")
	}
	dir := filepath.Dir(thisFile)
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", errors.New("harness: go.work not found above tests/harness/")
}

// terminate swallows Terminate errors so cleanup never panics the
// test goroutine. Containers are best-effort torn down; the test has
// already either succeeded or failed by the time Cleanup runs.
func terminate(ctx context.Context, c testcontainers.Container) {
	if c == nil {
		return
	}
	_ = c.Terminate(ctx)
}
