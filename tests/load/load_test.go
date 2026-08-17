package load_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/t0pm1x/orderflow/tests/harness"
)

// TestLoad_100RPS_p95Under1s brings up the order binary against the
// harness, runs k6 in a child process, and asserts k6 exits 0 (which
// means p95<1s AND failure rate<5% thresholds passed).
func TestLoad_100RPS_p95Under1s(t *testing.T) {
	if testing.Short() { t.Skip("load test requires docker + k6") }
	h := harness.New(t)

	stopOrder := h.StartService(t, "order", "order", map[string]string{
		"DATABASE_URL": h.PostgresURLs["order"],
		"KAFKA_BROKER": h.KafkaBrokers[0],
		"HTTP_ADDR":    "127.0.0.1:18081",
	})
	defer stopOrder()

	// Wait for order service to be healthy.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://127.0.0.1:18081/healthz")
		if err == nil { resp.Body.Close(); if resp.StatusCode == 200 { break } }
		time.Sleep(500 * time.Millisecond)
	}

	// Find k6 binary.
	k6Bin := "k6"
	if runtime.GOOS == "windows" { k6Bin = "k6.exe" }
	if _, err := exec.LookPath(k6Bin); err != nil {
		t.Skipf("k6 not installed (install: winget install k6). Skipping.")
	}

	// Find repo root.
	_, thisFile, _, _ := runtime.Caller(0)
	root := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	scriptPath := filepath.Join(root, "tests", "load", "k6.js")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, k6Bin, "run", scriptPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(), "K6_PROMETHEUS_RW_SERVER_URL=")
	if err := cmd.Run(); err != nil {
		t.Fatalf("k6 run failed: %v", err)
	}
}

func init() { fmt.Println("load test compiled") }
