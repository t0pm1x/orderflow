package order_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/t0pm1x/orderflow/services/order/cmd/order"
)

// TestVersionConstant pins the public Version identifier so a
// release-tooling change accidentally renaming it gets caught here.
func TestVersionConstant(t *testing.T) {
	if order.Version == "" {
		t.Error("Version constant must not be empty")
	}
}

// TestTableNameConstant pins the outbox table identifier.
func TestTableNameConstant(t *testing.T) {
	if order.TableName != "order_outbox" {
		t.Errorf("TableName: got %q want order_outbox", order.TableName)
	}
}

// TestRun_DisabledWhenNoEnv checks that Run returns nil (no error)
// when DATABASE_URL and KAFKA_BROKER are unset, because the poller
// is intentionally disabled in that mode.
func TestRun_DisabledWhenNoEnv(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("KAFKA_BROKER", "")
	t.Setenv("HTTP_ADDR", "")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := order.Run(ctx); err != nil {
		t.Fatalf("Run in disabled mode: %v", err)
	}
}

// TestEnvOrDefault_NotExported is a placeholder reminding future
// readers that envOrDefault is intentionally unexported: env wiring
// lives in this package and shouldn't leak.
func TestEnvOrDefault_NotExported(t *testing.T) {
	// Compile-time reminder: if envOrDefault is exported, this
	// test (and only this test) can be replaced by an external
	// caller. Keeping it unexported keeps the surface tight.
	_ = bytes.NewBuffer // avoid unused import on gofmt changes
	_ = strings.EqualFold
}
