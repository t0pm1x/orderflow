package inventory_test

import (
	"context"
	"testing"
	"time"

	"github.com/t0pm1x/orderflow/services/inventory/cmd/inventory"
)

func TestVersionConstant(t *testing.T) {
	if inventory.Version == "" {
		t.Error("Version constant must not be empty")
	}
}

func TestTableNameConstant(t *testing.T) {
	if inventory.TableName != "inventory_outbox" {
		t.Errorf("TableName: got %q want inventory_outbox", inventory.TableName)
	}
}

func TestRun_DisabledWhenNoEnv(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("KAFKA_BROKER", "")
	t.Setenv("HTTP_ADDR", "")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := inventory.Run(ctx); err != nil {
		t.Fatalf("Run in disabled mode: %v", err)
	}
}
