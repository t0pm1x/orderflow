package payment_test

import (
	"context"
	"testing"
	"time"

	"github.com/t0pm1x/orderflow/services/payment/cmd/payment"
)

func TestVersionConstant(t *testing.T) {
	if payment.Version == "" {
		t.Error("Version constant must not be empty")
	}
}

func TestTableNameConstant(t *testing.T) {
	if payment.TableName != "payment_outbox" {
		t.Errorf("TableName: got %q want payment_outbox", payment.TableName)
	}
}

func TestRun_DisabledWhenNoEnv(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("KAFKA_BROKER", "")
	t.Setenv("HTTP_ADDR", "")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := payment.Run(ctx); err != nil {
		t.Fatalf("Run in disabled mode: %v", err)
	}
}
