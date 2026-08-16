package platform

import (
	"context"
	"testing"
)

func TestInitTracing_Stdout(t *testing.T) {
	t.Setenv("OTEL_EXPORTER", "stdout")
	shutdown, err := InitTracing(context.Background(), "test-svc")
	if err != nil {
		t.Fatalf("InitTracing: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil shutdown")
	}
	_ = shutdown(context.Background())
}