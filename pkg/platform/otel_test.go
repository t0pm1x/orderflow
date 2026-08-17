package platform

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func TestInitTracing_Stdout(t *testing.T) {
	t.Setenv("OTEL_EXPORTER", "stdout")
	shutdown, err := InitTracing(context.Background(), "test-svc", "0.0.0-test")
	if err != nil {
		t.Fatalf("InitTracing: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil shutdown")
	}
	_ = shutdown(context.Background())
}

// TestInitTracing_ServiceVersionPropagated asserts that the
// service.version resource attribute is set on the resource built
// by InitTracingForTest (sub-stage 3.10.e).
func TestInitTracing_ServiceVersionPropagated(t *testing.T) {
	ctx, shutdown, err := InitTracingForTest(context.Background(), "svc", "1.2.3")
	if err != nil {
		t.Fatalf("InitTracingForTest: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil shutdown")
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	res := ctx.Value(testResourceCtxKey{}).(*resource.Resource)
	if res == nil {
		t.Fatal("expected resource attached to context")
	}

	attrs := map[string]string{}
	for _, a := range res.Attributes() {
		attrs[string(a.Key)] = a.Value.AsString()
	}
	if got := attrs["service.name"]; got != "svc" {
		t.Errorf("service.name: got %q want %q", got, "svc")
	}
	if got := attrs["service.version"]; got != "1.2.3" {
		t.Errorf("service.version: got %q want %q", got, "1.2.3")
	}

	// Also verify semconv.ServiceVersionKey matches (sanity check
	// that the key the rest of the platform reads back is the one
	// we set).
	var found bool
	for _, a := range res.Attributes() {
		if a.Key == semconv.ServiceVersionKey {
			found = true
			if a.Value.AsString() != "1.2.3" {
				t.Errorf("ServiceVersionKey value: got %q want %q", a.Value.AsString(), "1.2.3")
			}
		}
	}
	if !found {
		t.Errorf("expected attribute %q in resource", semconv.ServiceVersionKey)
	}
}
