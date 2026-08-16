package events

import (
	"testing"
)

func TestNewEnvelope(t *testing.T) {
	payload := map[string]string{"hello": "world"}
	env, err := NewEnvelope("OrderCreated", "Order", "123", payload, "trace1", "span1")
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	if env.EventType != "OrderCreated" {
		t.Errorf("expected OrderCreated, got %s", env.EventType)
	}
	if env.AggregateID != "123" {
		t.Errorf("expected 123, got %s", env.AggregateID)
	}
	if env.TraceID != "trace1" {
		t.Errorf("expected trace1, got %s", env.TraceID)
	}
	if env.EventID == "" {
		t.Error("expected EventID generated")
	}
	if env.OccurredAt.IsZero() {
		t.Error("expected OccurredAt set")
	}
}
