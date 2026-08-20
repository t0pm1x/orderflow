package api

import (
	"encoding/json"
	"testing"

	"github.com/t0pm1x/orderflow/services/order/internal/domain"
)

// ptr is a tiny helper for test code: turn an int64 literal into
// the *int64 domain.OrderItem.UnitPriceCents shape.
func ptr(v int64) *int64 { return &v }

func TestBuildOrderCreatedRecord_HappyPath(t *testing.T) {
	custID, _ := parseCustomerID("550e8400-e29b-41d4-a716-446655440000")
	o := domain.NewOrder(
		custID,
		[]domain.OrderItem{
			{SKU: "A", Quantity: 2, UnitPriceCents: ptr(150)},
			{SKU: "B", Quantity: 1, UnitPriceCents: ptr(500)},
		},
	)
	rec, err := buildOrderCreatedRecord(o)
	if err != nil {
		t.Fatalf("buildOrderCreatedRecord: %v", err)
	}
	if rec.EventType != "OrderCreated" {
		t.Errorf("event_type: got %q want OrderCreated", rec.EventType)
	}
	if rec.AggregateType != "Order" {
		t.Errorf("aggregate_type: got %q want Order", rec.AggregateType)
	}
	if rec.AggregateID != o.ID.String() {
		t.Errorf("aggregate_id: got %q want %q", rec.AggregateID, o.ID.String())
	}
	if rec.Topic != TopicOrderEvents {
		t.Errorf("topic: got %q want %q", rec.Topic, TopicOrderEvents)
	}
	if rec.SchemaVersion != "1.0" {
		t.Errorf("schema_version: got %q want 1.0", rec.SchemaVersion)
	}
	if rec.EventID == "" {
		t.Error("event_id must be assigned")
	}

	var payload OrderCreatedPayload
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		t.Fatalf("payload not valid JSON: %v", err)
	}
	if payload.OrderID != o.ID.String() {
		t.Errorf("payload.order_id: got %q want %q", payload.OrderID, o.ID.String())
	}
	if payload.CustomerID != "550e8400-e29b-41d4-a716-446655440000" {
		t.Errorf("payload.customer_id: got %q want known UUID", payload.CustomerID)
	}
	if payload.TotalCents != int64(800) { // 2*150 + 1*500
		t.Errorf("payload.total_cents: got %d want 800", payload.TotalCents)
	}
	if payload.State != "pending" {
		t.Errorf("payload.state: got %q want pending", payload.State)
	}
	if len(payload.Items) != 2 {
		t.Errorf("payload.items: got %d want 2", len(payload.Items))
	}
}
