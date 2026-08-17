package domain

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/t0pm1x/orderflow/platform/types"
)

func TestNewOrder(t *testing.T) {
	items := []OrderItem{
		{SKU: "A", Quantity: 2, UnitPriceCents: 1000},
		{SKU: "B", Quantity: 1, UnitPriceCents: 500},
	}
	o := NewOrder(types.NewCustomerID(), items)
	if o.State != StatePending {
		t.Errorf("expected pending, got %s", o.State)
	}
	if int64(o.TotalCents) != 2500 {
		t.Errorf("expected 2500 cents, got %d", o.TotalCents)
	}
	if o.ID == (types.OrderID)(uuid.Nil) {
		t.Error("expected ID assigned")
	}
}

func TestTransition_HappyPath(t *testing.T) {
	o := NewOrder(types.NewCustomerID(), []OrderItem{{SKU: "A", Quantity: 1, UnitPriceCents: 100}})
	if err := o.Transition(StateReserved); err != nil {
		t.Fatal(err)
	}
	if err := o.Transition(StateConfirmed); err != nil {
		t.Fatal(err)
	}
	if o.State != StateConfirmed {
		t.Errorf("expected confirmed, got %s", o.State)
	}
	if o.CompletedAt == nil {
		t.Error("expected CompletedAt set")
	}
}

func TestTransition_Invalid(t *testing.T) {
	o := NewOrder(types.NewCustomerID(), nil)
	if err := o.Transition(StateConfirmed); err == nil {
		t.Error("expected error: pending → confirmed is invalid")
	}
}

func TestCanTransition_TerminalStates(t *testing.T) {
	// Confirmed, cancelled, failed are terminal
	for _, s := range []OrderState{StateConfirmed, StateCancelled, StateFailed} {
		if CanTransition(s, StatePending) {
			t.Errorf("terminal %s should not transition back", s)
		}
	}
}

// Regression test for the e2e TestE2E_HappyPath_OrderConfirmed
// failure: POST /v1/orders response body decoded by the e2e harness
// as `{"id": "<uuid-string>"}`, but was previously emitted as
// `{"id": [16 byte array]}`. Verify Order.ID and Order.CustomerID
// marshal as JSON strings.
func TestOrder_MarshalJSON_IDsAreStrings(t *testing.T) {
	o := NewOrder(types.NewCustomerID(), []OrderItem{{SKU: "A", Quantity: 1, UnitPriceCents: 100}})
	data, err := json.Marshal(o)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	body := string(data)
	if !strings.Contains(body, `"id":"`+o.ID.String()+`"`) {
		t.Errorf(`expected "id":"<uuid>" in body, got: %s`, body)
	}
	if !strings.Contains(body, `"customer_id":"`+o.CustomerID.String()+`"`) {
		t.Errorf(`expected "customer_id":"<uuid>" in body, got: %s`, body)
	}
}
