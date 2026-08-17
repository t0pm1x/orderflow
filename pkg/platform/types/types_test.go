package types

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/uuid"
)

func TestMoney_Cents(t *testing.T) {
	m := NewMoneyFromCents(1999)
	if m.Cents() != 1999 {
		t.Errorf("expected 1999 cents, got %d", m.Cents())
	}
}

func TestMoney_String(t *testing.T) {
	cases := []struct {
		m    Money
		want string
	}{
		{NewMoneyFromCents(199), "$1.99"},
		{NewMoneyFromCents(100), "$1.00"},
		{NewMoneyFromCents(0), "$0.00"},
	}
	for _, c := range cases {
		if got := c.m.String(); got != c.want {
			t.Errorf("%d cents: got %q, want %q", c.m, got, c.want)
		}
	}
}

func TestMoney_FromMajor(t *testing.T) {
	m := NewMoneyFromMajor(19.99)
	if m.Cents() != 1999 {
		t.Errorf("expected 1999 cents, got %d", m.Cents())
	}
}

// UUIDs must serialize as JSON strings, not as 16-byte arrays. This
// is the regression test for the e2e TestE2E_HappyPath_OrderConfirmed
// failure where the Order.Service POST /v1/orders response failed to
// decode with: `cannot unmarshal array into Go struct field .id of
// type string`.
func TestUUID_MarshalJSON_IsString(t *testing.T) {
	id := NewOrderID()
	data, err := json.Marshal(id)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `"` + id.String() + `"`
	if string(data) != want {
		t.Errorf("got %s, want %s", data, want)
	}
}

func TestUUID_MarshalJSON_WorksForAllIDTypes(t *testing.T) {
	cases := []struct {
		name string
		id   fmt.Stringer
	}{
		{"OrderID", NewOrderID()},
		{"PaymentID", NewPaymentID()},
		{"StockID", NewStockID()},
		{"CustomerID", NewCustomerID()},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			data, err := json.Marshal(c.id)
			if err != nil {
				t.Fatalf("marshal %s: %v", c.name, err)
			}
			want := `"` + c.id.String() + `"`
			if string(data) != want {
				t.Errorf("%s: got %s, want %s", c.name, data, want)
			}
		})
	}
}

func TestUUID_RoundTrip_String(t *testing.T) {
	original := NewOrderID()
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got OrderID
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got != original {
		t.Errorf("round-trip mismatch: got %s, want %s", got, original)
	}
}

// UnmarshalJSON also accepts the raw 16-byte array form so any
// pre-existing payload (or one serialized by code that bypassed our
// MarshalJSON) still decodes.
func TestUUID_UnmarshalJSON_AcceptsArrayForm(t *testing.T) {
	original := NewOrderID()
	raw, err := json.Marshal(uuid.UUID(original))
	if err != nil {
		t.Fatalf("marshal uuid.UUID: %v", err)
	}
	var got OrderID
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal array form: %v", err)
	}
	if got != original {
		t.Errorf("array round-trip mismatch: got %s, want %s", got, original)
	}
}
