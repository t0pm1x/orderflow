package types

import (
	"encoding/json"

	"github.com/google/uuid"
)

// IDs are typed wrappers around UUIDs for type safety across services.
// Each method (String, MarshalJSON, UnmarshalJSON) is declared on every
// named type because Go does NOT promote methods between named types
// with the same underlying type — only struct embedding promotes.
type (
	// OrderID is a UUID that uniquely identifies an Order aggregate.
	OrderID uuid.UUID
	// PaymentID is a UUID that uniquely identifies a Payment aggregate.
	PaymentID uuid.UUID
	// StockID is a UUID that uniquely identifies a StockItem aggregate.
	StockID uuid.UUID
	// CustomerID is a UUID that uniquely identifies a Customer aggregate.
	CustomerID uuid.UUID
)

// NewOrderID returns a freshly generated OrderID.
func NewOrderID() OrderID { return OrderID(uuid.New()) }

// NewPaymentID returns a freshly generated PaymentID.
func NewPaymentID() PaymentID { return PaymentID(uuid.New()) }

// NewStockID returns a freshly generated StockID.
func NewStockID() StockID { return StockID(uuid.New()) }

// NewCustomerID returns a freshly generated CustomerID.
func NewCustomerID() CustomerID { return CustomerID(uuid.New()) }

// String returns the canonical UUID string form.
func (id OrderID) String() string { return uuid.UUID(id).String() }

// String returns the canonical UUID string form.
func (id PaymentID) String() string { return uuid.UUID(id).String() }

// String returns the canonical UUID string form.
func (id StockID) String() string { return uuid.UUID(id).String() }

// String returns the canonical UUID string form.
func (id CustomerID) String() string { return uuid.UUID(id).String() }

// MarshalJSON encodes the UUID as a quoted JSON string (RFC 4122
// textual form), which is what every HTTP/JSON consumer expects. The
// default behavior for [16]byte is to encode as a JSON array of 16
// numbers, which breaks every external client.
func (id OrderID) MarshalJSON() ([]byte, error) {
	return json.Marshal(id.String())
}

// MarshalJSON encodes the UUID as a quoted JSON string.
func (id PaymentID) MarshalJSON() ([]byte, error) {
	return json.Marshal(id.String())
}

// MarshalJSON encodes the UUID as a quoted JSON string.
func (id StockID) MarshalJSON() ([]byte, error) {
	return json.Marshal(id.String())
}

// MarshalJSON encodes the UUID as a quoted JSON string.
func (id CustomerID) MarshalJSON() ([]byte, error) {
	return json.Marshal(id.String())
}

// UnmarshalJSON accepts both the canonical string form and the raw
// 16-byte array form. The string form is what every producer in this
// codebase emits; the array form is supported as a fallback so any
// pre-existing payload (or one serialized by code that bypassed our
// MarshalJSON) still decodes.
func (id *OrderID) UnmarshalJSON(data []byte) error {
	return unmarshalUUID(data, func(u uuid.UUID) { *id = OrderID(u) })
}

// UnmarshalJSON accepts both the canonical string and 16-byte array forms.
func (id *PaymentID) UnmarshalJSON(data []byte) error {
	return unmarshalUUID(data, func(u uuid.UUID) { *id = PaymentID(u) })
}

// UnmarshalJSON accepts both the canonical string and 16-byte array forms.
func (id *StockID) UnmarshalJSON(data []byte) error {
	return unmarshalUUID(data, func(u uuid.UUID) { *id = StockID(u) })
}

// UnmarshalJSON accepts both the canonical string and 16-byte array forms.
func (id *CustomerID) UnmarshalJSON(data []byte) error {
	return unmarshalUUID(data, func(u uuid.UUID) { *id = CustomerID(u) })
}

// unmarshalUUID is the shared body for every typed UnmarshalJSON. It
// tries the canonical string form first, then falls back to the raw
// 16-byte array form.
func unmarshalUUID(data []byte, set func(uuid.UUID)) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		parsed, err := uuid.Parse(s)
		if err != nil {
			return err
		}
		set(parsed)
		return nil
	}
	var raw [16]byte
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	set(raw)
	return nil
}
