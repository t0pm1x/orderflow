package types

import "github.com/google/uuid"

// IDs are typed wrappers around UUIDs for type safety across services.
type (
	OrderID    uuid.UUID
	PaymentID  uuid.UUID
	StockID    uuid.UUID
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