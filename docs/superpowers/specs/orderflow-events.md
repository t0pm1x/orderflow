# orderflow Domain Events

All events flow through Kafka topics. Every event uses the standard
`EventEnvelope` so consumers can route, trace, and dedupe uniformly
regardless of which topic the event arrived on.

Topics:

| Topic              | Producer(s)         | Consumer(s)             |
|--------------------|---------------------|-------------------------|
| `order-events`     | order service, saga | order, inventory, pay   |
| `payment-events`   | payment service     | order, saga             |
| `inventory-events` | inventory service   | order, saga             |

Partitioning: `aggregate_id` is the Kafka message key, so all events
for a single order land on the same partition and are processed in order.

---

## Envelope

Standard envelope across **all** topics.

```go
type EventEnvelope struct {
    EventID       string          `json:"event_id"`
    EventType     string          `json:"event_type"`
    AggregateID   string          `json:"aggregate_id"`
    AggregateType string          `json:"aggregate_type"`
    OccurredAt    time.Time       `json:"occurred_at"`
    TraceID       string          `json:"trace_id"`   // W3C trace context (32 hex chars)
    SpanID        string          `json:"span_id"`    // W3C trace context (16 hex chars)
    SchemaVersion int             `json:"schema_version"` // bumped on payload shape change
    Payload       json.RawMessage `json:"payload"`
}
```

Wire format: JSON for human-readable logs (per spec). Proto stub lives
at `api/proto/events.proto` for v2.

Idempotency: consumers dedupe on `(aggregate_id, event_type, schema_version)`
via Redis SETNX with a 7-day TTL.

### Shared item type

Referenced by several payloads below; declared here once so every event
file is self-contained.

```go
type OrderItem struct {
    SKU            string `json:"sku"`
    Quantity       int    `json:"quantity"`
    UnitPriceCents int64  `json:"unit_price_cents"`
}
```

---

## Topic: `order-events`

### `OrderCreated`

Emitted by **Order Service** when `POST /v1/orders` succeeds.

```go
type OrderCreatedPayload struct {
    CustomerID  string      `json:"customer_id"`
    Items       []OrderItem `json:"items"`
    TotalCents  int64       `json:"total_cents"`
}
```

```json
{
  "event_id": "9f0c2a14-1b7d-4d8a-9b3e-1c5f8e7a0b1c",
  "event_type": "OrderCreated",
  "aggregate_id": "0fa1b8e2-7c14-4d39-9b1e-3f8c0a7b2d5e",
  "aggregate_type": "Order",
  "occurred_at": "2026-08-17T10:00:00Z",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "00f067aa0ba902b7",
  "schema_version": 1,
  "payload": {
    "customer_id": "8d2f1a40-cf51-4a8b-8e72-1a4d2c8e6b3f",
    "items": [
      {"sku": "SKU-001", "quantity": 2, "unit_price_cents": 1999}
    ],
    "total_cents": 3998
  }
}
```

Consumed by: **saga** → starts reserve + payment flow.

---

### `OrderReserved`

Emitted by **Order Service** after it consumes `StockReserved`.

```go
type OrderReservedPayload struct {
    ReservationID string      `json:"reservation_id"`
    Items         []OrderItem `json:"items"`
}
```

```json
{
  "event_id": "...",
  "event_type": "OrderReserved",
  "aggregate_id": "0fa1b8e2-7c14-4d39-9b1e-3f8c0a7b2d5e",
  "aggregate_type": "Order",
  "occurred_at": "2026-08-17T10:00:01Z",
  "trace_id": "...",
  "span_id": "...",
  "schema_version": 1,
  "payload": {
    "reservation_id": "5a2c8e1f-9b3d-4e7a-8c1f-2d5a7b9e0c4f",
    "items": [
      {"sku": "SKU-001", "quantity": 2, "unit_price_cents": 1999}
    ]
  }
}
```

Consumed by: **saga** → emits `PaymentRequested`.

---

### `OrderConfirmed`

Emitted by **Order Service** after it consumes `PaymentCompleted`.

```go
type OrderConfirmedPayload struct {
    ConfirmedAt time.Time `json:"confirmed_at"`
}
```

```json
{
  "event_id": "...",
  "event_type": "OrderConfirmed",
  "aggregate_id": "0fa1b8e2-7c14-4d39-9b1e-3f8c0a7b2d5e",
  "aggregate_type": "Order",
  "occurred_at": "2026-08-17T10:00:03Z",
  "trace_id": "...",
  "span_id": "...",
  "schema_version": 1,
  "payload": {
    "confirmed_at": "2026-08-17T10:00:03Z"
  }
}
```

Consumed by: **notification** (out of scope for v1).

---

### `OrderCancelled`

Emitted by **Order Service** for both user-initiated cancellation
(via `DELETE /v1/orders/{id}`) and saga compensation.

```go
type OrderCancelledPayload struct {
    Reason   string `json:"reason"`   // "user_request" | "stock_failed" | "payment_failed" | "timeout"
    Source   string `json:"source"`   // "user" | "saga"
}
```

```json
{
  "event_id": "...",
  "event_type": "OrderCancelled",
  "aggregate_id": "0fa1b8e2-7c14-4d39-9b1e-3f8c0a7b2d5e",
  "aggregate_type": "Order",
  "occurred_at": "2026-08-17T10:00:02Z",
  "trace_id": "...",
  "span_id": "...",
  "schema_version": 1,
  "payload": {
    "reason": "stock_failed",
    "source": "saga"
  }
}
```

Consumed by: **inventory** → release reservation.

---

### `OrderFailed`

Emitted by **Order Service** when the saga times out or hits an
unrecoverable error. Terminal — no further state transitions.

```go
type OrderFailedPayload struct {
    Reason     string `json:"reason"`
    FailedStep string `json:"failed_step"` // "reserve" | "payment" | "confirm"
}
```

```json
{
  "event_id": "...",
  "event_type": "OrderFailed",
  "aggregate_id": "0fa1b8e2-7c14-4d39-9b1e-3f8c0a7b2d5e",
  "aggregate_type": "Order",
  "occurred_at": "2026-08-17T10:05:00Z",
  "trace_id": "...",
  "span_id": "...",
  "schema_version": 1,
  "payload": {
    "reason": "saga_timeout",
    "failed_step": "payment"
  }
}
```

Consumed by: **alerting** (out of scope for v1).

---

## Topic: `payment-events`

### `PaymentRequested`

Emitted by **Order Service / saga** after `OrderReserved`.

```go
type PaymentRequestedPayload struct {
    OrderID        string `json:"order_id"`
    AmountCents    int64  `json:"amount_cents"`
    IdempotencyKey string `json:"idempotency_key"` // UUID, replay-safe
}
```

```json
{
  "event_id": "...",
  "event_type": "PaymentRequested",
  "aggregate_id": "0fa1b8e2-7c14-4d39-9b1e-3f8c0a7b2d5e",
  "aggregate_type": "Order",
  "occurred_at": "2026-08-17T10:00:02Z",
  "trace_id": "...",
  "span_id": "...",
  "schema_version": 1,
  "payload": {
    "order_id": "0fa1b8e2-7c14-4d39-9b1e-3f8c0a7b2d5e",
    "amount_cents": 3998,
    "idempotency_key": "7c1a4b9e-2f5d-4e8a-9c1b-6a3f8e2d4b7c"
  }
}
```

Consumed by: **payment service** → mock charge.

---

### `PaymentCompleted`

Emitted by **Payment Service** after a successful mock charge.

```go
type PaymentCompletedPayload struct {
    PaymentID string `json:"payment_id"`
    OrderID   string `json:"order_id"`
}
```

```json
{
  "event_id": "...",
  "event_type": "PaymentCompleted",
  "aggregate_id": "0fa1b8e2-7c14-4d39-9b1e-3f8c0a7b2d5e",
  "aggregate_type": "Order",
  "occurred_at": "2026-08-17T10:00:03Z",
  "trace_id": "...",
  "span_id": "...",
  "schema_version": 1,
  "payload": {
    "payment_id": "2e4f8a1c-5b7d-4e9a-8c2f-1a6b3e7d9c4f",
    "order_id": "0fa1b8e2-7c14-4d39-9b1e-3f8c0a7b2d5e"
  }
}
```

Consumed by: **order service** → state → `confirmed`, emit `OrderConfirmed`.

---

### `PaymentFailed`

Emitted by **Payment Service** when mock charge fails.

```go
type PaymentFailedPayload struct {
    PaymentID string `json:"payment_id"`
    OrderID   string `json:"order_id"`
    ErrorCode string `json:"error_code"` // "card_declined" | "insufficient_funds" | "network_error"
}
```

```json
{
  "event_id": "...",
  "event_type": "PaymentFailed",
  "aggregate_id": "0fa1b8e2-7c14-4d39-9b1e-3f8c0a7b2d5e",
  "aggregate_type": "Order",
  "occurred_at": "2026-08-17T10:00:03Z",
  "trace_id": "...",
  "span_id": "...",
  "schema_version": 1,
  "payload": {
    "payment_id": "2e4f8a1c-5b7d-4e9a-8c2f-1a6b3e7d9c4f",
    "order_id": "0fa1b8e2-7c14-4d39-9b1e-3f8c0a7b2d5e",
    "error_code": "card_declined"
  }
}
```

Consumed by: **order service** → release stock + cancel.

---

## Topic: `inventory-events`

### `StockReserved`

Emitted by **Inventory Service** after Redis reservation succeeds.

```go
type StockReservedPayload struct {
    ReservationID string    `json:"reservation_id"`
    OrderID       string    `json:"order_id"`
    SKU           string    `json:"sku"`
    Quantity      int       `json:"quantity"`
    ExpiresAt     time.Time `json:"expires_at"`
}
```

```json
{
  "event_id": "...",
  "event_type": "StockReserved",
  "aggregate_id": "0fa1b8e2-7c14-4d39-9b1e-3f8c0a7b2d5e",
  "aggregate_type": "Order",
  "occurred_at": "2026-08-17T10:00:01Z",
  "trace_id": "...",
  "span_id": "...",
  "schema_version": 1,
  "payload": {
    "reservation_id": "5a2c8e1f-9b3d-4e7a-8c1f-2d5a7b9e0c4f",
    "order_id": "0fa1b8e2-7c14-4d39-9b1e-3f8c0a7b2d5e",
    "sku": "SKU-001",
    "quantity": 2,
    "expires_at": "2026-08-17T10:05:00Z"
  }
}
```

Consumed by: **order service** → state → `reserved`.

---

### `StockReleased`

Emitted by **Inventory Service** when a reservation is released
(order cancelled, payment failed, or TTL expiry).

```go
type StockReleasedPayload struct {
    ReservationID string `json:"reservation_id"`
    Reason        string `json:"reason"` // "order_cancelled" | "payment_failed" | "ttl_expired"
}
```

```json
{
  "event_id": "...",
  "event_type": "StockReleased",
  "aggregate_id": "5a2c8e1f-9b3d-4e7a-8c1f-2d5a7b9e0c4f",
  "aggregate_type": "Reservation",
  "occurred_at": "2026-08-17T10:00:03Z",
  "trace_id": "...",
  "span_id": "...",
  "schema_version": 1,
  "payload": {
    "reservation_id": "5a2c8e1f-9b3d-4e7a-8c1f-2d5a7b9e0c4f",
    "reason": "payment_failed"
  }
}
```

Consumed by: **saga** (audit log), **analytics** (out of scope for v1).

---

### `StockReservationFailed`

Emitted by **Inventory Service** when reservation cannot be fulfilled.

```go
type StockReservationFailedPayload struct {
    OrderID string `json:"order_id"`
    SKU     string `json:"sku"`
    Reason  string `json:"reason"` // "insufficient_stock" | "sku_unknown" | "redis_unavailable"
}
```

```json
{
  "event_id": "...",
  "event_type": "StockReservationFailed",
  "aggregate_id": "0fa1b8e2-7c14-4d39-9b1e-3f8c0a7b2d5e",
  "aggregate_type": "Order",
  "occurred_at": "2026-08-17T10:00:01Z",
  "trace_id": "...",
  "span_id": "...",
  "schema_version": 1,
  "payload": {
    "order_id": "0fa1b8e2-7c14-4d39-9b1e-3f8c0a7b2d5e",
    "sku": "SKU-001",
    "reason": "insufficient_stock"
  }
}
```

Consumed by: **order service** → cancel order (no payment initiated).

---

## Consumers

| Event                    | Consumed by     | Action                                   |
|--------------------------|-----------------|------------------------------------------|
| `OrderCreated`           | saga            | Start reserve + payment flow             |
| `StockReserved`          | order           | Update state → `reserved`                |
| `StockReservationFailed` | order           | Cancel order, no payment initiated       |
| `PaymentRequested`       | payment         | Mock charge card                         |
| `PaymentCompleted`       | order           | Update state → `confirmed`               |
| `PaymentFailed`          | order, inventory| Order: cancel + release. Inv: release.   |
| `OrderCancelled`         | inventory       | Release reservation                      |
| `OrderConfirmed`         | notification    | (out of scope for v1)                    |
| `OrderFailed`            | alerting        | (out of scope for v1)                    |
| `StockReleased`          | saga, analytics | (audit only)                             |
| `OrderReserved`          | saga            | Emit `PaymentRequested`                  |

Every consumer:

1. Checks idempotency via Redis `(aggregate_id, event_type, schema_version)`.
2. Starts a child span linked to the event's `trace_id`.
3. Writes business state + outbox row in the same DB transaction.
4. Commits the Kafka offset **only after** DB commit succeeds.

---

## Event count

11 domain events total:

- 5 on `order-events` (Created, Reserved, Confirmed, Cancelled, Failed)
- 3 on `payment-events` (Requested, Completed, Failed)
- 3 on `inventory-events` (Reserved, Released, ReservationFailed)