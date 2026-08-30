// TypeScript types shared by the SPA + (implicitly) the Go BFF.
// These mirror the JSON wire format produced by the Go API
// gateway in services/web/internal/server/api.go. Keeping them
// in sync is a manual contract test (see the `// wire-format`
// comment in api.go for the upstream field names).

export type OrderState =
  | 'pending'
  | 'reserved'
  | 'confirmed'
  | 'cancelled'
  | 'failed';

export interface OrderItem {
  sku: string;
  quantity: number;
  /** Cents. Undefined ⇒ server derived the unit price from a SKU lookup. */
  unit_price_cents?: number;
}

export interface Order {
  id: string;
  customer_id?: string;
  items: OrderItem[];
  state: OrderState;
  total_cents?: number;
  /** Last 4 of the card submitted with the order. Empty when not supplied. */
  last_four?: string;
  /** ISO-8601. */
  created_at: string;
  updated_at: string;
  /** ISO-8601, set when state ∈ {confirmed, cancelled, failed}. */
  completed_at?: string;
}

export interface OrderListResponse {
  items: Order[];
  next_cursor: string;
}

export interface StockItem {
  sku: string;
  available: number;
  reserved: number;
  /** Optimistic-locking version, increments on each update. */
  version: number;
  updated_at: string;
}

export interface PaymentWebhook {
  payment_id: string;
  /** Equal to payment_id in the playground (mock provider is
   *  deterministic on order_id). F-009: sent explicitly so the
   *  payment service's payments.order_id UUID column never receives
   *  an empty string when the SPA's force-webhook button fires. */
  order_id?: string;
  status: 'succeeded' | 'failed';
  error_code?: string;
  last_four?: string;
}

export interface SubmitOrderRequest {
  /** Optional UUID; server generates one when blank. */
  customer_id?: string;
  /** Required. SPA generates a UUID per form render so duplicate
   *  double-submits are caught at the BFF layer. */
  idempotency_key: string;
  items: OrderItem[];
  /** Optional payment hint, e.g. prefill CTA. */
  payment?: { last_four?: string };
}

export interface ApiError {
  error: string;
  message: string;
}

/** SSE event payload, mirrors pkg/platform/events.Envelope. */
export interface SseEvent {
  event_id: string;
  event_type: string;
  aggregate_id: string;
  aggregate_type: string;
  schema_version: string;
  /** ISO-8601. */
  occurred_at: string;
  trace_id?: string;
  span_id?: string;
  /** Event-specific payload; we treat as opaque JSON. */
  payload: unknown;
}

// Health snapshot from GET /api/health/all. Wire format matches
// services/web/internal/server/probe.go ServiceHealth +
// HealthSnapshot. Keep these two definitions in sync.

export type ServiceStatus = 'ok' | 'degraded' | 'down';

export interface ServiceHealth {
  status: ServiceStatus;
  latency_ms: number;
  taken_at: string;
  detail?: string;
}

export interface HealthSnapshot {
  order: ServiceHealth;
  payment: ServiceHealth;
  inventory: ServiceHealth;
  saga: ServiceHealth;
  /** Kafka tail has only ok|down (no degraded middle ground). */
  kafka: { status: 'ok' | 'down'; latency_ms: number; taken_at: string; detail?: string };
  snapshot_at: string;
}
