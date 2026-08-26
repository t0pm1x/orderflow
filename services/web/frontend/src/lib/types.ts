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
