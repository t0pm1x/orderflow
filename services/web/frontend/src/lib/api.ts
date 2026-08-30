// HTTP client for the Go BFF. All requests hit same-origin /api/*
// (the BFF proxies to the backend services). Same-origin keeps
// the backend URLs server-side secrets and avoids CORS config on
// each service.
//
// On any non-2xx we throw an ApiError with a stable `code` field
// (NOT_FOUND, BAD_REQUEST, UPSTREAM_UNAVAILABLE, etc.) so the SPA
// can branch on the code and show a friendly banner.

import type {
  OrderListResponse,
  Order,
  OrderState,
  PaymentWebhook,
  StockItem,
  SubmitOrderRequest,
  HealthSnapshot,
} from './types';

export class ApiError extends Error {
  readonly code: string;
  readonly status: number;
  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = 'ApiError';
    this.code = code;
    this.status = status;
  }
}

async function jsonOrThrow<T>(res: Response): Promise<T> {
  if (!res.ok) {
    let code = 'UNKNOWN';
    let message = `HTTP ${res.status}`;
    try {
      const body = (await res.json()) as { error?: string; message?: string };
      code = body.error ?? code;
      message = body.message ?? message;
    } catch {
      // response body wasn't JSON; fall back to status text
    }
    throw new ApiError(res.status, code, message);
  }
  return (await res.json()) as T;
}

export async function listOrders(opts: {
  state?: OrderState;
  skus?: string[];
} = {}): Promise<Order[]> {
  const params = new URLSearchParams();
  if (opts.state) params.set('state', opts.state);
  for (const sku of opts.skus ?? []) params.append('sku', sku);
  const qs = params.toString();
  const url = `/api/orders${qs ? '?' + qs : ''}`;
  const res = await fetch(url, { headers: { Accept: 'application/json' } });
  const body = (await res.json()) as OrderListResponse;
  // F-007: the BFF once serialized a nil slice as JSON null;
  // TypeScript declared Order[] (non-nullable) and the SPA used
  // [...items] which threw on null. Belt-and-suspenders: BFF
  // also coerces (api.go) but defend here too in case any future
  // endpoint regresses.
  return Array.isArray(body.items) ? body.items : [];
}

export async function getOrder(id: string): Promise<Order> {
  const res = await fetch(`/api/orders/${encodeURIComponent(id)}`, {
    headers: { Accept: 'application/json' }
  });
  return jsonOrThrow<Order>(res);
}

export async function submitOrder(req: SubmitOrderRequest): Promise<Order> {
  const res = await fetch('/api/orders', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(req)
  });
  return jsonOrThrow<Order>(res);
}

export async function cancelOrder(id: string): Promise<void> {
  const res = await fetch(`/api/orders/${encodeURIComponent(id)}`, {
    method: 'DELETE'
  });
  if (res.status === 204) return;
  // 4xx/5xx with body — try to decode the error envelope
  await jsonOrThrow(res);
}

export async function getStock(sku: string): Promise<StockItem> {
  const res = await fetch(
    `/api/inventory/stock/${encodeURIComponent(sku)}`,
    { headers: { Accept: 'application/json' } }
  );
  return jsonOrThrow<StockItem>(res);
}

export async function fireWebhook(wh: PaymentWebhook): Promise<void> {
  const res = await fetch('/api/payments/webhook', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(wh)
  });
  if (res.status === 200) return;
  await jsonOrThrow(res);
}

export async function getHealthAll(): Promise<HealthSnapshot> {
  const res = await fetch('/api/health/all', {
    headers: { Accept: 'application/json' }
  });
  return jsonOrThrow<HealthSnapshot>(res);
}
