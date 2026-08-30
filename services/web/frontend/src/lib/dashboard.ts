// Dashboard derivation helpers. Pure functions — no DOM, no
// Svelte runes, no fetch. The dashboard page imports these and
// feeds them reactive state.

import type { HealthSnapshot, Order, ServiceStatus } from './types';

export interface KpiSummary {
  /** Number of orders created today (browser-local time). */
  ordersToday: number;
  /** Confirmed / (confirmed + cancelled + failed) * 100. Null when
   *  no terminal orders exist in the window. */
  successRatePct: number | null;
  /** Orders currently in pending or reserved. */
  inFlight: number;
  /** Mean (completed_at − created_at) over completed orders in the
   *  window. Null when no completed orders exist. */
  avgCompletionMs: number | null;
}

const TERMINAL_STATES = new Set(['confirmed', 'cancelled', 'failed']);

function startOfToday(): Date {
  const d = new Date();
  d.setHours(0, 0, 0, 0);
  return d;
}

export function kpiFromOrders(orders: Order[]): KpiSummary {
  const startToday = startOfToday();
  let ordersToday = 0;
  let inFlight = 0;
  let confirmed = 0;
  let cancelled = 0;
  let failed = 0;
  let completionSum = 0;
  let completionCount = 0;
  for (const o of orders) {
    if (new Date(o.created_at) >= startToday) {
      ordersToday++;
      if (o.state === 'pending' || o.state === 'reserved') inFlight++;
      if (TERMINAL_STATES.has(o.state)) {
        if (o.state === 'confirmed') confirmed++;
        else if (o.state === 'cancelled') cancelled++;
        else failed++;
        if (o.completed_at) {
          const ms = new Date(o.completed_at).getTime() - new Date(o.created_at).getTime();
          if (Number.isFinite(ms) && ms >= 0) {
            completionSum += ms;
            completionCount++;
          }
        }
      }
    }
  }
  const terminals = confirmed + cancelled + failed;
  const successRatePct = terminals === 0 ? null : (confirmed / terminals) * 100;
  const avgCompletionMs = completionCount === 0 ? null : completionSum / completionCount;
  return { ordersToday, successRatePct, inFlight, avgCompletionMs };
}

export function hasDown(snap: HealthSnapshot | null): boolean {
  if (!snap) return false;
  return (
    snap.order.status === 'down' ||
    snap.payment.status === 'down' ||
    snap.inventory.status === 'down' ||
    snap.saga.status === 'down' ||
    snap.kafka.status === 'down'
  );
}

export function downServiceNames(snap: HealthSnapshot): string[] {
  const names: string[] = [];
  if (snap.order.status === 'down') names.push('Order');
  if (snap.payment.status === 'down') names.push('Payment');
  if (snap.inventory.status === 'down') names.push('Inventory');
  if (snap.saga.status === 'down') names.push('Saga');
  if (snap.kafka.status === 'down') names.push('Kafka tail');
  return names;
}

export function statusClass(status: ServiceStatus | 'ok' | 'down'): string {
  return `chip-${status}`;
}

/** Formats an ISO timestamp for display: "14:32:07" or "14:32:07.123". */
export function fmtClock(iso: string): string {
  return iso.slice(11, 19);
}