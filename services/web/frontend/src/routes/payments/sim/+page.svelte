<script lang="ts">
  import { onMount } from 'svelte';
  import { ApiError, fireWebhook, listOrders } from '$lib/api';
  import type { Order, PaymentWebhook } from '$lib/types';

  // Payment webhook simulator. Lists in-flight orders (state ∈
  // {pending, reserved}) so the operator can fire force-success /
  // force-fail webhooks against any of them. error_code is
  // operator-selectable (4 paths: card_declined, insufficient_funds,
  // network_error, provider_timeout) so the test surface covers the
  // full provider error taxonomy — see services/payment/internal/provider/provider.go
  // for the matching decline reasons.

  let pending: Order[] = $state([]);
  let reserved: Order[] = $state([]);
  let error: { error: string; message: string } | null = $state(null);
  let loading = $state(true);
  let idempotencyKeys = $state<Record<string, string>>({});
  let errorCode = $state<Record<string, string>>({});

  function newIdempotencyKey(): string {
    return crypto.randomUUID();
  }

  function newOrderKeys(): Record<string, string> {
    return { ok: newIdempotencyKey(), fail: newIdempotencyKey() };
  }

  async function load(showSpinner = true): Promise<void> {
    if (showSpinner) loading = true;
    try {
      const [p, r] = await Promise.all([
        listOrders({ state: 'pending' }),
        listOrders({ state: 'reserved' })
      ]);
      pending = p;
      reserved = r;
      error = null;
      // generate fresh keys for any order we haven't seen yet
      const next: Record<string, string> = {};
      const nextCodes: Record<string, string> = {};
      for (const o of [...p, ...r]) {
        next[o.id] = idempotencyKeys[o.id] ?? newOrderKeys().ok;
        nextCodes[o.id] = errorCode[o.id] ?? 'card_declined';
      }
      idempotencyKeys = next;
      errorCode = nextCodes;
    } catch (e) {
      if (e instanceof ApiError) {
        error = { error: e.code, message: e.message };
      } else {
        error = { error: 'NETWORK', message: String(e) };
      }
    } finally {
      loading = false;
    }
  }

  async function onFire(order: Order, status: 'succeeded' | 'failed', errorCode: string): Promise<void> {
    try {
      const wh: PaymentWebhook = {
        payment_id: order.id, // deterministic on order_id (mock dedupes)
        status,
        error_code: errorCode,
        last_four: order.last_four
      };
      await fireWebhook(wh);
      await load(false);
    } catch (e) {
      if (e instanceof ApiError) {
        error = { error: e.code, message: e.message };
      } else {
        error = { error: 'NETWORK', message: String(e) };
      }
    }
  }

  onMount(() => {
    load();
  });
</script>

<svelte:head>
  <title>Payments sim — OrderFlow</title>
</svelte:head>

<section>
  <h1>Payment webhook simulator</h1>
  <p class="muted">
    Fire a webhook into the Payment Service for any in-flight order below.
    payment_id is derived deterministically from order_id so replays are idempotent.
  </p>

  {#if error}
    <div class="error">{error.message}</div>
  {/if}

  {#if loading}
    <p class="muted">Loading…</p>
  {:else if pending.length + reserved.length === 0}
    <p class="muted">
      No in-flight orders. Create one on <a href="/orders">Orders</a> first.
    </p>
  {:else}
    <table>
      <thead>
        <tr>
          <th>Order</th>
          <th>State</th>
          <th>Fire succeeded</th>
          <th>Fire failed</th>
        </tr>
      </thead>
      <tbody>
        {#each [...pending, ...reserved] as o (o.id)}
          <tr>
            <td class="mono">{o.id.slice(0, 8)}…</td>
            <td><span class="badge state-{o.state}">{o.state}</span></td>
            <td>
              <button
                class="ok"
                onclick={() => onFire(o, 'succeeded', '')}
                aria-label={`Fire succeeded webhook for order ${o.id}`}
              >
                Force succeed ✓
              </button>
            </td>
            <td>
              <div class="row">
                <select
                  value={errorCode[o.id] ?? 'card_declined'}
                  onchange={(e) => { errorCode[o.id] = (e.currentTarget as HTMLSelectElement).value; }}
                  aria-label="Choose failure reason"
                >
                  <option value="card_declined">card_declined</option>
                  <option value="insufficient_funds">insufficient_funds</option>
                  <option value="network_error">network_error</option>
                  <option value="provider_timeout">provider_timeout</option>
                </select>
                <button
                  class="fail"
                  onclick={() => onFire(o, 'failed', errorCode[o.id])}
                  aria-label={`Fire failed webhook for order ${o.id}`}
                >
                  Force fail ✗
                </button>
              </div>
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  {/if}
</section>

<style>
  h1 { margin: 0 0 var(--gap-3); font-size: var(--fs-xl); }
  table { width: 100%; border-collapse: collapse; margin-top: var(--gap-3); display: block; overflow-x: auto; }
  th, td { padding: var(--gap-2) var(--gap-3); text-align: left; border-bottom: 1px solid var(--border); vertical-align: middle; }
  th { color: var(--fg-muted); font-weight: 600; font-size: var(--fs-sm); }
  .row { display: flex; gap: var(--gap-2); align-items: center; }
  select {
    padding: var(--gap-1) var(--gap-2);
    background: var(--bg); color: var(--fg);
    border: 1px solid var(--border); border-radius: var(--radius);
    font: inherit;
  }
  button {
    padding: var(--gap-2) var(--gap-3);
    border: 0; border-radius: var(--radius); font-weight: 600; color: white;
  }
  button.ok { background: var(--good); }
  button.fail { background: var(--bad); }
  .badge { padding: 2px var(--gap-2); border-radius: var(--radius-pill); font-size: var(--fs-xs); font-weight: 600; }
  .badge.state-pending { background: rgba(210,153,34,0.15); color: var(--warn); }
  .badge.state-reserved { background: rgba(68,147,248,0.15); color: var(--accent); }
</style>
