<script lang="ts">
  import { page } from '$app/stores';
  import { goto } from '$app/navigation';
  import { onDestroy, onMount } from 'svelte';
  import { ApiError, cancelOrder, getOrder } from '$lib/api';
  import { liveEvents } from '$lib/sse';
  import type { Order, SseEvent } from '$lib/types';

  let order = $state<Order | null>(null);
  let events = $state<SseEvent[]>([]);
  let error: { error: string; message: string } | null = $state(null);
  let loading = $state(true);
  let cancelling = $state(false);
  let pollTimer: ReturnType<typeof setInterval> | null = null;

  let id = $derived(($page.params.id ?? '').trim());
  let backHref = $derived(buildBackHref());

  function buildBackHref(): string {
    const sp = new URLSearchParams();
    if ($page.url.searchParams.get('state')) {
      sp.set('state', $page.url.searchParams.get('state')!);
    }
    for (const sku of $page.url.searchParams.getAll('sku')) {
      sp.append('sku', sku);
    }
    const qs = sp.toString();
    return '/orders' + (qs ? '?' + qs : '');
  }

  function fmtTime(iso: string): string {
    return iso.replace('T', ' ').slice(0, 19);
  }

  function fmtSkuLine(items: Order['items']): string {
    return items.map((it) => `${it.sku}×${it.quantity}`).join(', ');
  }

  async function load(showSpinner = true): Promise<void> {
    if (showSpinner) loading = true;
    try {
      order = await getOrder(id);
      error = null;
    } catch (e) {
      if (e instanceof ApiError) {
        error = { error: e.code, message: e.message };
      } else {
        error = { error: 'NETWORK', message: String(e) };
      }
      order = null;
    } finally {
      loading = false;
    }
  }

  $effect(() => {
    // refresh timeline from SSE store filtered to this order
    void id;
    events = $liveEvents.filter((ev) => ev.aggregate_id === id);
  });

  async function onCancel(): Promise<void> {
    if (!confirm('Cancel this order?')) return;
    cancelling = true;
    try {
      await cancelOrder(id);
      await load(false);
    } catch (e) {
      if (e instanceof ApiError) {
        error = { error: e.code, message: e.message };
      } else {
        error = { error: 'NETWORK', message: String(e) };
      }
    } finally {
      cancelling = false;
    }
  }

  onMount(() => {
    load();
    // Poll every 1s while non-terminal; stop polling when terminal
    pollTimer = setInterval(() => {
      if (order && (order.state === 'confirmed' || order.state === 'cancelled' || order.state === 'failed')) return;
      load(false);
    }, 1_000);
  });

  onDestroy(() => {
    if (pollTimer) clearInterval(pollTimer);
  });

  let isTerminal = $derived(
    order?.state === 'confirmed' ||
    order?.state === 'cancelled' ||
    order?.state === 'failed'
  );
</script>

<svelte:head>
  <title>Order {id.slice(0, 8)}… — OrderFlow</title>
</svelte:head>

<section>
  <div class="row-between">
    <h1 class="mono">Order {id.slice(0, 8)}…</h1>
    <div class="row">
      <a href={backHref}>← back to list</a>
    </div>
  </div>

  {#if error && !order}
    <div class="error">{error.message}</div>
  {/if}

  {#if error && order}
    <div class="error">{error.message} (retrying)</div>
  {/if}

  {#if order}
    <p>
      <span class="badge state-{order.state}">{order.state}</span>
      <span class="muted" title="created {fmtTime(order.created_at)}">created {fmtTime(order.created_at)}</span>
      {#if order.completed_at}
        <span class="muted">· completed {fmtTime(order.completed_at)}</span>
      {/if}
    </p>

    <h3>Items</h3>
    <table>
      <thead><tr><th>SKU</th><th>Qty</th><th>Unit price</th></tr></thead>
      <tbody>
        {#each order.items as it}
          <tr>
            <td class="mono">{it.sku}</td>
            <td>{it.quantity}</td>
            <td>{it.unit_price_cents ?? 'auto'}</td>
          </tr>
        {/each}
      </tbody>
    </table>

    {#if !isTerminal}
      <button class="danger" onclick={onCancel} disabled={cancelling} aria-busy={cancelling}>
        {cancelling ? 'Cancelling…' : 'Cancel order'}
      </button>
    {/if}

    <h3>Saga timeline</h3>
    {#if events.length === 0}
      <p class="muted">No events received yet for this order. The timeline will populate as the saga runs.</p>
    {:else}
      <ol class="timeline">
        {#each events as ev (ev.event_id)}
          <li class="timeline-node timeline-{ev.event_type}">
            <span class="timeline-time mono" title={ev.occurred_at}>{fmtTime(ev.occurred_at)}</span>
            <span class="timeline-type mono">{ev.event_type}</span>
          </li>
        {/each}
      </ol>
    {/if}
  {/if}

  {#if loading && !order}
    <p class="muted">Loading…</p>
  {/if}
</section>

<style>
  .row-between { display: flex; align-items: center; justify-content: space-between; margin-bottom: var(--gap-4); }
  .row-between h1 { margin: 0; font-size: var(--fs-xl); font-family: var(--font-mono); }
  .row { display: flex; gap: var(--gap-3); align-items: center; }

  .badge { padding: 2px var(--gap-2); border-radius: var(--radius-pill); font-size: var(--fs-xs); font-weight: 600; }
  .badge.state-pending { background: rgba(210,153,34,0.15); color: var(--warn); }
  .badge.state-reserved { background: rgba(68,147,248,0.15); color: var(--accent); }
  .badge.state-confirmed { background: rgba(86,211,100,0.15); color: var(--good); }
  .badge.state-cancelled,
  .badge.state-failed { background: rgba(248,81,73,0.15); color: var(--bad); }

  button {
    margin-top: var(--gap-3);
    padding: var(--gap-2) var(--gap-3);
    background: var(--bad); color: white;
    border: 0; border-radius: var(--radius); font-weight: 600;
  }
  button:disabled { opacity: 0.5; cursor: not-allowed; }

  table { width: 100%; border-collapse: collapse; margin: var(--gap-3) 0; }
  th, td { padding: var(--gap-2) var(--gap-3); text-align: left; border-bottom: 1px solid var(--border); }
  th { color: var(--fg-muted); font-weight: 600; font-size: var(--fs-sm); }

  .timeline {
    list-style: none; padding: 0 0 0 var(--gap-3);
    margin: var(--gap-3) 0;
    border-left: 2px solid var(--border);
  }
  .timeline-node { padding: var(--gap-1) 0 var(--gap-1) var(--gap-3); position: relative; }
  .timeline-node::before {
    content: ''; position: absolute; left: calc(var(--gap-3) * -1 - 1px);
    top: 12px; width: 10px; height: 10px; border-radius: 50%;
    background: var(--fg-muted);
  }
  .timeline-time { color: var(--fg-muted); margin-right: var(--gap-2); font-size: var(--fs-xs); }
  .timeline-type { font-weight: 600; }
</style>
