<script lang="ts">
  import { page } from '$app/stores';
  import { goto } from '$app/navigation';
  import { onMount } from 'svelte';
  import { ApiError, listOrders } from '$lib/api';
  import type { Order, OrderState } from '$lib/types';

  let orders = $state<Order[]>([]);
  let loading = $state(true);
  let error: { error: string; message: string } | null = $state(null);
  let polling = $state(false);
  let pollTimer: ReturnType<typeof setInterval> | null = null;

  let stateFilter = $derived(($page.url.searchParams.get('state') ?? '') as OrderState | '');
  let skuFilters = $derived(
    ($page.url.searchParams.getAll('sku') ?? []).filter((s) => s.length > 0)
  );

  const STATES: { value: OrderState; label: string }[] = [
    { value: 'pending', label: 'Pending' },
    { value: 'reserved', label: 'Reserved' },
    { value: 'confirmed', label: 'Confirmed' },
    { value: 'cancelled', label: 'Cancelled' },
    { value: 'failed', label: 'Failed' }
  ];

  function chipHref(target: OrderState | ''): string {
    const params = new URLSearchParams();
    if (target) params.set('state', target);
    for (const sku of skuFilters) params.append('sku', sku);
    const qs = params.toString();
    return '/orders' + (qs ? '?' + qs : '');
  }

  function skuChipHref(targetSku: string): string {
    const params = new URLSearchParams();
    if (stateFilter) params.set('state', stateFilter);
    // merge: remove this sku if already present, otherwise add
    const next = skuFilters.includes(targetSku)
      ? skuFilters.filter((s) => s !== targetSku)
      : [...skuFilters, targetSku];
    for (const sku of next) params.append('sku', sku);
    const qs = params.toString();
    return '/orders' + (qs ? '?' + qs : '');
  }

  function clearSkus(): string {
    const params = new URLSearchParams();
    if (stateFilter) params.set('state', stateFilter);
    const qs = params.toString();
    return '/orders' + (qs ? '?' + qs : '');
  }

  async function load(showSpinner = true): Promise<void> {
    if (showSpinner) loading = true;
    try {
      const list = await listOrders({
        state: stateFilter || undefined,
        skus: skuFilters.length ? skuFilters : undefined
      });
      orders = list;
      error = null;
    } catch (e) {
      if (e instanceof ApiError) {
        error = { error: e.code, message: e.message };
      } else {
        error = { error: 'NETWORK', message: String(e) };
      }
    } finally {
      loading = false;
      polling = false;
    }
  }

  function fmtTime(iso: string): string {
    return iso.slice(0, 16).replace('T', ' ');
  }

  function fmtSkuLine(items: Order['items']): string {
    return items.map((it) => `${it.sku}×${it.quantity}`).join(' ');
  }

  onMount(() => {
    load();
    pollTimer = setInterval(() => {
      polling = true;
      load(false);
    }, 2_000);
  });

  $effect(() => {
    // re-fetch when URL filter changes
    void stateFilter;
    void skuFilters;
    load();
  });

  import { onDestroy } from 'svelte';
  onDestroy(() => {
    if (pollTimer) clearInterval(pollTimer);
  });
</script>

<svelte:head>
  <title>Orders — OrderFlow</title>
</svelte:head>

<section>
  <div class="row-between">
    <h1>Orders</h1>
    <a class="btn" href="/orders/new">+ New order</a>
  </div>

  <nav class="filter-chips" aria-label="Filter orders by state">
    <a
      class="chip"
      class:active={!stateFilter && skuFilters.length === 0}
      href={'/orders' + (skuFilters.length ? '?' + new URLSearchParams(skuFilters.map((s) => ['sku', s])).toString() : '')}
    >All</a>
    {#each STATES as st}
      <a
        class="chip"
        class:active={stateFilter === st.value}
        href={chipHref(st.value)}
      >{st.label}</a>
    {/each}
  </nav>

  {#if skuFilters.length > 0}
    <p class="muted">
      Filtered by SKU:
      {#each skuFilters as sku, i}
        {#if i > 0}, {/if}
        <a class="mono sku-chip" href={skuChipHref(sku)} title="Click to remove">{sku}</a>
      {/each}
      <a href={clearSkus()}>clear SKU filter</a>
    </p>
  {/if}

  {#if error}
    <div class="error">
      {error.message}
      {#if polling} (retrying){/if}
    </div>
  {:else if loading && orders.length === 0}
    <p class="muted">Loading…</p>
  {:else if orders.length === 0}
    <p class="muted">No orders match the active filters.</p>
  {:else}
    <table>
      <thead>
        <tr>
          <th>ID</th>
          <th>State</th>
          <th>Items</th>
          <th>Created</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {#each orders as o (o.id)}
          <tr>
            <td class="mono">{o.id.slice(0, 8)}…</td>
            <td>
              <span class="badge state-{o.state}">{o.state}</span>
            </td>
            <td class="mono small">{fmtSkuLine(o.items)}</td>
            <td class="muted small">{fmtTime(o.created_at)}</td>
            <td><a href={`/orders/${o.id}`}>view →</a></td>
          </tr>
        {/each}
      </tbody>
    </table>
  {/if}

  <p class="muted small">Page polls every 2s{polling ? ' (refreshing…)' : ''}.</p>
</section>

<style>
  .row-between { display: flex; align-items: center; justify-content: space-between; margin-bottom: var(--gap-4); }
  .row-between h1 { margin: 0; font-size: var(--fs-xl); }

  .filter-chips { display: flex; flex-wrap: wrap; gap: var(--gap-1); margin: 0 0 var(--gap-3); }
  .chip {
    display: inline-block; padding: var(--gap-1) var(--gap-3);
    border-radius: var(--radius-pill);
    border: 1px solid var(--border);
    background: transparent; color: var(--fg);
    font-size: var(--fs-sm); font-weight: 600;
  }
  .chip:hover { border-color: var(--accent); text-decoration: none; }
  .chip.active { background: var(--accent); color: white; border-color: var(--accent); }

  .sku-chip { padding: 2px 6px; border: 1px solid var(--border); border-radius: var(--radius-sm); margin-right: var(--gap-1); }
  .sku-chip:hover { border-color: var(--bad); color: var(--bad); }

  table { width: 100%; border-collapse: collapse; margin-top: var(--gap-3); display: block; overflow-x: auto; }
  th, td { padding: var(--gap-2) var(--gap-3); text-align: left; border-bottom: 1px solid var(--border); }
  th { color: var(--fg-muted); font-weight: 600; font-size: var(--fs-sm); }
  .small { font-size: var(--fs-sm); }

  .badge { padding: 2px var(--gap-2); border-radius: var(--radius-pill); font-size: var(--fs-xs); font-weight: 600; }
  .badge.state-pending { background: rgba(210,153,34,0.15); color: var(--warn); }
  .badge.state-reserved { background: rgba(68,147,248,0.15); color: var(--accent); }
  .badge.state-confirmed { background: rgba(86,211,100,0.15); color: var(--good); }
  .badge.state-cancelled,
  .badge.state-failed { background: rgba(248,81,73,0.15); color: var(--bad); }

  .btn {
    display: inline-block; padding: var(--gap-2) var(--gap-3);
    background: var(--accent); color: white;
    border: 0; border-radius: var(--radius);
    font-weight: 600;
  }
  .btn:hover { text-decoration: none; opacity: 0.9; }
</style>
