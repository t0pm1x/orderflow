<script lang="ts">
  import { onMount } from 'svelte';
  import { ApiError, getStock, listOrders } from '$lib/api';
  import type { Order, StockItem } from '$lib/types';

  // Per-SKU stock viewer. The inventory service only exposes
  // single-SKU reads; we derive the SKU list from the most-recent
  // orders' items (mirrors the pre-SvelteKit behavior). Clicking
  // a SKU cell navigates back to the orders list with ?sku=…

  let rows = $state<{ sku: string; stock: StockItem | null; missing: boolean }[]>([]);
  let error: { error: string; message: string } | null = $state(null);
  let loading = $state(true);
  let pollTimer: ReturnType<typeof setInterval> | null = null;

  function buildSkuList(orders: Order[]): string[] {
    const seen = new Set<string>();
    const out: string[] = [];
    for (const o of orders) {
      for (const it of o.items) {
        if (!seen.has(it.sku)) {
          seen.add(it.sku);
          out.push(it.sku);
        }
      }
    }
    return out;
  }

  async function load(showSpinner = true): Promise<void> {
    if (showSpinner) loading = true;
    try {
      const orders = await listOrders({});
      const skus = buildSkuList(orders);
      const stocks = await Promise.all(
        skus.map(async (sku) => {
          try {
            const s = await getStock(sku);
            return { sku, stock: s, missing: false };
          } catch (e) {
            if (e instanceof ApiError && e.code === 'NOT_FOUND') {
              return { sku, stock: null, missing: true };
            }
            return { sku, stock: null, missing: true };
          }
        })
      );
      rows = stocks;
      error = null;
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

  onMount(() => {
    load();
    pollTimer = setInterval(() => load(false), 3_000);
  });

  import { onDestroy } from 'svelte';
  onDestroy(() => {
    if (pollTimer) clearInterval(pollTimer);
  });
</script>

<svelte:head>
  <title>Inventory — OrderFlow</title>
</svelte:head>

<section>
  <h1>Inventory</h1>
  <p class="muted">Click any SKU to see orders that include it.</p>

  {#if error}
    <div class="error">{error.message}</div>
  {:else if loading && rows.length === 0}
    <p class="muted">Loading…</p>
  {:else if rows.length === 0}
    <p class="muted">No stock items yet. Submit some orders to populate this view.</p>
  {:else}
    <table>
      <thead><tr><th>SKU</th><th>Available</th><th>Reserved</th></tr></thead>
      <tbody>
        {#each rows as r (r.sku)}
          <tr>
            <td class="mono">
              <a href={`/orders?sku=${encodeURIComponent(r.sku)}`}>{r.sku}</a>
            </td>
            <td>{r.missing ? '—' : r.stock?.available ?? '—'}</td>
            <td>{r.missing ? '—' : r.stock?.reserved ?? '—'}</td>
          </tr>
        {/each}
      </tbody>
    </table>
  {/if}

  <p class="muted small">Page polls every 3s.</p>
</section>

<style>
  h1 { margin: 0 0 var(--gap-3); font-size: var(--fs-xl); }
  table { width: 100%; border-collapse: collapse; margin-top: var(--gap-3); display: block; overflow-x: auto; }
  th, td { padding: var(--gap-2) var(--gap-3); text-align: left; border-bottom: 1px solid var(--border); }
  th { color: var(--fg-muted); font-weight: 600; font-size: var(--fs-sm); }
  .small { font-size: var(--fs-sm); }
  a.mono:hover { color: var(--accent); }
</style>
