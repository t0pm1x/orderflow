<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import { ApiError, getHealthAll, listOrders } from '$lib/api';
  import type { HealthSnapshot, Order } from '$lib/types';
  import {
    downServiceNames,
    fmtClock,
    hasDown,
    kpiFromOrders
  } from '$lib/dashboard';

  let health = $state<HealthSnapshot | null>(null);
  let orders = $state<Order[]>([]);
  let healthErr = $state<string | null>(null);
  let ordersErr = $state<string | null>(null);

  let healthTimer: ReturnType<typeof setInterval> | null = null;
  let ordersTimer: ReturnType<typeof setInterval> | null = null;

  let kpis = $derived(kpiFromOrders(orders));
  let degradedNames = $derived(health ? downServiceNames(health).join(', ') : '');
  let showBanner = $derived(hasDown(health));
  let isEmpty = $derived(orders.length === 0 && !ordersErr);

  async function refreshHealth(): Promise<void> {
    try {
      health = await getHealthAll();
      healthErr = null;
    } catch (e) {
      if (e instanceof ApiError) healthErr = e.message;
      else healthErr = String(e);
      health = null;
    }
  }

  async function refreshOrders(): Promise<void> {
    try {
      orders = await listOrders({});
      ordersErr = null;
    } catch (e) {
      if (e instanceof ApiError) ordersErr = e.message;
      else ordersErr = String(e);
    }
  }

  function startTimers(): void {
    stopTimers();
    refreshHealth();
    refreshOrders();
    healthTimer = setInterval(refreshHealth, 5_000);
    ordersTimer = setInterval(refreshOrders, 2_000);
  }

  function stopTimers(): void {
    if (healthTimer) { clearInterval(healthTimer); healthTimer = null; }
    if (ordersTimer) { clearInterval(ordersTimer); ordersTimer = null; }
  }

  function onVisibility(): void {
    if (document.visibilityState === 'visible') startTimers();
    else stopTimers();
  }

  function fmtTime(iso: string): string {
    return iso.slice(0, 16).replace('T', ' ');
  }

  function fmtSkuLine(items: Order['items']): string {
    return items.map((it) => `${it.sku}×${it.quantity}`).join(' ');
  }

  onMount(() => {
    startTimers();
    document.addEventListener('visibilitychange', onVisibility);
  });

  onDestroy(() => {
    stopTimers();
    document.removeEventListener('visibilitychange', onVisibility);
  });
</script>

<svelte:head>
  <title>Dashboard — OrderFlow</title>
</svelte:head>

<section>
  {#if healthErr}
    <div class="banner banner-fatal">
      Backend unreachable — {healthErr}
      <button onclick={refreshHealth}>retry</button>
    </div>
  {:else if showBanner}
    <div class="banner banner-down">
      {downServiceNames(health!).length} service(s) unreachable: {degradedNames}
    </div>
  {/if}

  <div class="row-between">
    <h1>Dashboard</h1>
    <a class="btn" href="/orders/new">+ New order</a>
  </div>

  <div class="kpis">
    <div class="kpi">
      <div class="kpi-label">Orders today</div>
      <div class="kpi-value">{kpis.ordersToday}</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Success rate</div>
      <div class="kpi-value">
        {kpis.successRatePct === null ? '—' : `${kpis.successRatePct.toFixed(1)}%`}
      </div>
    </div>
    <div class="kpi">
      <div class="kpi-label">In-flight</div>
      <div class="kpi-value">{kpis.inFlight}</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Avg completion</div>
      <div class="kpi-value">
        {kpis.avgCompletionMs === null
          ? '—'
          : kpis.avgCompletionMs < 1000
            ? `${Math.round(kpis.avgCompletionMs)} ms`
            : `${(kpis.avgCompletionMs / 1000).toFixed(2)} s`}
      </div>
    </div>
  </div>

  <div class="grid">
    <section class="panel">
      <h2>Health</h2>
      {#if health}
        <ul class="chips">
          {#each [
            { name: 'Order',     h: health.order },
            { name: 'Payment',   h: health.payment },
            { name: 'Inventory', h: health.inventory },
            { name: 'Saga',      h: health.saga },
            { name: 'Kafka tail', h: health.kafka }
          ] as item}
            <li>
              <span class="chip chip-{item.h.status}" title={`latency ${item.h.latency_ms}ms · taken ${fmtClock(item.h.taken_at)}${item.h.detail ? ' · ' + item.h.detail : ''}`}>
                {item.name}
                <span class="chip-status">{item.h.status}</span>
              </span>
            </li>
          {/each}
        </ul>
      {:else}
        <p class="muted">Awaiting first probe…</p>
      {/if}
    </section>

    <section class="panel">
      <h2>Recent orders</h2>
      {#if ordersErr}
        <div class="banner banner-soft">{ordersErr} (retrying)</div>
      {:else if orders.length === 0}
        <p class="muted">No orders yet.</p>
      {:else}
        <table>
          <thead>
            <tr>
              <th>ID</th><th>State</th><th>Items</th><th>Created</th><th></th>
            </tr>
          </thead>
          <tbody>
            {#each orders.slice(0, 10) as o (o.id)}
              <tr>
                <td class="mono">{o.id.slice(0, 8)}…</td>
                <td><span class="badge state-{o.state}">{o.state}</span></td>
                <td class="mono small">{fmtSkuLine(o.items)}</td>
                <td class="muted small">{fmtTime(o.created_at)}</td>
                <td><a href={`/orders/${o.id}`}>view →</a></td>
              </tr>
            {/each}
          </tbody>
        </table>
      {/if}
    </section>
  </div>

  {#if isEmpty && !healthErr}
    <section class="welcome">
      <h2>Welcome to OrderFlow</h2>
      <p>
        This is the playground for the orderflow distributed order
        processing platform. Create your first order to see the
        saga run in real time.
      </p>
      <div class="welcome-actions">
        <a class="btn" href="/orders/new">+ Create order</a>
        <button class="btn-secondary" disabled title="Coming soon — Spec #2">
          Seed demo data
        </button>
      </div>
    </section>
  {/if}
</section>

<style>
  .row-between { display: flex; align-items: center; justify-content: space-between; margin-bottom: var(--gap-4); }
  .row-between h1 { margin: 0; font-size: var(--fs-xl); }

  .banner {
    padding: var(--gap-2) var(--gap-4);
    border-radius: var(--radius);
    margin-bottom: var(--gap-4);
    display: flex; align-items: center; gap: var(--gap-3);
  }
  .banner-down { background: var(--bad-soft); color: var(--bad); border: 1px solid var(--bad); }
  .banner-fatal { background: var(--bad-soft); color: var(--bad); border: 1px solid var(--bad); justify-content: space-between; }
  .banner-soft { background: var(--panel-2); color: var(--fg-muted); border: 1px solid var(--border); font-size: var(--fs-sm); }

  .kpis {
    display: grid; grid-template-columns: repeat(4, 1fr);
    gap: var(--gap-3); margin-bottom: var(--gap-4);
  }
  @media (max-width: 720px) {
    .kpis { grid-template-columns: repeat(2, 1fr); }
  }
  .kpi {
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: var(--gap-3) var(--gap-4);
  }
  .kpi-label { color: var(--fg-muted); font-size: var(--fs-sm); margin-bottom: var(--gap-1); }
  .kpi-value { font-size: var(--fs-2xl); font-weight: 600; font-family: var(--font-mono); }

  .grid {
    display: grid; grid-template-columns: 1fr 2fr;
    gap: var(--gap-4); margin-bottom: var(--gap-4);
  }
  @media (max-width: 960px) { .grid { grid-template-columns: 1fr; } }

  .panel {
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: var(--gap-4);
  }
  .panel h2 { margin: 0 0 var(--gap-3); font-size: var(--fs-md); color: var(--fg-muted); text-transform: uppercase; letter-spacing: 0.05em; }

  .chips { list-style: none; padding: 0; margin: 0; display: flex; flex-direction: column; gap: var(--gap-2); }
  .chip {
    display: flex; align-items: center; justify-content: space-between;
    padding: var(--gap-2) var(--gap-3);
    border-radius: var(--radius);
    border: 1px solid var(--border);
    background: var(--panel-2);
    cursor: help;
  }
  .chip-status { font-family: var(--font-mono); font-size: var(--fs-xs); }
  .chip-ok      { border-color: var(--good); }
  .chip-ok .chip-status      { color: var(--good); }
  .chip-degraded { border-color: var(--warn); }
  .chip-degraded .chip-status { color: var(--warn); }
  .chip-down    { border-color: var(--bad); }
  .chip-down .chip-status    { color: var(--bad); }

  table { width: 100%; border-collapse: collapse; }
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
    border: 0; border-radius: var(--radius); font-weight: 600;
  }
  .btn:hover { text-decoration: none; opacity: 0.9; }
  .btn-secondary {
    padding: var(--gap-2) var(--gap-3);
    background: var(--panel-2); color: var(--fg-muted);
    border: 1px solid var(--border); border-radius: var(--radius);
    cursor: not-allowed; font-weight: 600;
  }

  .welcome {
    background: var(--panel);
    border: 1px dashed var(--border-strong);
    border-radius: var(--radius-lg);
    padding: var(--gap-5);
    text-align: center;
  }
  .welcome h2 { margin: 0 0 var(--gap-3); font-size: var(--fs-lg); }
  .welcome p { margin: 0 auto var(--gap-4); max-width: 480px; color: var(--fg-muted); }
  .welcome-actions { display: flex; gap: var(--gap-3); justify-content: center; }
</style>
