<script lang="ts">
  import '../app.css';
  import { onMount, onDestroy } from 'svelte';
  import { page } from '$app/stores';
  import { connectSSE, disconnectSSE, liveEvents, sseConnected } from '$lib/sse';

  let { children } = $props();

  onMount(() => {
    connectSSE();
  });
  onDestroy(() => {
    disconnectSSE();
  });
</script>

<div class="app">
  <header class="topbar">
    <a class="brand" href="/">OrderFlow</a>
    <span class="muted">Distributed order processing — playground</span>
    <nav>
      <a href="/" class:active={$page.url.pathname === '/' || $page.url.pathname.startsWith('/dashboard')}>Dashboard</a>
      <a href="/orders" class:active={!$page.url.pathname.startsWith('/dashboard') && ($page.url.pathname === '/' || $page.url.pathname.startsWith('/orders'))}>Orders</a>
      <a href="/inventory" class:active={$page.url.pathname.startsWith('/inventory')}>Inventory</a>
      <a href="/payments/sim" class:active={$page.url.pathname.startsWith('/payments')}>Payments sim</a>
    </nav>
  </header>

  <main class="main">
    <section class="content">
      {@render children()}
    </section>

    <aside class="sidebar">
      <h3>
        Live events
        {#if !$sseConnected}
          <span class="badge disconnected" title="Kafka tail not started or BFF unreachable">disconnected</span>
        {/if}
      </h3>
      <ul class="events" aria-live="polite" aria-label="Order event stream">
        {#each $liveEvents as ev (ev.event_id)}
          <li class="event event-{ev.event_type}">
            <span class="event-time mono">{ev.occurred_at.slice(11, 19)}</span>
            <span class="event-type mono">{ev.event_type}</span>
            <span class="event-id mono">{ev.aggregate_id.slice(0, 8)}</span>
          </li>
        {/each}
      </ul>
      {#if $liveEvents.length === 0}
        <p class="muted">No events yet. Kafka events will appear here when the tail is running.</p>
      {/if}
    </aside>
  </main>
</div>

<style>
  .app { min-height: 100vh; display: flex; flex-direction: column; }
  .topbar {
    display: flex; align-items: center; gap: var(--gap-4);
    padding: var(--gap-3) var(--gap-5);
    border-bottom: 1px solid var(--border);
    background: var(--panel);
  }
  .brand { font-weight: 600; color: var(--fg); }
  .topbar nav { margin-left: auto; display: flex; gap: var(--gap-4); }
  .topbar nav a { color: var(--fg-muted); }
  .topbar nav a.active { color: var(--accent); }
  .topbar nav a:hover { color: var(--fg); text-decoration: none; }

  .main { display: grid; grid-template-columns: 1fr 360px; flex: 1; min-height: 0; }
  .content { padding: var(--gap-5); overflow: auto; }
  .sidebar {
    background: var(--panel);
    border-left: 1px solid var(--border);
    padding: var(--gap-4);
    overflow-y: auto;
  }
  .sidebar h3 {
    margin: 0 0 var(--gap-3);
    font-size: var(--fs-md);
    display: flex; align-items: center; gap: var(--gap-2);
  }

  .events {
    list-style: none; padding: 0; margin: 0;
    font-family: var(--font-mono); font-size: var(--fs-xs);
  }
  .events li {
    padding: var(--gap-1) var(--gap-2);
    border-bottom: 1px solid var(--border);
    display: grid; grid-template-columns: auto 1fr auto; gap: var(--gap-2);
    word-break: break-all;
  }
  .event-OrderConfirmed { color: var(--good); }
  .event-OrderCancelled,
  .event-PaymentFailed,
  .event-StockReservationFailed { color: var(--bad); }
  .event-OrderCreated,
  .event-StockReserveRequested,
  .event-StockReleased,
  .event-StockReserved,
  .event-PaymentRequested,
  .event-PaymentCompleted { color: var(--accent); }

  .badge { display: inline-block; padding: 2px 8px; border-radius: var(--radius-pill); font-size: var(--fs-xs); font-weight: 600; }
  .badge.disconnected { background: var(--bad-soft); color: var(--bad); border: 1px solid var(--bad); }

  @media (max-width: 720px) {
    .main { grid-template-columns: 1fr; }
    .sidebar { border-left: 0; border-top: 1px solid var(--border); max-height: 40vh; }
  }
</style>
