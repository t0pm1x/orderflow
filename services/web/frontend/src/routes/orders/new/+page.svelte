<script lang="ts">
  import { page } from '$app/stores';
  import { goto } from '$app/navigation';
  import { onDestroy, onMount } from 'svelte';
  import { ApiError, submitOrder } from '$lib/api';

  let sku = $state('SKU-001');
  let quantity = $state(1);
  let unitPriceCents = $state<number | ''>('');
  let customerId = $state('');
  let idempotencyKey = $state(crypto.randomUUID());
  let error: { error: string; message: string } | null = $state(null);
  let submitting = $state(false);

  // Prefill via ?prefill=happy|fail — same UX as the previous
  // htmx hero CTAs (see services/web/internal/templates/orders_list.html
  // / order_hero.html for the historic context).
  let prefillKind = $derived(($page.url.searchParams.get('prefill') ?? '') as '' | 'happy' | 'fail');
  let lastFour = $state('');

  $effect(() => {
    if (prefillKind === 'happy') {
      sku = 'SKU-001';
      quantity = 1;
      unitPriceCents = 1999;
      lastFour = '4242';
    } else if (prefillKind === 'fail') {
      sku = 'SKU-001';
      quantity = 1;
      unitPriceCents = 1999;
      lastFour = '0001';
    }
  });

  async function onSubmit(e: SubmitEvent): Promise<void> {
    e.preventDefault();
    error = null;
    submitting = true;
    try {
      const price = typeof unitPriceCents === 'number' ? unitPriceCents : undefined;
      const order = await submitOrder({
        customer_id: customerId || undefined,
        idempotency_key: idempotencyKey,
        items: [{ sku, quantity, unit_price_cents: price }],
        payment: lastFour ? { last_four: lastFour } : undefined
      });
      // SPA navigation to the detail page after a successful POST.
      // The BFF returns the created Order in 201 with the new ID.
      await goto(`/orders/${order.id}`);
    } catch (e) {
      if (e instanceof ApiError) {
        error = { error: e.code, message: e.message };
      } else {
        error = { error: 'NETWORK', message: String(e) };
      }
      submitting = false;
    }
  }
</script>

<svelte:head>
  <title>New order — OrderFlow</title>
</svelte:head>

<section>
  <h1>New order</h1>

  {#if prefillKind}
    <p class="muted">
      Prefilled for {prefillKind === 'happy' ? 'happy path (card 4242)' : 'compensation path (card 0001)'}.
      Adjust any field before submitting.
    </p>
  {/if}

  {#if error}
    <div class="error">{error.message}</div>
  {/if}

  <form class="sheet" onsubmit={onSubmit}>
    <label>
      SKU
      <input bind:value={sku} required maxlength="64" />
    </label>
    <label>
      Quantity
      <input type="number" bind:value={quantity} min="1" max="10000" required />
    </label>
    <label>
      Unit price (cents, optional)
      <input
        type="number"
        bind:value={unitPriceCents}
        min="0"
        placeholder="auto"
      />
    </label>
    <label>
      Customer ID (optional — leave blank for auto-generated UUID)
      <input
        bind:value={customerId}
        placeholder="leave blank for auto-generated UUID"
        autocomplete="off"
      />
    </label>
    <label>
      Last 4 of card (optional — used for deterministic success/fail branch)
      <input
        bind:value={lastFour}
        maxlength="4"
        placeholder="e.g. 4242"
        autocomplete="off"
      />
    </label>
    <div class="row">
      <button type="submit" disabled={submitting} aria-busy={submitting}>
        {submitting ? 'Submitting…' : 'Submit order'}
      </button>
      <a href="/orders">Cancel</a>
    </div>
  </form>
</section>

<style>
  h1 { margin: 0 0 var(--gap-4); font-size: var(--fs-xl); }
  .sheet { display: grid; gap: var(--gap-3); max-width: 480px; }
  .sheet label { display: grid; gap: var(--gap-1); font-weight: 600; color: var(--fg-muted); }
  .sheet input {
    padding: var(--gap-2); background: var(--bg);
    color: var(--fg); border: 1px solid var(--border); border-radius: var(--radius);
    font: inherit;
  }
  .row { display: flex; gap: var(--gap-3); align-items: center; }
  button {
    padding: var(--gap-2) var(--gap-3);
    background: var(--accent); color: white;
    border: 0; border-radius: var(--radius); font-weight: 600;
  }
  button:disabled { opacity: 0.5; cursor: not-allowed; }
  a { color: var(--fg-muted); }
</style>
