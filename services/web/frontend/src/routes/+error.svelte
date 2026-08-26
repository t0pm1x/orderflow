<script lang="ts">
  import { page } from '$app/stores';
  import { goto } from '$app/navigation';
  import { onMount } from 'svelte';

  // Generic error boundary. The BFF returns `{error, message}`
  // envelopes on 4xx/5xx (see services/web/internal/server/api.go);
  // we surface them in a friendly banner. The back-link target is
  // the URL the SPA was on when the error fired (or the orders
  // list as a safe default).
  let { error, status } = $props();

  function goBack(): void {
    history.back();
  }

  let friendlyMessage = $derived(
    error?.message ?? 'Something went wrong. Please try again.'
  );
  let isUpstream = $derived(
    error?.error === 'UPSTREAM_UNAVAILABLE' ||
    (status !== undefined && status >= 500)
  );
</script>

<section class="error-page">
  <div class="error">
    <h2>
      {#if status === 404}Not found
      {:else if isUpstream}Backend unavailable
      {:else if status && status >= 400}Request rejected
      {:else}Unexpected error{/if}
    </h2>
    <p>{friendlyMessage}</p>
    {#if error?.error}
      <p class="muted mono">code: {error.error}</p>
    {/if}
    <button onclick={goBack}>← Go back</button>
    <a href="/orders">or go to Orders list</a>
  </div>
</section>

<style>
  .error-page { padding: var(--gap-5); }
  .error {
    max-width: 540px;
    margin: var(--gap-5) auto;
    padding: var(--gap-5);
    background: var(--panel);
    border: 1px solid var(--bad);
    border-radius: var(--radius);
  }
  .error h2 { margin: 0 0 var(--gap-3); color: var(--bad); }
  .error p { margin: 0 0 var(--gap-3); }
  button { margin-right: var(--gap-3); }
</style>
