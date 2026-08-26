<script lang="ts">
  import { goto } from '$app/navigation';
  import { onMount } from 'svelte';

  // Root route — redirect to /orders so the SPA lands on the
  // primary list view. Server-side redirect is also configured
  // by the BFF (any non-API GET falls back to index.html), but
  // doing it client-side avoids an unnecessary round trip + lets
  // the browser's back-button work correctly.
  onMount(() => {
    goto('/orders', { replaceState: true });
  });
</script>

<p class="muted">Redirecting to orders list…</p>
