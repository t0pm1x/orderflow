<script lang="ts">
  import { goto } from '$app/navigation';
  import { onMount } from 'svelte';

  // Root route — redirect to /dashboard so the SPA lands on
  // the at-a-glance summary rather than the raw orders list.
  // Server-side fallback in the BFF returns index.html for any
  // non-API GET, but doing it client-side avoids a round trip
  // and lets the browser's back-button return to the list.
  onMount(() => {
    goto('/dashboard', { replaceState: true });
  });
</script>

<p class="muted">Redirecting to dashboard…</p>
