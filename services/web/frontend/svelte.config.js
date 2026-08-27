import adapter from '@sveltejs/adapter-static';
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';

/** @type {import('@sveltejs/kit').Config} */
const config = {
  preprocess: vitePreprocess(),

  kit: {
    adapter: adapter({
      // Output both SSR pages and static assets to `dist/` (a sibling
      // of `build/`) so the Go embed directive in services/web/spa.go
      // can match `frontend/dist/index.html` without SvelteKit
      // overwriting the embed. `build/` is the adapter-static
      // default — we route to `dist/` to keep the Go embed path
      // predictable.
      pages: 'dist',
      assets: 'dist',
      fallback: 'index.html',
      precompress: false,
      strict: true
    })
  }
};

export default config;
