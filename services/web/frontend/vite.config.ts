import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

// Vite config for the orderflow-web SvelteKit SPA.
//
// `build.outDir: 'dist'` overrides SvelteKit's default `build/` so
// the Go embed.FS in services/web/internal/server/server.go can
// point at a stable, well-known directory:
//
//   //go:embed all:frontend/dist
//   var spaFS embed.FS
//
// During `vite dev` (npm run dev) requests to /api/* and
// /events/stream are proxied to the running Go binary on :8085
// so the SPA can hit the real backend while running on Vite's dev
// port (5173). In production the SPA hits the same origin (the Go
// binary serves both the static assets and the API).
export default defineConfig({
  plugins: [sveltekit()],

  build: {
    outDir: 'dist'
  },

  server: {
    port: 5173,
    strictPort: true,
    proxy: {
      '/api': 'http://localhost:8085',
      '/events/stream': {
        target: 'http://localhost:8085',
        ws: false
      }
    }
  }
});
