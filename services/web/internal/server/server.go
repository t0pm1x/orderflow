// Package server wires the orderflow-web HTTP server. Single binary:
// serves the embedded SvelteKit SPA + the /api/* JSON proxy to
// the four backend services + the /events/stream SSE bridge from
// the in-process Kafka tail.
//
// Route map:
//
//   GET  /healthz                            — liveness
//   GET  /readyz                             — readiness
//   GET  /_app/*                             — SvelteKit code-split assets
//   GET  /favicon.svg, /static/*            — SvelteKit static dir
//   GET  /, /orders, /orders/:id, ...        — SPA fallback (index.html)
//   GET  /api/orders                         — proxy listOrders
//   GET  /api/orders/{id}                    — proxy getOrder
//   POST /api/orders                         — proxy submitOrder
//   DEL  /api/orders/{id}                    — proxy cancelOrder
//   GET  /api/inventory/stock/{sku}          — proxy getInventoryStock
//   POST /api/payments/webhook               — proxy fireWebhook
//   GET  /api/health/all                     — multi-service health probe
//   GET  /events/stream                      — SSE from in-process bus
//
// The SPA hits /api/* same-origin, the Go BFF proxies to the
// backend services via the existing backend.* clients. No CORS
// configuration on the backends is required, and the backend
// URLs stay server-side secrets.
package server

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"

	mw "github.com/t0pm1x/orderflow/platform/middleware"
	"github.com/t0pm1x/orderflow/services/web/internal/backend"
	"github.com/t0pm1x/orderflow/services/web/internal/events"
	webroot "github.com/t0pm1x/orderflow/services/web"
)

// ServiceURLs captures the resolved upstream base URLs. The
// probe handler reads these via Options.Urls so it can fan out
// to /healthz without re-reading env vars.
type ServiceURLs struct {
	Order     string
	Payment   string
	Inventory string
	Saga      string
}

// Options configures the web HTTP server. Mirrors the pattern of
// services/order/cmd/order's Options struct so platform/middleware
// can stay generic.
type Options struct {
	Name          string
	Logger        *slog.Logger
	Order         backend.OrderClient
	Payment       backend.PaymentClient
	Inventory     backend.InventoryClient
	Bus           *events.Bus
	EventsEnabled bool // toggles /events/stream 503 vs 200
	Urls          ServiceURLs
	// KafkaHealth reports whether the in-process Kafka tail is
	// consuming. Supplied as a closure so internal/server does not
	// import internal/kafkatail. nil is treated as "down".
	KafkaHealth func() bool
}

// Server hosts the HTTP listener. One instance per process.
type Server struct {
	opt  Options
	srv  *http.Server
	addr atomic.Value // string
	api  *API

	// healthCache holds the last /api/health/all snapshot for 1s.
	// nil means cache miss; cacheEntry is package-level so the
	// Server field can be a typed *cacheEntry (the previous `any`
	// + runtime type assertion pattern was a footgun that let
	// future maintainers cache any type by accident).
	healthCacheMu sync.Mutex
	healthCache   *healthCacheEntry
}

// healthCacheEntry is one snapshot of /api/health/all: when it was
// taken and the snapshot itself. healthCache stores a pointer to
// this struct (or nil for a cold cache); see Server.healthCache.
type healthCacheEntry struct {
	taken    time.Time
	snapshot HealthSnapshot
}

// New constructs a non-listening Server.
func New(opt Options) *Server {
	return &Server{
		opt: opt,
		api: &API{
			Order:     opt.Order,
			Payment:   opt.Payment,
			Inventory: opt.Inventory,
			Logger:    opt.Logger,
		},
	}
}

// Addr returns the bound address (host:port) or "" if Start has
// not completed. Tests + the playground smoke script poll this to
// discover the OS-picked port when HTTP_ADDR ends in ":0".
func (s *Server) Addr() string {
	v, _ := s.addr.Load().(string)
	return v
}

// Start binds the listener and serves until ctx is cancelled.
func (s *Server) Start(ctx context.Context, addr string) error {
	r := chi.NewRouter()
	r.Use(mw.Stack(s.opt.Name, s.opt.Logger)...)

	// probes (same contract as the other services so k8s probes
	// and the smoke script don't need to special-case web).
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	r.Get("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// ready iff Kafka tail is wired (so /events/stream can serve
		// real events). Without Kafka the SPA still works — it just
		// shows "Live events: disconnected" — so we don't gate
		// readiness on it. Always 200.
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	// API proxy + SSE
	r.Get("/api/orders", s.api.ListOrders)
	r.Get("/api/orders/{id}", s.api.GetOrder)
	r.Post("/api/orders", s.api.SubmitOrder)
	r.Delete("/api/orders/{id}", s.api.CancelOrder)
	r.Get("/api/inventory/stock/{sku}", s.api.GetInventoryStock)
	r.Post("/api/payments/webhook", s.api.FireWebhook)
	r.Get("/api/health/all", s.HealthAll)
	r.Get("/events/stream", sseHandler(s.opt.Bus, s.opt.Logger, s.opt.EventsEnabled))

	// SPA: serve the embedded SvelteKit build. The Vite output
	// uses two layout roots: `_app/...` for code-split chunks and
	// the static dir (favicon, etc.) at the root. SPA fallback
	// serves index.html for any non-API GET so client-side routes
	// (/orders/123) work on a hard refresh.
	if err := s.mountSPA(r); err != nil {
		// Empty embed (no frontend/dist yet) is recoverable: the
		// server still starts, only the SPA is missing. Log a warning
		// and continue — operators hitting `make web-build` after
		// `make web-frontend-build` will see this in the log.
		s.opt.Logger.Warn("spa embed empty; run `make web-frontend-build` before `go build` to populate frontend/dist/", "err", err)
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.srv = &http.Server{
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
	}
	s.addr.Store(ln.Addr().String())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.opt.Logger.Error("web http exited", "err", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		_ = s.srv.Shutdown(shutdownCtx)
	}()

	wgDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(wgDone)
	}()

	select {
	case <-wgDone:
	case <-shutdownCtx.Done():
		return shutdownCtx.Err()
	}
	return nil
}

// mountSPA wires the SvelteKit build output into r. The embed
// lives in services/web/spa.go (sibling of frontend/, because
// embed patterns can't contain ".." — see the comment there);
// internal/server consumes three typed exports:
// webroot.appFS (the `_app/` code-split bundle, served at
// `/_app/*` with real Content-Type), webroot.faviconSVG (the
// favicon at /favicon.svg), and webroot.indexHTML (the SPA
// fallback served for every other non-API GET).
//
// Layout:
//   frontend/dist/_app/...   — code-split JS/CSS chunks
//   frontend/dist/favicon.svg — static asset (Vite copies public/)
//   any other GET              — index.html (SPA fallback)
//
// The SPA fallback for non-API GET routes serves dist/index.html so
// SvelteKit client-side routes (/orders/123) survive a hard
// refresh (SvelteKit's adapter-static emits a single index.html by
// default since we set `fallback: 'index.html'` in svelte.config.js).
func (s *Server) mountSPA(r chi.Router) error {
	// _app/* — SvelteKit code-split JS/CSS bundles. AppFS is exposed
	// as a sub-FS rooted at the _app/ directory (see services/web/spa.go
	// F-004 fix: fs.Sub strips the embed pattern's `frontend/dist/_app`
	// prefix). Lookup paths inside AppFS are therefore RELATIVE to the
	// _app/ root — i.e. they start with `immutable/...`, not `_app/immutable/...`.
	// We strip the leading `_app/` from the URL path before opening so
	// the lookup matches.
	r.Get("/_app/*", func(w http.ResponseWriter, req *http.Request) {
		path := strings.TrimPrefix(req.URL.Path, "/")
		path = strings.TrimPrefix(path, "_app/")
		f, err := webroot.AppFS.Open(path)
		if err != nil {
			http.NotFound(w, req)
			return
		}
		defer f.Close()
		setContentTypeByExt(w, path)
		_, _ = w.Write(readAll(f))
	})

	// /static/* — anything Vite copied from frontend/static/ (none
	// today beyond favicon.svg, but future-proof). Mount under
	// /static so SvelteKit's <img src="/static/..."> resolves.
	// After the F-004 fs.Sub fix, AppFS is rooted at _app/ and does
	// not contain a top-level `static/` directory; SvelteKit's static
	// files land under _app/immutable/assets/ alongside the code-split
	// bundles, served with the correct content-type by the /_app/*
	// handler above. The /static/* route stays as a no-op 404 so any
	// future-proof <img src="/static/..."> reference fails fast with
	// a useful status instead of mysteriously inheriting a stale SPA
	// HTML shell from the r.Get("/*") fallback below.
	r.Get("/static/*", func(w http.ResponseWriter, req *http.Request) {
		http.NotFound(w, req)
	})

	// favicon at the root (browsers auto-fetch /favicon.ico).
	r.Get("/favicon.svg", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "image/svg+xml")
		_, _ = w.Write(webroot.FaviconSVG)
	})

	// SPA fallback: any other GET (non-API, non-asset) gets the
	// SvelteKit-rendered index.html so client-side routes survive
	// a hard refresh. POST/PUT/DELETE outside /api/* get a 404 —
	// the SPA never issues those outside the API gateway.
	r.Get("/*", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(webroot.IndexHTML)
	})
	return nil
}

// readAll reads a fs.File into a byte slice. fs.File doesn't have a
// io.ReadSeeker convenience method we can use directly, so this is a
// tiny helper instead of pulling in bytes.Buffer or io.Copy.
func readAll(f fs.File) []byte {
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 4096)
	for {
		n, err := f.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if err != nil {
			return buf
		}
	}
}

// setContentTypeByExt sets a Content-Type based on the file
// extension. Falls back to application/octet-stream for unknown
// extensions. Keeps the SvelteKit-emitted .js (text/javascript),
// .css (text/css), and .svg (image/svg+xml) serving as a real
// browser would expect.
func setContentTypeByExt(w http.ResponseWriter, path string) {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".js":
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	case ".mjs":
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	case ".css":
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
	case ".svg":
		w.Header().Set("Content-Type", "image/svg+xml")
	case ".json":
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	case ".woff2":
		w.Header().Set("Content-Type", "font/woff2")
	case ".txt":
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	case ".html":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
	default:
		w.Header().Set("Content-Type", "application/octet-stream")
	}
}
