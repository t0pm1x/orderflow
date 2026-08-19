// Package server wires the orderflow-web HTTP server: chi router,
// shared middleware, route registration, and graceful shutdown.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"

	mw "github.com/t0pm1x/orderflow/platform/middleware"
	"github.com/t0pm1x/orderflow/services/web/internal/handlers"
	"github.com/t0pm1x/orderflow/services/web/internal/static"
)

// Options controls server behavior.
type Options struct {
	Name         string
	Logger       *slog.Logger
	OrderURL     string
	PaymentURL   string
	InventoryURL string
	Handlers     *handlers.Set
}

// Server hosts the HTTP listener. One instance per process.
type Server struct {
	opt    Options
	srv    *http.Server
	addr   atomic.Value // string
	styles []byte       // cached styles.css, read once in New
}

// New creates a non-listening Server. Call Start to bind + serve.
func New(opt Options) *Server {
	// styles.css is requested on every page render; read it once at
	// startup instead of paying an embed.FS.ReadFile per request.
	data, _ := static.FS.ReadFile("styles.css")
	return &Server{opt: opt, styles: data}
}

// Addr returns the bound address (host:port) or "" if Start has not
// completed.
func (s *Server) Addr() string {
	v, _ := s.addr.Load().(string)
	return v
}

// Start binds the listener and serves until ctx is cancelled.
func (s *Server) Start(ctx context.Context, addr string) error {
	r := chi.NewRouter()
	r.Use(mw.Stack(s.opt.Name, s.opt.Logger)...)

	// Probes
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	r.Get("/readyz", func(w http.ResponseWriter, req *http.Request) {
		// /readyz succeeds iff Order, Payment, and Inventory upstreams
		// all answer /healthz with a 2xx within 2s. Probes run in
		// parallel so a single dead upstream doesn't serialize the
		// whole check. Failures => 503 + JSON listing failed URLs.
		urls := []string{
			s.opt.OrderURL + "/healthz",
			s.opt.PaymentURL + "/healthz",
			s.opt.InventoryURL + "/healthz",
		}
		failed := pingUpstreams(req.Context(), urls)
		w.Header().Set("Content-Type", "application/json")
		if len(failed) > 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(struct {
				Status string   `json:"status"`
				Failed []string `json:"failed"`
			}{Status: "down", Failed: failed})
			return
		}
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	// Static assets (CSS, vendored JS). Mounted before the handler
	// set so the layout.html <link rel=stylesheet href=/static/styles.css>
	// and <script src=/static/vendor/htmx.min.js> resolve on every page.
	// styles.css is served from a startup-time cache to avoid a
	// per-request embed.FS.ReadFile; everything else falls through
	// to the generic /static/* handler which reads from embed.FS.
	r.Get("/static/styles.css", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
		_, _ = w.Write(s.styles)
	})
	r.Get("/static/*", func(w http.ResponseWriter, req *http.Request) {
		p := strings.TrimPrefix(req.URL.Path, "/static/")
		data, err := static.FS.ReadFile(p)
		if err != nil {
			http.NotFound(w, req)
			return
		}
		contentType := mime.TypeByExtension(filepath.Ext(p))
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		w.Header().Set("Content-Type", contentType+"; charset=utf-8")
		_, _ = w.Write(data)
	})

	if s.opt.Handlers != nil {
		s.opt.Handlers.Routes(r)
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	s.addr.Store(ln.Addr().String())

	s.srv = &http.Server{
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.srv.Serve(ln); err != nil && err != http.ErrServerClosed {
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
	go func() { wg.Wait(); close(wgDone) }()

	select {
	case <-wgDone:
	case <-shutdownCtx.Done():
		return shutdownCtx.Err()
	}
	return nil
}
