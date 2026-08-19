// Package handlers holds the HTTP route surface for the orderflow-web
// UI. Pages render via html/template fragments composed into
// templates/layout.html; actions take form posts + return either an
// htmx fragment or a 303 redirect. Construct a *Set once in
// internal/web.Main() with NewSet, then mount Routes on a chi router.
package handlers

import (
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/t0pm1x/orderflow/services/web/internal/backend"
	"github.com/t0pm1x/orderflow/services/web/internal/events"
	"github.com/t0pm1x/orderflow/services/web/internal/templates"
)

// Set holds dependencies for page + action handlers. Construct once
// in main, then call Routes(r) to mount everything on a chi router.
// Subsequent tasks extend this struct with new fields and handlers.
type Set struct {
	Order     backend.OrderClient
	Payment   backend.PaymentClient
	Inventory backend.InventoryClient
	Bus       *events.Bus
	Templates *template.Template
	// Logger receives structured upstream-error context (status,
	// body, URL) so operators can diagnose without exposing the
	// payload in the user-facing banner. mapUpstreamError writes
	// here; nil falls back to slog.Default() so defensive callers
	// never panic.
	Logger *slog.Logger
	// replays is the in-process replay-guard cache shared by all
	// mutating action handlers (POST /v1/orders, /v1/orders/{id},
	// /payments/sim/fire). Initialized in NewSet so handlers can
	// always assume non-nil; tests get a fresh cache per Set.
	replays *replayCache
	// EventsEnabled is true when the Kafka tail goroutine has
	// actually started. When false (KAFKA_BROKERS unset, etc.),
	// the layout renders a "Live events: disconnected" banner and
	// the SSE endpoint short-circuits with 503. Set by the caller
	// after construction via SetEventsEnabled.
	EventsEnabled bool
}

// SetEventsEnabled toggles whether the SSE endpoint serves an event
// stream and whether the layout shows the "Live events" sidebar.
// Wired by services/web after kafkatail.Start reports whether a tail
// goroutine was actually launched.
func (s *Set) SetEventsEnabled(b bool) {
	s.EventsEnabled = b
}

// NewSet builds a Set with the layout + body templates parsed once.
// The full set of body templates is registered incrementally across
// Tasks 5-10; this constructor only needs the two files referenced
// by the orders-list page (Task 5). The logger is for structured
// upstream-error logging; pass slog.Default() from main.
func NewSet(order backend.OrderClient, payment backend.PaymentClient,
	inventory backend.InventoryClient, bus *events.Bus, logger *slog.Logger) *Set {
	if logger == nil {
		logger = slog.Default()
	}
	t := template.New("").Funcs(template.FuncMap{"timeAgo": timeAgo})
	t = template.Must(t.ParseFS(templates.FS, "layout.html", "_icons.html", "orders_list.html", "order_hero.html", "order_new.html", "order_detail.html", "order_events.html", "inventory.html", "payments.html"))
	return &Set{
		Order:     order,
		Payment:   payment,
		Inventory: inventory,
		Bus:       bus,
		Templates: t,
		Logger:    logger,
		replays:   newReplayCache(),
	}
}

// Routes registers all page + action routes on r. Tasks 6+ add
// methods on *Set for the routes listed below; until those tasks
// land, those handlers don't exist yet and chi will panic at startup
// if you mount this against an empty Set. Wire Set.Routes only after
// the corresponding handler method exists on the type.
//
// Routes wires the orderflow-web playground's HTTP routes on a chi
// router: 6 GET pages + 3 POST actions.
func (s *Set) Routes(r chi.Router) {
	r.Get("/", s.PageOrdersList)
	r.Get("/orders/new", s.PageOrderNew)
	r.Get("/orders/{id}", s.PageOrderDetail)
	r.Get("/orders/{id}/events", s.PageOrderEvents)
	r.Get("/inventory", s.PageInventory)
	r.Get("/payments/sim", s.PagePaymentsSim)
	r.Get("/events/stream", s.PageEventsStream)
	r.Post("/v1/orders", s.ActionOrderSubmit)
	r.Post("/v1/orders/{id}", s.ActionOrderCancel)
	r.Post("/payments/sim/fire", s.ActionPaymentsFire)
}

// ordersListVM is the view model for the GET / page.
type ordersListVM struct {
	Body          string
	Orders        []backend.Order
	BackendDown   bool
	Error         string
	EventsEnabled bool
}

// PageOrdersList serves GET / (orders list). On backend failure it
// still returns 200 with a banner so the rest of the layout (navbar,
// live-events sidebar) stays usable. When called with ?frag=1 the
// handler renders only the body fragment (no layout shell) so htmx
// polling can swap just the page-content region.
func (s *Set) PageOrdersList(w http.ResponseWriter, r *http.Request) {
	var vm ordersListVM
	vm.Body = "ordersListBody"
	vm.EventsEnabled = s.EventsEnabled
	list, err := s.Order.List(r.Context(), "", 50)
	if err != nil {
		msg, _ := mapUpstreamError(s.Logger, "GET /v1/orders", err)
		vm.BackendDown = true
		vm.Error = msg
	} else {
		vm.Orders = list.Items
	}
	if r.URL.Query().Get("frag") == "1" {
		s.renderPageFrag(w, "ordersListBody", vm)
		return
	}
	s.renderPage(w, vm)
}

// renderFragment executes the named body template only (no layout
// shell). Used by htmx polling: the poller fetches the same page
// path with ?frag=1 and hx-swap targets the inner #page-content div,
// so the browser gets back just the inner HTML and not the whole
// <html> shell.
func renderFragment(w http.ResponseWriter, t *template.Template, name string, vm any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := t.ExecuteTemplate(w, name, vm); err != nil {
		// Bubble up to the network error path — the page-status code
		// is already set by the caller, so the caller must log with
		// its own logger. We still map to a generic 500 here so the
		// browser doesn't get a half-rendered fragment.
		http.Error(w, "rendering failed", http.StatusInternalServerError)
	}
}

// renderPage executes the layout template with the given view model
// and the standard text/html content-type. Errors are logged with
// the set's logger and surfaced as a generic 500 so the caller
// doesn't need to repeat the header/log/execute/error boilerplate
// at every call site.
func (s *Set) renderPage(w http.ResponseWriter, vm any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.Templates.ExecuteTemplate(w, "layout", vm); err != nil {
		s.Logger.Error("template execute failed", "err", err)
		http.Error(w, "rendering failed", http.StatusInternalServerError)
	}
}

// renderPageFrag is the thin *Set method wrapper around the
// package-level renderFragment helper. Lets page handlers stay
// symmetric with renderPage (both are methods on *Set) while the
// shared renderFragment stays available for any non-method call
// sites that need it.
func (s *Set) renderPageFrag(w http.ResponseWriter, name string, vm any) {
	renderFragment(w, s.Templates, name, vm)
}

// timeAgo renders a time.Time as a compact "Ns/m/h/d ago" string
// for the order-detail page header. Band edges: < 1m -> seconds,
// < 1h -> minutes, < 24h -> hours, else whole days. Used as a
// html/template func (registered in NewSet via t.Funcs) so the
// template can call {{timeAgo .Order.CreatedAt}} next to a
// title= attribute carrying the full timestamp for hover detail.
func timeAgo(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}
