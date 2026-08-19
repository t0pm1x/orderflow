// Package handlers holds the HTTP route surface for the orderflow-web
// UI. Pages render via html/template fragments composed into
// templates/layout.html; actions take form posts + return either an
// htmx fragment or a 303 redirect. Construct a *Set once in
// internal/web.Main() with NewSet, then mount Routes on a chi router.
package handlers

import (
	"html/template"
	"log/slog"
	"net/http"

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
	t := template.Must(template.ParseFS(templates.FS, "layout.html", "orders_list.html", "order_new.html", "order_detail.html", "order_events.html", "inventory.html", "payments.html"))
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
	Body        string
	Orders      []backend.Order
	BackendDown bool
	Error       string
}

// PageOrdersList serves GET / (orders list). On backend failure it
// still returns 200 with a banner so the rest of the layout (navbar,
// live-events sidebar) stays usable. When called with ?frag=1 the
// handler renders only the body fragment (no layout shell) so htmx
// polling can swap just the page-content region.
func (s *Set) PageOrdersList(w http.ResponseWriter, r *http.Request) {
	var vm ordersListVM
	vm.Body = "ordersListBody"
	list, err := s.Order.List(r.Context(), "", 50)
	if err != nil {
		msg, _ := mapUpstreamError(s.Logger, "GET /v1/orders", err)
		vm.BackendDown = true
		vm.Error = msg
	} else {
		vm.Orders = list.Items
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if r.URL.Query().Get("frag") == "1" {
		renderFragment(w, s.Templates, "ordersListBody", vm)
		return
	}
	if err := s.Templates.ExecuteTemplate(w, "layout", vm); err != nil {
		s.Logger.Error("template execute failed", "route", "GET /", "template", "layout", "err", err)
		http.Error(w, "rendering failed", http.StatusInternalServerError)
	}
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
