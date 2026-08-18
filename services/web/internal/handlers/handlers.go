// Package handlers holds the HTTP route surface for the orderflow-web
// UI. Pages render via html/template fragments composed into
// templates/layout.html; actions take form posts + return either an
// htmx fragment or a 303 redirect. Construct a *Set once in
// internal/web.Main() with NewSet, then mount Routes on a chi router.
package handlers

import (
	"html/template"
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
}

// NewSet builds a Set with the layout + body templates parsed once.
// The full set of body templates is registered incrementally across
// Tasks 5-10; this constructor only needs the two files referenced
// by the orders-list page (Task 5).
func NewSet(order backend.OrderClient, payment backend.PaymentClient,
	inventory backend.InventoryClient, bus *events.Bus) *Set {
	t := template.Must(template.ParseFS(templates.FS, "layout.html", "orders_list.html", "order_new.html", "order_detail.html", "inventory.html", "payments.html"))
	return &Set{
		Order:     order,
		Payment:   payment,
		Inventory: inventory,
		Bus:       bus,
		Templates: t,
	}
}

// Routes registers all page + action routes on r. Tasks 6+ add
// methods on *Set for the routes listed below; until those tasks
// land, those handlers don't exist yet and chi will panic at startup
// if you mount this against an empty Set. Wire Set.Routes only after
// the corresponding handler method exists on the type.
func (s *Set) Routes(r chi.Router) {
	r.Get("/", s.PageOrdersList)
	r.Get("/orders/new", s.PageOrderNew)
	r.Get("/orders/{id}", s.PageOrderDetail)
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
// live-events sidebar) stays usable.
func (s *Set) PageOrdersList(w http.ResponseWriter, r *http.Request) {
	var vm ordersListVM
	vm.Body = "ordersListBody"
	list, err := s.Order.List(r.Context(), "", 50)
	if err != nil {
		vm.BackendDown = true
		vm.Error = err.Error()
	} else {
		vm.Orders = list.Items
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.Templates.ExecuteTemplate(w, "layout", vm); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}