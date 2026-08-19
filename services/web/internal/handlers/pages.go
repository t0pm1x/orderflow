package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	pkgEvents "github.com/t0pm1x/orderflow/platform/events"
	"github.com/t0pm1x/orderflow/services/web/internal/backend"
)

type orderNewVM struct {
	Body            string
	SKU             string
	Quantity        int
	UnitPriceCents  int64
	CustomerID      string
	IdempotencyToken string
	Error           string
}

// PageOrderNew serves GET /orders/new — renders the create-order
// form. Always 200; the form is empty until the user submits.
// Generates a fresh idempotency token on every render so the
// resulting POST is unique; the BFF replay cache catches replays
// within the replay window.
func (s *Set) PageOrderNew(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = s.Templates.ExecuteTemplate(w, "layout", orderNewVM{
		Body:             "orderNewBody",
		IdempotencyToken: newIdempotencyToken(),
	})
}

// ActionOrderSubmit serves POST /v1/orders — form submission for
// the create-order page. On success it returns HX-Redirect to
// /orders/{id}; on failure it re-renders the form with an Error
// banner (4xx for validation, 502 for upstream). The handler
// rejects forms that omit the `idempotency_token` field (400),
// rejects repeat submissions of the same token within the
// replay window (409), and rejects non-UUID `customer_id`
// values (400).
func (s *Set) ActionOrderSubmit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	vm := orderNewVM{
		Body:             "orderNewBody",
		SKU:              r.FormValue("sku"),
		Quantity:         atoi(r.FormValue("quantity")),
		CustomerID:       r.FormValue("customer_id"),
		IdempotencyToken: newIdempotencyToken(),
	}
	if up := r.FormValue("unit_price_cents"); up != "" {
		vm.UnitPriceCents, _ = strconv.ParseInt(up, 10, 64)
	}
	if vm.SKU == "" || vm.Quantity <= 0 {
		vm.Error = "SKU and quantity (>0) are required"
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		_ = s.Templates.ExecuteTemplate(w, "layout", vm)
		return
	}
	if vm.CustomerID != "" {
		if _, ok := parseUUID(vm.CustomerID); !ok {
			vm.Error = "customer_id must be a UUID (or leave blank for auto-generation)"
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusBadRequest)
			_ = s.Templates.ExecuteTemplate(w, "layout", vm)
			return
		}
	}
	token := r.FormValue("idempotency_token")
	if token == "" {
		http.Error(w, "missing idempotency token", http.StatusBadRequest)
		return
	}
	if s.replays.check(token) {
		http.Error(w, "duplicate submission", http.StatusConflict)
		return
	}
	in := backend.OrderSubmit{Items: []backend.OrderItem{{SKU: vm.SKU, Quantity: vm.Quantity}}}
	if vm.UnitPriceCents > 0 {
		c := vm.UnitPriceCents
		in.Items[0].UnitPriceCents = &c
	}
	// customer_id is required by the order service; auto-generate a
	// random UUID when the form is left blank (the placeholder text
	// promises this). Otherwise echo what the user typed.
	if vm.CustomerID == "" {
		auto := uuid.NewString()
		in.CustomerID = &auto
	} else {
		in.CustomerID = &vm.CustomerID
	}
	in.IdempotencyKey = token
	out, err := s.Order.Submit(r.Context(), in)
	if err != nil {
		msg, status := mapUpstreamError(s.Logger, "POST /v1/orders", err)
		vm.Error = msg
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(status)
		_ = s.Templates.ExecuteTemplate(w, "layout", vm)
		return
	}
	w.Header().Set("HX-Redirect", "/orders/"+out.ID)
	w.WriteHeader(http.StatusOK)
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

// parseUUID validates that s is a syntactically valid UUID and
// returns it unchanged on success. Used at every trust boundary
// (form inputs, path params, query params) that downstream code
// interpolates into an upstream URL path — keeps `uuid.Parse` usage
// symmetric and avoids re-validating on the caller side.
func parseUUID(s string) (string, bool) {
	if _, err := uuid.Parse(s); err != nil {
		return "", false
	}
	return s, true
}

type orderDetailVM struct {
	Body             string
	Order            *backend.Order
	BackendDown      bool
	Error            string
	Events           []pkgEvents.Envelope
	IdempotencyToken string
}

// PageOrderDetail serves GET /orders/{id}. It rejects non-UUID
// ids with 400 before hitting the backend; on a valid id with
// upstream failure it returns 404 (not found / unreachable) with
// the layout shell + banner so the navbar stays usable. When
// called with ?frag=1 the handler renders only the body fragment
// (no layout shell) so htmx polling can swap just the
// page-content region.
// PageOrderDetail serves GET /orders/{id} — renders the order
// detail page. While the order is non-terminal the page polls
// itself every 1s via htmx; otherwise it renders once.
func (s *Set) PageOrderDetail(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if _, ok := parseUUID(id); !ok {
		http.Error(w, "order id must be a UUID", http.StatusBadRequest)
		return
	}
	vm := orderDetailVM{Body: "orderDetailBody", IdempotencyToken: newIdempotencyToken()}
	o, err := s.Order.Get(r.Context(), id)
	if err != nil {
		msg, status := mapUpstreamError(s.Logger, "GET /v1/orders/{id}", err)
		vm.BackendDown = true
		vm.Error = msg
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(status)
		if r.URL.Query().Get("frag") == "1" {
			renderFragment(w, s.Templates, "orderDetailBody", vm)
			return
		}
		_ = s.Templates.ExecuteTemplate(w, "layout", vm)
		return
	}
	vm.Order = o
	vm.Events = s.Bus.History(o.ID)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if r.URL.Query().Get("frag") == "1" {
		renderFragment(w, s.Templates, "orderDetailBody", vm)
		return
	}
	if err := s.Templates.ExecuteTemplate(w, "layout", vm); err != nil {
		s.Logger.Error("template execute failed", "route", "GET /orders/{id}", "template", "layout", "err", err)
		http.Error(w, "rendering failed", http.StatusInternalServerError)
	}
}

// orderEventsVM is the view model for the per-order saga timeline.
// Body points the layout at the orderEventsBody template; OrderID
// is echoed into the DOM id so the hx-get target is unique per page;
// Events is the bus history snapshot for the requested aggregate.
type orderEventsVM struct {
	Body    string
	OrderID string
	Events  []pkgEvents.Envelope
}

// PageOrderEvents serves GET /orders/{id}/events — the per-order
// saga timeline fragment. On a non-UUID id the timeline is empty
// (the OG handler treats any path tail as a valid id; we refuse
// here so a stray path doesn't waste a Bus.History scan). With
// ?frag=1 the handler renders only the inner<div> so it can be
// hx-polled by the order-detail page's timeline block.
func (s *Set) PageOrderEvents(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	vm := orderEventsVM{Body: "orderEventsBody", OrderID: id}
	if _, err := uuid.Parse(id); err == nil {
		vm.Events = s.Bus.History(id)
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if r.URL.Query().Get("frag") == "1" {
		renderFragment(w, s.Templates, "orderEventsBody", vm)
		return
	}
	if err := s.Templates.ExecuteTemplate(w, "layout", vm); err != nil {
		s.Logger.Error("template execute failed", "route", "GET /orders/{id}/events", "template", "layout", "err", err)
		http.Error(w, "rendering failed", http.StatusInternalServerError)
	}
}

// ActionOrderCancel serves POST /v1/orders/{id} — form submission
// for the Cancel button on the order detail page. On success
// returns HX-Redirect to /orders/{id}; upstream 401/404 surface
// as 404 BFF (cancel can't proceed if the order doesn't exist),
// 5xx + transport errors stay 502. The handler requires an
// `idempotency_token` form field and rejects repeat submissions
// within the replay window with 409, and rejects non-UUID {id}
// path params with 400.
func (s *Set) ActionOrderCancel(w http.ResponseWriter, r *http.Request) {
	token := r.FormValue("idempotency_token")
	if token == "" {
		http.Error(w, "missing idempotency token", http.StatusBadRequest)
		return
	}
	if s.replays.check(token) {
		http.Error(w, "duplicate submission", http.StatusConflict)
		return
	}
	id := chi.URLParam(r, "id")
	if _, ok := parseUUID(id); !ok {
		http.Error(w, "order id must be a UUID", http.StatusBadRequest)
		return
	}
	if err := s.Order.Cancel(r.Context(), id); err != nil {
		msg, status := mapUpstreamError(s.Logger, "DELETE /v1/orders/{id}", err)
		if status == http.StatusNotFound {
			// Cancel can't proceed if the order doesn't exist; fold
			// 404 upstream into BFF 404 regardless of the helper's
			// status (which keeps 404 as 404 in the map).
			http.Error(w, msg, http.StatusNotFound)
			return
		}
		http.Error(w, msg, status)
		return
	}
	w.Header().Set("HX-Redirect", "/orders/"+id)
	w.WriteHeader(http.StatusOK)
}

type inventoryRow struct {
	SKU       string
	Available int
	Reserved  int
	Version   int64
	Missing   bool
}

type inventoryVM struct {
	Body        string
	Rows        []inventoryRow
	BackendDown bool
	Error       string
}

// PageInventory serves GET /inventory. The inventory service only
// exposes per-SKU reads, so the SKU list is derived from the most
// recent orders' items (List, limit 50). Missing/unknown SKUs show
// as "—" so the page still surfaces order-side activity even when
// the inventory backend has gaps. When called with ?frag=1 the
// handler renders only the body fragment (no layout shell) so htmx
// polling can swap just the page-content region.
// PageInventory serves GET /inventory — the per-SKU stock viewer.
// SKU list is derived from the most recent orders' items; missing
// inventory rows render as em-dashes (Missing: true).
func (s *Set) PageInventory(w http.ResponseWriter, r *http.Request) {
	vm := inventoryVM{Body: "inventoryBody"}
	list, err := s.Order.List(r.Context(), "", 50)
	if err != nil {
		msg, _ := mapUpstreamError(s.Logger, "GET /v1/orders (inventory)", err)
		vm.BackendDown = true
		vm.Error = msg
	} else {
		seen := make(map[string]struct{})
		for _, o := range list.Items {
			for _, it := range o.Items {
				if it.SKU == "" {
					continue
				}
				if _, ok := seen[it.SKU]; ok {
					continue
				}
				seen[it.SKU] = struct{}{}
				row := inventoryRow{SKU: it.SKU}
				stock, gerr := s.Inventory.GetStock(r.Context(), it.SKU)
				if gerr != nil || stock == nil {
					row.Missing = true
				} else {
					row.Available = stock.Available
					row.Reserved = stock.Reserved
					row.Version = stock.Version
				}
				vm.Rows = append(vm.Rows, row)
			}
		}
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if r.URL.Query().Get("frag") == "1" {
		renderFragment(w, s.Templates, "inventoryBody", vm)
		return
	}
	if err := s.Templates.ExecuteTemplate(w, "layout", vm); err != nil {
		s.Logger.Error("template execute failed", "route", "GET /inventory", "template", "layout", "err", err)
		http.Error(w, "rendering failed", http.StatusInternalServerError)
	}
}

type paymentsSimVM struct {
	Body        string
	InFlight    []paymentsRow
	BackendDown bool
	Error       string
}

// paymentsRow is the per-order view model for the payments
// simulator table. Each row carries its own per-form tokens so
// a double-click on force ✓ cannot trigger a duplicate force ✗
// (and vice-versa) — the form-render contract is "one token per
// submit button, valid for the lifetime of this page render".
type paymentsRow struct {
	ID            string
	State         backend.OrderState
	TokenForceOK  string
	TokenForceFail string
}

// PagePaymentsSim serves GET /payments/sim. Lists in-flight orders
// (state=pending + state=reserved) so the operator can fire a webhook
// for any of them. Both lists are queried independently so a partial
// failure (one state errors out) still surfaces the other state.
// BackendDown is only set when both queries fail. Errors are routed
// through mapUpstreamError so the upstream payload is logged
// server-side and the operator sees a safe, context-appropriate
// message (4xx for bad filter, 5xx "try again", transport "check
// connection") rather than a hardcoded "temporarily unavailable"
// for every failure mode.
// PagePaymentsSim serves GET /payments/sim — the payment-webhook
// simulator. Lists orders in pending or reserved state so an
// operator can fire a force-success or force-fail webhook.
func (s *Set) PagePaymentsSim(w http.ResponseWriter, r *http.Request) {
	pending, perr := s.Order.List(r.Context(), backend.OrderStatePending, 50)
	reserved, rerr := s.Order.List(r.Context(), backend.OrderStateReserved, 50)
	vm := paymentsSimVM{Body: "paymentsSimBody"}
	addRows := func(items []backend.Order) {
		for _, o := range items {
			vm.InFlight = append(vm.InFlight, paymentsRow{
				ID:             o.ID,
				State:          o.State,
				TokenForceOK:   newIdempotencyToken(),
				TokenForceFail: newIdempotencyToken(),
			})
		}
	}
	if perr == nil && pending != nil {
		addRows(pending.Items)
	}
	if rerr == nil && reserved != nil {
		addRows(reserved.Items)
	}
	switch {
	case perr != nil && rerr != nil:
		msg, _ := mapUpstreamError(s.Logger, "GET /v1/orders (payments-sim: pending)", perr)
		if alt, _ := mapUpstreamError(s.Logger, "GET /v1/orders (payments-sim: reserved)", rerr); alt != "" && msg == "" {
			msg = alt
		}
		vm.BackendDown = true
		vm.Error = msg
	case perr != nil:
		msg, _ := mapUpstreamError(s.Logger, "GET /v1/orders (payments-sim: pending)", perr)
		vm.Error = msg
	case rerr != nil:
		msg, _ := mapUpstreamError(s.Logger, "GET /v1/orders (payments-sim: reserved)", rerr)
		vm.Error = msg
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.Templates.ExecuteTemplate(w, "layout", vm); err != nil {
		s.Logger.Error("template execute failed", "route", "GET /payments/sim", "template", "layout", "err", err)
		http.Error(w, "rendering failed", http.StatusInternalServerError)
	}
}

// ActionPaymentsFire serves POST /payments/sim/fire. Builds a
// PaymentWebhook from form fields (order_id, status, error_code) and
// proxies to PaymentClient.FireWebhook. The payment_id is set to
// order_id so the payment mock's replay guard accepts repeat fires
// for the same order. Responds with HX-Redirect to /payments/sim so
// htmx reloads the page after the mutation. Backend failures surface
// as 502 with the error body.
// ActionPaymentsFire serves POST /payments/sim/fire — the
// form submission behind the force ✓/force ✗ buttons. Builds a
// PaymentWebhook from the form and proxies to the Payment service.
// Sets a deterministic Idempotency-Key so the upstream's
// idempotency cache dedupes identical replays. The handler also
// rejects missing/duplicate `idempotency_token` (BFF-level replay
// guard, see ActionOrderSubmit) and rejects non-UUID `order_id`
// form values with 400.
func (s *Set) ActionPaymentsFire(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	token := r.FormValue("idempotency_token")
	if token == "" {
		http.Error(w, "missing idempotency token", http.StatusBadRequest)
		return
	}
	if s.replays.check(token) {
		http.Error(w, "duplicate submission", http.StatusConflict)
		return
	}
	orderID := r.FormValue("order_id")
	status := r.FormValue("status")
	errorCode := r.FormValue("error_code")
	if orderID == "" || (status != "succeeded" && status != "failed") {
		http.Error(w, "order_id and status required", http.StatusBadRequest)
		return
	}
	if _, ok := parseUUID(orderID); !ok {
		http.Error(w, "order_id must be a UUID", http.StatusBadRequest)
		return
	}
	wh := backend.PaymentWebhook{
		PaymentID: orderID, // deterministic on order_id (idempotent in mock)
		Status:    status,
		ErrorCode: errorCode,
	}
	if err := s.Payment.FireWebhook(r.Context(), wh); err != nil {
		msg, status := mapUpstreamError(s.Logger, "POST /payments/sim/fire", err)
		http.Error(w, msg, status)
		return
	}
	w.Header().Set("HX-Redirect", "/payments/sim")
	w.WriteHeader(http.StatusOK)
}

// PageEventsStream serves GET /events/stream as Server-Sent Events.
// It subscribes to s.Bus and emits one `event: <type>\ndata: <json>\n\n`
// line per envelope. A 15s heartbeat keeps proxies from idling the
// connection; ctx.Done() is honored for client disconnect.
// PageEventsStream serves GET /events/stream — Server-Sent Events
// subscribed to the in-process event bus. The Kafka tail goroutine
// publishes each Kafka envelope onto the bus; this handler
// relays them to the browser with a 15s heartbeat.
func (s *Set) PageEventsStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch, unsub := s.Bus.Subscribe()
	defer unsub()

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	if _, err := fmt.Fprintf(w, ": connected\n\n"); err != nil {
		return
	}
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			if _, err := fmt.Fprintf(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case ev, ok := <-ch:
			if !ok {
				return
			}
			data, err := json.Marshal(ev.Envelope)
			if err != nil {
				continue
			}
			if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Envelope.EventType, data); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
