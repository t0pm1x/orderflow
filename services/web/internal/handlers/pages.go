package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/t0pm1x/orderflow/services/web/internal/backend"
)

type orderNewVM struct {
	Body           string
	SKU            string
	Quantity       int
	UnitPriceCents int64
	CustomerID     string
	Error          string
}

func (s *Set) PageOrderNew(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = s.Templates.ExecuteTemplate(w, "layout", orderNewVM{Body: "orderNewBody"})
}

func (s *Set) ActionOrderSubmit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", 400)
		return
	}
	vm := orderNewVM{
		Body:       "orderNewBody",
		SKU:        r.FormValue("sku"),
		Quantity:   atoi(r.FormValue("quantity")),
		CustomerID: r.FormValue("customer_id"),
	}
	if up := r.FormValue("unit_price_cents"); up != "" {
		vm.UnitPriceCents, _ = strconv.ParseInt(up, 10, 64)
	}
	if vm.SKU == "" || vm.Quantity <= 0 {
		vm.Error = "SKU and quantity (>0) are required"
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(400)
		_ = s.Templates.ExecuteTemplate(w, "layout", vm)
		return
	}
	in := backend.OrderSubmit{Items: []backend.OrderItem{{SKU: vm.SKU, Quantity: vm.Quantity}}}
	if vm.UnitPriceCents > 0 {
		c := vm.UnitPriceCents
		in.Items[0].UnitPriceCents = &c
	}
	if vm.CustomerID != "" {
		in.CustomerID = &vm.CustomerID
	}
	out, err := s.Order.Submit(r.Context(), in)
	if err != nil {
		vm.Error = "Order service error: " + err.Error()
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(502)
		_ = s.Templates.ExecuteTemplate(w, "layout", vm)
		return
	}
	w.Header().Set("HX-Redirect", "/orders/"+out.ID)
	w.WriteHeader(200)
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

type orderDetailVM struct {
	Body        string
	Order       *backend.Order
	BackendDown bool
	Error       string
}

// PageOrderDetail serves GET /orders/{id}. On backend failure it
// returns 404 (id not found / unreachable) with the layout shell +
// banner so the navbar stays usable.
func (s *Set) PageOrderDetail(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	vm := orderDetailVM{Body: "orderDetailBody"}
	o, err := s.Order.Get(r.Context(), id)
	if err != nil {
		vm.BackendDown = true
		vm.Error = err.Error()
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusNotFound)
		_ = s.Templates.ExecuteTemplate(w, "layout", vm)
		return
	}
	vm.Order = o
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.Templates.ExecuteTemplate(w, "layout", vm); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// ActionOrderCancel serves POST /v1/orders/{id}. Wraps OrderClient.Cancel
// and returns HX-Redirect to /orders/{id} so the page reloads after
// the mutation. Backend failures surface as 502 with the error body.
func (s *Set) ActionOrderCancel(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := s.Order.Cancel(r.Context(), id); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
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
// the inventory backend has gaps.
func (s *Set) PageInventory(w http.ResponseWriter, r *http.Request) {
	vm := inventoryVM{Body: "inventoryBody"}
	list, err := s.Order.List(r.Context(), "", 50)
	if err != nil {
		vm.BackendDown = true
		vm.Error = err.Error()
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
	if err := s.Templates.ExecuteTemplate(w, "layout", vm); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type paymentsSimVM struct {
	Body        string
	InFlight    []backend.Order
	BackendDown bool
	Error       string
}

// PagePaymentsSim serves GET /payments/sim. Lists in-flight orders
// (state=pending + state=reserved) so the operator can fire a webhook
// for any of them. Both lists are queried independently so a partial
// failure (one state errors out) still surfaces the other state.
// BackendDown is only set when both queries fail.
func (s *Set) PagePaymentsSim(w http.ResponseWriter, r *http.Request) {
	pending, _ := s.Order.List(r.Context(), backend.OrderStatePending, 50)
	reserved, _ := s.Order.List(r.Context(), backend.OrderStateReserved, 50)
	vm := paymentsSimVM{Body: "paymentsSimBody"}
	if pending == nil && reserved == nil {
		vm.BackendDown = true
		vm.Error = "Order service unavailable"
	}
	if pending != nil {
		vm.InFlight = append(vm.InFlight, pending.Items...)
	}
	if reserved != nil {
		vm.InFlight = append(vm.InFlight, reserved.Items...)
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.Templates.ExecuteTemplate(w, "layout", vm); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// ActionPaymentsFire serves POST /payments/sim/fire. Builds a
// PaymentWebhook from form fields (order_id, status, error_code) and
// proxies to PaymentClient.FireWebhook. The payment_id is set to
// order_id so the payment mock's replay guard accepts repeat fires
// for the same order. Responds with HX-Redirect to /payments/sim so
// htmx reloads the page after the mutation. Backend failures surface
// as 502 with the error body.
func (s *Set) ActionPaymentsFire(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	orderID := r.FormValue("order_id")
	status := r.FormValue("status")
	errorCode := r.FormValue("error_code")
	if orderID == "" || (status != "succeeded" && status != "failed") {
		http.Error(w, "order_id and status required", http.StatusBadRequest)
		return
	}
	wh := backend.PaymentWebhook{
		PaymentID: orderID, // deterministic on order_id (idempotent in mock)
		Status:    status,
		ErrorCode: errorCode,
	}
	if err := s.Payment.FireWebhook(r.Context(), wh); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("HX-Redirect", "/payments/sim")
	w.WriteHeader(http.StatusOK)
}

// PageEventsStream serves GET /events/stream as Server-Sent Events.
// It subscribes to s.Bus and emits one `event: <type>\ndata: <json>\n\n`
// line per envelope. A 15s heartbeat keeps proxies from idling the
// connection; ctx.Done() is honored for client disconnect.
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
