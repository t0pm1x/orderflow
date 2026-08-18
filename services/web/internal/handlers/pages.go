package handlers

import (
	"net/http"
	"strconv"

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
