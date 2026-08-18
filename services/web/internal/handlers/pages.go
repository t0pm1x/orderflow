package handlers

import (
	"net/http"
	"strconv"

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
