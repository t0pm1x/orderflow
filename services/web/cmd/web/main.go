// Package web is the web Service binary entry point. Mirrors
// services/order/cmd/order/main.go (package order, Main() func)
// so cmd/web/main.go can delegate to it.
package web

import internalweb "github.com/t0pm1x/orderflow/services/web/internal/web"

// Main is the function called by cmd/web/main.go.
func Main() {
	internalweb.Main()
}
