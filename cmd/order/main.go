// Order Service binary — wiring lives in services/order/cmd/order
// so it can access the service's internal packages; this top-level
// cmd is just the binary entry point.
package main

import "github.com/t0pm1x/orderflow/services/order/cmd/order"

func main() {
	order.Main()
}
