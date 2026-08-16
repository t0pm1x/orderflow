// Inventory Service binary — wiring lives in services/inventory/cmd/inventory.
package main

import "github.com/t0pm1x/orderflow/services/inventory/cmd/inventory"

func main() {
	inventory.Main()
}
