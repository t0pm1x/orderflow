// orderflow Inventory Service — stock reservation with optimistic locking
// + Redis-backed TTL reservations.
package main

import (
	"fmt"

	"github.com/t0pm1x/orderflow/platform"
)

var version = "0.0.0-dev"

func main() {
	logger := platform.NewLogger()
	logger.Info("orderflow-inventory starting", "version", version)

	// TODO: wire DB, Redis, REST API, lock package
	fmt.Printf("orderflow-inventory version %s\n", version)
}
