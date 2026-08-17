// Saga Service binary — wiring lives in services/saga/cmd/saga so
// it can access the service's internal packages; this top-level
// cmd is just the binary entry point.
package main

import "github.com/t0pm1x/orderflow/services/saga/cmd/saga"

func main() {
	saga.Main()
}
