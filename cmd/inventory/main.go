// Inventory: stock reservation with optimistic locking and Redis-backed reservation TTL.
package main

import "fmt"

var version = "0.0.0-dev"

func main() {
	fmt.Printf("orderflow-inventory version %s\n", version)
}
