// Payment: mock card processor with idempotency-key table and webhook receiver.
package main

import "fmt"

var version = "0.0.0-dev"

func main() {
	fmt.Printf("orderflow-payment version %s\n", version)
}
