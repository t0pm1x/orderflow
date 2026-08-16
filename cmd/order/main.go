// Order: accepts POST /v1/orders, owns the order aggregate, publishes OrderCreated events via outbox.
package main

import "fmt"

var version = "0.0.0-dev"

func main() {
	fmt.Printf("orderflow-order version %s\n", version)
}
