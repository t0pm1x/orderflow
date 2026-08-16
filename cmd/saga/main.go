// Saga: explicit state machine for the order saga with compensation actions and TTL watchdog.
package main

import "fmt"

var version = "0.0.0-dev"

func main() {
	fmt.Printf("orderflow-saga version %s\n", version)
}
