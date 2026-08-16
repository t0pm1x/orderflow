// Payment Service binary — wiring lives in services/payment/cmd/payment.
package main

import "github.com/t0pm1x/orderflow/services/payment/cmd/payment"

func main() {
	payment.Main()
}
