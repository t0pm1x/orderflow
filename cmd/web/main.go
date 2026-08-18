// web Service binary — wiring lives in services/web/cmd/web
// so it can access the service's internal packages; this top-level
// cmd is just the binary entry point.
package main

import "github.com/t0pm1x/orderflow/services/web/cmd/web"

func main() {
	web.Main()
}
