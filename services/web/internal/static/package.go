// Package static holds CSS assets embedded at compile time.
package static

import "embed"

//go:embed styles.css
var FS embed.FS