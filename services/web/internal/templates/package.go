// Package templates holds html/template assets embedded at compile
// time. Body fragments define the "body" block; layout.html is the
// shared shell.
package templates

import "embed"

//go:embed *.html
var FS embed.FS