// Package templates holds html/template assets embedded at compile
// time. Body fragments define the "body" block; layout.html is the
// shared shell.
package templates

import "embed"

// FS contains every .html file under this package, embedded into
// the binary at build time. The web server's handler set loads
// templates via template.ParseFS(FS, "layout.html", "*.html").
//
//go:embed *.html
var FS embed.FS
