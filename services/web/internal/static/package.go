// Package static holds CSS assets embedded at compile time.
package static

import "embed"

// FS contains every file under this package, embedded into the
// binary at build time. The web server's /static handler serves
// it via io/fs.
//
//go:embed styles.css vendor/* diagrams/*
var FS embed.FS
