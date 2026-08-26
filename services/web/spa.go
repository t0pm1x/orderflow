// Package web embeds the SvelteKit SPA build output so the single
// binary serves both the Go BFF (under /api/*, /events/stream)
// and the static SPA (under /, /_app/*, /favicon.svg).
//
// Why is the embed directive in services/web/spa.go and not in
// services/web/internal/server/server.go? Go's embed patterns are
// interpreted relative to the package directory and must NOT
// contain ".." (per Go docs: "Patterns must not contain '.' or '..'
// or empty path elements"). The SvelteKit build output lives at
// services/web/frontend/dist/, and internal/server/ cannot reach
// it via embed because that would require "../../frontend/dist".
// Putting the embed here in services/web/ (sibling of frontend/)
// lets us write the path as "frontend/dist/index.html" directly.
//
// The embed is read by internal/server/server.go (the only place
// the SPA files are served) via the exported spaFS variable.
package web

import "embed"

//go:embed frontend/dist/index.html
//
// The placeholder index.html is committed so a fresh checkout
// without `npm run build` still produces a buildable binary; the
// real SvelteKit build overwrites it on every CI/local build.
//
// NOTE: when running `make web-frontend-build` for the first
// time, add new //go:embed directives here for any new top-level
// paths the SvelteKit adapter-static emits (currently _app/ and
// favicon.svg — see services/web/Makefile target `web-frontend-build`).
var SpaFS embed.FS
