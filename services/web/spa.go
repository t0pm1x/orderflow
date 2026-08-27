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
// Why three separate directives (index.html / _app / favicon.svg)
// and not a single `frontend/dist`? Each //go:embed pattern must
// match at least one file or the build fails with "no matching
// files found". When the SPA hasn't been built yet, only
// `dist/index.html` exists (the placeholder). Once the real
// SvelteKit build runs, `dist/_app/` and `dist/favicon.svg`
// appear too — so we list them explicitly to make the embed
// succeed both before and after `npm run build`. When the adapter
// emits additional top-level directories in a future SvelteKit
// version, add new //go:embed directives here for each.
package web

import "embed"

//go:embed frontend/dist/index.html
//go:embed frontend/dist/_app
//go:embed frontend/dist/favicon.svg

// SpaFS is the SvelteKit SPA's static build output, embedded into
// the Go binary at compile time. Consumed by
// services/web/internal/server/server.go (mountSPA() reads the
// _app/*, /favicon.svg, and falls back to index.html for any
// other GET path that doesn't start with /api).
var SpaFS embed.FS
