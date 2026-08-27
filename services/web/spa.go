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
//
// Each embed produces a typed asset that maps cleanly to its
// consumer:
//
//   indexHTML  []byte  — SPA fallback HTML (served for every
//                          non-API GET that has a more specific
//                          handler match). Read once at startup,
//                          written verbatim for every fallback.
//   appFS      embed.FS — SvelteKit code-split bundle. The
//                          /_app/* handler opens files at their
//                          native path ("_app/immutable/..."), so
//                          we DON'T strip the prefix here.
//                          Stripping caused the previous bug: the
//                          fallback `/*` handler caught the 404
//                          and returned the SPA HTML with
//                          text/plain, which the browser refused
//                          to apply as a stylesheet.
//   faviconSVG  []byte  — Static favicon served at /favicon.svg.
package web

import "embed"

//go:embed frontend/dist/index.html
//
// The SPA fallback served for every non-API GET path that has
// no more-specific handler. The same file also serves as the
// placeholder shell when the SPA hasn't been built yet — its
// content tells the operator to run `make web-frontend-build`.
var IndexHTML []byte

//go:embed frontend/dist/_app
//
// SvelteKit code-split chunks. The /_app/* HTTP handler in
// services/web/internal/server/server.go reads from this FS
// using the FULL path (`_app/immutable/chunks/file.js`) — no
// prefix stripping, so the lookup can't silently miss the file.
var AppFS embed.FS

//go:embed frontend/dist/favicon.svg
//
// SvelteKit copies src/static/* (and the public/ folder) into
// dist/. Served at /favicon.svg with image/svg+xml content-type.
var FaviconSVG []byte
