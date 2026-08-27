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

import (
	"embed"
	"io/fs"
)

//go:embed frontend/dist/index.html
//
// The SPA fallback served for every non-API GET path that has
// no more-specific handler. The same file also serves as the
// placeholder shell when the SPA hasn't been built yet — its
// content tells the operator to run `make web-frontend-build`.
var IndexHTML []byte

//go:embed frontend/dist/_app
//
// Raw SvelteKit code-split chunks, including the `frontend/dist/`
// path prefix that Go's embed pattern preserves. We strip the
// prefix via fs.Sub so the HTTP handler can look up files by
// their URL path directly (`_app/immutable/entry/start.X.js`).
//
// Pre-fix (F-004): the embed pattern's author believed the
// `_app` directory would be embedded without the prefix, so the
// server looked up paths like `_app/immutable/entry/start.X.js`
// in AppFS — but the actual path inside the FS was
// `frontend/dist/_app/immutable/entry/start.X.js`. Every JS
// request 404'd and the SPA rendered an empty page.
//
// F-004 fix: keep the embed pattern (it has to point at a real
// directory tree), but expose AppFS as a sub-FS rooted at
// `frontend/dist/_app`. The handler stays unchanged.
var appFSRaw embed.FS

// AppFS is the embedded SPA bundle rooted at the _app/ directory.
// See appFSRaw above for why this is a sub-FS of the raw embed.
var AppFS = mustSub(appFSRaw, "frontend/dist/_app")

// mustSub wraps fs.Sub, panicking on the impossible error case.
// It exists so the package init is a single line of declarative
// state, not a two-step "var ... ; init() { ... }".
func mustSub(fsys embed.FS, dir string) fs.FS {
	sub, err := fs.Sub(fsys, dir)
	if err != nil {
		panic("web: fs.Sub(" + dir + "): " + err.Error())
	}
	return sub
}

//go:embed frontend/dist/favicon.svg
//
// SvelteKit copies src/static/* (and the public/ folder) into
// dist/. Served at /favicon.svg with image/svg+xml content-type.
var FaviconSVG []byte
