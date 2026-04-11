# HTTP Server & Routing DSL — Design Spec (Phase 6a)

**Date:** 2026-04-11
**Phase:** 6a (HTTP subsystem — server core + routing DSL) from `.plan/20260410.md`
**Gap:** G7 — `pekko-http`-equivalent module (sub-phase: server + routing)

---

## 1. Scope

Implement the HTTP server core and routing DSL under `http/` at the project root:

- **Routing DSL** with Pekko-style composable functional directives
- **Server** wrapping Go's `net/http.Server` (no custom HTTP parsing)
- **Path directives:** `Path`, `PathPrefix`, `PathEnd`, `PathParam`
- **Method directives:** `Get`, `Post`, `Put`, `Delete`, `Patch`, `Head`, `Options`
- **Extraction directives:** `Parameter`, `OptionalParameter`, `Header`, `OptionalHeader`, `ExtractRequest`
- **Completion:** `Complete`, `CompleteWith`, `Redirect`, `Reject`
- **Route combination:** `Concat` (try routes in order, first non-reject wins)
- **Rejection handling:** Default 404 (unmatched path), 405 with `Allow` header (wrong method)

Import path: `github.com/sopranoworks/gekka/http`

**Not in scope for 6a** (deferred to later sub-phases):
- `Entity` / body unmarshalling (Phase 6b: Marshallers)
- HTTP client (Phase 6c)
- WebSocket (Phase 6d)
- File serving, cookie/session directives, async completion

---

## 2. Core Types

### RequestContext

```go
type RequestContext struct {
    Request        *http.Request
    Writer         http.ResponseWriter
    Params         map[string]string   // path parameters ("id" -> "42")
    Remaining      string              // unconsumed path suffix for PathPrefix
    Rejected       bool                // true if route didn't match
    Completed      bool                // true if response was written
    AllowedMethods []string            // methods that matched path but not method (for 405)
}
```

### Route and Directive

```go
// Route is a function that handles a RequestContext.
type Route func(ctx *RequestContext)

// Directive wraps inner routes with matching/extraction logic.
type Directive func(inner Route) Route
```

### How composition works

1. A `Route` receives a `RequestContext`. It either writes a response (`Completed=true`) or sets `Rejected=true` to signal "try the next route".
2. A `Directive` (like `Get`, `Path`) wraps an inner `Route` — it checks conditions and calls the inner route if matched, otherwise sets `Rejected=true`.
3. `Concat(routes...)` tries each route in order. First one that doesn't reject wins.
4. `ToHandler(route Route) http.Handler` adapts the top-level route into a standard `http.Handler` with default rejection handling.

---

## 3. Path Directives

### `Path(segments ...string, inner Route) Route`

Matches when the remaining path equals the given segments exactly (no trailing segments). Signature uses variadic segments followed by inner route — implemented as `Path(segment string, inner Route)` for single segment, or a multi-segment variant.

### `PathPrefix(prefix string, inner Route) Route`

Matches when remaining path starts with prefix. Consumes matched prefix, passes rest in `ctx.Remaining`.

### `PathEnd(inner Route) Route`

Matches only when remaining path is empty or "/". Used after `PathPrefix` for exact-prefix matching.

### `PathParam(name string, inner func(string) Route) Route`

Extracts a single path segment as a named parameter. Stores in `ctx.Params[name]` and passes value to inner route factory.

---

## 4. Method Directives

All share a common `method(m string, inner Route) Route` implementation that checks `ctx.Request.Method` and rejects on mismatch. On mismatch, appends the method to `ctx.AllowedMethods` so the rejection handler can build a 405 `Allow` header.

- `Get(inner Route) Route`
- `Post(inner Route) Route`
- `Put(inner Route) Route`
- `Delete(inner Route) Route`
- `Patch(inner Route) Route`
- `Head(inner Route) Route`
- `Options(inner Route) Route`

---

## 5. Extraction Directives

### `Parameter(name string, inner func(string) Route) Route`

Extracts query parameter `name`. Rejects if absent.

### `OptionalParameter(name string, inner func(string, bool) Route) Route`

Same but passes `("", false)` when absent instead of rejecting.

### `Header(name string, inner func(string) Route) Route`

Extracts request header `name`. Rejects if absent.

### `OptionalHeader(name string, inner func(string, bool) Route) Route`

Same but passes `("", false)` when absent instead of rejecting.

### `ExtractRequest(inner func(*http.Request) Route) Route`

Passes the raw `*http.Request`. Never rejects.

---

## 6. Completion & Rejection

### `Complete(status int, body any) Route`

Writes response. Body handling:
- `string` → `text/plain; charset=utf-8`
- `[]byte` → `application/octet-stream`
- `nil` → no body, just status code
- Any other type → JSON via `encoding/json`, `application/json`

### `CompleteWith(status int, contentType string, body []byte) Route`

Low-level: explicit content type and raw bytes.

### `Redirect(url string, status int) Route`

Sends HTTP redirect.

### `Reject() Route`

Explicit rejection — signals "try next route".

### `Concat(routes ...Route) Route`

Tries routes in order. First non-reject wins. If all reject, Concat itself rejects. Merges `AllowedMethods` from all child routes.

### Default rejection handler (in `ToHandler`)

Applied when the top-level route rejects:
- If `ctx.AllowedMethods` is non-empty → `405 Method Not Allowed` with `Allow` header
- Otherwise → `404 Not Found`

---

## 7. Server

### `ServerBinding`

```go
type ServerBinding struct {
    Addr     string
    listener net.Listener
    server   *http.Server
}
```

### `NewServer(addr string, route Route) (*ServerBinding, error)`

Creates and starts an HTTP server:
1. Calls `ToHandler(route)` to produce an `http.Handler`
2. Creates `net.Listener` on addr
3. Starts `http.Server` with the handler in a background goroutine
4. Returns `ServerBinding` with the resolved address (supports port 0)

### `(b *ServerBinding) Shutdown(ctx context.Context) error`

Delegates to `http.Server.Shutdown` for graceful shutdown.

---

## 8. File Layout

```
http/
  route.go          # RequestContext, Route, Directive, Concat, ToHandler
  server.go         # ServerBinding, NewServer, Shutdown
  path.go           # Path, PathPrefix, PathEnd, PathParam
  method.go         # Get, Post, Put, Delete, Patch, Head, Options
  extract.go        # Parameter, OptionalParameter, Header, OptionalHeader, ExtractRequest
  complete.go       # Complete, CompleteWith, Redirect, Reject
  rejection.go      # Default rejection handler, 404/405 logic

  route_test.go     # Core route/concat/rejection tests
  path_test.go      # Path directive tests
  method_test.go    # Method directive tests
  extract_test.go   # Extraction directive tests
  complete_test.go  # Completion tests
  server_test.go    # httptest-based round-trip integration tests
```

---

## 9. Testing

### Unit tests (per directive file)

Create `RequestContext` manually with `httptest.NewRecorder()`, run the route, assert on `Completed`/`Rejected` state and recorded response.

### Integration tests (server_test.go)

Wire up a full route tree, use `ToHandler` + `httptest.NewServer`, make real HTTP requests. Test cases:

- Path matching + method filtering
- Path parameter extraction
- Query parameter extraction
- Header extraction
- Concat fallthrough (first match wins)
- 404 on unmatched path
- 405 with correct `Allow` header
- Nested PathPrefix + Path combinations
- Complete with various body types (string, bytes, struct→JSON, nil)
- Redirect
