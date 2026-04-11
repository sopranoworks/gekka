# HTTP Server & Routing DSL Implementation Plan (Phase 6a)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a Pekko-style HTTP routing DSL and server wrapper under `http/`, providing composable functional directives for path matching, method filtering, extraction, and completion.

**Architecture:** Routes are `func(ctx *RequestContext)` closures composed via directives. Directives wrap inner routes with matching logic, setting `Rejected=true` on mismatch. `Concat` tries routes in order. `ToHandler` adapts the top-level route into a standard `http.Handler` with default 404/405 rejection handling. `NewServer` wraps Go's `net/http.Server`.

**Tech Stack:** Go stdlib only — `net/http`, `encoding/json`, `net/http/httptest`, `testing`

---

## File Map

| File | Responsibility |
|------|----------------|
| `http/route.go` | `RequestContext`, `Route`, `Directive` types, `Concat`, `ToHandler` |
| `http/complete.go` | `Complete`, `CompleteWith`, `Redirect`, `Reject` |
| `http/path.go` | `Path`, `PathPrefix`, `PathEnd`, `PathParam` |
| `http/method.go` | `Get`, `Post`, `Put`, `Delete`, `Patch`, `Head`, `Options` |
| `http/extract.go` | `Parameter`, `OptionalParameter`, `Header`, `OptionalHeader`, `ExtractRequest` |
| `http/rejection.go` | Default rejection handler (404/405) |
| `http/server.go` | `ServerBinding`, `NewServer`, `Shutdown` |
| `http/route_test.go` | Core route/concat tests |
| `http/complete_test.go` | Completion directive tests |
| `http/path_test.go` | Path directive tests |
| `http/method_test.go` | Method directive tests |
| `http/extract_test.go` | Extraction directive tests |
| `http/server_test.go` | httptest round-trip integration tests |

---

### Task 1: Core types, Concat, and ToHandler

**Files:**
- Create: `http/route.go`
- Create: `http/rejection.go`
- Test: `http/route_test.go`

- [ ] **Step 1: Create route.go with core types**

```go
/*
 * route.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package http

import (
	"net/http"
	"strings"
)

// RequestContext carries the request and extracted values through the directive chain.
type RequestContext struct {
	Request        *http.Request
	Writer         http.ResponseWriter
	Params         map[string]string // path parameters ("id" -> "42")
	Remaining      string            // unconsumed path suffix for PathPrefix
	Rejected       bool              // true if route didn't match
	Completed      bool              // true if response was written
	AllowedMethods []string          // methods that matched path but not method (for 405)
}

// NewRequestContext creates a RequestContext from an HTTP request/response pair.
func NewRequestContext(w http.ResponseWriter, r *http.Request) *RequestContext {
	return &RequestContext{
		Request:   r,
		Writer:    w,
		Params:    make(map[string]string),
		Remaining: r.URL.Path,
	}
}

// Route is a function that handles a RequestContext.
// It either writes a response (Completed=true) or rejects (Rejected=true).
type Route func(ctx *RequestContext)

// Directive wraps inner routes with matching/extraction logic.
type Directive func(inner Route) Route

// Concat tries each route in order. The first route that does not reject wins.
// If all routes reject, Concat itself rejects. AllowedMethods are merged from
// all child routes so the rejection handler can build a 405 Allow header.
func Concat(routes ...Route) Route {
	return func(ctx *RequestContext) {
		var allAllowed []string
		for _, route := range routes {
			// Reset rejection state for each attempt.
			ctx.Rejected = false
			ctx.AllowedMethods = nil

			route(ctx)

			if !ctx.Rejected {
				return
			}
			allAllowed = append(allAllowed, ctx.AllowedMethods...)
		}
		// All routes rejected.
		ctx.Rejected = true
		ctx.AllowedMethods = dedup(allAllowed)
	}
}

// dedup removes duplicate strings from a slice.
func dedup(ss []string) []string {
	seen := make(map[string]struct{}, len(ss))
	var out []string
	for _, s := range ss {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

// ToHandler adapts a Route into a standard http.Handler.
// If the route rejects, the default rejection handler is applied.
func ToHandler(route Route) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := NewRequestContext(w, r)
		route(ctx)
		if ctx.Rejected {
			handleRejection(ctx)
		}
	})
}
```

- [ ] **Step 2: Create rejection.go**

```go
/*
 * rejection.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package http

import (
	"net/http"
	"strings"
)

// handleRejection is the default rejection handler applied by ToHandler when
// the top-level route rejects.
func handleRejection(ctx *RequestContext) {
	if len(ctx.AllowedMethods) > 0 {
		ctx.Writer.Header().Set("Allow", strings.Join(ctx.AllowedMethods, ", "))
		ctx.Writer.WriteHeader(http.StatusMethodNotAllowed)
	} else {
		ctx.Writer.WriteHeader(http.StatusNotFound)
	}
}
```

- [ ] **Step 3: Write route_test.go**

```go
/*
 * route_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package http_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	gekkahttp "github.com/sopranoworks/gekka/http"
)

func TestConcat_FirstMatchWins(t *testing.T) {
	route1 := func(ctx *gekkahttp.RequestContext) {
		ctx.Rejected = true
	}
	route2 := func(ctx *gekkahttp.RequestContext) {
		ctx.Writer.WriteHeader(http.StatusOK)
		ctx.Completed = true
	}
	route3 := func(ctx *gekkahttp.RequestContext) {
		t.Fatal("route3 should not be called")
	}

	combined := gekkahttp.Concat(route1, route2, route3)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	combined(ctx)

	if ctx.Rejected {
		t.Fatal("expected route to not be rejected")
	}
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestConcat_AllReject(t *testing.T) {
	rejectRoute := func(ctx *gekkahttp.RequestContext) {
		ctx.Rejected = true
	}

	combined := gekkahttp.Concat(rejectRoute, rejectRoute)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	combined(ctx)

	if !ctx.Rejected {
		t.Fatal("expected rejection")
	}
}

func TestToHandler_404OnRejection(t *testing.T) {
	route := func(ctx *gekkahttp.RequestContext) {
		ctx.Rejected = true
	}

	handler := gekkahttp.ToHandler(route)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/nothing", nil)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestToHandler_405WithAllowHeader(t *testing.T) {
	route := func(ctx *gekkahttp.RequestContext) {
		ctx.Rejected = true
		ctx.AllowedMethods = []string{"GET", "POST"}
	}

	handler := gekkahttp.ToHandler(route)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("DELETE", "/resource", nil)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
	allow := w.Header().Get("Allow")
	if allow != "GET, POST" {
		t.Fatalf("expected Allow: GET, POST, got %q", allow)
	}
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./http/ -v -run "TestConcat|TestToHandler"`
Expected: All 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add http/route.go http/rejection.go http/route_test.go
git commit -m "feat(http): add core Route types, Concat, ToHandler with rejection handling"
```

---

### Task 2: Completion directives

**Files:**
- Create: `http/complete.go`
- Test: `http/complete_test.go`

- [ ] **Step 1: Create complete.go**

```go
/*
 * complete.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package http

import (
	"encoding/json"
	"net/http"
)

// Complete writes a response with the given status code and body.
// Body type determines content type:
//   - string → text/plain; charset=utf-8
//   - []byte → application/octet-stream
//   - nil → no body
//   - any other type → JSON (application/json)
func Complete(status int, body any) Route {
	return func(ctx *RequestContext) {
		switch v := body.(type) {
		case nil:
			ctx.Writer.WriteHeader(status)
		case string:
			ctx.Writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
			ctx.Writer.WriteHeader(status)
			ctx.Writer.Write([]byte(v))
		case []byte:
			ctx.Writer.Header().Set("Content-Type", "application/octet-stream")
			ctx.Writer.WriteHeader(status)
			ctx.Writer.Write(v)
		default:
			data, err := json.Marshal(v)
			if err != nil {
				ctx.Writer.WriteHeader(http.StatusInternalServerError)
				ctx.Completed = true
				return
			}
			ctx.Writer.Header().Set("Content-Type", "application/json")
			ctx.Writer.WriteHeader(status)
			ctx.Writer.Write(data)
		}
		ctx.Completed = true
	}
}

// CompleteWith writes a response with explicit content type and raw bytes.
func CompleteWith(status int, contentType string, body []byte) Route {
	return func(ctx *RequestContext) {
		ctx.Writer.Header().Set("Content-Type", contentType)
		ctx.Writer.WriteHeader(status)
		ctx.Writer.Write(body)
		ctx.Completed = true
	}
}

// Redirect sends an HTTP redirect response.
func Redirect(url string, status int) Route {
	return func(ctx *RequestContext) {
		http.Redirect(ctx.Writer, ctx.Request, url, status)
		ctx.Completed = true
	}
}

// Reject explicitly rejects the current route, signaling "try next route".
func Reject() Route {
	return func(ctx *RequestContext) {
		ctx.Rejected = true
	}
}
```

- [ ] **Step 2: Create complete_test.go**

```go
/*
 * complete_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package http_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	gekkahttp "github.com/sopranoworks/gekka/http"
)

func TestComplete_String(t *testing.T) {
	route := gekkahttp.Complete(http.StatusOK, "hello")
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if !ctx.Completed {
		t.Fatal("expected completed")
	}
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if w.Header().Get("Content-Type") != "text/plain; charset=utf-8" {
		t.Fatalf("unexpected content type: %s", w.Header().Get("Content-Type"))
	}
	if w.Body.String() != "hello" {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}

func TestComplete_Bytes(t *testing.T) {
	route := gekkahttp.Complete(http.StatusOK, []byte{0x01, 0x02})
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if w.Header().Get("Content-Type") != "application/octet-stream" {
		t.Fatalf("unexpected content type: %s", w.Header().Get("Content-Type"))
	}
	if len(w.Body.Bytes()) != 2 {
		t.Fatalf("unexpected body length: %d", len(w.Body.Bytes()))
	}
}

func TestComplete_Nil(t *testing.T) {
	route := gekkahttp.Complete(http.StatusNoContent, nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if w.Body.Len() != 0 {
		t.Fatal("expected empty body")
	}
}

func TestComplete_JSON(t *testing.T) {
	type User struct {
		Name string `json:"name"`
	}
	route := gekkahttp.Complete(http.StatusCreated, User{Name: "alice"})
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("unexpected content type: %s", w.Header().Get("Content-Type"))
	}
	var u User
	if err := json.Unmarshal(w.Body.Bytes(), &u); err != nil {
		t.Fatalf("json decode: %v", err)
	}
	if u.Name != "alice" {
		t.Fatalf("expected alice, got %s", u.Name)
	}
}

func TestCompleteWith(t *testing.T) {
	route := gekkahttp.CompleteWith(http.StatusOK, "text/xml", []byte("<ok/>"))
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if w.Header().Get("Content-Type") != "text/xml" {
		t.Fatalf("unexpected content type: %s", w.Header().Get("Content-Type"))
	}
	if w.Body.String() != "<ok/>" {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}

func TestRedirect(t *testing.T) {
	route := gekkahttp.Redirect("/new-location", http.StatusMovedPermanently)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/old", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if !ctx.Completed {
		t.Fatal("expected completed")
	}
	if w.Code != http.StatusMovedPermanently {
		t.Fatalf("expected 301, got %d", w.Code)
	}
	if w.Header().Get("Location") != "/new-location" {
		t.Fatalf("unexpected Location: %s", w.Header().Get("Location"))
	}
}

func TestReject(t *testing.T) {
	route := gekkahttp.Reject()
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if !ctx.Rejected {
		t.Fatal("expected rejection")
	}
}
```

- [ ] **Step 3: Run tests**

Run: `go test ./http/ -v -run "TestComplete|TestRedirect|TestReject"`
Expected: All 7 tests pass.

- [ ] **Step 4: Commit**

```bash
git add http/complete.go http/complete_test.go
git commit -m "feat(http): add Complete, CompleteWith, Redirect, Reject directives"
```

---

### Task 3: Path directives

**Files:**
- Create: `http/path.go`
- Test: `http/path_test.go`

- [ ] **Step 1: Create path.go**

```go
/*
 * path.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package http

import "strings"

// normalizePath strips leading/trailing slashes and returns clean segments.
func normalizePath(p string) string {
	return strings.Trim(p, "/")
}

// Path matches when the remaining path equals segment exactly (no trailing segments).
func Path(segment string, inner Route) Route {
	return func(ctx *RequestContext) {
		remaining := normalizePath(ctx.Remaining)
		if remaining == segment {
			saved := ctx.Remaining
			ctx.Remaining = ""
			inner(ctx)
			if ctx.Rejected {
				ctx.Remaining = saved
			}
		} else {
			ctx.Rejected = true
		}
	}
}

// PathPrefix matches when the remaining path starts with prefix.
// Consumes the matched prefix and passes the rest in ctx.Remaining.
func PathPrefix(prefix string, inner Route) Route {
	return func(ctx *RequestContext) {
		remaining := normalizePath(ctx.Remaining)
		prefix = normalizePath(prefix)

		if remaining == prefix || strings.HasPrefix(remaining, prefix+"/") {
			saved := ctx.Remaining
			if remaining == prefix {
				ctx.Remaining = ""
			} else {
				ctx.Remaining = "/" + remaining[len(prefix)+1:]
			}
			inner(ctx)
			if ctx.Rejected {
				ctx.Remaining = saved
			}
		} else {
			ctx.Rejected = true
		}
	}
}

// PathEnd matches only when the remaining path is empty or "/".
func PathEnd(inner Route) Route {
	return func(ctx *RequestContext) {
		remaining := normalizePath(ctx.Remaining)
		if remaining == "" {
			inner(ctx)
		} else {
			ctx.Rejected = true
		}
	}
}

// PathParam extracts a single path segment as a named parameter.
// The extracted value is stored in ctx.Params[name] and passed to the inner factory.
func PathParam(name string, inner func(string) Route) Route {
	return func(ctx *RequestContext) {
		remaining := normalizePath(ctx.Remaining)
		if remaining == "" {
			ctx.Rejected = true
			return
		}

		var segment, rest string
		if idx := strings.IndexByte(remaining, '/'); idx >= 0 {
			segment = remaining[:idx]
			rest = "/" + remaining[idx+1:]
		} else {
			segment = remaining
			rest = ""
		}

		saved := ctx.Remaining
		ctx.Remaining = rest
		ctx.Params[name] = segment
		inner(segment)(ctx)
		if ctx.Rejected {
			ctx.Remaining = saved
			delete(ctx.Params, name)
		}
	}
}
```

- [ ] **Step 2: Create path_test.go**

```go
/*
 * path_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package http_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	gekkahttp "github.com/sopranoworks/gekka/http"
)

func ok(ctx *gekkahttp.RequestContext) {
	ctx.Writer.WriteHeader(http.StatusOK)
	ctx.Completed = true
}

func TestPath_ExactMatch(t *testing.T) {
	route := gekkahttp.Path("users", ok)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/users", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if ctx.Rejected {
		t.Fatal("should not reject /users")
	}
}

func TestPath_RejectsTrailingSegment(t *testing.T) {
	route := gekkahttp.Path("users", ok)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/users/123", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if !ctx.Rejected {
		t.Fatal("should reject /users/123")
	}
}

func TestPath_RejectsMismatch(t *testing.T) {
	route := gekkahttp.Path("users", ok)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/posts", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if !ctx.Rejected {
		t.Fatal("should reject /posts")
	}
}

func TestPathPrefix_Match(t *testing.T) {
	var capturedRemaining string
	inner := func(ctx *gekkahttp.RequestContext) {
		capturedRemaining = ctx.Remaining
		ctx.Completed = true
	}

	route := gekkahttp.PathPrefix("api", inner)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/api/users/123", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if ctx.Rejected {
		t.Fatal("should not reject /api/users/123")
	}
	if capturedRemaining != "/users/123" {
		t.Fatalf("expected remaining /users/123, got %q", capturedRemaining)
	}
}

func TestPathPrefix_ExactMatch(t *testing.T) {
	route := gekkahttp.PathPrefix("api", ok)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/api", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if ctx.Rejected {
		t.Fatal("should not reject /api")
	}
}

func TestPathPrefix_RejectsMismatch(t *testing.T) {
	route := gekkahttp.PathPrefix("api", ok)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/web/page", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if !ctx.Rejected {
		t.Fatal("should reject /web/page")
	}
}

func TestPathEnd_Empty(t *testing.T) {
	route := gekkahttp.PathPrefix("users", gekkahttp.PathEnd(ok))

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/users", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if ctx.Rejected {
		t.Fatal("should not reject /users with PathEnd")
	}
}

func TestPathEnd_RejectsTrailing(t *testing.T) {
	route := gekkahttp.PathPrefix("users", gekkahttp.PathEnd(ok))

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/users/123", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if !ctx.Rejected {
		t.Fatal("should reject /users/123 with PathEnd")
	}
}

func TestPathParam_Extract(t *testing.T) {
	var capturedID string
	route := gekkahttp.PathPrefix("users", gekkahttp.PathParam("id", func(id string) gekkahttp.Route {
		capturedID = id
		return ok
	}))

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/users/42", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if ctx.Rejected {
		t.Fatal("should not reject /users/42")
	}
	if capturedID != "42" {
		t.Fatalf("expected id=42, got %q", capturedID)
	}
	if ctx.Params["id"] != "42" {
		t.Fatalf("expected ctx.Params[id]=42, got %q", ctx.Params["id"])
	}
}

func TestPathParam_RejectsEmpty(t *testing.T) {
	route := gekkahttp.PathParam("id", func(id string) gekkahttp.Route { return ok })

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	ctx.Remaining = ""
	route(ctx)

	if !ctx.Rejected {
		t.Fatal("should reject empty remaining")
	}
}

func TestNestedPathPrefix(t *testing.T) {
	var capturedID string
	route := gekkahttp.PathPrefix("api",
		gekkahttp.PathPrefix("users",
			gekkahttp.PathParam("id", func(id string) gekkahttp.Route {
				capturedID = id
				return ok
			}),
		),
	)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/api/users/99", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if ctx.Rejected {
		t.Fatal("should not reject /api/users/99")
	}
	if capturedID != "99" {
		t.Fatalf("expected 99, got %q", capturedID)
	}
}
```

- [ ] **Step 3: Run tests**

Run: `go test ./http/ -v -run "TestPath|TestNested"`
Expected: All 10 tests pass.

- [ ] **Step 4: Commit**

```bash
git add http/path.go http/path_test.go
git commit -m "feat(http): add Path, PathPrefix, PathEnd, PathParam directives"
```

---

### Task 4: Method directives

**Files:**
- Create: `http/method.go`
- Test: `http/method_test.go`

- [ ] **Step 1: Create method.go**

```go
/*
 * method.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package http

import "net/http"

// method is the shared implementation for all method directives.
// On mismatch, it appends the expected method to ctx.AllowedMethods
// so the rejection handler can build a 405 Allow header.
func method(m string, inner Route) Route {
	return func(ctx *RequestContext) {
		if ctx.Request.Method == m {
			inner(ctx)
		} else {
			ctx.AllowedMethods = append(ctx.AllowedMethods, m)
			ctx.Rejected = true
		}
	}
}

// Get matches HTTP GET requests.
func Get(inner Route) Route { return method(http.MethodGet, inner) }

// Post matches HTTP POST requests.
func Post(inner Route) Route { return method(http.MethodPost, inner) }

// Put matches HTTP PUT requests.
func Put(inner Route) Route { return method(http.MethodPut, inner) }

// Delete matches HTTP DELETE requests.
func Delete(inner Route) Route { return method(http.MethodDelete, inner) }

// Patch matches HTTP PATCH requests.
func Patch(inner Route) Route { return method(http.MethodPatch, inner) }

// Head matches HTTP HEAD requests.
func Head(inner Route) Route { return method(http.MethodHead, inner) }

// Options matches HTTP OPTIONS requests.
func Options(inner Route) Route { return method(http.MethodOptions, inner) }
```

- [ ] **Step 2: Create method_test.go**

```go
/*
 * method_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package http_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	gekkahttp "github.com/sopranoworks/gekka/http"
)

func TestGet_Matches(t *testing.T) {
	route := gekkahttp.Get(ok)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if ctx.Rejected {
		t.Fatal("GET should match GET")
	}
}

func TestGet_RejectsPost(t *testing.T) {
	route := gekkahttp.Get(ok)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if !ctx.Rejected {
		t.Fatal("GET should reject POST")
	}
	if len(ctx.AllowedMethods) != 1 || ctx.AllowedMethods[0] != "GET" {
		t.Fatalf("expected AllowedMethods=[GET], got %v", ctx.AllowedMethods)
	}
}

func TestPost_Matches(t *testing.T) {
	route := gekkahttp.Post(ok)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if ctx.Rejected {
		t.Fatal("POST should match POST")
	}
}

func TestMethodConcat_405(t *testing.T) {
	route := gekkahttp.Path("resource", gekkahttp.Concat(
		gekkahttp.Get(ok),
		gekkahttp.Post(ok),
	))

	handler := gekkahttp.ToHandler(route)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("DELETE", "/resource", nil)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
	allow := w.Header().Get("Allow")
	if allow != "GET, POST" {
		t.Fatalf("expected Allow: GET, POST, got %q", allow)
	}
}

func TestAllMethods(t *testing.T) {
	methods := []struct {
		directive func(gekkahttp.Route) gekkahttp.Route
		method    string
	}{
		{gekkahttp.Get, "GET"},
		{gekkahttp.Post, "POST"},
		{gekkahttp.Put, "PUT"},
		{gekkahttp.Delete, "DELETE"},
		{gekkahttp.Patch, "PATCH"},
		{gekkahttp.Head, "HEAD"},
		{gekkahttp.Options, "OPTIONS"},
	}

	for _, m := range methods {
		t.Run(m.method, func(t *testing.T) {
			route := m.directive(ok)
			w := httptest.NewRecorder()
			r := httptest.NewRequest(m.method, "/", nil)
			ctx := gekkahttp.NewRequestContext(w, r)
			route(ctx)

			if ctx.Rejected {
				t.Fatalf("%s should match %s", m.method, m.method)
			}
		})
	}
}
```

- [ ] **Step 3: Run tests**

Run: `go test ./http/ -v -run "TestGet|TestPost|TestMethod|TestAllMethods"`
Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add http/method.go http/method_test.go
git commit -m "feat(http): add method directives (Get, Post, Put, Delete, Patch, Head, Options)"
```

---

### Task 5: Extraction directives

**Files:**
- Create: `http/extract.go`
- Test: `http/extract_test.go`

- [ ] **Step 1: Create extract.go**

```go
/*
 * extract.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package http

import "net/http"

// Parameter extracts a query parameter by name. Rejects if absent.
func Parameter(name string, inner func(string) Route) Route {
	return func(ctx *RequestContext) {
		v := ctx.Request.URL.Query().Get(name)
		if v == "" && !ctx.Request.URL.Query().Has(name) {
			ctx.Rejected = true
			return
		}
		inner(v)(ctx)
	}
}

// OptionalParameter extracts a query parameter by name.
// If absent, passes ("", false) instead of rejecting.
func OptionalParameter(name string, inner func(string, bool) Route) Route {
	return func(ctx *RequestContext) {
		if ctx.Request.URL.Query().Has(name) {
			inner(ctx.Request.URL.Query().Get(name), true)(ctx)
		} else {
			inner("", false)(ctx)
		}
	}
}

// Header extracts a request header by name. Rejects if absent.
func Header(name string, inner func(string) Route) Route {
	return func(ctx *RequestContext) {
		v := ctx.Request.Header.Get(name)
		if v == "" {
			ctx.Rejected = true
			return
		}
		inner(v)(ctx)
	}
}

// OptionalHeader extracts a request header by name.
// If absent, passes ("", false) instead of rejecting.
func OptionalHeader(name string, inner func(string, bool) Route) Route {
	return func(ctx *RequestContext) {
		v := ctx.Request.Header.Get(name)
		if v != "" {
			inner(v, true)(ctx)
		} else {
			inner("", false)(ctx)
		}
	}
}

// ExtractRequest passes the raw *http.Request to the inner route factory.
// Never rejects.
func ExtractRequest(inner func(*http.Request) Route) Route {
	return func(ctx *RequestContext) {
		inner(ctx.Request)(ctx)
	}
}
```

- [ ] **Step 2: Create extract_test.go**

```go
/*
 * extract_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package http_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	gekkahttp "github.com/sopranoworks/gekka/http"
)

func TestParameter_Present(t *testing.T) {
	var captured string
	route := gekkahttp.Parameter("q", func(v string) gekkahttp.Route {
		captured = v
		return ok
	})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/search?q=hello", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if ctx.Rejected {
		t.Fatal("should not reject when param present")
	}
	if captured != "hello" {
		t.Fatalf("expected hello, got %q", captured)
	}
}

func TestParameter_Missing(t *testing.T) {
	route := gekkahttp.Parameter("q", func(v string) gekkahttp.Route { return ok })

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/search", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if !ctx.Rejected {
		t.Fatal("should reject when param missing")
	}
}

func TestParameter_EmptyValue(t *testing.T) {
	var captured string
	route := gekkahttp.Parameter("q", func(v string) gekkahttp.Route {
		captured = v
		return ok
	})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/search?q=", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if ctx.Rejected {
		t.Fatal("should not reject when param present with empty value")
	}
	if captured != "" {
		t.Fatalf("expected empty, got %q", captured)
	}
}

func TestOptionalParameter_Present(t *testing.T) {
	var captured string
	var found bool
	route := gekkahttp.OptionalParameter("q", func(v string, ok bool) gekkahttp.Route {
		captured = v
		found = ok
		return func(ctx *gekkahttp.RequestContext) { ctx.Completed = true }
	})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/?q=world", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if !found || captured != "world" {
		t.Fatalf("expected (world, true), got (%q, %v)", captured, found)
	}
}

func TestOptionalParameter_Missing(t *testing.T) {
	var found bool
	route := gekkahttp.OptionalParameter("q", func(v string, ok bool) gekkahttp.Route {
		found = ok
		return func(ctx *gekkahttp.RequestContext) { ctx.Completed = true }
	})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if found {
		t.Fatal("expected found=false for missing param")
	}
	if ctx.Rejected {
		t.Fatal("OptionalParameter should not reject")
	}
}

func TestHeader_Present(t *testing.T) {
	var captured string
	route := gekkahttp.Header("Authorization", func(v string) gekkahttp.Route {
		captured = v
		return ok
	})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer token123")
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if ctx.Rejected {
		t.Fatal("should not reject with header present")
	}
	if captured != "Bearer token123" {
		t.Fatalf("expected Bearer token123, got %q", captured)
	}
}

func TestHeader_Missing(t *testing.T) {
	route := gekkahttp.Header("Authorization", func(v string) gekkahttp.Route { return ok })

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if !ctx.Rejected {
		t.Fatal("should reject when header missing")
	}
}

func TestOptionalHeader_Missing(t *testing.T) {
	var found bool
	route := gekkahttp.OptionalHeader("X-Custom", func(v string, ok bool) gekkahttp.Route {
		found = ok
		return func(ctx *gekkahttp.RequestContext) { ctx.Completed = true }
	})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if found {
		t.Fatal("expected found=false")
	}
	if ctx.Rejected {
		t.Fatal("OptionalHeader should not reject")
	}
}

func TestExtractRequest(t *testing.T) {
	var capturedMethod string
	route := gekkahttp.ExtractRequest(func(r *http.Request) gekkahttp.Route {
		capturedMethod = r.Method
		return ok
	})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	route(ctx)

	if ctx.Rejected {
		t.Fatal("ExtractRequest should never reject")
	}
	if capturedMethod != "POST" {
		t.Fatalf("expected POST, got %q", capturedMethod)
	}
}
```

- [ ] **Step 3: Run tests**

Run: `go test ./http/ -v -run "TestParameter|TestOptional|TestHeader|TestExtract"`
Expected: All 9 tests pass.

- [ ] **Step 4: Commit**

```bash
git add http/extract.go http/extract_test.go
git commit -m "feat(http): add extraction directives (Parameter, Header, ExtractRequest)"
```

---

### Task 6: Server binding

**Files:**
- Create: `http/server.go`

- [ ] **Step 1: Create server.go**

```go
/*
 * server.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package http

import (
	"context"
	"fmt"
	"net"
	gohttp "net/http"
)

// ServerBinding represents a running HTTP server.
type ServerBinding struct {
	Addr     string
	listener net.Listener
	server   *gohttp.Server
}

// NewServer creates and starts an HTTP server serving the given route.
// The addr format is "host:port" (use ":0" for a random port).
// The resolved address is available via binding.Addr after return.
func NewServer(addr string, route Route) (*ServerBinding, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("gekkahttp: listen %s: %w", addr, err)
	}

	handler := ToHandler(route)
	server := &gohttp.Server{Handler: handler}

	binding := &ServerBinding{
		Addr:     listener.Addr().String(),
		listener: listener,
		server:   server,
	}

	go func() {
		if err := server.Serve(listener); err != nil && err != gohttp.ErrServerClosed {
			// Server terminated unexpectedly — logged but not returned
			// since the goroutine can't propagate errors.
		}
	}()

	return binding, nil
}

// Shutdown gracefully stops the server.
func (b *ServerBinding) Shutdown(ctx context.Context) error {
	return b.server.Shutdown(ctx)
}
```

- [ ] **Step 2: Verify build**

Run: `go build ./http/`
Expected: Build succeeds.

- [ ] **Step 3: Commit**

```bash
git add http/server.go
git commit -m "feat(http): add ServerBinding with NewServer and Shutdown"
```

---

### Task 7: Integration tests (httptest round-trips)

**Files:**
- Create: `http/server_test.go`

- [ ] **Step 1: Create server_test.go**

```go
/*
 * server_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package http_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	gekkahttp "github.com/sopranoworks/gekka/http"
)

// buildTestRoute creates a realistic route tree for integration testing.
func buildTestRoute() gekkahttp.Route {
	type User struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}

	return gekkahttp.PathPrefix("api", gekkahttp.Concat(
		// GET /api/users → list
		gekkahttp.Path("users", gekkahttp.Concat(
			gekkahttp.Get(gekkahttp.Complete(http.StatusOK, []User{
				{ID: "1", Name: "Alice"},
				{ID: "2", Name: "Bob"},
			})),
			gekkahttp.Post(gekkahttp.Complete(http.StatusCreated, User{ID: "3", Name: "Charlie"})),
		)),

		// GET /api/users/:id → get by id
		gekkahttp.PathPrefix("users", gekkahttp.PathParam("id", func(id string) gekkahttp.Route {
			return gekkahttp.Get(gekkahttp.Complete(http.StatusOK, User{ID: id, Name: "User-" + id}))
		})),

		// GET /api/search?q=... → search
		gekkahttp.Path("search", gekkahttp.Get(
			gekkahttp.Parameter("q", func(q string) gekkahttp.Route {
				return gekkahttp.Complete(http.StatusOK, "search: "+q)
			}),
		)),

		// GET /api/health → plain text
		gekkahttp.Path("health", gekkahttp.Get(
			gekkahttp.Complete(http.StatusOK, "ok"),
		)),

		// GET /api/redirect → redirect
		gekkahttp.Path("redirect", gekkahttp.Get(
			gekkahttp.Redirect("/api/health", http.StatusFound),
		)),
	))
}

func TestIntegration_GetUsers(t *testing.T) {
	srv := httptest.NewServer(gekkahttp.ToHandler(buildTestRoute()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/users")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("expected application/json, got %s", resp.Header.Get("Content-Type"))
	}
}

func TestIntegration_PostUsers(t *testing.T) {
	srv := httptest.NewServer(gekkahttp.ToHandler(buildTestRoute()))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/api/users", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
}

func TestIntegration_GetUserByID(t *testing.T) {
	srv := httptest.NewServer(gekkahttp.ToHandler(buildTestRoute()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/users/42")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var user struct{ ID, Name string }
	json.NewDecoder(resp.Body).Decode(&user)
	if user.ID != "42" {
		t.Fatalf("expected id=42, got %q", user.ID)
	}
}

func TestIntegration_Search(t *testing.T) {
	srv := httptest.NewServer(gekkahttp.ToHandler(buildTestRoute()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/search?q=hello")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "search: hello" {
		t.Fatalf("expected 'search: hello', got %q", string(body))
	}
}

func TestIntegration_Health(t *testing.T) {
	srv := httptest.NewServer(gekkahttp.ToHandler(buildTestRoute()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "ok" {
		t.Fatalf("expected 'ok', got %q", string(body))
	}
}

func TestIntegration_404(t *testing.T) {
	srv := httptest.NewServer(gekkahttp.ToHandler(buildTestRoute()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestIntegration_405(t *testing.T) {
	srv := httptest.NewServer(gekkahttp.ToHandler(buildTestRoute()))
	defer srv.Close()

	req, _ := http.NewRequest("DELETE", srv.URL+"/api/users", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
	allow := resp.Header.Get("Allow")
	if allow == "" {
		t.Fatal("expected Allow header")
	}
}

func TestIntegration_Redirect(t *testing.T) {
	srv := httptest.NewServer(gekkahttp.ToHandler(buildTestRoute()))
	defer srv.Close()

	client := &http.Client{CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}}

	resp, err := client.Get(srv.URL + "/api/redirect")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusFound {
		t.Fatalf("expected 302, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Location") != "/api/health" {
		t.Fatalf("unexpected Location: %s", resp.Header.Get("Location"))
	}
}

func TestIntegration_HeaderExtraction(t *testing.T) {
	route := gekkahttp.Path("auth", gekkahttp.Get(
		gekkahttp.Header("Authorization", func(token string) gekkahttp.Route {
			return gekkahttp.Complete(http.StatusOK, "token: "+token)
		}),
	))

	srv := httptest.NewServer(gekkahttp.ToHandler(route))
	defer srv.Close()

	req, _ := http.NewRequest("GET", srv.URL+"/auth", nil)
	req.Header.Set("Authorization", "Bearer abc")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "token: Bearer abc" {
		t.Fatalf("expected 'token: Bearer abc', got %q", string(body))
	}
}
```

- [ ] **Step 2: Run all tests**

Run: `go test ./http/ -v`
Expected: All tests pass (unit + integration).

- [ ] **Step 3: Commit**

```bash
git add http/server_test.go
git commit -m "test(http): add httptest round-trip integration tests"
```

---

### Task 8: Full build gate

- [ ] **Step 1: Run full build**

Run: `go build ./...`
Expected: All packages build.

- [ ] **Step 2: Run all tests**

Run: `go test ./...`
Expected: All tests pass including new `http/` package.

- [ ] **Step 3: Run lint**

Run: `~/go/bin/golangci-lint run ./http/...`
Expected: No issues.
