// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	gekkahttp "github.com/sopranoworks/gekka/http"
)

func newCtx(remaining string) (*gekkahttp.RequestContext, *httptest.ResponseRecorder) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)
	ctx.Remaining = remaining
	return ctx, w
}

func TestPath_Match(t *testing.T) {
	called := false
	inner := func(ctx *gekkahttp.RequestContext) { called = true }

	ctx, _ := newCtx("/users")
	gekkahttp.Path("/users", inner)(ctx)

	if !called {
		t.Error("expected inner to be called on exact match")
	}
	if ctx.Rejected {
		t.Error("expected Rejected=false on match")
	}
	if ctx.Remaining != "" {
		t.Errorf("expected Remaining=\"\" after match, got %q", ctx.Remaining)
	}
}

func TestPath_NoMatch(t *testing.T) {
	called := false
	inner := func(ctx *gekkahttp.RequestContext) { called = true }

	ctx, _ := newCtx("/other")
	gekkahttp.Path("/users", inner)(ctx)

	if called {
		t.Error("expected inner NOT to be called on mismatch")
	}
	if !ctx.Rejected {
		t.Error("expected Rejected=true on mismatch")
	}
}

func TestPathPrefix_Match(t *testing.T) {
	called := false
	inner := func(ctx *gekkahttp.RequestContext) { called = true }

	ctx, _ := newCtx("/api/v1")
	gekkahttp.PathPrefix("/api", inner)(ctx)

	if !called {
		t.Error("expected inner to be called on prefix match")
	}
	if ctx.Rejected {
		t.Error("expected Rejected=false on match")
	}
	if ctx.Remaining != "/v1" {
		t.Errorf("expected Remaining=\"/v1\", got %q", ctx.Remaining)
	}
}

func TestPathPrefix_NoMatch(t *testing.T) {
	called := false
	inner := func(ctx *gekkahttp.RequestContext) { called = true }

	ctx, _ := newCtx("/other")
	gekkahttp.PathPrefix("/api", inner)(ctx)

	if called {
		t.Error("expected inner NOT to be called on mismatch")
	}
	if !ctx.Rejected {
		t.Error("expected Rejected=true on mismatch")
	}
}

func TestPathEnd_Match(t *testing.T) {
	called := false
	inner := func(ctx *gekkahttp.RequestContext) { called = true }

	ctx, _ := newCtx("")
	gekkahttp.PathEnd(inner)(ctx)

	if !called {
		t.Error("expected inner to be called when Remaining is empty")
	}
	if ctx.Rejected {
		t.Error("expected Rejected=false on match")
	}
}

func TestPathEnd_NoMatch(t *testing.T) {
	called := false
	inner := func(ctx *gekkahttp.RequestContext) { called = true }

	ctx, _ := newCtx("/extra")
	gekkahttp.PathEnd(inner)(ctx)

	if called {
		t.Error("expected inner NOT to be called when Remaining is non-empty")
	}
	if !ctx.Rejected {
		t.Error("expected Rejected=true on mismatch")
	}
}

func TestPathParam_Extract(t *testing.T) {
	var extractedVal string
	var innerCtxRemaining string

	ctx, _ := newCtx("/42/sub")
	gekkahttp.PathParam("id", func(v string) gekkahttp.Route {
		extractedVal = v
		return func(ctx *gekkahttp.RequestContext) {
			innerCtxRemaining = ctx.Remaining
		}
	})(ctx)

	if ctx.Rejected {
		t.Error("expected Rejected=false on successful extraction")
	}
	if extractedVal != "42" {
		t.Errorf("expected extracted value \"42\", got %q", extractedVal)
	}
	if innerCtxRemaining != "/sub" {
		t.Errorf("expected Remaining=\"/sub\" inside inner, got %q", innerCtxRemaining)
	}
	if ctx.Params["id"] != "42" {
		t.Errorf("expected ctx.Params[\"id\"]=\"42\", got %q", ctx.Params["id"])
	}
}

func TestPathParam_Empty(t *testing.T) {
	called := false
	ctx, _ := newCtx("")
	gekkahttp.PathParam("id", func(v string) gekkahttp.Route {
		called = true
		return func(ctx *gekkahttp.RequestContext) {}
	})(ctx)

	if called {
		t.Error("expected inner factory NOT to be called when Remaining is empty")
	}
	if !ctx.Rejected {
		t.Error("expected Rejected=true when Remaining is empty")
	}
}
