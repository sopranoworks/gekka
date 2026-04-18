// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http_test

import (
	"net/http/httptest"
	"testing"

	gekkahttp "github.com/sopranoworks/gekka/http"
)

func TestCORS_DefaultAllowsAll(t *testing.T) {
	inner := func(ctx *gekkahttp.RequestContext) {
		ctx.Writer.WriteHeader(200)
		ctx.Completed = true
	}
	route := gekkahttp.CORS(gekkahttp.DefaultCorsSettings(), inner)

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Origin", "https://example.com")
	w := httptest.NewRecorder()
	gekkahttp.ToHandler(route).ServeHTTP(w, req)

	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("expected Access-Control-Allow-Origin=*, got %q", got)
	}
}

func TestCORS_Preflight(t *testing.T) {
	inner := func(ctx *gekkahttp.RequestContext) {
		t.Fatal("inner should not be called for preflight")
	}
	route := gekkahttp.CORS(gekkahttp.DefaultCorsSettings(), inner)

	req := httptest.NewRequest("OPTIONS", "/api/data", nil)
	req.Header.Set("Origin", "https://example.com")
	req.Header.Set("Access-Control-Request-Method", "POST")
	w := httptest.NewRecorder()
	gekkahttp.ToHandler(route).ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if got := w.Header().Get("Access-Control-Allow-Methods"); got == "" {
		t.Fatal("expected Access-Control-Allow-Methods to be set")
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("expected Access-Control-Allow-Origin=*, got %q", got)
	}
}

func TestCORS_CustomBlocksUnknown(t *testing.T) {
	innerCalled := false
	inner := func(ctx *gekkahttp.RequestContext) {
		innerCalled = true
		ctx.Writer.WriteHeader(200)
		ctx.Completed = true
	}
	settings := gekkahttp.CorsSettings{
		AllowedOrigins: []string{"https://trusted.com"},
		AllowedMethods: []string{"GET", "POST"},
	}
	route := gekkahttp.CORS(settings, inner)

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Origin", "https://evil.com")
	w := httptest.NewRecorder()
	gekkahttp.ToHandler(route).ServeHTTP(w, req)

	if !innerCalled {
		t.Fatal("inner should still be called for non-matching origin")
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("expected no Access-Control-Allow-Origin header, got %q", got)
	}
}

func TestCORS_NoOriginPassthrough(t *testing.T) {
	innerCalled := false
	inner := func(ctx *gekkahttp.RequestContext) {
		innerCalled = true
		ctx.Writer.WriteHeader(200)
		ctx.Completed = true
	}
	route := gekkahttp.CORS(gekkahttp.DefaultCorsSettings(), inner)

	req := httptest.NewRequest("GET", "/", nil)
	// No Origin header set.
	w := httptest.NewRecorder()
	gekkahttp.ToHandler(route).ServeHTTP(w, req)

	if !innerCalled {
		t.Fatal("inner should be called when no Origin header")
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("expected no CORS headers, got Access-Control-Allow-Origin=%q", got)
	}
}
