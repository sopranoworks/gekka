// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	gekkahttp "github.com/sopranoworks/gekka/http"
)

func TestRespondWithHeader_Sets(t *testing.T) {
	inner := func(ctx *gekkahttp.RequestContext) {
		ctx.Writer.WriteHeader(200)
		ctx.Completed = true
	}
	route := gekkahttp.RespondWithHeader("X-Custom", "hello", inner)

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	gekkahttp.ToHandler(route).ServeHTTP(w, req)

	if got := w.Header().Get("X-Custom"); got != "hello" {
		t.Fatalf("expected X-Custom=hello, got %q", got)
	}
}

func TestRespondWithHeaders_Multiple(t *testing.T) {
	inner := func(ctx *gekkahttp.RequestContext) {
		ctx.Writer.WriteHeader(200)
		ctx.Completed = true
	}
	headers := map[string]string{
		"X-One": "1",
		"X-Two": "2",
	}
	route := gekkahttp.RespondWithHeaders(headers, inner)

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	gekkahttp.ToHandler(route).ServeHTTP(w, req)

	if got := w.Header().Get("X-One"); got != "1" {
		t.Fatalf("expected X-One=1, got %q", got)
	}
	if got := w.Header().Get("X-Two"); got != "2" {
		t.Fatalf("expected X-Two=2, got %q", got)
	}
}

func TestMapResponseHeaders_Transforms(t *testing.T) {
	inner := func(ctx *gekkahttp.RequestContext) {
		ctx.Writer.WriteHeader(200)
		ctx.Completed = true
	}
	transform := func(h http.Header) {
		h.Set("X-Processed", "true")
	}
	route := gekkahttp.MapResponseHeaders(transform, inner)

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	gekkahttp.ToHandler(route).ServeHTTP(w, req)

	if got := w.Header().Get("X-Processed"); got != "true" {
		t.Fatalf("expected X-Processed=true, got %q", got)
	}
}
