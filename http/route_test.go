// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	gekkahttp "github.com/sopranoworks/gekka/http"
)

func TestConcat_FirstMatchWins(t *testing.T) {
	called := make([]bool, 3)

	route1 := func(ctx *gekkahttp.RequestContext) {
		called[0] = true
		ctx.Rejected = true
	}
	route2 := func(ctx *gekkahttp.RequestContext) {
		called[1] = true
		ctx.Completed = true
		ctx.Writer.WriteHeader(http.StatusOK)
	}
	route3 := func(ctx *gekkahttp.RequestContext) {
		called[2] = true
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)

	gekkahttp.Concat(route1, route2, route3)(ctx)

	if !called[0] {
		t.Error("expected route1 to be called")
	}
	if !called[1] {
		t.Error("expected route2 to be called")
	}
	if called[2] {
		t.Error("expected route3 NOT to be called after route2 matched")
	}
	if ctx.Rejected {
		t.Error("expected Concat to not be rejected when route2 matched")
	}
}

func TestConcat_AllReject(t *testing.T) {
	route1 := func(ctx *gekkahttp.RequestContext) {
		ctx.Rejected = true
		ctx.AllowedMethods = []string{"GET"}
	}
	route2 := func(ctx *gekkahttp.RequestContext) {
		ctx.Rejected = true
		ctx.AllowedMethods = []string{"POST"}
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodDelete, "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)

	gekkahttp.Concat(route1, route2)(ctx)

	if !ctx.Rejected {
		t.Error("expected Concat to be rejected when all routes reject")
	}
}

func TestToHandler_404OnRejection(t *testing.T) {
	route := func(ctx *gekkahttp.RequestContext) {
		ctx.Rejected = true
	}

	handler := gekkahttp.ToHandler(route)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/missing", nil)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestToHandler_405WithAllowHeader(t *testing.T) {
	route := func(ctx *gekkahttp.RequestContext) {
		ctx.Rejected = true
		ctx.AllowedMethods = []string{"GET", "POST"}
	}

	handler := gekkahttp.ToHandler(route)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodDelete, "/resource", nil)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
	allow := w.Header().Get("Allow")
	if allow == "" {
		t.Error("expected Allow header to be set")
	}
	// Verify both methods appear
	if allow != "GET, POST" && allow != "POST, GET" {
		t.Errorf("unexpected Allow header value: %q", allow)
	}
}
