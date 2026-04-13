// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	gekkahttp "github.com/sopranoworks/gekka/http"
)

func TestGet_Match(t *testing.T) {
	innerCalled := false
	inner := func(ctx *gekkahttp.RequestContext) {
		innerCalled = true
	}

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := gekkahttp.NewRequestContext(httptest.NewRecorder(), r)

	gekkahttp.Get(inner)(ctx)

	if !innerCalled {
		t.Error("expected inner to be called on GET match")
	}
	if ctx.Rejected {
		t.Error("expected Rejected=false on GET match")
	}
}

func TestGet_NoMatch(t *testing.T) {
	innerCalled := false
	inner := func(ctx *gekkahttp.RequestContext) {
		innerCalled = true
	}

	r := httptest.NewRequest(http.MethodPost, "/", nil)
	ctx := gekkahttp.NewRequestContext(httptest.NewRecorder(), r)

	gekkahttp.Get(inner)(ctx)

	if innerCalled {
		t.Error("expected inner NOT to be called on method mismatch")
	}
	if !ctx.Rejected {
		t.Error("expected Rejected=true on GET/POST mismatch")
	}
	found := false
	for _, m := range ctx.AllowedMethods {
		if m == http.MethodGet {
			found = true
		}
	}
	if !found {
		t.Errorf("expected AllowedMethods to contain GET, got %v", ctx.AllowedMethods)
	}
}

func TestPost_Match(t *testing.T) {
	innerCalled := false
	inner := func(ctx *gekkahttp.RequestContext) {
		innerCalled = true
	}

	r := httptest.NewRequest(http.MethodPost, "/", nil)
	ctx := gekkahttp.NewRequestContext(httptest.NewRecorder(), r)

	gekkahttp.Post(inner)(ctx)

	if !innerCalled {
		t.Error("expected inner to be called on POST match")
	}
	if ctx.Rejected {
		t.Error("expected Rejected=false on POST match")
	}
}

func TestMethodConcat_405(t *testing.T) {
	inner := func(ctx *gekkahttp.RequestContext) {
		ctx.Writer.WriteHeader(http.StatusOK)
	}

	r := httptest.NewRequest(http.MethodDelete, "/", nil)
	ctx := gekkahttp.NewRequestContext(httptest.NewRecorder(), r)

	gekkahttp.Concat(gekkahttp.Get(inner), gekkahttp.Post(inner))(ctx)

	if !ctx.Rejected {
		t.Error("expected Rejected=true when DELETE not in GET|POST")
	}

	hasGet, hasPost := false, false
	for _, m := range ctx.AllowedMethods {
		if m == http.MethodGet {
			hasGet = true
		}
		if m == http.MethodPost {
			hasPost = true
		}
	}
	if !hasGet {
		t.Errorf("expected AllowedMethods to contain GET, got %v", ctx.AllowedMethods)
	}
	if !hasPost {
		t.Errorf("expected AllowedMethods to contain POST, got %v", ctx.AllowedMethods)
	}
}

func TestMethodNoDuplicateAllowed(t *testing.T) {
	inner := func(ctx *gekkahttp.RequestContext) {}

	r := httptest.NewRequest(http.MethodPost, "/", nil)
	ctx := gekkahttp.NewRequestContext(httptest.NewRecorder(), r)

	// Run two Get directives via Concat — both reject; GET should appear only once.
	gekkahttp.Concat(gekkahttp.Get(inner), gekkahttp.Get(inner))(ctx)

	count := 0
	for _, m := range ctx.AllowedMethods {
		if m == http.MethodGet {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected GET to appear exactly once in AllowedMethods, got %d (list: %v)", count, ctx.AllowedMethods)
	}
}
