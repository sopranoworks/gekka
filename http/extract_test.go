// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	gekkahttp "github.com/sopranoworks/gekka/http"
)

func TestParameter_Present(t *testing.T) {
	called := false
	var got string
	route := gekkahttp.Parameter("q", func(v string) gekkahttp.Route {
		return func(ctx *gekkahttp.RequestContext) {
			called = true
			got = v
		}
	})

	req := httptest.NewRequest("GET", "/?q=hello", nil)
	ctx := gekkahttp.NewRequestContext(httptest.NewRecorder(), req)
	route(ctx)

	if !called {
		t.Fatal("inner not called")
	}
	if got != "hello" {
		t.Fatalf("expected 'hello', got %q", got)
	}
	if ctx.Rejected {
		t.Fatal("should not be rejected")
	}
}

func TestParameter_Absent(t *testing.T) {
	route := gekkahttp.Parameter("q", func(v string) gekkahttp.Route {
		return func(ctx *gekkahttp.RequestContext) {
			t.Fatal("inner should not be called")
		}
	})

	req := httptest.NewRequest("GET", "/", nil)
	ctx := gekkahttp.NewRequestContext(httptest.NewRecorder(), req)
	route(ctx)

	if !ctx.Rejected {
		t.Fatal("expected Rejected=true")
	}
}

func TestOptionalParameter_Present(t *testing.T) {
	called := false
	var gotVal string
	var gotPresent bool
	route := gekkahttp.OptionalParameter("q", func(v string, present bool) gekkahttp.Route {
		return func(ctx *gekkahttp.RequestContext) {
			called = true
			gotVal = v
			gotPresent = present
		}
	})

	req := httptest.NewRequest("GET", "/?q=hello", nil)
	ctx := gekkahttp.NewRequestContext(httptest.NewRecorder(), req)
	route(ctx)

	if !called {
		t.Fatal("inner not called")
	}
	if gotVal != "hello" {
		t.Fatalf("expected 'hello', got %q", gotVal)
	}
	if !gotPresent {
		t.Fatal("expected present=true")
	}
	if ctx.Rejected {
		t.Fatal("should not be rejected")
	}
}

func TestOptionalParameter_Absent(t *testing.T) {
	called := false
	var gotVal string
	var gotPresent bool
	route := gekkahttp.OptionalParameter("q", func(v string, present bool) gekkahttp.Route {
		return func(ctx *gekkahttp.RequestContext) {
			called = true
			gotVal = v
			gotPresent = present
		}
	})

	req := httptest.NewRequest("GET", "/", nil)
	ctx := gekkahttp.NewRequestContext(httptest.NewRecorder(), req)
	route(ctx)

	if !called {
		t.Fatal("inner not called")
	}
	if gotVal != "" {
		t.Fatalf("expected empty string, got %q", gotVal)
	}
	if gotPresent {
		t.Fatal("expected present=false")
	}
	if ctx.Rejected {
		t.Fatal("should not be rejected")
	}
}

func TestHeader_Present(t *testing.T) {
	called := false
	var got string
	route := gekkahttp.Header("X-Token", func(v string) gekkahttp.Route {
		return func(ctx *gekkahttp.RequestContext) {
			called = true
			got = v
		}
	})

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Token", "mytoken")
	ctx := gekkahttp.NewRequestContext(httptest.NewRecorder(), req)
	route(ctx)

	if !called {
		t.Fatal("inner not called")
	}
	if got != "mytoken" {
		t.Fatalf("expected 'mytoken', got %q", got)
	}
	if ctx.Rejected {
		t.Fatal("should not be rejected")
	}
}

func TestHeader_Absent(t *testing.T) {
	route := gekkahttp.Header("X-Token", func(v string) gekkahttp.Route {
		return func(ctx *gekkahttp.RequestContext) {
			t.Fatal("inner should not be called")
		}
	})

	req := httptest.NewRequest("GET", "/", nil)
	ctx := gekkahttp.NewRequestContext(httptest.NewRecorder(), req)
	route(ctx)

	if !ctx.Rejected {
		t.Fatal("expected Rejected=true")
	}
}

func TestOptionalHeader_Present(t *testing.T) {
	called := false
	var gotVal string
	var gotPresent bool
	route := gekkahttp.OptionalHeader("X-Token", func(v string, present bool) gekkahttp.Route {
		return func(ctx *gekkahttp.RequestContext) {
			called = true
			gotVal = v
			gotPresent = present
		}
	})

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Token", "mytoken")
	ctx := gekkahttp.NewRequestContext(httptest.NewRecorder(), req)
	route(ctx)

	if !called {
		t.Fatal("inner not called")
	}
	if gotVal != "mytoken" {
		t.Fatalf("expected 'mytoken', got %q", gotVal)
	}
	if !gotPresent {
		t.Fatal("expected present=true")
	}
	if ctx.Rejected {
		t.Fatal("should not be rejected")
	}
}

func TestOptionalHeader_Absent(t *testing.T) {
	called := false
	var gotVal string
	var gotPresent bool
	route := gekkahttp.OptionalHeader("X-Token", func(v string, present bool) gekkahttp.Route {
		return func(ctx *gekkahttp.RequestContext) {
			called = true
			gotVal = v
			gotPresent = present
		}
	})

	req := httptest.NewRequest("GET", "/", nil)
	ctx := gekkahttp.NewRequestContext(httptest.NewRecorder(), req)
	route(ctx)

	if !called {
		t.Fatal("inner not called")
	}
	if gotVal != "" {
		t.Fatalf("expected empty string, got %q", gotVal)
	}
	if gotPresent {
		t.Fatal("expected present=false")
	}
	if ctx.Rejected {
		t.Fatal("should not be rejected")
	}
}

func TestExtractRequest_Called(t *testing.T) {
	called := false
	var gotReq *http.Request
	route := gekkahttp.ExtractRequest(func(r *http.Request) gekkahttp.Route {
		return func(ctx *gekkahttp.RequestContext) {
			called = true
			gotReq = r
		}
	})

	req := httptest.NewRequest("GET", "/", nil)
	ctx := gekkahttp.NewRequestContext(httptest.NewRecorder(), req)
	route(ctx)

	if !called {
		t.Fatal("inner not called")
	}
	if gotReq != req {
		t.Fatal("inner received wrong request")
	}
	if ctx.Rejected {
		t.Fatal("should not be rejected")
	}
}
