// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	gekkahttp "github.com/sopranoworks/gekka/http"
)

func TestComplete_String(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)

	gekkahttp.Complete(http.StatusOK, "hello")(ctx)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/plain") {
		t.Errorf("expected text/plain Content-Type, got %q", ct)
	}
	if w.Body.String() != "hello" {
		t.Errorf("expected body %q, got %q", "hello", w.Body.String())
	}
	if !ctx.Completed {
		t.Error("expected ctx.Completed to be true")
	}
}

func TestComplete_Bytes(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)

	payload := []byte{1, 2, 3}
	gekkahttp.Complete(http.StatusOK, payload)(ctx)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if ct != "application/octet-stream" {
		t.Errorf("expected application/octet-stream Content-Type, got %q", ct)
	}
	if !bytes.Equal(w.Body.Bytes(), payload) {
		t.Errorf("expected body %v, got %v", payload, w.Body.Bytes())
	}
	if !ctx.Completed {
		t.Error("expected ctx.Completed to be true")
	}
}

func TestComplete_Nil(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)

	gekkahttp.Complete(http.StatusNoContent, nil)(ctx)

	if w.Code != http.StatusNoContent {
		t.Errorf("expected status 204, got %d", w.Code)
	}
	if w.Body.Len() != 0 {
		t.Errorf("expected no body, got %q", w.Body.String())
	}
	if !ctx.Completed {
		t.Error("expected ctx.Completed to be true")
	}
}

func TestComplete_Struct(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)

	type payload struct{ Foo string }
	gekkahttp.Complete(http.StatusOK, payload{Foo: "bar"})(ctx)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "application/json") {
		t.Errorf("expected application/json Content-Type, got %q", ct)
	}
	body := strings.TrimSpace(w.Body.String())
	if body != `{"Foo":"bar"}` {
		t.Errorf("expected body %q, got %q", `{"Foo":"bar"}`, body)
	}
	if !ctx.Completed {
		t.Error("expected ctx.Completed to be true")
	}
}

func TestCompleteWith(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)

	gekkahttp.CompleteWith(http.StatusOK, "text/xml", []byte("<x/>"))(ctx)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if ct != "text/xml" {
		t.Errorf("expected text/xml Content-Type, got %q", ct)
	}
	if w.Body.String() != "<x/>" {
		t.Errorf("expected body %q, got %q", "<x/>", w.Body.String())
	}
	if !ctx.Completed {
		t.Error("expected ctx.Completed to be true")
	}
}

func TestRedirect(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/old", nil)
	ctx := gekkahttp.NewRequestContext(w, r)

	gekkahttp.Redirect("/new", http.StatusFound)(ctx)

	if w.Code != http.StatusFound {
		t.Errorf("expected status 302, got %d", w.Code)
	}
	loc := w.Header().Get("Location")
	if loc != "/new" {
		t.Errorf("expected Location /new, got %q", loc)
	}
	if !ctx.Completed {
		t.Error("expected ctx.Completed to be true")
	}
}

func TestReject(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := gekkahttp.NewRequestContext(w, r)

	gekkahttp.Reject()(ctx)

	if !ctx.Rejected {
		t.Error("expected ctx.Rejected to be true")
	}
	if ctx.Completed {
		t.Error("expected ctx.Completed to be false")
	}
}
