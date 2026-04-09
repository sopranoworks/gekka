/*
 * client_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package client

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestServices_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cluster/services" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"billing":["host1:2551","host2:2551"],"orders":["host3:2551"]}`))
	}))
	defer srv.Close()

	c := New(srv.URL)
	got, err := c.Services()
	if err != nil {
		t.Fatalf("Services: %v", err)
	}
	want := map[string][]string{
		"billing": {"host1:2551", "host2:2551"},
		"orders":  {"host3:2551"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestServices_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	if _, err := New(srv.URL).Services(); err == nil {
		t.Fatal("expected error on 500")
	}
}

func TestServices_BadJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`not json`))
	}))
	defer srv.Close()

	if _, err := New(srv.URL).Services(); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestConfigEntries_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cluster/config" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"pekko.cluster.roles":["frontend"],"pekko.remote.artery.canonical.port":2551}`))
	}))
	defer srv.Close()

	got, err := New(srv.URL).ConfigEntries()
	if err != nil {
		t.Fatalf("ConfigEntries: %v", err)
	}
	want := map[string]any{
		"pekko.cluster.roles":                []any{"frontend"},
		"pekko.remote.artery.canonical.port": float64(2551),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestConfigEntries_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	if _, err := New(srv.URL).ConfigEntries(); err == nil {
		t.Fatal("expected error on 500")
	}
}

func TestConfigEntries_BadJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`not json`))
	}))
	defer srv.Close()
	if _, err := New(srv.URL).ConfigEntries(); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestAlive_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health/alive" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"alive"}`))
	}))
	defer srv.Close()

	ok, msg, err := New(srv.URL).Alive()
	if err != nil {
		t.Fatalf("Alive: %v", err)
	}
	if !ok {
		t.Errorf("expected ok=true")
	}
	if msg != "alive" {
		t.Errorf("expected status=alive, got %q", msg)
	}
}

func TestAlive_NotAlive(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"status":"not_alive","reason":"starting"}`))
	}))
	defer srv.Close()

	ok, msg, err := New(srv.URL).Alive()
	if err != nil {
		t.Fatalf("Alive: %v", err)
	}
	if ok {
		t.Errorf("expected ok=false")
	}
	if msg != "starting" {
		t.Errorf("expected reason=starting, got %q", msg)
	}
}

func TestReady_NotReady(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"status":"not_ready","reason":"not_up"}`))
	}))
	defer srv.Close()

	ok, msg, err := New(srv.URL).Ready()
	if err != nil {
		t.Fatalf("Ready: %v", err)
	}
	if ok {
		t.Errorf("expected ok=false")
	}
	if msg != "not_up" {
		t.Errorf("expected reason=not_up, got %q", msg)
	}
}

func TestReady_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"status":"ready"}`))
	}))
	defer srv.Close()

	ok, msg, err := New(srv.URL).Ready()
	if err != nil {
		t.Fatalf("Ready: %v", err)
	}
	if !ok || msg != "ready" {
		t.Errorf("expected ok=true, msg=ready; got ok=%v, msg=%q", ok, msg)
	}
}

func TestDebugCRDTList_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cluster/debug/crdt" {
			t.Errorf("path: %s", r.URL.Path)
		}
		w.Write([]byte(`{"kind":"crdt-list","data":[{"key":"a","type":"gcounter"},{"key":"b","type":"orset"}]}`))
	}))
	defer srv.Close()

	got, err := New(srv.URL).DebugCRDTList()
	if err != nil {
		t.Fatalf("DebugCRDTList: %v", err)
	}
	if len(got) != 2 || got[0].Key != "a" || got[1].Type != "orset" {
		t.Errorf("got %+v", got)
	}
}

func TestDebugCRDT_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cluster/debug/crdt/my-counter" {
			t.Errorf("path: %s", r.URL.Path)
		}
		w.Write([]byte(`{"kind":"crdt","data":{"key":"my-counter","type":"gcounter","value":{"total":42}}}`))
	}))
	defer srv.Close()

	got, err := New(srv.URL).DebugCRDT("my-counter")
	if err != nil {
		t.Fatalf("DebugCRDT: %v", err)
	}
	if got == nil || got.Key != "my-counter" || got.Type != "gcounter" {
		t.Errorf("got %+v", got)
	}
}

func TestDebugCRDT_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":"key missing not found"}`))
	}))
	defer srv.Close()

	got, err := New(srv.URL).DebugCRDT("missing")
	if err != nil {
		t.Errorf("expected nil error on 404, got %v", err)
	}
	if got != nil {
		t.Errorf("expected nil value on 404, got %+v", got)
	}
}

func TestDebugActors_UserOnly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("system") != "" {
			t.Errorf("unexpected system query: %s", r.URL.RawQuery)
		}
		w.Write([]byte(`{"kind":"actors","data":[{"path":"/user/alpha","kind":"user"}]}`))
	}))
	defer srv.Close()

	got, err := New(srv.URL).DebugActors(false)
	if err != nil {
		t.Fatalf("DebugActors: %v", err)
	}
	if len(got) != 1 || got[0].Path != "/user/alpha" {
		t.Errorf("got %+v", got)
	}
}

func TestDebugActors_IncludeSystem(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("system") != "true" {
			t.Errorf("missing system=true: %s", r.URL.RawQuery)
		}
		w.Write([]byte(`{"kind":"actors","data":[{"path":"/user/alpha","kind":"user"},{"path":"/system/x","kind":"system"}]}`))
	}))
	defer srv.Close()

	got, err := New(srv.URL).DebugActors(true)
	if err != nil {
		t.Fatalf("DebugActors: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("len = %d", len(got))
	}
}
