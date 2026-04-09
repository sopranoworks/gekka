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
