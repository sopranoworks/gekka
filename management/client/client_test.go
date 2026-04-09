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
	if len(got) != 2 {
		t.Errorf("expected 2 entries, got %d: %v", len(got), got)
	}
	if _, ok := got["pekko.cluster.roles"]; !ok {
		t.Errorf("missing pekko.cluster.roles key")
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
