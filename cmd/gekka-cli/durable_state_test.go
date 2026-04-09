/*
 * durable_state_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunDurableState_JSONBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/durable-state/") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Write([]byte(`{"persistence_id":"user-42","revision":137,"state":{"name":"alice","age":30}}`))
	}))
	defer srv.Close()

	var out bytes.Buffer
	err := runDurableState(srv.URL, "user-42", false, false, &out)
	if err != nil {
		t.Fatalf("runDurableState: %v", err)
	}
	s := out.String()
	if !strings.Contains(s, "user-42") || !strings.Contains(s, "137") {
		t.Errorf("missing header fields: %q", s)
	}
	if !strings.Contains(s, "alice") {
		t.Errorf("expected JSON body rendering, got %q", s)
	}
}

func TestRunDurableState_HexBody(t *testing.T) {
	// State is a non-JSON-shaped value (raw number) → should hex-dump.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"persistence_id":"bin-1","revision":1,"state":42}`))
	}))
	defer srv.Close()

	var out bytes.Buffer
	err := runDurableState(srv.URL, "bin-1", false, false, &out)
	if err != nil {
		t.Fatalf("runDurableState: %v", err)
	}
	// The numeric state marshals to "42" which is not prefixed by {[" → hexdump.
	if !strings.Contains(out.String(), "00000000") {
		t.Errorf("expected hex offset in output, got %q", out.String())
	}
}

func TestIsLikelyJSON(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{`{"x":1}`, true},
		{`[1,2,3]`, true},
		{`"hello"`, true},
		{`42`, false},
		{`   {"x":1}`, true},
		{`garbage`, false},
		{``, false},
	}
	for _, c := range cases {
		got := isLikelyJSON([]byte(c.in))
		if got != c.want {
			t.Errorf("isLikelyJSON(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}
