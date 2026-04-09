/*
 * members_leave_test.go
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

func TestRunMemberLeave_AutoYes(t *testing.T) {
	var called bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if r.Method != http.MethodPost {
			t.Errorf("want POST, got %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "/leave") {
			t.Errorf("path missing /leave: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	var out bytes.Buffer
	err := runMemberAction(memberActionLeave, srv.URL, "pekko://System@host:2551",
		true, false, false, &bytes.Buffer{}, &out)
	if err != nil {
		t.Fatalf("runMemberAction: %v", err)
	}
	if !called {
		t.Error("server was not called")
	}
	if !strings.Contains(out.String(), "LEAVE") {
		t.Errorf("expected LEAVE in output, got %q", out.String())
	}
}
