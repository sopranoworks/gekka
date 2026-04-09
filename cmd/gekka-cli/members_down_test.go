/*
 * members_down_test.go
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

func TestRunMemberDown_AutoYes(t *testing.T) {
	var called bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if r.Method != http.MethodPost {
			t.Errorf("want POST, got %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "/down") {
			t.Errorf("path missing /down: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	var out bytes.Buffer
	err := runMemberAction(memberActionDown, srv.URL, "pekko://System@host:2551",
		true /*autoYes*/, false /*jsonOut*/, false /*quiet*/,
		&bytes.Buffer{}, &out)
	if err != nil {
		t.Fatalf("runMemberAction: %v", err)
	}
	if !called {
		t.Error("server was not called")
	}
	if !strings.Contains(out.String(), "DOWN") {
		t.Errorf("expected success message, got %q", out.String())
	}
}

func TestRunMemberDown_Rejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("server should not be called when user says no")
	}))
	defer srv.Close()

	in := strings.NewReader("n\n")
	err := runMemberAction(memberActionDown, srv.URL, "pekko://System@host:2551",
		false, false, false, in, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("runMemberAction: %v", err)
	}
}

func TestRunMemberDown_JSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	var out bytes.Buffer
	err := runMemberAction(memberActionDown, srv.URL, "pekko://System@host:2551",
		true, true, false, &bytes.Buffer{}, &out)
	if err != nil {
		t.Fatalf("runMemberAction: %v", err)
	}
	if !strings.Contains(out.String(), `"action":"down"`) {
		t.Errorf("expected JSON output, got %q", out.String())
	}
	if !strings.Contains(out.String(), `"status":"ok"`) {
		t.Errorf("expected status ok, got %q", out.String())
	}
}

func TestRunMemberDown_Quiet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	var out bytes.Buffer
	err := runMemberAction(memberActionDown, srv.URL, "pekko://System@host:2551",
		true, false, true, &bytes.Buffer{}, &out)
	if err != nil {
		t.Fatalf("runMemberAction: %v", err)
	}
	if out.Len() != 0 {
		t.Errorf("expected no output in quiet mode, got %q", out.String())
	}
}
