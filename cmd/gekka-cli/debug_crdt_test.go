/*
 * debug_crdt_test.go
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

func TestRunDebugCRDTList(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"kind":"crdt-list","data":[{"key":"a","type":"gcounter"},{"key":"b","type":"orset"}]}`))
	}))
	defer srv.Close()

	var out bytes.Buffer
	err := runDebugCRDTList(srv.URL, false, &out)
	if err != nil {
		t.Fatalf("runDebugCRDTList: %v", err)
	}
	s := out.String()
	if !strings.Contains(s, "KEY") || !strings.Contains(s, "TYPE") {
		t.Errorf("missing header: %q", s)
	}
	if !strings.Contains(s, "a") || !strings.Contains(s, "gcounter") {
		t.Errorf("missing entries: %q", s)
	}
}

func TestRunDebugCRDTList_JSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"kind":"crdt-list","data":[{"key":"a","type":"gcounter"}]}`))
	}))
	defer srv.Close()

	var out bytes.Buffer
	err := runDebugCRDTList(srv.URL, true, &out)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if !strings.Contains(out.String(), `"key"`) {
		t.Errorf("expected JSON: %q", out.String())
	}
}

func TestRunDebugCRDT_Detail(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"kind":"crdt","data":{"key":"my-counter","type":"gcounter","value":{"total":42,"per_node":{"node-1":42}}}}`))
	}))
	defer srv.Close()

	var out bytes.Buffer
	err := runDebugCRDT(srv.URL, "my-counter", false, &out)
	if err != nil {
		t.Fatalf("%v", err)
	}
	s := out.String()
	if !strings.Contains(s, "my-counter") {
		t.Errorf("missing key in header: %q", s)
	}
	if !strings.Contains(s, "42") {
		t.Errorf("missing value 42: %q", s)
	}
}

func TestRunDebugCRDT_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":"key missing not found"}`))
	}))
	defer srv.Close()

	err := runDebugCRDT(srv.URL, "missing", false, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected not found error, got %v", err)
	}
}
