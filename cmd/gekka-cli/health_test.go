/*
 * health_test.go
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
	"time"
)

func newHealthServer(aliveOK, readyOK bool) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health/alive":
			if !aliveOK {
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte(`{"status":"dead"}`))
				return
			}
			w.Write([]byte(`{"status":"alive"}`))
		case "/health/ready":
			if !readyOK {
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte(`{"status":"not_ready","reason":"not_up"}`))
				return
			}
			w.Write([]byte(`{"status":"ready"}`))
		}
	}))
}

func TestRunHealth_BothOK(t *testing.T) {
	srv := newHealthServer(true, true)
	defer srv.Close()
	var out bytes.Buffer
	exit := runHealth(srv.URL, 3*time.Second, false, false, &out)
	if exit != 0 {
		t.Errorf("expected exit 0, got %d", exit)
	}
	if !strings.Contains(out.String(), "alive") || !strings.Contains(out.String(), "ready") {
		t.Errorf("output missing probe rows: %q", out.String())
	}
}

func TestRunHealth_ReadyFails(t *testing.T) {
	srv := newHealthServer(true, false)
	defer srv.Close()
	var out bytes.Buffer
	exit := runHealth(srv.URL, 3*time.Second, false, false, &out)
	if exit != 1 {
		t.Errorf("expected exit 1, got %d", exit)
	}
	if !strings.Contains(out.String(), "not_up") {
		t.Errorf("expected reason in output, got %q", out.String())
	}
}

func TestRunHealth_Unreachable(t *testing.T) {
	// Closed server → connection refused.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	srv.Close()
	exit := runHealth(srv.URL, 500*time.Millisecond, false, false, &bytes.Buffer{})
	if exit != 2 {
		t.Errorf("expected exit 2, got %d", exit)
	}
}

func TestRunHealth_Quiet(t *testing.T) {
	srv := newHealthServer(true, true)
	defer srv.Close()
	var out bytes.Buffer
	exit := runHealth(srv.URL, 3*time.Second, false, true, &out)
	if exit != 0 {
		t.Errorf("expected exit 0, got %d", exit)
	}
	if out.Len() != 0 {
		t.Errorf("quiet mode should produce no output, got %q", out.String())
	}
}

func TestRunHealth_JSON(t *testing.T) {
	srv := newHealthServer(true, false)
	defer srv.Close()
	var out bytes.Buffer
	exit := runHealth(srv.URL, 3*time.Second, true, false, &out)
	if exit != 1 {
		t.Errorf("expected exit 1, got %d", exit)
	}
	if !strings.Contains(out.String(), `"alive"`) || !strings.Contains(out.String(), `"ready"`) {
		t.Errorf("expected JSON keys, got %q", out.String())
	}
}
