/*
 * proxy_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package singleton

import (
	"bytes"
	"context"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

// ── #12: singleton-identification-interval config flow ──────────────────────

func TestProxy_IdentificationInterval_Default(t *testing.T) {
	proxy := &ClusterSingletonProxy{
		managerPath:            "/user/singletonManager",
		singletonName:          "singleton",
		identificationInterval: 1 * time.Second,
		bufferSize:             1000,
		stopCh:                 make(chan struct{}),
	}

	if proxy.IdentificationInterval() != 1*time.Second {
		t.Errorf("default IdentificationInterval = %v, want 1s", proxy.IdentificationInterval())
	}
}

func TestProxy_IdentificationInterval_ConfigFlow(t *testing.T) {
	// Simulate proxy created with defaults, then HOCON config applied
	// (as Cluster.SingletonProxy() does via WithIdentificationInterval)
	proxy := &ClusterSingletonProxy{
		managerPath:            "/user/singletonManager",
		singletonName:          "singleton",
		identificationInterval: 1 * time.Second,
		bufferSize:             1000,
		stopCh:                 make(chan struct{}),
	}

	proxy.WithIdentificationInterval(3 * time.Second)

	if proxy.IdentificationInterval() != 3*time.Second {
		t.Errorf("IdentificationInterval after config = %v, want 3s", proxy.IdentificationInterval())
	}
}

func TestProxy_IdentificationInterval_IgnoresZero(t *testing.T) {
	proxy := &ClusterSingletonProxy{
		managerPath:            "/user/singletonManager",
		singletonName:          "singleton",
		identificationInterval: 1 * time.Second,
		bufferSize:             1000,
		stopCh:                 make(chan struct{}),
	}

	proxy.WithIdentificationInterval(0)

	if proxy.IdentificationInterval() != 1*time.Second {
		t.Errorf("IdentificationInterval after zero = %v, want 1s (unchanged)", proxy.IdentificationInterval())
	}
}

// ── #13: buffer-size config flow ────────────────────────────────────────────

func TestProxy_BufferSize_Default(t *testing.T) {
	proxy := &ClusterSingletonProxy{
		managerPath:            "/user/singletonManager",
		singletonName:          "singleton",
		identificationInterval: 1 * time.Second,
		bufferSize:             1000,
		stopCh:                 make(chan struct{}),
	}

	if proxy.BufferSizeLimit() != 1000 {
		t.Errorf("default BufferSizeLimit = %d, want 1000", proxy.BufferSizeLimit())
	}
}

func TestProxy_BufferSize_ConfigFlow(t *testing.T) {
	proxy := &ClusterSingletonProxy{
		managerPath:            "/user/singletonManager",
		singletonName:          "singleton",
		identificationInterval: 1 * time.Second,
		bufferSize:             1000,
		stopCh:                 make(chan struct{}),
	}

	proxy.WithBufferSize(500)

	if proxy.BufferSizeLimit() != 500 {
		t.Errorf("BufferSizeLimit after config = %d, want 500", proxy.BufferSizeLimit())
	}
}

// ── #13: buffer overflow warning log ────────────────────────────────────────

func TestProxy_BufferOverflow_WarningLog(t *testing.T) {
	proxy := &ClusterSingletonProxy{
		managerPath:            "/user/singletonManager",
		singletonName:          "singleton",
		identificationInterval: 1 * time.Second,
		bufferSize:             2,
		stopCh:                 make(chan struct{}),
	}

	// Capture log output
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	// Fill buffer to capacity (no overflow yet)
	proxy.bufferMessage(bufferedMessage{ctx: context.Background(), msg: "a"})
	proxy.bufferMessage(bufferedMessage{ctx: context.Background(), msg: "b"})
	if buf.Len() > 0 {
		t.Fatalf("unexpected warning before overflow: %s", buf.String())
	}

	// 3rd message causes overflow → should warn and drop oldest
	proxy.bufferMessage(bufferedMessage{ctx: context.Background(), msg: "c"})

	logOutput := buf.String()
	if !strings.Contains(logOutput, "Singleton proxy buffer is full") {
		t.Errorf("expected 'Singleton proxy buffer is full' warning, got: %q", logOutput)
	}

	// Buffer should still have bufferSize items (oldest dropped)
	if len(proxy.buffer) != 2 {
		t.Errorf("buffer len = %d, want 2 after overflow", len(proxy.buffer))
	}
}
