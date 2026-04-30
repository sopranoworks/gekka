/*
 * tls_config_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"crypto/tls"
	"strings"
	"testing"
)

func TestParseCipherSuiteNames_Empty(t *testing.T) {
	got, err := ParseCipherSuiteNames(nil)
	if err != nil {
		t.Fatalf("nil input: unexpected error: %v", err)
	}
	if got != nil {
		t.Fatalf("nil input: expected nil result, got %v", got)
	}

	got, err = ParseCipherSuiteNames([]string{"", "  "})
	if err != nil {
		t.Fatalf("blank input: unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("blank input: expected empty result, got %v", got)
	}
}

func TestParseCipherSuiteNames_KnownSuites(t *testing.T) {
	names := []string{
		"TLS_AES_128_GCM_SHA256",
		"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
	}
	got, err := ParseCipherSuiteNames(names)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []uint16{tls.TLS_AES_128_GCM_SHA256, tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256}
	if len(got) != len(want) {
		t.Fatalf("got %d ids, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("ids[%d] = 0x%04x, want 0x%04x", i, got[i], want[i])
		}
	}
}

func TestParseCipherSuiteNames_UnknownReturnsError(t *testing.T) {
	_, err := ParseCipherSuiteNames([]string{"TLS_AES_128_GCM_SHA256", "TLS_NOT_REAL"})
	if err == nil {
		t.Fatal("expected error for unknown suite, got nil")
	}
	if !strings.Contains(err.Error(), "TLS_NOT_REAL") {
		t.Fatalf("error %q should mention the offending suite", err)
	}
}
