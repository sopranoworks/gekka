/*
 * hocon_tls_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"crypto/tls"
	"reflect"
	"testing"
)

// TestHOCON_TLSCipherSuites_GekkaNative verifies the gekka-native
// `pekko.remote.artery.tls.cipher-suites` path maps IANA names to the Go
// `crypto/tls` numeric IDs.
func TestHOCON_TLSCipherSuites_GekkaNative(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical { hostname = "127.0.0.1", port = 2552 }
pekko.remote.artery.tls.cipher-suites = [
  "TLS_AES_128_GCM_SHA256",
  "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
]
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	want := []uint16{tls.TLS_AES_128_GCM_SHA256, tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256}
	if !reflect.DeepEqual(cfg.TLS.CipherSuites, want) {
		t.Fatalf("CipherSuites = %v, want %v", cfg.TLS.CipherSuites, want)
	}
}

// TestHOCON_TLSCipherSuites_PekkoAlias verifies that the Pekko-native
// `ssl.config-ssl-engine.enabled-algorithms` path is honored when the
// gekka-native path is absent.
func TestHOCON_TLSCipherSuites_PekkoAlias(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical { hostname = "127.0.0.1", port = 2552 }
pekko.remote.artery.ssl.config-ssl-engine.enabled-algorithms = [
  "TLS_CHACHA20_POLY1305_SHA256"
]
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	want := []uint16{tls.TLS_CHACHA20_POLY1305_SHA256}
	if !reflect.DeepEqual(cfg.TLS.CipherSuites, want) {
		t.Fatalf("CipherSuites = %v, want %v", cfg.TLS.CipherSuites, want)
	}
}

// TestHOCON_TLSCipherSuites_GekkaWinsOverAlias verifies the gekka-native
// path takes precedence when both forms are set.
func TestHOCON_TLSCipherSuites_GekkaWinsOverAlias(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical { hostname = "127.0.0.1", port = 2552 }
pekko.remote.artery.tls.cipher-suites = ["TLS_AES_256_GCM_SHA384"]
pekko.remote.artery.ssl.config-ssl-engine.enabled-algorithms = ["TLS_CHACHA20_POLY1305_SHA256"]
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	want := []uint16{tls.TLS_AES_256_GCM_SHA384}
	if !reflect.DeepEqual(cfg.TLS.CipherSuites, want) {
		t.Fatalf("CipherSuites = %v, want %v (gekka-native should win over alias)",
			cfg.TLS.CipherSuites, want)
	}
}

// TestHOCON_TLSCipherSuites_UnknownErrors verifies an unknown cipher name
// surfaces as a parse error rather than silently dropping.
func TestHOCON_TLSCipherSuites_UnknownErrors(t *testing.T) {
	_, err := parseHOCONString(`
pekko.remote.artery.canonical { hostname = "127.0.0.1", port = 2552 }
pekko.remote.artery.tls.cipher-suites = ["TLS_NOT_REAL"]
`)
	if err == nil {
		t.Fatal("expected error for unknown cipher suite, got nil")
	}
}

// TestHOCON_TLSCipherSuites_DefaultEmpty verifies omitting both forms
// leaves the field nil so Go falls back to its `crypto/tls` defaults.
func TestHOCON_TLSCipherSuites_DefaultEmpty(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical { hostname = "127.0.0.1", port = 2552 }
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if len(cfg.TLS.CipherSuites) != 0 {
		t.Fatalf("CipherSuites = %v, want empty (default)", cfg.TLS.CipherSuites)
	}
}
