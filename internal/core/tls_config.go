/*
 * tls_config.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
)

// TLSConfig holds the parameters for Artery TLS transport.
// All fields are ignored when Transport == "tcp".
type TLSConfig struct {
	// CertFile is the path to the PEM-encoded node certificate.
	CertFile string

	// KeyFile is the path to the PEM-encoded private key.
	KeyFile string

	// CAFile is the path to the PEM-encoded CA bundle for peer verification.
	CAFile string

	// MinVersion is the minimum TLS version (tls.VersionTLS12 or tls.VersionTLS13).
	// Defaults to tls.VersionTLS12.
	MinVersion uint16

	// RequireClientAuth enables mutual TLS (both sides send certificates).
	RequireClientAuth bool

	// ServerName overrides the SNI hostname used in the client handshake.
	ServerName string

	// CipherSuites is the explicit allow-list of TLS 1.2 cipher suite IDs
	// (Go `crypto/tls` constants such as tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256).
	// When non-empty it is passed through to *tls.Config.CipherSuites.
	// Note: Go's TLS 1.3 cipher set is spec-fixed and ignores this field;
	// the list only affects TLS 1.2 handshakes.
	// When empty (default) Go's `crypto/tls` defaults are used.
	CipherSuites []uint16
}

// ParseCipherSuiteNames maps IANA cipher suite names (the same names Pekko's
// `ssl.config-ssl-engine.enabled-algorithms` accepts, e.g.
// "TLS_AES_128_GCM_SHA256", "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256") to the
// Go `crypto/tls` numeric IDs.
//
// Both secure and explicitly-insecure suites Go knows about are accepted —
// callers selecting an insecure suite are taking responsibility for that.
// Unknown names return an error listing the offending entries.
func ParseCipherSuiteNames(names []string) ([]uint16, error) {
	if len(names) == 0 {
		return nil, nil
	}
	known := make(map[string]uint16)
	for _, s := range tls.CipherSuites() {
		known[s.Name] = s.ID
	}
	for _, s := range tls.InsecureCipherSuites() {
		known[s.Name] = s.ID
	}
	out := make([]uint16, 0, len(names))
	var unknown []string
	for _, raw := range names {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		id, ok := known[name]
		if !ok {
			unknown = append(unknown, name)
			continue
		}
		out = append(out, id)
	}
	if len(unknown) > 0 {
		return nil, fmt.Errorf("tls: unknown cipher suite name(s): %s", strings.Join(unknown, ", "))
	}
	return out, nil
}

// BuildTLSConfig converts a TLSConfig into a *tls.Config suitable for use on
// both the server (listener) and client (dialer) side.
//
// Server side: tls.Config.ClientAuth is set to tls.RequireAndVerifyClientCert
// when TLSConfig.RequireClientAuth is true.
// Client side: tls.Config.ServerName is set from TLSConfig.ServerName.
func BuildTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("tls: load key pair: %w", err)
	}

	pool := x509.NewCertPool()
	caPEM, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return nil, fmt.Errorf("tls: read CA file: %w", err)
	}
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("tls: no valid CA certificates in %s", cfg.CAFile)
	}

	minVersion := cfg.MinVersion
	if minVersion == 0 {
		minVersion = tls.VersionTLS12
	}

	clientAuth := tls.NoClientCert
	if cfg.RequireClientAuth {
		clientAuth = tls.RequireAndVerifyClientCert
	}

	out := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool, // used by client to verify server cert
		ClientCAs:    pool, // used by server to verify client cert
		ClientAuth:   clientAuth,
		MinVersion:   minVersion,
		ServerName:   cfg.ServerName,
	}
	if len(cfg.CipherSuites) > 0 {
		out.CipherSuites = append([]uint16(nil), cfg.CipherSuites...)
	}
	return out, nil
}
