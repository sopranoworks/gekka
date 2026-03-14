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

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool, // used by client to verify server cert
		ClientCAs:    pool, // used by server to verify client cert
		ClientAuth:   clientAuth,
		MinVersion:   minVersion,
		ServerName:   cfg.ServerName,
	}, nil
}
