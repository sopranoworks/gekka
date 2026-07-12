/*
 * tls_config.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"crypto/tls"
	"fmt"

	"github.com/sopranoworks/gekka/internal/core"
)

// TLSConfig is a root-package alias for the internal Artery TLS parameter
// struct. It lets external callers name and construct the HOCON-equivalent TLS
// configuration (the same struct that populates ClusterConfig.TLS) without
// importing internal/core, which the Go toolchain forbids from outside this
// module. Mirrors the existing Serializer / SerializationRegistry aliases.
type TLSConfig = core.TLSConfig

// resolveArteryTLSConfig selects the *tls.Config used for the Artery TLS
// listener and dialer. It is the single decision point shared by the
// production wiring (NewCluster) and the unit tests.
//
// Precedence rule (see ClusterConfig.RawTLSConfig):
//
//   - When raw is non-nil it is an opt-in escape hatch: it is returned
//     verbatim (never mutated), bypassing BuildTLSConfig's CA-chain
//     construction entirely, so callers can supply a custom
//     VerifyPeerCertificate/VerifyConnection callback — e.g. public-key
//     fingerprint pinning with no CA at all.
//   - Supplying BOTH raw AND a populated HOCON TLS struct is rejected as a
//     configuration error rather than silently preferring one source, so the
//     intent is never ambiguous.
//   - When raw is nil the behavior is exactly as before this patch:
//     BuildTLSConfig constructs the *tls.Config from the HOCON tls.* fields.
func resolveArteryTLSConfig(raw *tls.Config, hocon core.TLSConfig) (*tls.Config, error) {
	if raw != nil {
		if tlsConfigPopulated(hocon) {
			return nil, fmt.Errorf("gekka: RawTLSConfig and HOCON tls.* config are mutually exclusive; set exactly one")
		}
		return raw, nil
	}
	return core.BuildTLSConfig(hocon)
}

// tlsConfigPopulated reports whether the HOCON TLS struct carries any of the
// PEM-path fields that BuildTLSConfig consumes — i.e. whether the caller has
// actually configured TLS via HOCON. MinVersion/ServerName/CipherSuites alone
// do not count: they are inert without the certificate material, so they never
// trigger the mutual-exclusion error against a RawTLSConfig.
func tlsConfigPopulated(cfg core.TLSConfig) bool {
	return cfg.CertFile != "" || cfg.KeyFile != "" || cfg.CAFile != ""
}
