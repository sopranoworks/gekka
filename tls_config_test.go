/*
 * tls_config_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/internal/core"
)

// writeSelfSignedTLSFiles generates one self-signed ECDSA cert/key and writes
// cert.pem + key.pem to a temp dir, returning a core.TLSConfig that points at
// them (cert doubles as its own CA, so BuildTLSConfig's pool accepts it). This
// is the minimal "HOCON TLS is populated with valid PEM material" fixture.
func writeSelfSignedTLSFiles(t *testing.T) core.TLSConfig {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "gekka-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}

	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return core.TLSConfig{CertFile: certPath, KeyFile: keyPath, CAFile: certPath}
}

// (a) RawTLSConfig is honored verbatim when set (and HOCON TLS is empty): the
// exact pointer is returned, its custom VerifyPeerCertificate callback intact.
func TestResolveArteryTLSConfig_RawHonoredVerbatim(t *testing.T) {
	verifyCalled := false
	raw := &tls.Config{
		MinVersion: tls.VersionTLS13,
		VerifyPeerCertificate: func(_ [][]byte, _ [][]*x509.Certificate) error {
			verifyCalled = true
			return nil
		},
	}

	got, err := resolveArteryTLSConfig(raw, core.TLSConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != raw {
		t.Fatalf("expected the exact RawTLSConfig pointer to be returned verbatim, got a different *tls.Config")
	}
	if got.VerifyPeerCertificate == nil {
		t.Fatal("VerifyPeerCertificate callback was lost")
	}
	// Prove the returned callback is the caller's own (fingerprint-pinning hook).
	if err := got.VerifyPeerCertificate(nil, nil); err != nil || !verifyCalled {
		t.Fatalf("returned VerifyPeerCertificate is not the caller's callback (called=%v, err=%v)", verifyCalled, err)
	}
}

// (b) When RawTLSConfig is unset, behavior is exactly the pre-patch path:
// BuildTLSConfig constructs the config from the HOCON tls.* fields.
func TestResolveArteryTLSConfig_HoconPathUnchangedWhenRawUnset(t *testing.T) {
	// b1: populated HOCON → a real config built via BuildTLSConfig.
	hocon := writeSelfSignedTLSFiles(t)
	got, err := resolveArteryTLSConfig(nil, hocon)
	if err != nil {
		t.Fatalf("unexpected error building from HOCON: %v", err)
	}
	if got == nil {
		t.Fatal("expected a non-nil *tls.Config from the HOCON path")
	}
	if len(got.Certificates) != 1 {
		t.Fatalf("expected 1 certificate from BuildTLSConfig, got %d", len(got.Certificates))
	}
	if got.RootCAs == nil || got.ClientCAs == nil {
		t.Fatal("expected BuildTLSConfig to populate RootCAs/ClientCAs from the CA file")
	}

	// b2: empty HOCON + no Raw → delegates to BuildTLSConfig, which errors on
	// the missing key pair — identical to the behavior before this patch.
	if _, err := resolveArteryTLSConfig(nil, core.TLSConfig{}); err == nil {
		t.Fatal("expected BuildTLSConfig to error on empty HOCON TLS config")
	}
}

// (c) The precedence rule: supplying BOTH RawTLSConfig and a populated HOCON
// TLS struct is a configuration error; supplying only Raw is fine.
func TestResolveArteryTLSConfig_MutualExclusion(t *testing.T) {
	raw := &tls.Config{MinVersion: tls.VersionTLS12}

	// Each PEM-path field alone is enough to count as "HOCON populated".
	for _, hocon := range []core.TLSConfig{
		{CertFile: "cert.pem"},
		{KeyFile: "key.pem"},
		{CAFile: "ca.pem"},
	} {
		if _, err := resolveArteryTLSConfig(raw, hocon); err == nil {
			t.Fatalf("expected mutual-exclusion error for Raw + HOCON %+v, got nil", hocon)
		}
	}

	// Raw + inert HOCON fields (no cert material) is NOT ambiguous → Raw wins.
	inert := core.TLSConfig{MinVersion: tls.VersionTLS13, ServerName: "peer"}
	got, err := resolveArteryTLSConfig(raw, inert)
	if err != nil {
		t.Fatalf("inert HOCON fields must not trigger the mutual-exclusion error: %v", err)
	}
	if got != raw {
		t.Fatal("expected RawTLSConfig to win when HOCON carries no cert material")
	}
}
