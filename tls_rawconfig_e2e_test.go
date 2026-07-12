//go:build integration

/*
 * tls_rawconfig_e2e_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// tlsPinGenSelfSigned generates one self-signed ECDSA keypair and returns a
// ready-to-use tls.Certificate plus the SHA-256 fingerprint of its DER-encoded
// leaf certificate. The fingerprint is exactly what a CA-less pinning peer
// compares against — sha256(cert.Raw) — so callers pin a node by this value.
func tlsPinGenSelfSigned(t *testing.T, cn string, serial int64) (tls.Certificate, [32]byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("%s: generate key: %v", cn, err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:              []string{"127.0.0.1", "localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("%s: create cert: %v", cn, err)
	}
	fp := sha256.Sum256(der)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, fp
}

// tlsPinRawConfig builds the *tls.Config that a node hands to
// ClusterConfig.RawTLSConfig for CA-less, fingerprint-pinned mutual TLS. There
// is NO CA and NO chain verification: InsecureSkipVerify disables the default
// chain/hostname checks, and the VerifyPeerCertificate callback is the sole
// trust decision — it pins the peer's leaf certificate by SHA-256 fingerprint.
// (Go always invokes VerifyPeerCertificate even when InsecureSkipVerify is set,
// which is precisely the pattern this whole RawTLSConfig escape hatch exists to
// enable.) The same config serves both the Artery listener and dialer, so
// ClientAuth requests the peer's cert on inbound connections too, making the
// pin mutual.
func tlsPinRawConfig(own tls.Certificate, expectedPeerFP [32]byte, invocations *int32, who string) *tls.Config {
	return &tls.Config{
		Certificates:       []tls.Certificate{own},
		ClientAuth:         tls.RequireAnyClientCert, // ask inbound peers for a cert so the server side can pin it too
		InsecureSkipVerify: true,                     //nolint:gosec // intentional: CA-less pinning, trust decided by VerifyPeerCertificate below
		MinVersion:         tls.VersionTLS13,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			atomic.AddInt32(invocations, 1)
			if len(rawCerts) == 0 {
				return fmt.Errorf("%s: peer presented no certificate", who)
			}
			got := sha256.Sum256(rawCerts[0])
			if got != expectedPeerFP {
				return fmt.Errorf("%s: peer fingerprint mismatch: got %x want %x", who, got, expectedPeerFP)
			}
			return nil
		},
	}
}

// TestRawTLSConfig_EndToEnd_FingerprintPinning is the genuine end-to-end test
// the resolver unit tests (tls_config_test.go) do NOT provide: it stands up two
// real Gekka nodes, each configured only via ClusterConfig.RawTLSConfig with a
// fingerprint-pinning VerifyPeerCertificate callback and no CA at all, and has
// the client node join the target over the "tls-tcp" transport. The Artery
// handshake only completes if a real TLS handshake — and thus the pinning
// callback — succeeds on a live connection between the two processes.
//
//   - MatchingFingerprint: each node pins the other's true fingerprint; the
//     join must complete and the callback must have actually run.
//   - MismatchedFingerprint: the client pins the WRONG fingerprint for the
//     target; the join must fail, proving the callback is enforced during the
//     handshake rather than merely constructed.
func TestRawTLSConfig_EndToEnd_FingerprintPinning(t *testing.T) {
	certA, fpA := tlsPinGenSelfSigned(t, "gekka-node-A", 1)
	certB, fpB := tlsPinGenSelfSigned(t, "gekka-node-B", 2)

	t.Run("MatchingFingerprint_JoinSucceeds", func(t *testing.T) {
		var aVerify, bVerify int32

		target, err := NewCluster(ClusterConfig{
			SystemName:   "TLSPinMatch",
			Host:         "127.0.0.1",
			Port:         0,
			Transport:    "tls-tcp",
			RawTLSConfig: tlsPinRawConfig(certA, fpB, &aVerify, "A(pins B)"),
		})
		if err != nil {
			t.Fatalf("NewCluster target: %v", err)
		}
		defer func() { _ = target.Shutdown() }()

		client, err := NewCluster(ClusterConfig{
			SystemName:   "TLSPinMatch",
			Host:         "127.0.0.1",
			Port:         0,
			Transport:    "tls-tcp",
			RawTLSConfig: tlsPinRawConfig(certB, fpA, &bVerify, "B(pins A)"),
		})
		if err != nil {
			t.Fatalf("NewCluster client: %v", err)
		}
		defer func() { _ = client.Shutdown() }()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		if err := client.Join(target.localAddr.GetHostname(), target.localAddr.GetPort()); err != nil {
			t.Fatalf("Join over tls-tcp: %v", err)
		}
		if err := client.WaitForHandshake(ctx, target.localAddr.GetHostname(), target.localAddr.GetPort()); err != nil {
			t.Fatalf("WaitForHandshake with matching pinned fingerprints (expected success): %v", err)
		}

		// The handshake could only complete because the real TLS handshake
		// succeeded, which means the pinning callback ran and accepted the peer.
		if atomic.LoadInt32(&bVerify) == 0 {
			t.Fatal("client VerifyPeerCertificate never fired — the pinning callback did not run during the real TLS handshake")
		}
		t.Logf("matching case: handshake OK; client callback fired %d time(s), target callback %d time(s)",
			atomic.LoadInt32(&bVerify), atomic.LoadInt32(&aVerify))
	})

	t.Run("MismatchedFingerprint_JoinFails", func(t *testing.T) {
		// A third, unrelated cert supplies a fingerprint the target will NEVER
		// present — the client pins that instead of the target's real fpA.
		_, fpWrong := tlsPinGenSelfSigned(t, "wrong-pin", 3)
		var aVerify, bVerify int32

		target, err := NewCluster(ClusterConfig{
			SystemName:   "TLSPinMismatch",
			Host:         "127.0.0.1",
			Port:         0,
			Transport:    "tls-tcp",
			RawTLSConfig: tlsPinRawConfig(certA, fpB, &aVerify, "A(pins B)"),
		})
		if err != nil {
			t.Fatalf("NewCluster target: %v", err)
		}
		defer func() { _ = target.Shutdown() }()

		client, err := NewCluster(ClusterConfig{
			SystemName:   "TLSPinMismatch",
			Host:         "127.0.0.1",
			Port:         0,
			Transport:    "tls-tcp",
			RawTLSConfig: tlsPinRawConfig(certB, fpWrong, &bVerify, "B(pins WRONG)"),
		})
		if err != nil {
			t.Fatalf("NewCluster client: %v", err)
		}
		defer func() { _ = client.Shutdown() }()

		// Bounded short: on a bad pin the TLS handshake can never succeed, so we
		// only need long enough for at least one dial attempt to run the callback.
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()

		// Join may return nil (association kicked off asynchronously) or an
		// error; either way the handshake must NOT complete.
		_ = client.Join(target.localAddr.GetHostname(), target.localAddr.GetPort())
		err = client.WaitForHandshake(ctx, target.localAddr.GetHostname(), target.localAddr.GetPort())
		if err == nil {
			t.Fatal("WaitForHandshake succeeded despite a mismatched pinned fingerprint — the pinning callback was NOT enforced during the handshake")
		}

		// Prove the failure came from the pin being rejected on a live
		// handshake, not from some unrelated setup error: the callback must
		// have actually fired (and, by pinning the wrong fingerprint, rejected).
		if atomic.LoadInt32(&bVerify) == 0 {
			t.Fatal("client VerifyPeerCertificate never fired in the mismatch case — cannot conclude the pin was enforced on a real handshake")
		}
		t.Logf("mismatch case: handshake correctly rejected (%v); client callback fired %d time(s)",
			err, atomic.LoadInt32(&bVerify))
	})
}
