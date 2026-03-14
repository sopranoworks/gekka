/*
 * tls_handshake_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/internal/core"
)

// generateSelfSignedCA creates an in-memory CA certificate + key pair.
func generateSelfSignedCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	return cert, key
}

// generateSignedCert creates a node certificate signed by the given CA, for
// the given hostnames. Returns (certPEM, keyPEM).
func generateSignedCert(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, hosts ...string) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: hosts[0]},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     hosts,
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create node cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal node key: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}

// buildInMemoryTLSConfig builds a *tls.Config from raw PEM bytes without
// touching the filesystem.
func buildInMemoryTLSConfig(t *testing.T, certPEM, keyPEM, caPEM []byte, requireClient bool) *tls.Config {
	t.Helper()
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatalf("AppendCertsFromPEM: no valid certs")
	}
	clientAuth := tls.NoClientCert
	if requireClient {
		clientAuth = tls.RequireAndVerifyClientCert
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ClientCAs:    pool,
		ClientAuth:   clientAuth,
		MinVersion:   tls.VersionTLS12,
		ServerName:   "127.0.0.1",
	}
}

// TestTcpServer_TLS_Artery verifies that a TLS-enabled TcpServer and TcpClient
// can exchange Artery magic bytes + a framed payload over a mutually-authenticated
// TLS connection. No Pekko node is required.
func TestTcpServer_TLS_Artery(t *testing.T) {
	// ── 1. Generate in-memory PKI ─────────────────────────────────────────────
	ca, caKey := generateSelfSignedCA(t)
	caPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: ca.Raw,
	})
	certPEM, keyPEM := generateSignedCert(t, ca, caKey, "127.0.0.1")

	serverTLS := buildInMemoryTLSConfig(t, certPEM, keyPEM, caPEM, true /* mTLS */)
	clientTLS := buildInMemoryTLSConfig(t, certPEM, keyPEM, caPEM, false)
	clientTLS.ServerName = "127.0.0.1"

	// ── 2. Payload to exchange ────────────────────────────────────────────────
	// We send a minimal Artery magic + 4-byte LE frame wrapping "hello".
	const streamID = 1
	magic := []byte{'A', 'K', 'K', 'A', streamID}
	payload := []byte("hello artery tls")
	frame := make([]byte, 4+len(payload))
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)

	received := make(chan []byte, 1)

	// ── 3. Start TLS server ───────────────────────────────────────────────────
	srv, err := core.NewTcpServer(core.TcpServerConfig{
		Addr:      "127.0.0.1:0",
		TLSConfig: serverTLS,
		Handler: func(ctx context.Context, conn net.Conn) error {
			// Read magic (5 bytes).
			buf := make([]byte, len(magic))
			if _, err := conn.Read(buf); err != nil {
				return err
			}
			// Read 4-byte LE frame header.
			hdr := make([]byte, 4)
			if _, err := conn.Read(hdr); err != nil {
				return err
			}
			length := binary.LittleEndian.Uint32(hdr)
			body := make([]byte, length)
			if _, err := conn.Read(body); err != nil {
				return err
			}
			received <- body
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewTcpServer: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("TcpServer.Start: %v", err)
	}
	defer srv.Shutdown() //nolint:errcheck

	addr := srv.Addr().String()

	// ── 4. Connect with TLS client ────────────────────────────────────────────
	clientDone := make(chan error, 1)
	cli, err := core.NewTcpClient(core.TcpClientConfig{
		Addr:      addr,
		TLSConfig: clientTLS,
		Handler: func(ctx context.Context, conn net.Conn) error {
			if _, err := conn.Write(magic); err != nil {
				return err
			}
			if _, err := conn.Write(frame); err != nil {
				return err
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewTcpClient: %v", err)
	}

	go func() {
		clientDone <- cli.Connect(ctx)
	}()

	// ── 5. Verify payload received ────────────────────────────────────────────
	select {
	case got := <-received:
		if !bytes.Equal(got, payload) {
			t.Errorf("server received %q, want %q", got, payload)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for server to receive payload")
	}

	cancel() // stop client's <-ctx.Done() wait

	select {
	case err := <-clientDone:
		if err != nil && err != context.Canceled {
			t.Errorf("client Connect: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("client goroutine did not finish")
	}
}

// TestTcpClient_TLS_HandshakeFail verifies that a TLS handshake fails when the
// client trusts a different CA than the one that signed the server certificate.
func TestTcpClient_TLS_HandshakeFail(t *testing.T) {
	// CA1 signs the server cert; client trusts CA2 (different) → mismatch.
	ca1, ca1Key := generateSelfSignedCA(t)
	ca1PEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca1.Raw})
	serverCertPEM, serverKeyPEM := generateSignedCert(t, ca1, ca1Key, "127.0.0.1")

	ca2, ca2Key := generateSelfSignedCA(t)
	ca2PEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca2.Raw})
	// Client cert signed by CA2 (just to have a cert to present)
	clientCertPEM, clientKeyPEM := generateSignedCert(t, ca2, ca2Key, "127.0.0.1")

	serverTLS := buildInMemoryTLSConfig(t, serverCertPEM, serverKeyPEM, ca1PEM, false)
	// Client trusts only CA2 — server cert (signed by CA1) will fail verification.
	clientTLS := buildInMemoryTLSConfig(t, clientCertPEM, clientKeyPEM, ca2PEM, false)
	clientTLS.ServerName = "127.0.0.1"

	srv, err := core.NewTcpServer(core.TcpServerConfig{
		Addr:      "127.0.0.1:0",
		TLSConfig: serverTLS,
		Handler: func(ctx context.Context, conn net.Conn) error {
			// just keep alive briefly
			time.Sleep(500 * time.Millisecond)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewTcpServer: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("TcpServer.Start: %v", err)
	}
	defer srv.Shutdown() //nolint:errcheck

	addr := srv.Addr().String()

	cli, err := core.NewTcpClient(core.TcpClientConfig{
		Addr:      addr,
		TLSConfig: clientTLS,
		Handler: func(ctx context.Context, conn net.Conn) error {
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewTcpClient: %v", err)
	}

	err = cli.Connect(ctx)
	if err == nil {
		t.Fatal("expected TLS handshake to fail, got nil error")
	}
	t.Logf("Got expected handshake error: %v", err)
}
