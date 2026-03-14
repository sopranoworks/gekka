<!--
  TLS.md — Artery TLS Transport Design for Gekka
  Copyright (c) 2026 Sopranoworks, Osamu Takahashi
  SPDX-License-Identifier: MIT
-->

# Artery TLS Transport

This document describes the design for adding mutual-TLS (mTLS) support to Gekka's
Artery TCP transport layer.  Plain TCP (`transport = tcp`) remains the default and
is unchanged.  TLS is opt-in via `transport = tls-tcp` in the HOCON configuration.

---

## 1. Pekko / Akka Reference

Pekko and Akka Artery expose a `tls-tcp` transport option that wraps every Artery
TCP connection in TLS 1.2 / 1.3.  The key configuration path is:

```hocon
# Pekko
pekko.remote.artery {
  transport = tls-tcp       # "tcp" (plain) | "tls-tcp" (mutual TLS)

  ssl {
    # Path to the Java KeyStore that holds the node certificate + private key.
    key-store           = "keystore.jks"
    key-store-password  = ${?SSL_KEY_STORE_PASSWORD}
    key-password        = ${?SSL_KEY_PASSWORD}

    # Path to the Java TrustStore that holds trusted CA certificates.
    trust-store         = "truststore.jks"
    trust-store-password = ${?SSL_TRUST_STORE_PASSWORD}

    protocol            = "TLSv1.3"   # minimum TLS version to accept
    enabled-algorithms  = ["TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256"]
    require-mutual-auth = on           # mandatory mTLS — both sides present certs
    random-number-generator = ""       # "" → Java default SecureRandom
  }
}

# Akka 2.6 — identical keys, just swap "pekko" → "akka"
akka.remote.artery {
  transport = tls-tcp
  ssl { ... }  # same sub-keys
}
```

Gekka will mirror this structure using PEM files instead of Java KeyStore, which is
more idiomatic for Go.

---

## 2. Gekka HOCON Configuration

Gekka extends the existing `pekko.remote.artery` (or `akka.remote.artery`) block:

```hocon
pekko.remote.artery {
  transport = tls-tcp   # "tcp" (default) | "tls-tcp"

  tls {
    # PEM-encoded certificate for this node.
    certificate = "/etc/gekka/tls/node.crt"

    # PEM-encoded private key for this node (RSA or ECDSA).
    private-key = "/etc/gekka/tls/node.key"

    # PEM-encoded CA bundle; all peer certificates must chain to one of these CAs.
    ca-certificates = "/etc/gekka/tls/ca.crt"

    # Minimum TLS version. "TLS1.2" or "TLS1.3" (default: "TLS1.2").
    min-version = "TLS1.2"

    # Whether to require the peer to present a certificate (mutual TLS).
    # Should always be true in production cluster deployments.
    require-client-auth = true

    # Optional: restrict to a fixed cipher list (default: Go's safe defaults).
    # cipher-suites = ["TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"]

    # Optional: server-name-override used during TLS handshake SNI.
    # Useful when canonical.hostname differs from the certificate CN.
    # server-name = ""
  }
}
```

All paths are absolute; relative paths are resolved from the working directory of
the Gekka process.  Environment variable substitution (`${?VAR}`) is supported
via the existing HOCON parser.

### Mapping to `ClusterConfig`

A new `TLSConfig` struct will be added and populated by `hoconToClusterConfig`:

```go
// TLSConfig holds the parameters for Artery TLS transport.
// All fields are ignored when Transport == "tcp".
type TLSConfig struct {
    // CertFile is the path to the PEM-encoded node certificate.
    CertFile string

    // KeyFile is the path to the PEM-encoded private key.
    KeyFile string

    // CAFile is the path to the PEM-encoded CA bundle for peer verification.
    CAFile string

    // MinVersion is the minimum TLS version ("TLS1.2" or "TLS1.3").
    // Defaults to tls.VersionTLS12.
    MinVersion uint16

    // RequireClientAuth enables mutual TLS (both sides send certificates).
    RequireClientAuth bool

    // ServerName overrides the SNI hostname used in the client handshake.
    ServerName string
}

type ClusterConfig struct {
    // ... existing fields ...

    // Transport selects the Artery transport: "tcp" (default) or "tls-tcp".
    Transport string

    // TLS holds TLS parameters; only used when Transport == "tls-tcp".
    TLS TLSConfig
}
```

---

## 3. Implementation Plan

### 3.1 `internal/core/tls_config.go`  *(new file)*

Responsibility: load `TLSConfig` → `*tls.Config` using Go's `crypto/tls`.

```go
package core

import (
    "crypto/tls"
    "crypto/x509"
    "fmt"
    "os"
)

// BuildTLSConfig converts a TLSConfig into a *tls.Config suitable for use on
// both the server (listener) and client (dialer) side.
//
// Server side: tls.Config.ClientAuth is set to tls.RequireAndVerifyClientCert
//   when TLSConfig.RequireClientAuth is true.
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
        RootCAs:      pool,          // used by client to verify server cert
        ClientCAs:    pool,          // used by server to verify client cert
        ClientAuth:   clientAuth,
        MinVersion:   minVersion,
        ServerName:   cfg.ServerName,
    }, nil
}
```

### 3.2 `internal/core/tcp_server.go`

Add an optional `TLSConfig *tls.Config` field to `TcpServerConfig`.

```go
type TcpServerConfig struct {
    Addr    string
    Handler TcpHandler

    // TLSConfig enables TLS when non-nil.  The caller is responsible for
    // building the *tls.Config (typically via BuildTLSConfig).
    TLSConfig *tls.Config

    // ... existing fields (Logger, MaxConns, timeouts, KeepAlive) ...
}
```

In `TcpServer.Start`, replace `net.Listen("tcp", s.cfg.Addr)` with a conditional:

```go
if s.cfg.TLSConfig != nil {
    ln, err = tls.Listen("tcp", s.cfg.Addr, s.cfg.TLSConfig)
} else {
    ln, err = net.Listen("tcp", s.cfg.Addr)
}
```

Because `tls.Listener` implements `net.Listener` and each accepted `tls.Conn`
implements `net.Conn`, the rest of the server code (`acceptLoop`, `handleConn`,
the `TcpHandler` interface) requires **no changes**.

The `applyTCPOptions` helper type-asserts to `*net.TCPConn` and already handles
the case where the assertion fails gracefully (`ok` check), so it is also
unchanged.

### 3.3 `internal/core/tcp_client.go`

Add an optional `TLSConfig *tls.Config` field to `TcpClientConfig`.

```go
type TcpClientConfig struct {
    Addr    string
    Handler TcpHandler

    // TLSConfig enables TLS when non-nil.
    TLSConfig *tls.Config

    // ... existing fields ...
}
```

In `TcpClient.Connect`, after the plain `d.DialContext` call, wrap the conn:

```go
conn, err := d.DialContext(ctx, "tcp", c.cfg.Addr)
if err != nil {
    return fmt.Errorf("gekka: TcpClient dial: %w", err)
}

// Upgrade to TLS if configured.
var netConn net.Conn = conn
if c.cfg.TLSConfig != nil {
    serverName := c.cfg.TLSConfig.ServerName
    if serverName == "" {
        serverName, _, _ = net.SplitHostPort(c.cfg.Addr)
    }
    tlsCfg := c.cfg.TLSConfig.Clone()
    tlsCfg.ServerName = serverName
    tlsConn := tls.Client(conn, tlsCfg)
    if err := tlsConn.HandshakeContext(ctx); err != nil {
        _ = conn.Close()
        return fmt.Errorf("gekka: TLS handshake: %w", err)
    }
    netConn = tlsConn
}
```

The `TcpHandler` receives `netConn` (`*tls.Conn` or `*net.TCPConn`), so all
downstream Artery framing code is unchanged.

### 3.4 Wire-up in `cluster.go` / `NewCluster`

In `NewCluster`, after resolving `ClusterConfig`:

```go
var tlsCfg *tls.Config
if strings.EqualFold(cfg.Transport, "tls-tcp") {
    var err error
    tlsCfg, err = core.BuildTLSConfig(cfg.TLS)
    if err != nil {
        return nil, fmt.Errorf("gekka: TLS config: %w", err)
    }
}

// Pass tlsCfg into TcpServerConfig and TcpClientConfig as appropriate.
```

### 3.5 HOCON parsing in `hocon_config.go`

Extend `hoconToClusterConfig` to read the `tls` sub-block under
`pekko.remote.artery` (or `akka.remote.artery`):

```
pekko.remote.artery.transport          → ClusterConfig.Transport
pekko.remote.artery.tls.certificate   → ClusterConfig.TLS.CertFile
pekko.remote.artery.tls.private-key   → ClusterConfig.TLS.KeyFile
pekko.remote.artery.tls.ca-certificates → ClusterConfig.TLS.CAFile
pekko.remote.artery.tls.min-version   → ClusterConfig.TLS.MinVersion
pekko.remote.artery.tls.require-client-auth → ClusterConfig.TLS.RequireClientAuth
pekko.remote.artery.tls.server-name   → ClusterConfig.TLS.ServerName
```

---

## 4. Testing Strategy

### 4.1 Unit Tests

- `TestBuildTLSConfig_Valid` — load PEM fixtures from `testdata/tls/`, assert
  `*tls.Config` fields.
- `TestBuildTLSConfig_MissingCA` — ensure useful error message.
- `TestTcpServer_TLS_Echo` — create a self-signed CA, spin up `TcpServer` with
  TLS, connect with `TcpClient` + matching `*tls.Config`, echo a payload.
- `TestTcpClient_TLS_HandshakeFail` — wrong CA → expect `tls: certificate signed
  by unknown authority`.

Test certificates are generated once with `go generate` using `crypto/x509` and
committed as PEM files under `internal/core/testdata/tls/`.

### 4.2 Integration Tests (build tag `integration`)

- `TestPekkoIntegrationNode_TLS` — start `PekkoIntegrationNode` with
  `transport = tls-tcp` (requires JKS ↔ PEM bridge via `openssl`), join from
  Gekka with `TLSConfig` set, verify RemoteAsk and PubSubBridge.
- `TestAkkaIntegrationNode_TLS` — same for Akka 2.6.21.

> **NOTE:** These integration tests require generating a shared CA and distributing
> PEM/JKS credentials to both the Go and Scala sides.  A `testdata/tls/gen.sh`
> script using `openssl` will be provided.

---

## 5. Security Notes

- TLS 1.2 is the **minimum**; TLS 1.3 is preferred and will be negotiated
  automatically when both sides support it (Go's default cipher list).
- Mutual TLS (`require-client-auth = true`) is **strongly recommended** for
  production deployments; unauthenticated TLS only encrypts but does not prevent
  rogue nodes from joining the cluster.
- Certificate rotation without restart is **out of scope** for v0.6.0.  A future
  release may use `tls.Config.GetCertificate` / `GetConfigForClient` hooks.
- Private keys must never be logged.  Ensure `TLSConfig` is excluded from any
  debug-dump utilities.

---

## 6. Plain-TCP Backward Compatibility

The default is `transport = tcp`.  When `ClusterConfig.Transport` is empty or
`"tcp"`:

- `TcpServerConfig.TLSConfig` is `nil` → `net.Listen` is used (unchanged).
- `TcpClientConfig.TLSConfig` is `nil` → plain `net.DialContext` is used
  (unchanged).
- All existing unit tests and E2E integration tests (`TestPekkoIntegrationNode`,
  `TestAkkaIntegrationNode`) continue to pass without modification.

No feature flag, no build tag — TLS is simply not activated unless `Transport:
"tls-tcp"` is set in `ClusterConfig` or the HOCON file.
