/*
 * tcp_client.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"time"
)

type TcpClientConfig struct {
	Addr string // e.g. "127.0.0.1:9000"

	// Handler is required.
	Handler TcpHandler

	// Optional.
	Logger *log.Logger

	// TLSConfig enables TLS when non-nil. When set, the dialed connection is
	// wrapped with tls.Client and a handshake is performed before calling Handler.
	// When nil, plain TCP is used (default).
	TLSConfig *tls.Config

	// Timeouts (0 => disabled).
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	KeepAlive       bool
	KeepAlivePeriod time.Duration // 0 => OS default

	// LocalAddr, when non-nil, sets the local source address used by the
	// dialer (net.Dialer.LocalAddr). Corresponds to
	// pekko.remote.artery.advanced.tcp.outbound-client-hostname.
	LocalAddr *net.TCPAddr
}

type TcpClient struct {
	cfg TcpClientConfig
}

func NewTcpClient(cfg TcpClientConfig) (*TcpClient, error) {
	if cfg.Addr == "" {
		return nil, errors.New("gekka: TcpClient requires Addr")
	}
	if cfg.Handler == nil {
		return nil, errors.New("gekka: TcpClient requires Handler")
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	return &TcpClient{
		cfg: cfg,
	}, nil
}

// Connect establishes a connection and calls the handler.
// It returns when the handler returns or when the connection fails.
func (c *TcpClient) Connect(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	d := net.Dialer{
		Timeout: c.cfg.DialTimeout,
	}
	if c.cfg.LocalAddr != nil {
		d.LocalAddr = c.cfg.LocalAddr
	}

	if c.cfg.KeepAlive {
		if c.cfg.KeepAlivePeriod > 0 {
			d.KeepAlive = c.cfg.KeepAlivePeriod
		} else {
			d.KeepAlive = 15 * time.Second
		}
	} else {
		d.KeepAlive = -1
	}

	conn, err := d.DialContext(ctx, "tcp", c.cfg.Addr)
	if err != nil {
		return fmt.Errorf("gekka: TcpClient dial: %w", err)
	}

	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}

	defer func() {
		c.logf("closing connection %p to %s (Connect finished)", conn, c.cfg.Addr)
		_ = conn.Close()
	}()

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
			return fmt.Errorf("gekka: TLS handshake: %w", err)
		}
		netConn = tlsConn
	}

	var wrappedConn net.Conn = netConn
	if c.cfg.ReadTimeout > 0 || c.cfg.WriteTimeout > 0 || c.cfg.IdleTimeout > 0 {
		wrappedConn = &tcpTimeoutConn{
			Conn:         conn,
			readTimeout:  c.cfg.ReadTimeout,
			writeTimeout: c.cfg.WriteTimeout,
			idleTimeout:  c.cfg.IdleTimeout,
		}
	}

	if err := c.cfg.Handler(ctx, wrappedConn); err != nil {
		return err
	}

	// Handshake finished, but we must keep the connection open for Artery data
	c.logf("handshake handler for %p finished, keeping connection alive", conn)
	<-ctx.Done()
	return nil
}

func (c *TcpClient) logf(format string, args ...any) {
	if c.cfg.Logger != nil {
		c.cfg.Logger.Printf("TcpClient: "+format, args...)
	}
}
