/*
 * tcp_server.go
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
	"sync"
	"sync/atomic"
	"time"
)

// TcpHandler is called for each accepted TCP connection.
// The handler should return when ctx is done or when the connection is closed.
//
// The server closes the connection after the handler returns.
type TcpHandler func(ctx context.Context, conn net.Conn) error

type TcpServerConfig struct {
	Addr string // e.g. "0.0.0.0:9000"

	// Handler is required.
	Handler TcpHandler

	// Optional.
	Logger *log.Logger

	// TLSConfig enables TLS when non-nil. The caller is responsible for
	// building the *tls.Config (typically via BuildTLSConfig).
	// When nil, plain net.Listen is used (default).
	TLSConfig *tls.Config

	// Limits / timeouts (0 => disabled).
	MaxConns        int
	AcceptTimeout   time.Duration // periodically unblocks Accept to observe ctx cancellation (TCP listeners)
	ReadTimeout     time.Duration // per Read deadline
	WriteTimeout    time.Duration // per Write deadline
	IdleTimeout     time.Duration // closes connection if no successful read/write for this duration
	KeepAlive       bool
	KeepAlivePeriod time.Duration // 0 => OS default

	// BindTimeout caps how long the listener may block while binding to Addr.
	// Zero => no timeout (plain net.Listen). When non-zero, net.ListenConfig
	// is used with a context cancelled after BindTimeout.
	// Corresponds to pekko.remote.artery.bind.bind-timeout.
	BindTimeout time.Duration

	// Listener can be injected for tests; if nil, net.Listen("tcp", Addr) is used.
	Listener net.Listener
}

type TcpServer struct {
	cfg TcpServerConfig

	mu       sync.Mutex
	ln       net.Listener
	conns    map[net.Conn]struct{}
	started  bool
	stopping bool

	active int64
}

func NewTcpServer(cfg TcpServerConfig) (*TcpServer, error) {
	if cfg.Addr == "" && cfg.Listener == nil {
		return nil, errors.New("gekka: TcpServer requires Addr or Listener")
	}
	if cfg.Handler == nil {
		return nil, errors.New("gekka: TcpServer requires Handler")
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	return &TcpServer{
		cfg:   cfg,
		conns: make(map[net.Conn]struct{}),
	}, nil
}

// Start begins accepting connections in background goroutines.
// It returns after the listener is created and the accept loop is started.
func (s *TcpServer) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return errors.New("gekka: TcpServer already started")
	}
	s.started = true
	s.mu.Unlock()

	ln := s.cfg.Listener
	if ln == nil {
		var err error
		if s.cfg.BindTimeout > 0 {
			ln, err = listenWithBindTimeout(ctx, s.cfg.Addr, s.cfg.BindTimeout, s.cfg.TLSConfig)
		} else if s.cfg.TLSConfig != nil {
			ln, err = tls.Listen("tcp", s.cfg.Addr, s.cfg.TLSConfig)
		} else {
			ln, err = net.Listen("tcp", s.cfg.Addr)
		}
		if err != nil {
			return fmt.Errorf("gekka: TcpServer listen: %w", err)
		}
	}

	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()

	go s.acceptLoop(ctx)
	return nil
}

// listenWithBindTimeout binds to addr using net.ListenConfig with a context
// that cancels after timeout. When tlsCfg is non-nil the returned listener
// wraps the raw TCP listener with tls.NewListener.
//
// This honors pekko.remote.artery.bind.bind-timeout. A bind that exceeds the
// timeout returns context.DeadlineExceeded wrapped in a listen error.
func listenWithBindTimeout(parent context.Context, addr string, timeout time.Duration, tlsCfg *tls.Config) (net.Listener, error) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("gekka: bind to %q timed out after %s: %w", addr, timeout, err)
		}
		return nil, err
	}
	if tlsCfg != nil {
		ln = tls.NewListener(ln, tlsCfg)
	}
	return ln, nil
}

// Shutdown stops accepting new connections and closes all active connections.
// It is safe to call multiple times.
func (s *TcpServer) Shutdown() error {
	s.mu.Lock()
	if s.stopping {
		ln := s.ln
		s.mu.Unlock()
		if ln != nil {
			_ = ln.Close()
		}
		return nil
	}
	s.stopping = true

	ln := s.ln
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	if ln != nil {
		s.logf("closing listener %p", ln)
		_ = ln.Close()
	}
	for _, c := range conns {
		s.logf("closing active connection %p (shutdown)", c)
		_ = c.Close()
	}
	return nil
}

// Addr returns the bound address after Start.
func (s *TcpServer) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

func (s *TcpServer) ActiveConns() int {
	return int(atomic.LoadInt64(&s.active))
}

func (s *TcpServer) acceptLoop(ctx context.Context) {
	ln := s.getListener()

	for {
		select {
		case <-ctx.Done():
			_ = s.Shutdown()
			return
		default:
		}

		if s.cfg.AcceptTimeout > 0 {
			if tl, ok := ln.(*net.TCPListener); ok {
				_ = tl.SetDeadline(time.Now().Add(s.cfg.AcceptTimeout))
			}
		}

		conn, err := ln.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			s.logf("accept error: %v", err)
			continue
		}

		if !s.tryTrackConn(conn) {
			_ = conn.Close()
			continue
		}

		go s.handleConn(ctx, conn)
	}
}

func (s *TcpServer) handleConn(ctx context.Context, conn net.Conn) {
	s.logf("handling new connection %p from %s", conn, conn.RemoteAddr())
	defer func() {
		s.logf("closing connection %p from %s (handler finished)", conn, conn.RemoteAddr())
		s.untrackConn(conn)
		_ = conn.Close()
	}()

	atomic.AddInt64(&s.active, 1)
	defer atomic.AddInt64(&s.active, -1)

	s.applyTCPOptions(conn)

	c := conn
	if s.cfg.ReadTimeout > 0 || s.cfg.WriteTimeout > 0 || s.cfg.IdleTimeout > 0 {
		c = &tcpTimeoutConn{
			Conn:         conn,
			readTimeout:  s.cfg.ReadTimeout,
			writeTimeout: s.cfg.WriteTimeout,
			idleTimeout:  s.cfg.IdleTimeout,
		}
	}

	err := s.cfg.Handler(ctx, c)
	if err != nil {
		s.logf("handler for %p returned error: %v", conn, err)
	}
}

func (s *TcpServer) tryTrackConn(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopping {
		return false
	}
	if s.cfg.MaxConns > 0 && len(s.conns) >= s.cfg.MaxConns {
		s.logf("max conns reached (%d), rejecting %s", s.cfg.MaxConns, conn.RemoteAddr())
		return false
	}

	s.conns[conn] = struct{}{}
	return true
}

func (s *TcpServer) untrackConn(conn net.Conn) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
}

func (s *TcpServer) getListener() net.Listener {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ln
}

func (s *TcpServer) applyTCPOptions(conn net.Conn) {
	tc, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tc.SetNoDelay(true)
	if s.cfg.KeepAlive {
		_ = tc.SetKeepAlive(true)
		if s.cfg.KeepAlivePeriod > 0 {
			_ = tc.SetKeepAlivePeriod(s.cfg.KeepAlivePeriod)
		}
	}
}

func (s *TcpServer) logf(format string, args ...any) {
	if s.cfg.Logger != nil {
		s.cfg.Logger.Printf("TcpServer: "+format, args...)
	}
}

type tcpTimeoutConn struct {
	net.Conn
	readTimeout  time.Duration
	writeTimeout time.Duration
	idleTimeout  time.Duration

	lastMu sync.Mutex
	last   time.Time
}

func (c *tcpTimeoutConn) touch() {
	c.lastMu.Lock()
	c.last = time.Now()
	c.lastMu.Unlock()
}

func (c *tcpTimeoutConn) maybeCloseOnIdle() {
	if c.idleTimeout <= 0 {
		return
	}
	c.lastMu.Lock()
	last := c.last
	c.lastMu.Unlock()
	if !last.IsZero() && time.Since(last) > c.idleTimeout {
		_ = c.Conn.Close()
	}
}

func (c *tcpTimeoutConn) Read(p []byte) (int, error) {
	c.maybeCloseOnIdle()
	if c.readTimeout > 0 {
		_ = c.Conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.touch()
	}
	return n, err
}

func (c *tcpTimeoutConn) Write(p []byte) (int, error) {
	c.maybeCloseOnIdle()
	if c.writeTimeout > 0 {
		_ = c.Conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	n, err := c.Conn.Write(p)
	if n > 0 {
		c.touch()
	}
	return n, err
}
