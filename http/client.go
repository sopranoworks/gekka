// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http

import (
	"context"
	"net"
	nethttp "net/http"
	"time"
)

// ClientConfig configures the HTTP client.
type ClientConfig struct {
	// MaxIdleConns limits idle keep-alive connections (0 = default 100).
	MaxIdleConns int
	// IdleConnTimeout controls how long idle connections are kept (0 = 90s).
	IdleConnTimeout time.Duration
	// DialTimeout is the TCP dial timeout (0 = 30s).
	DialTimeout time.Duration
}

// Client wraps net/http.Client with a pekko-http-style API.
type Client struct {
	inner *nethttp.Client
}

// NewClient creates a new HTTP client with optional config.
func NewClient(cfg ClientConfig) *Client {
	maxIdle := cfg.MaxIdleConns
	if maxIdle == 0 {
		maxIdle = 100
	}
	idleTimeout := cfg.IdleConnTimeout
	if idleTimeout == 0 {
		idleTimeout = 90 * time.Second
	}
	dialTimeout := cfg.DialTimeout
	if dialTimeout == 0 {
		dialTimeout = 30 * time.Second
	}
	transport := &nethttp.Transport{
		MaxIdleConns:    maxIdle,
		IdleConnTimeout: idleTimeout,
		DialContext: (&net.Dialer{
			Timeout: dialTimeout,
		}).DialContext,
	}
	return &Client{inner: &nethttp.Client{Transport: transport}}
}

// SingleRequest executes a one-shot HTTP request and returns the response.
// The caller is responsible for closing resp.Body.
func (c *Client) SingleRequest(ctx context.Context, req *nethttp.Request) (*nethttp.Response, error) {
	return c.inner.Do(req.WithContext(ctx))
}

// PoolConfig configures a connection pool for a single host.
type PoolConfig struct {
	// MaxConns is the maximum number of idle+active connections (0 = 10).
	MaxConns int
	// ResponseTimeout is the deadline for a single response (0 = no timeout).
	ResponseTimeout time.Duration
}

// HostConnectionPool manages persistent connections to a single host.
type HostConnectionPool struct {
	inner   *nethttp.Client
	baseURL string
}

// NewHostConnectionPool creates a pool targeting addr ("host:port").
func NewHostConnectionPool(addr string, cfg PoolConfig) *HostConnectionPool {
	maxConns := cfg.MaxConns
	if maxConns == 0 {
		maxConns = 10
	}
	transport := &nethttp.Transport{
		MaxIdleConnsPerHost: maxConns,
		IdleConnTimeout:     90 * time.Second,
		DialContext: (&net.Dialer{
			Timeout: 30 * time.Second,
		}).DialContext,
	}
	timeout := cfg.ResponseTimeout
	inner := &nethttp.Client{Transport: transport, Timeout: timeout}
	return &HostConnectionPool{inner: inner, baseURL: "http://" + addr}
}

// Get performs a GET against path on the pooled host.
func (p *HostConnectionPool) Get(ctx context.Context, path string) (*nethttp.Response, error) {
	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodGet, p.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	return p.inner.Do(req)
}

// Do executes an arbitrary request against the pooled host.
// req.URL should contain only path+query; the host is set from pool config.
func (p *HostConnectionPool) Do(ctx context.Context, req *nethttp.Request) (*nethttp.Response, error) {
	req = req.WithContext(ctx)
	req.URL.Scheme = "http"
	req.URL.Host = p.baseURL[7:] // strip "http://"
	return p.inner.Do(req)
}

// Close releases idle connections held by the pool.
func (p *HostConnectionPool) Close() {
	p.inner.CloseIdleConnections()
}
