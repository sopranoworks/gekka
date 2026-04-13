// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http

import (
	"context"
	"net"
	nethttp "net/http"
)

// ServerBinding holds a running HTTP server.
type ServerBinding struct {
	Addr     string
	listener net.Listener
	server   *nethttp.Server
}

// NewServer creates and starts an HTTP server wrapping the given Route.
// addr may be "host:port" or ":port"; use ":0" for a random available port.
// Returns a ServerBinding with the resolved Addr (useful when port was 0).
func NewServer(addr string, route Route) (*ServerBinding, error) {
	handler := ToHandler(route)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := &nethttp.Server{Handler: handler}
	b := &ServerBinding{
		Addr:     ln.Addr().String(),
		listener: ln,
		server:   srv,
	}
	go srv.Serve(ln) //nolint:errcheck
	return b, nil
}

// Shutdown gracefully stops the server.
func (b *ServerBinding) Shutdown(ctx context.Context) error {
	return b.server.Shutdown(ctx)
}
