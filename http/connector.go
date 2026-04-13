// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http

import (
	nethttp "net/http"

	"github.com/sopranoworks/gekka/stream"
)

// NewClientFlow returns a stream.Flow that executes each incoming *http.Request
// via a shared Client and emits the *http.Response downstream.
// parallelism is reserved for future use; current implementation is sequential.
func NewClientFlow(cfg ClientConfig, parallelism int) stream.Flow[*nethttp.Request, *nethttp.Response, stream.NotUsed] {
	client := NewClient(cfg)
	return stream.MapE(func(req *nethttp.Request) (*nethttp.Response, error) {
		return client.SingleRequest(req.Context(), req)
	})
}
