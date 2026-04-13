// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http

import (
	"encoding/json"
	nethttp "net/http"
)

// Complete writes a response. Body handling:
//   - string → text/plain; charset=utf-8
//   - []byte → application/octet-stream
//   - nil → no body, just status code
//   - any other type → JSON via encoding/json, application/json
func Complete(status int, body any) Route {
	return func(ctx *RequestContext) {
		ctx.Completed = true
		switch v := body.(type) {
		case string:
			ctx.Writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
			ctx.Writer.WriteHeader(status)
			_, _ = ctx.Writer.Write([]byte(v))
		case []byte:
			ctx.Writer.Header().Set("Content-Type", "application/octet-stream")
			ctx.Writer.WriteHeader(status)
			_, _ = ctx.Writer.Write(v)
		case nil:
			ctx.Writer.WriteHeader(status)
		default:
			ctx.Writer.Header().Set("Content-Type", "application/json")
			ctx.Writer.WriteHeader(status)
			enc := json.NewEncoder(ctx.Writer)
			_ = enc.Encode(v)
		}
	}
}

// CompleteWith writes a response with explicit content type and raw bytes.
func CompleteWith(status int, contentType string, body []byte) Route {
	return func(ctx *RequestContext) {
		ctx.Completed = true
		ctx.Writer.Header().Set("Content-Type", contentType)
		ctx.Writer.WriteHeader(status)
		_, _ = ctx.Writer.Write(body)
	}
}

// Redirect sends an HTTP redirect.
func Redirect(url string, status int) Route {
	return func(ctx *RequestContext) {
		ctx.Completed = true
		nethttp.Redirect(ctx.Writer, ctx.Request, url, status)
	}
}

// Reject explicitly rejects — signals "try next route".
func Reject() Route {
	return func(ctx *RequestContext) {
		ctx.Rejected = true
	}
}
