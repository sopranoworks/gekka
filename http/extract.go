// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http

import nethttp "net/http"

// Parameter extracts query parameter name. Rejects if absent.
func Parameter(name string, inner func(string) Route) Route {
	return func(ctx *RequestContext) {
		values := ctx.Request.URL.Query()
		_, present := values[name]
		if !present {
			ctx.Rejected = true
			return
		}
		inner(values.Get(name))(ctx)
	}
}

// OptionalParameter extracts query parameter name.
// Passes ("", false) if absent instead of rejecting.
func OptionalParameter(name string, inner func(string, bool) Route) Route {
	return func(ctx *RequestContext) {
		values := ctx.Request.URL.Query()
		val, present := values[name]
		if present {
			inner(val[0], true)(ctx)
		} else {
			inner("", false)(ctx)
		}
	}
}

// Header extracts request header name. Rejects if absent.
func Header(name string, inner func(string) Route) Route {
	return func(ctx *RequestContext) {
		val := ctx.Request.Header.Get(name)
		if val == "" {
			ctx.Rejected = true
			return
		}
		inner(val)(ctx)
	}
}

// OptionalHeader extracts request header name.
// Passes ("", false) if absent instead of rejecting.
func OptionalHeader(name string, inner func(string, bool) Route) Route {
	return func(ctx *RequestContext) {
		val := ctx.Request.Header.Get(name)
		if val != "" {
			inner(val, true)(ctx)
		} else {
			inner("", false)(ctx)
		}
	}
}

// ExtractRequest passes the raw *http.Request to inner. Never rejects.
func ExtractRequest(inner func(*nethttp.Request) Route) Route {
	return func(ctx *RequestContext) {
		inner(ctx.Request)(ctx)
	}
}

// RespondWithHeader sets a response header before delegating to inner.
func RespondWithHeader(name, value string, inner Route) Route {
	return func(ctx *RequestContext) {
		ctx.Writer.Header().Set(name, value)
		inner(ctx)
	}
}

// RespondWithHeaders sets multiple response headers before delegating to inner.
func RespondWithHeaders(headers map[string]string, inner Route) Route {
	return func(ctx *RequestContext) {
		for k, v := range headers {
			ctx.Writer.Header().Set(k, v)
		}
		inner(ctx)
	}
}

// MapResponseHeaders applies a transformation to response headers after inner runs.
func MapResponseHeaders(transform func(nethttp.Header), inner Route) Route {
	return func(ctx *RequestContext) {
		inner(ctx)
		transform(ctx.Writer.Header())
	}
}
