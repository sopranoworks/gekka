// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http

import "strings"

// Path matches when ctx.Remaining equals segment exactly (full match, no trailing slash).
// On match, sets ctx.Remaining="" and calls inner.
// On mismatch, sets ctx.Rejected=true.
func Path(segment string, inner Route) Route {
	return func(ctx *RequestContext) {
		if ctx.Remaining == segment || ctx.Remaining == segment+"/" {
			ctx.Remaining = ""
			inner(ctx)
		} else {
			ctx.Rejected = true
		}
	}
}

// PathPrefix matches when ctx.Remaining starts with prefix.
// On match, strips prefix from ctx.Remaining and calls inner.
// On mismatch, sets ctx.Rejected=true.
func PathPrefix(prefix string, inner Route) Route {
	return func(ctx *RequestContext) {
		if strings.HasPrefix(ctx.Remaining, prefix) {
			ctx.Remaining = ctx.Remaining[len(prefix):]
			inner(ctx)
		} else {
			ctx.Rejected = true
		}
	}
}

// PathEnd matches only when ctx.Remaining is "" or "/".
// On match, calls inner. On mismatch, sets ctx.Rejected=true.
func PathEnd(inner Route) Route {
	return func(ctx *RequestContext) {
		if ctx.Remaining == "" || ctx.Remaining == "/" {
			inner(ctx)
		} else {
			ctx.Rejected = true
		}
	}
}

// PathParam extracts the first path segment from ctx.Remaining as a named parameter.
// Stores it in ctx.Params[name] and passes the value to the inner factory.
// On mismatch (empty remaining), sets ctx.Rejected=true.
func PathParam(name string, inner func(string) Route) Route {
	return func(ctx *RequestContext) {
		remaining := ctx.Remaining
		// Strip leading slash before extracting segment
		remaining = strings.TrimPrefix(remaining, "/")
		if remaining == "" {
			ctx.Rejected = true
			return
		}
		// Find end of first segment
		idx := strings.Index(remaining, "/")
		var segment string
		var rest string
		if idx == -1 {
			segment = remaining
			rest = ""
		} else {
			segment = remaining[:idx]
			rest = remaining[idx:] // keeps leading "/"
		}
		ctx.Params[name] = segment
		saved := ctx.Remaining
		ctx.Remaining = rest
		inner(segment)(ctx)
		if ctx.Rejected {
			ctx.Remaining = saved
		}
	}
}
