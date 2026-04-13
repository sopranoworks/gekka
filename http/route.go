// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http

import "net/http"

// RequestContext carries the request and extracted values through the directive chain.
type RequestContext struct {
	Request        *http.Request
	Writer         http.ResponseWriter
	Params         map[string]string // path parameters ("id" -> "42")
	Remaining      string            // unconsumed path suffix for PathPrefix
	Rejected       bool              // true if route didn't match
	Completed      bool              // true if response was written
	AllowedMethods []string          // methods that matched path but not method (for 405)
}

// NewRequestContext creates a RequestContext from an HTTP request/response pair.
func NewRequestContext(w http.ResponseWriter, r *http.Request) *RequestContext {
	return &RequestContext{
		Request:   r,
		Writer:    w,
		Params:    make(map[string]string),
		Remaining: r.URL.Path,
	}
}

// Route is a function that handles a RequestContext.
type Route func(ctx *RequestContext)

// Directive wraps inner routes with matching/extraction logic.
type Directive func(inner Route) Route

// Concat tries each route in order. First non-reject wins.
// If all reject, Concat itself rejects. Merges AllowedMethods from all children.
func Concat(routes ...Route) Route {
	return func(ctx *RequestContext) {
		var allAllowed []string
		for _, route := range routes {
			ctx.Rejected = false
			ctx.AllowedMethods = nil
			route(ctx)
			if !ctx.Rejected {
				return
			}
			allAllowed = append(allAllowed, ctx.AllowedMethods...)
		}
		ctx.Rejected = true
		ctx.AllowedMethods = dedup(allAllowed)
	}
}

// dedup removes duplicate strings.
func dedup(ss []string) []string {
	seen := make(map[string]struct{}, len(ss))
	var out []string
	for _, s := range ss {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

// ToHandler adapts a Route into a standard http.Handler.
// If the route rejects, the default rejection handler is applied.
func ToHandler(route Route) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := NewRequestContext(w, r)
		route(ctx)
		if ctx.Rejected {
			handleRejection(ctx)
		}
	})
}
