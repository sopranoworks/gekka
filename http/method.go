// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http

import nethttp "net/http"

// method returns a Route that matches requests with the given HTTP method.
// On mismatch it records m in AllowedMethods (deduplicating) and sets Rejected.
func method(m string, inner Route) Route {
	return func(ctx *RequestContext) {
		if ctx.Request.Method == m {
			inner(ctx)
			return
		}
		for _, a := range ctx.AllowedMethods {
			if a == m {
				ctx.Rejected = true
				return
			}
		}
		ctx.AllowedMethods = append(ctx.AllowedMethods, m)
		ctx.Rejected = true
	}
}

// Get matches HTTP GET requests.
func Get(inner Route) Route { return method(nethttp.MethodGet, inner) }

// Post matches HTTP POST requests.
func Post(inner Route) Route { return method(nethttp.MethodPost, inner) }

// Put matches HTTP PUT requests.
func Put(inner Route) Route { return method(nethttp.MethodPut, inner) }

// Delete matches HTTP DELETE requests.
func Delete(inner Route) Route { return method(nethttp.MethodDelete, inner) }

// Patch matches HTTP PATCH requests.
func Patch(inner Route) Route { return method(nethttp.MethodPatch, inner) }

// Head matches HTTP HEAD requests.
func Head(inner Route) Route { return method(nethttp.MethodHead, inner) }

// Options matches HTTP OPTIONS requests.
func Options(inner Route) Route { return method(nethttp.MethodOptions, inner) }
