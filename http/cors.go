// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http

import (
	"fmt"
	"strings"
)

// CorsSettings configures the CORS directive behavior.
type CorsSettings struct {
	AllowedOrigins   []string
	AllowedMethods   []string
	AllowedHeaders   []string
	ExposedHeaders   []string
	AllowCredentials bool
	MaxAge           int
}

// DefaultCorsSettings returns permissive CORS settings that allow all origins.
func DefaultCorsSettings() CorsSettings {
	return CorsSettings{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"},
		AllowedHeaders: []string{"*"},
		MaxAge:         86400,
	}
}

// CORS wraps inner with Cross-Origin Resource Sharing headers.
func CORS(settings CorsSettings, inner Route) Route {
	return func(ctx *RequestContext) {
		origin := ctx.Request.Header.Get("Origin")
		if origin == "" {
			// No Origin header — pass through without CORS headers.
			inner(ctx)
			return
		}

		matched := matchOrigin(origin, settings.AllowedOrigins)
		if !matched {
			// Origin not allowed — pass through without CORS headers.
			inner(ctx)
			return
		}

		// Set CORS response headers.
		h := ctx.Writer.Header()
		if len(settings.AllowedOrigins) == 1 && settings.AllowedOrigins[0] == "*" {
			h.Set("Access-Control-Allow-Origin", "*")
		} else {
			h.Set("Access-Control-Allow-Origin", origin)
			h.Set("Vary", "Origin")
		}

		if settings.AllowCredentials {
			h.Set("Access-Control-Allow-Credentials", "true")
		}

		if len(settings.ExposedHeaders) > 0 {
			h.Set("Access-Control-Expose-Headers", strings.Join(settings.ExposedHeaders, ", "))
		}

		// Preflight request.
		if ctx.Request.Method == "OPTIONS" && ctx.Request.Header.Get("Access-Control-Request-Method") != "" {
			h.Set("Access-Control-Allow-Methods", strings.Join(settings.AllowedMethods, ", "))
			if len(settings.AllowedHeaders) > 0 {
				h.Set("Access-Control-Allow-Headers", strings.Join(settings.AllowedHeaders, ", "))
			}
			if settings.MaxAge > 0 {
				h.Set("Access-Control-Max-Age", fmt.Sprintf("%d", settings.MaxAge))
			}
			ctx.Writer.WriteHeader(200)
			ctx.Completed = true
			return
		}

		// Actual request — delegate to inner.
		inner(ctx)
	}
}

// matchOrigin checks if origin is in the allowed list.
func matchOrigin(origin string, allowed []string) bool {
	for _, a := range allowed {
		if a == "*" {
			return true
		}
		if strings.EqualFold(a, origin) {
			return true
		}
	}
	return false
}
