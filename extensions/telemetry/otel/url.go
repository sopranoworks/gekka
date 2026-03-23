/*
 * url.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package otel

import (
	"net/url"
	"strings"
)

// ParseEndpoint handles robust OTLP/HTTP endpoint parsing.
// It converts potential full URLs or raw host:port strings into a format
// suitable for the OpenTelemetry Go SDK's WithEndpoint() option.
//
// OTel Go SDK's WithEndpoint() behavior (v1.42.0):
//   - If the endpoint contains a scheme (http:// or https://), it uses it.
//   - If WithInsecure() is called, it prepends "http://" if no scheme is present.
//
// To avoid double-protocol bugs (e.g., "http://http://host:port"), we
// normalize the endpoint by stripping any scheme if WithInsecure() is
// desired, or better, we determine if it's secure from the scheme.
//
// Returns the host (with port if present) and whether the connection is secure (HTTPS).
func ParseEndpoint(endpoint string) (host string, secure bool) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "", false
	}

	// Try to parse as a full URL
	if strings.Contains(endpoint, "://") {
		u, err := url.Parse(endpoint)
		if err == nil && u.Host != "" {
			return u.Host, u.Scheme == "https"
		}
	}

	// Fallback to raw host:port
	// We assume insecure (HTTP) if no scheme is provided.
	return endpoint, false
}
