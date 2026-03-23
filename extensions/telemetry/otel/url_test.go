/*
 * url_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package otel

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestParseEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		wantHost string
		wantSec  bool
	}{
		{
			name:     "full URL with http",
			endpoint: "http://localhost:4318",
			wantHost: "localhost:4318",
			wantSec:  false,
		},
		{
			name:     "full URL with https",
			endpoint: "https://otel.example.com",
			wantHost: "otel.example.com",
			wantSec:  true,
		},
		{
			name:     "raw host:port",
			endpoint: "otel-collector:4318",
			wantHost: "otel-collector:4318",
			wantSec:  false,
		},
		{
			name:     "with trailing slash",
			endpoint: "http://localhost:4318/",
			wantHost: "localhost:4318",
			wantSec:  false,
		},
		{
			name:     "empty",
			endpoint: "",
			wantHost: "",
			wantSec:  false,
		},
		{
			name:     "just host",
			endpoint: "localhost",
			wantHost: "localhost",
			wantSec:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotHost, gotSec := ParseEndpoint(tt.endpoint)
			assert.Equal(t, tt.wantHost, gotHost)
			assert.Equal(t, tt.wantSec, gotSec)
		})
	}
}
