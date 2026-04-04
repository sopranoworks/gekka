/*
 * consul_provider_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package consul

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sopranoworks/gekka/discovery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsulProvider_FetchSeedNodes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/catalog/service/gekka", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
			{"Address":"10.0.0.1","ServiceAddress":"","ServicePort":2552},
			{"Address":"10.0.0.2","ServiceAddress":"10.0.1.2","ServicePort":2552},
			{"Address":"10.0.0.3","ServiceAddress":"","ServicePort":0}
		]`))
	}))
	defer srv.Close()

	p := NewConsulProviderWithClient(srv.URL, "gekka", 2553, srv.Client())
	seeds, err := p.FetchSeedNodes()

	require.NoError(t, err)
	assert.Len(t, seeds, 3)
	// Node address used when ServiceAddress is empty
	assert.Contains(t, seeds, "10.0.0.1:2552")
	// ServiceAddress overrides Address when non-empty
	assert.Contains(t, seeds, "10.0.1.2:2552")
	// ServicePort==0 falls back to defaultPort
	assert.Contains(t, seeds, "10.0.0.3:2553")
}

func TestConsulProvider_NoNodes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	p := NewConsulProviderWithClient(srv.URL, "missing-service", 2552, srv.Client())
	_, err := p.FetchSeedNodes()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no seed nodes found")
}

func TestConsulProvider_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	p := NewConsulProviderWithClient(srv.URL, "gekka", 2552, srv.Client())
	_, err := p.FetchSeedNodes()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

func TestFactory_MissingServiceName(t *testing.T) {
	_, err := Factory(discovery.DiscoveryConfig{Config: map[string]any{}})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "service-name")
}

func TestFactory_Valid(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"Address":"10.0.0.1","ServicePort":2552}]`))
	}))
	defer srv.Close()

	provider, err := Factory(discovery.DiscoveryConfig{Config: map[string]any{
		"service-name": "gekka",
		"address":      srv.URL,
		"port":         2552,
	}})
	require.NoError(t, err)
	require.NotNil(t, provider)
}
