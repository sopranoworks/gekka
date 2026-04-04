/*
 * consul_provider.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package consul provides a Consul-based discovery extension for the Gekka cluster.
//
// ConsulProvider implements discovery.SeedProvider by querying the Consul HTTP
// catalog API (/v1/catalog/service/{name}) to discover healthy service instances.
// Import this package for its side effect — the init function registers the
// "consul" provider with the discovery registry:
//
//	import _ "github.com/sopranoworks/gekka-extensions-cluster-consul"
package consul

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/sopranoworks/gekka/discovery"
)

func init() {
	discovery.Register("consul", Factory)
}

// Factory creates a ConsulProvider from a generic DiscoveryConfig.
//
// Recognised config keys:
//
//	"address"      string  Consul agent address (default "http://localhost:8500").
//	"service-name" string  Consul service name to query (required).
//	"port"         int     Default port appended when ServicePort is 0 (default 0).
func Factory(config discovery.DiscoveryConfig) (discovery.SeedProvider, error) {
	addr, _ := config.Config["address"].(string)
	if addr == "" {
		addr = "http://localhost:8500"
	}
	serviceName, _ := config.Config["service-name"].(string)
	if serviceName == "" {
		return nil, fmt.Errorf("consul: config key \"service-name\" is required")
	}
	port, _ := config.Config["port"].(int)
	return NewConsulProvider(addr, serviceName, port), nil
}

// catalogEntry mirrors the relevant fields of the Consul catalog service
// response (GET /v1/catalog/service/{name}).
type catalogEntry struct {
	// Address is the node address (IP or hostname) as known to Consul.
	Address string `json:"Address"`
	// ServiceAddress overrides Address at the service registration level when
	// the service has its own IP distinct from the node.
	ServiceAddress string `json:"ServiceAddress"`
	// ServicePort is the port number registered for the service instance.
	ServicePort int `json:"ServicePort"`
}

// ConsulProvider implements discovery.SeedProvider using the Consul HTTP API.
type ConsulProvider struct {
	address     string
	serviceName string
	defaultPort int
	client      *http.Client
}

// NewConsulProvider creates a ConsulProvider that queries the Consul agent at
// address for the named service.  defaultPort is used when a catalog entry
// carries a ServicePort of 0.
func NewConsulProvider(address, serviceName string, defaultPort int) *ConsulProvider {
	return &ConsulProvider{
		address:     address,
		serviceName: serviceName,
		defaultPort: defaultPort,
		client:      &http.Client{},
	}
}

// NewConsulProviderWithClient creates a ConsulProvider with a custom HTTP client.
// Use this constructor in tests to inject an httptest-backed transport.
func NewConsulProviderWithClient(address, serviceName string, defaultPort int, client *http.Client) *ConsulProvider {
	return &ConsulProvider{
		address:     address,
		serviceName: serviceName,
		defaultPort: defaultPort,
		client:      client,
	}
}

// FetchSeedNodes queries the Consul catalog API and returns a slice of
// "address:port" strings for all registered instances of the configured
// service.  An error is returned if the HTTP request fails, the response is
// not 200 OK, JSON decoding fails, or no instances are found.
func (p *ConsulProvider) FetchSeedNodes() ([]string, error) {
	url := fmt.Sprintf("%s/v1/catalog/service/%s", p.address, p.serviceName)
	resp, err := p.client.Get(url) //nolint:noctx // method has no ctx parameter per SeedProvider interface
	if err != nil {
		return nil, fmt.Errorf("consul: request to %s failed: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("consul: unexpected HTTP status %d for service %q", resp.StatusCode, p.serviceName)
	}

	var entries []catalogEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return nil, fmt.Errorf("consul: failed to decode catalog response: %w", err)
	}

	var seeds []string
	for _, e := range entries {
		addr := e.ServiceAddress
		if addr == "" {
			addr = e.Address
		}
		if addr == "" {
			continue
		}
		port := e.ServicePort
		if port == 0 {
			port = p.defaultPort
		}
		seeds = append(seeds, fmt.Sprintf("%s:%d", addr, port))
	}

	if len(seeds) == 0 {
		return nil, fmt.Errorf("consul: no seed nodes found for service %q", p.serviceName)
	}
	return seeds, nil
}

// Ensure ConsulProvider implements SeedProvider at compile time.
var _ discovery.SeedProvider = (*ConsulProvider)(nil)
