/*
 * config_provider.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package discovery

import (
	"fmt"
	"strconv"
	"strings"
)

// ConfigProvider implements [SeedProvider] over a static service map
// supplied via HOCON at `pekko.discovery.config.services`.
//
// Selection of the named service is driven by `service-name`, populated
// from `pekko.discovery.config.service-name` by hocon_config.go.
//
// Endpoint shape: gekka accepts the canonical Pekko services-path indirection,
// but the `endpoints` value is read as a list of `host[:port]` strings
// rather than the JVM-only list of `{host, port}` objects.  The underlying
// gekka-config HOCON library does not unmarshal lists of objects; this is the
// honest portable surface.
type ConfigProvider struct {
	serviceName string
	endpoints   []configEndpoint
	defaultPort int
}

type configEndpoint struct {
	host string
	port int
}

// NewConfigProvider constructs a ConfigProvider from a parsed services map.
//
//	services {
//	  svc1 { endpoints = [ "cat.com:1233", "dog.com" ] }
//	}
//
// `defaultPort` is used when an endpoint omits `:port`.  When it is 0 and an
// endpoint also omits the port, the endpoint is dropped (the gekka consumer
// requires a `host:port` string).
func NewConfigProvider(serviceName string, services map[string]any, defaultPort int) (*ConfigProvider, error) {
	if serviceName == "" {
		return nil, fmt.Errorf("discovery: pekko.discovery.config requires service-name")
	}
	endpoints, err := extractEndpoints(serviceName, services)
	if err != nil {
		return nil, err
	}
	return &ConfigProvider{
		serviceName: serviceName,
		endpoints:   endpoints,
		defaultPort: defaultPort,
	}, nil
}

func extractEndpoints(serviceName string, services map[string]any) ([]configEndpoint, error) {
	if services == nil {
		return nil, nil
	}
	raw, ok := services[serviceName]
	if !ok {
		return nil, nil
	}
	svc, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("discovery: pekko.discovery.config.services.%s is not a map", serviceName)
	}
	rawEndpoints, ok := svc["endpoints"]
	if !ok {
		return nil, nil
	}
	list, err := coerceStringList(rawEndpoints)
	if err != nil {
		return nil, fmt.Errorf("discovery: services.%s.endpoints: %w", serviceName, err)
	}
	out := make([]configEndpoint, 0, len(list))
	for i, raw := range list {
		entry := strings.TrimSpace(raw)
		if entry == "" {
			return nil, fmt.Errorf("discovery: services.%s.endpoints[%d] is empty", serviceName, i)
		}
		host, port, err := splitHostPort(entry)
		if err != nil {
			return nil, fmt.Errorf("discovery: services.%s.endpoints[%d]: %w", serviceName, i, err)
		}
		out = append(out, configEndpoint{host: host, port: port})
	}
	return out, nil
}

func coerceStringList(v any) ([]string, error) {
	switch list := v.(type) {
	case []string:
		return list, nil
	case []any:
		out := make([]string, 0, len(list))
		for i, item := range list {
			s, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("element %d is not a string", i)
			}
			out = append(out, s)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("expected list of strings")
	}
}

func splitHostPort(entry string) (string, int, error) {
	idx := strings.LastIndex(entry, ":")
	if idx < 0 {
		return entry, 0, nil
	}
	host := entry[:idx]
	portStr := entry[idx+1:]
	if host == "" {
		return "", 0, fmt.Errorf("missing host in %q", entry)
	}
	if portStr == "" {
		return host, 0, nil
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port in %q: %w", entry, err)
	}
	return host, port, nil
}

func coerceInt(v any) int {
	switch n := v.(type) {
	case nil:
		return 0
	case int:
		return n
	case int64:
		return int(n)
	case int32:
		return int(n)
	case float64:
		return int(n)
	case string:
		i, _ := strconv.Atoi(n)
		return i
	}
	return 0
}

// FetchSeedNodes returns "host:port" strings for every endpoint of the
// configured service.  Endpoints with no port and no defaultPort are dropped.
func (p *ConfigProvider) FetchSeedNodes() ([]string, error) {
	if len(p.endpoints) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(p.endpoints))
	for _, e := range p.endpoints {
		port := e.port
		if port == 0 {
			port = p.defaultPort
		}
		if port == 0 {
			continue
		}
		out = append(out, fmt.Sprintf("%s:%d", e.host, port))
	}
	return out, nil
}

func init() {
	Register("config", configFactory)
}

func configFactory(cfg DiscoveryConfig) (SeedProvider, error) {
	serviceName, _ := cfg.Config["service-name"].(string)
	defaultPort := coerceInt(cfg.Config["default-port"])
	servicesRaw := cfg.Config["services"]
	services, _ := servicesRaw.(map[string]any)
	return NewConfigProvider(serviceName, services, defaultPort)
}

var _ SeedProvider = (*ConfigProvider)(nil)
