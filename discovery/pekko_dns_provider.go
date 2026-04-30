/*
 * pekko_dns_provider.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package discovery

import (
	"fmt"
	"net"
)

// PekkoDNSProvider implements [SeedProvider] using DNS SRV records,
// matching the canonical Pekko `pekko-dns` discovery method.  It reads
// `pekko.discovery.pekko-dns.{service-name,port}` (populated by
// hocon_config.go onto [DiscoveryConfig.Config]).
//
// The Pekko `class` FQCN field is JVM-only and intentionally not honoured —
// gekka identifies providers by their registry name ("pekko-dns"), not by
// reflective class loading.
type PekkoDNSProvider struct {
	serviceName string
	defaultPort int

	// lookupSRV is overridable for tests; defaults to net.LookupSRV.
	lookupSRV func(service, proto, name string) (string, []*net.SRV, error)

	// lookupHost is overridable for tests; defaults to net.LookupHost.
	lookupHost func(host string) ([]string, error)
}

// NewPekkoDNSProvider constructs a PekkoDNSProvider for the given service
// name.  defaultPort is used as a fallback when an SRV record's port is 0.
func NewPekkoDNSProvider(serviceName string, defaultPort int) *PekkoDNSProvider {
	return &PekkoDNSProvider{
		serviceName: serviceName,
		defaultPort: defaultPort,
		lookupSRV:   net.LookupSRV,
		lookupHost:  net.LookupHost,
	}
}

// FetchSeedNodes resolves SRV records for the configured service name and
// returns "ip:port" strings for each resolved target.
func (p *PekkoDNSProvider) FetchSeedNodes() ([]string, error) {
	if p.serviceName == "" {
		return nil, fmt.Errorf("discovery: pekko-dns: service-name is empty")
	}
	_, srvs, err := p.lookupSRV("", "", p.serviceName)
	if err != nil {
		return nil, fmt.Errorf("discovery: pekko-dns: SRV lookup for %s: %w", p.serviceName, err)
	}
	var seeds []string
	for _, srv := range srvs {
		port := int(srv.Port)
		if port == 0 {
			port = p.defaultPort
		}
		ips, err := p.lookupHost(srv.Target)
		if err != nil {
			continue
		}
		for _, ip := range ips {
			seeds = append(seeds, fmt.Sprintf("%s:%d", ip, port))
		}
	}
	if len(seeds) == 0 {
		return nil, fmt.Errorf("discovery: pekko-dns: no seeds resolved for %s", p.serviceName)
	}
	return seeds, nil
}

func init() {
	Register("pekko-dns", pekkoDNSFactory)
}

func pekkoDNSFactory(cfg DiscoveryConfig) (SeedProvider, error) {
	serviceName, _ := cfg.Config["service-name"].(string)
	if serviceName == "" {
		return nil, fmt.Errorf("discovery: pekko-dns requires pekko.discovery.pekko-dns.service-name")
	}
	port := coerceInt(cfg.Config["port"])
	return NewPekkoDNSProvider(serviceName, port), nil
}

var _ SeedProvider = (*PekkoDNSProvider)(nil)
