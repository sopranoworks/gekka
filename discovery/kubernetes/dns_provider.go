package kubernetes

import (
	"fmt"
	"net"

	"github.com/sopranoworks/gekka/discovery"
)

func init() {
	discovery.Register("kubernetes-dns", DNSFactory)
}

// DNSFactory creates a new DNSProvider from generic DiscoveryConfig.
func DNSFactory(config discovery.DiscoveryConfig) (discovery.SeedProvider, error) {
	serviceName, _ := config.Config["service-name"].(string)
	port, _ := config.Config["port"].(int)

	return NewDNSProvider(serviceName, port), nil
}

// DNSProvider implements discovery.SeedProvider using DNS SRV records.
type DNSProvider struct {
	serviceName string
	defaultPort int
}

// NewDNSProvider creates a new DNS-based seed provider.
func NewDNSProvider(serviceName string, defaultPort int) *DNSProvider {
	return &DNSProvider{
		serviceName: serviceName,
		defaultPort: defaultPort,
	}
}

// FetchSeedNodes resolves SRV records and returns a list of node addresses.
func (p *DNSProvider) FetchSeedNodes() ([]string, error) {
	_, srvs, err := net.LookupSRV("", "", p.serviceName)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve SRV records for %s: %w", p.serviceName, err)
	}

	var seeds []string
	for _, srv := range srvs {
		port := int(srv.Port)
		if port == 0 {
			port = p.defaultPort
		}

		ips, err := net.LookupHost(srv.Target)
		if err != nil {
			// Skip individual resolve failures but continue for others
			continue
		}

		for _, ip := range ips {
			seeds = append(seeds, fmt.Sprintf("%s:%d", ip, port))
		}
	}

	if len(seeds) == 0 {
		return nil, fmt.Errorf("no seed nodes discovered via DNS SRV for %s", p.serviceName)
	}

	return seeds, nil
}

// Ensure DNSProvider implements SeedProvider.
var _ discovery.SeedProvider = (*DNSProvider)(nil)
