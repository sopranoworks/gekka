package discovery

import (
	"fmt"
	"sync"
)

// SeedProvider defines the interface for dynamically discovering seed nodes.
type SeedProvider interface {
	// FetchSeedNodes returns a list of node addresses (IP:Port or hostname:Port).
	FetchSeedNodes() ([]string, error)
}

// DiscoveryConfig holds generic provider configuration.
type DiscoveryConfig struct {
	Config map[string]any
}

// Factory is a function that creates a SeedProvider from a DiscoveryConfig.
type Factory func(DiscoveryConfig) (SeedProvider, error)

var (
	providersMu sync.RWMutex
	providers   = make(map[string]Factory)
)

// Register registers a seed provider factory by name.
func Register(name string, factory Factory) {
	providersMu.Lock()
	defer providersMu.Unlock()
	providers[name] = factory
}

// Get returns a seed provider by name, configured with the provided config.
func Get(name string, config DiscoveryConfig) (SeedProvider, error) {
	providersMu.RLock()
	factory, ok := providers[name]
	providersMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("discovery: provider %q not registered (forget to import implementation package?)", name)
	}

	return factory(config)
}
