/*
 * registry.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package telemetry

import (
	"fmt"
	"sync"
)

var (
	providerRegistryMu sync.RWMutex
	providerRegistry   = make(map[string]func() Provider)
)

func init() {
	RegisterProvider("no-op", func() Provider { return NoopProvider{} })
}

// RegisterProvider registers a factory function under name.
// The factory is called each time GetProvider(name) is invoked.
// The built-in "no-op" provider is pre-registered automatically.
func RegisterProvider(name string, factory func() Provider) {
	providerRegistryMu.Lock()
	providerRegistry[name] = factory
	providerRegistryMu.Unlock()
}

// GetProvider looks up the factory registered under name and returns a new Provider.
func GetProvider(name string) (Provider, error) {
	providerRegistryMu.RLock()
	f, ok := providerRegistry[name]
	providerRegistryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("telemetry: no provider registered under %q", name)
	}
	return f(), nil
}

// ProviderNames returns the names of all registered provider factories.
func ProviderNames() []string {
	providerRegistryMu.RLock()
	defer providerRegistryMu.RUnlock()
	names := make([]string, 0, len(providerRegistry))
	for n := range providerRegistry {
		names = append(names, n)
	}
	return names
}
