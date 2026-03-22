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

	hocon "github.com/sopranoworks/gekka-config"
)

var (
	providerRegistryMu sync.RWMutex
	providerRegistry   = make(map[string]func(hocon.Config) (Provider, error))
)

func init() {
	RegisterProvider("no-op", func(_ hocon.Config) (Provider, error) {
		return NoopProvider{}, nil
	})
}

// RegisterProvider registers a factory function under name.
// The factory receives the HOCON sub-config at telemetry.settings and returns
// a new Provider (or an error if configuration is invalid).
//
// The built-in "no-op" provider is pre-registered automatically.
func RegisterProvider(name string, factory func(hocon.Config) (Provider, error)) {
	providerRegistryMu.Lock()
	providerRegistry[name] = factory
	providerRegistryMu.Unlock()
}

// GetProvider looks up the factory registered under name, passes cfg to it,
// and returns the resulting Provider.
func GetProvider(name string, cfg hocon.Config) (Provider, error) {
	providerRegistryMu.RLock()
	f, ok := providerRegistry[name]
	providerRegistryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("telemetry: no provider registered under %q", name)
	}
	return f(cfg)
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
