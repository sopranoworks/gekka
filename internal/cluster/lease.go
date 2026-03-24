/*
 * lease.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Lease represents a distributed lock.
type Lease interface {
	// Acquire attempts to acquire the lease. Returns true if successful.
	// The callback is invoked if the lease is lost (e.g. due to connectivity issues).
	Acquire(ctx context.Context, onLeaseLost func(error)) (bool, error)

	// Release releases the lease. Returns true if the lease was held.
	Release(ctx context.Context) (bool, error)

	// CheckLease returns true if the lease is currently held by this node.
	CheckLease() bool

	// Settings returns the settings for this lease.
	Settings() LeaseSettings
}

// LeaseSettings contains configuration for a Lease.
type LeaseSettings struct {
	LeaseName     string
	OwnerName     string
	LeaseDuration time.Duration
	RetryInterval time.Duration
}

// LeaseProvider creates Lease instances.
type LeaseProvider interface {
	// GetLease creates a new Lease instance with the given settings.
	GetLease(settings LeaseSettings) Lease
}

// LeaseManager manages registered LeaseProviders.
type LeaseManager struct {
	mu        sync.RWMutex
	providers map[string]LeaseProvider
}

func NewLeaseManager() *LeaseManager {
	return &LeaseManager{
		providers: make(map[string]LeaseProvider),
	}
}

func (m *LeaseManager) RegisterProvider(name string, provider LeaseProvider) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.providers[name] = provider
}

func (m *LeaseManager) GetLease(providerName string, settings LeaseSettings) (Lease, error) {
	m.mu.RLock()
	provider, ok := m.providers[providerName]
	m.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("lease: unknown provider %q", providerName)
	}
	return provider.GetLease(settings), nil
}
