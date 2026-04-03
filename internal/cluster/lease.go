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

// ── TestLease ─────────────────────────────────────────────────────────────────

// TestLease is an in-memory Lease implementation for unit and integration
// tests. Acquire always succeeds immediately; the held state can be forced at
// any time via ForceHeld.
//
// TestLease is safe for concurrent use.
type TestLease struct {
	mu       sync.Mutex
	held     bool
	settings LeaseSettings
}

// NewTestLease returns a TestLease with the given settings. held controls the
// initial lease state.
func NewTestLease(settings LeaseSettings, held bool) *TestLease {
	return &TestLease{settings: settings, held: held}
}

// ForceHeld sets the held state directly, simulating a lease acquisition or
// loss without going through Acquire/Release.
func (l *TestLease) ForceHeld(held bool) {
	l.mu.Lock()
	l.held = held
	l.mu.Unlock()
}

// Acquire marks the lease as held and returns true. onLeaseLost is ignored.
func (l *TestLease) Acquire(_ context.Context, _ func(error)) (bool, error) {
	l.mu.Lock()
	l.held = true
	l.mu.Unlock()
	return true, nil
}

// Release marks the lease as not held.
func (l *TestLease) Release(_ context.Context) (bool, error) {
	l.mu.Lock()
	wasHeld := l.held
	l.held = false
	l.mu.Unlock()
	return wasHeld, nil
}

// CheckLease reports whether the lease is currently held.
func (l *TestLease) CheckLease() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.held
}

// Settings returns the lease configuration.
func (l *TestLease) Settings() LeaseSettings { return l.settings }
