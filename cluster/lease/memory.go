/*
 * memory.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package lease

import (
	"context"
	"sync"
	"time"

	icluster "github.com/sopranoworks/gekka/internal/cluster"
)

// MemoryLeaseProvider is the in-memory reference LeaseProvider.
//
// All Lease instances created by a single MemoryLeaseProvider share a
// process-wide table keyed by LeaseSettings.LeaseName, so concurrent
// Acquire calls from different OwnerNames compete and exactly one wins
// until the holder calls Release or the LeaseDuration elapses.
//
// MemoryLeaseProvider is safe for concurrent use.
type MemoryLeaseProvider struct {
	mu     sync.Mutex
	now    func() time.Time
	leases map[string]*memoryEntry
}

// memoryEntry tracks the current holder of a single LeaseName.
type memoryEntry struct {
	holder    string    // OwnerName of holder; empty when free
	expiresAt time.Time // zero when held without TTL
	onLost    func(error)
}

// NewMemoryLeaseProvider returns a fresh MemoryLeaseProvider.  Each provider
// keeps its own lease table — share one provider across goroutines that
// must coordinate on the same lease names.
func NewMemoryLeaseProvider() *MemoryLeaseProvider {
	return &MemoryLeaseProvider{
		now:    time.Now,
		leases: make(map[string]*memoryEntry),
	}
}

// SetClock overrides the clock used for TTL calculations; intended for tests.
func (p *MemoryLeaseProvider) SetClock(clock func() time.Time) {
	if clock == nil {
		clock = time.Now
	}
	p.mu.Lock()
	p.now = clock
	p.mu.Unlock()
}

// GetLease returns a Lease bound to the (settings.LeaseName, settings.OwnerName)
// pair.  Different OwnerNames against the same LeaseName compete on Acquire.
func (p *MemoryLeaseProvider) GetLease(settings icluster.LeaseSettings) icluster.Lease {
	return &memoryLease{provider: p, settings: settings}
}

// memoryLease is a single holder's view of a memory-backed lease.
type memoryLease struct {
	provider *MemoryLeaseProvider
	settings icluster.LeaseSettings
}

// Acquire attempts to take the lease for this owner.  Returns true on
// success, false if another owner currently holds an unexpired lease.
//
// On successful acquisition onLeaseLost is registered and invoked exactly
// once if the lease is later forcibly taken (e.g. by TTL expiration followed
// by another owner's Acquire).
func (l *memoryLease) Acquire(_ context.Context, onLeaseLost func(error)) (bool, error) {
	p := l.provider
	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.now()
	entry, ok := p.leases[l.settings.LeaseName]
	if !ok {
		entry = &memoryEntry{}
		p.leases[l.settings.LeaseName] = entry
	}

	owns := entry.holder == l.settings.OwnerName && !isExpired(entry, now)
	free := entry.holder == "" || isExpired(entry, now)

	switch {
	case owns:
		// Renewal — refresh expiry and update onLost callback.
		entry.expiresAt = computeExpiry(now, l.settings.LeaseDuration)
		entry.onLost = onLeaseLost
		return true, nil
	case free:
		// Acquisition — possibly preempting an expired holder.
		if entry.holder != "" && entry.holder != l.settings.OwnerName && entry.onLost != nil {
			cb := entry.onLost
			// Release the lock while invoking the callback to avoid
			// reentrancy if the callback calls back into this provider.
			go cb(errLeaseExpired{name: l.settings.LeaseName})
		}
		entry.holder = l.settings.OwnerName
		entry.expiresAt = computeExpiry(now, l.settings.LeaseDuration)
		entry.onLost = onLeaseLost
		return true, nil
	default:
		return false, nil
	}
}

// Release surrenders the lease if this owner currently holds it.  Returns
// true when the lease was held and has been released, false otherwise.
func (l *memoryLease) Release(_ context.Context) (bool, error) {
	p := l.provider
	p.mu.Lock()
	defer p.mu.Unlock()

	entry, ok := p.leases[l.settings.LeaseName]
	if !ok || entry.holder != l.settings.OwnerName {
		return false, nil
	}
	if isExpired(entry, p.now()) {
		// Held by us but already expired — nothing to do.
		entry.holder = ""
		entry.onLost = nil
		entry.expiresAt = time.Time{}
		return false, nil
	}
	entry.holder = ""
	entry.onLost = nil
	entry.expiresAt = time.Time{}
	return true, nil
}

// CheckLease reports whether this owner currently holds an unexpired lease.
func (l *memoryLease) CheckLease() bool {
	p := l.provider
	p.mu.Lock()
	defer p.mu.Unlock()

	entry, ok := p.leases[l.settings.LeaseName]
	if !ok {
		return false
	}
	return entry.holder == l.settings.OwnerName && !isExpired(entry, p.now())
}

// Settings returns the LeaseSettings supplied at construction.
func (l *memoryLease) Settings() icluster.LeaseSettings { return l.settings }

func isExpired(e *memoryEntry, now time.Time) bool {
	if e.expiresAt.IsZero() {
		return false
	}
	return !now.Before(e.expiresAt)
}

func computeExpiry(now time.Time, ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return now.Add(ttl)
}

// errLeaseExpired is the error reported via onLeaseLost when the lease
// is preempted after TTL expiration.
type errLeaseExpired struct{ name string }

func (e errLeaseExpired) Error() string {
	return "lease: " + e.name + " expired and was acquired by another owner"
}
