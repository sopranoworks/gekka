/*
 * lease_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package lease_test

import (
	"context"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/cluster/lease"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewDefaultManager_RegistersMemory verifies that NewDefaultManager
// pre-registers the in-memory provider under the documented name.
func TestNewDefaultManager_RegistersMemory(t *testing.T) {
	mgr := lease.NewDefaultManager()
	require.NotNil(t, mgr)

	settings := lease.LeaseSettings{
		LeaseName:     "registry-test",
		OwnerName:     "node-1",
		LeaseDuration: 0,
	}
	l, err := mgr.GetLease(lease.MemoryProviderName, settings)
	require.NoError(t, err)
	require.NotNil(t, l)
	assert.Equal(t, "registry-test", l.Settings().LeaseName)

	got, err := l.Acquire(context.Background(), nil)
	require.NoError(t, err)
	assert.True(t, got)
	assert.True(t, l.CheckLease())
}

// TestNewDefaultManager_UnknownProvider returns an error when an unknown
// implementation name is requested.
func TestNewDefaultManager_UnknownProvider(t *testing.T) {
	mgr := lease.NewDefaultManager()
	_, err := mgr.GetLease("does-not-exist", lease.LeaseSettings{})
	require.Error(t, err)
}

// TestNewManager_EmptyByDefault verifies NewManager returns an empty
// registry — callers must register providers explicitly.
func TestNewManager_EmptyByDefault(t *testing.T) {
	mgr := lease.NewManager()
	_, err := mgr.GetLease(lease.MemoryProviderName, lease.LeaseSettings{})
	require.Error(t, err, "empty manager must reject memory lookup")
}

// TestNewDefaultManager_RegisterCustomProvider confirms the manager is
// extensible: callers can layer additional providers on top of the default
// memory provider.
func TestNewDefaultManager_RegisterCustomProvider(t *testing.T) {
	mgr := lease.NewDefaultManager()
	mgr.RegisterProvider("custom", &fixedProvider{}) // satisfies LeaseProvider

	l, err := mgr.GetLease("custom", lease.LeaseSettings{LeaseName: "x", OwnerName: "y"})
	require.NoError(t, err)
	got, err := l.Acquire(context.Background(), nil)
	require.NoError(t, err)
	assert.True(t, got)
}

// fixedProvider is a minimal LeaseProvider used to verify the registry
// accepts user-defined implementations alongside the default memory one.
type fixedProvider struct{}

func (fixedProvider) GetLease(s lease.LeaseSettings) lease.Lease {
	return &fixedLease{settings: s}
}

type fixedLease struct {
	settings lease.LeaseSettings
}

func (l *fixedLease) Acquire(_ context.Context, _ func(error)) (bool, error) {
	return true, nil
}
func (l *fixedLease) Release(_ context.Context) (bool, error) { return true, nil }
func (l *fixedLease) CheckLease() bool                        { return true }
func (l *fixedLease) Settings() lease.LeaseSettings           { return l.settings }

// TestLeaseAlias_AssignableToInternal documents that the public Lease alias
// is interchangeable with the internal type used by Singleton/Sharding —
// so existing icluster.Lease consumers can accept lease.Lease values.
func TestLeaseAlias_AssignableToInternal(t *testing.T) {
	provider := lease.NewMemoryLeaseProvider()
	settings := lease.LeaseSettings{
		LeaseName:     "alias",
		OwnerName:     "node-1",
		LeaseDuration: time.Second,
	}
	var l lease.Lease = provider.GetLease(settings)
	got, err := l.Acquire(context.Background(), nil)
	require.NoError(t, err)
	assert.True(t, got)
}
