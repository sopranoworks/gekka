/*
 * lease_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockLease struct {
	settings LeaseSettings
	held     bool
}

func (l *mockLease) Acquire(ctx context.Context, onLeaseLost func(error)) (bool, error) {
	l.held = true
	return true, nil
}

func (l *mockLease) Release(ctx context.Context) (bool, error) {
	wasHeld := l.held
	l.held = false
	return wasHeld, nil
}

func (l *mockLease) CheckLease() bool {
	return l.held
}

func (l *mockLease) Settings() LeaseSettings {
	return l.settings
}

type mockLeaseProvider struct{}

func (p *mockLeaseProvider) GetLease(settings LeaseSettings) Lease {
	return &mockLease{settings: settings}
}

func TestLeaseManager(t *testing.T) {
	mgr := NewLeaseManager()
	mgr.RegisterProvider("mock", &mockLeaseProvider{})

	settings := LeaseSettings{LeaseName: "test-lease", OwnerName: "node1"}
	lease, err := mgr.GetLease("mock", settings)
	require.NoError(t, err)
	assert.NotNil(t, lease)
	assert.Equal(t, "test-lease", lease.Settings().LeaseName)

	ctx := context.Background()
	ok, err := lease.Acquire(ctx, nil)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.True(t, lease.CheckLease())

	ok, err = lease.Release(ctx)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.False(t, lease.CheckLease())
}
