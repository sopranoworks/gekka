/*
 * memory_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package lease_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/cluster/lease"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMemoryLease_AcquireRelease covers the basic happy path: acquire, then
// release, then re-acquire by the same owner.
func TestMemoryLease_AcquireRelease(t *testing.T) {
	provider := lease.NewMemoryLeaseProvider()
	settings := lease.LeaseSettings{
		LeaseName:     "alpha",
		OwnerName:     "node-1",
		LeaseDuration: 0, // no TTL
	}
	l := provider.GetLease(settings)
	ctx := context.Background()

	assert.False(t, l.CheckLease(), "fresh lease should not be held")

	ok, err := l.Acquire(ctx, nil)
	require.NoError(t, err)
	assert.True(t, ok, "Acquire should succeed when free")
	assert.True(t, l.CheckLease(), "Lease should report held after Acquire")

	released, err := l.Release(ctx)
	require.NoError(t, err)
	assert.True(t, released, "Release should report true when previously held")
	assert.False(t, l.CheckLease(), "Lease should not be held after Release")

	released, err = l.Release(ctx)
	require.NoError(t, err)
	assert.False(t, released, "Release should report false when not held")
}

// TestMemoryLease_ConcurrentAcquireRace verifies that under heavy contention
// only one of N concurrent Acquire callers wins.
func TestMemoryLease_ConcurrentAcquireRace(t *testing.T) {
	const owners = 64
	provider := lease.NewMemoryLeaseProvider()

	var winners atomic.Int32
	var wg sync.WaitGroup
	wg.Add(owners)

	start := make(chan struct{})
	for i := 0; i < owners; i++ {
		i := i
		go func() {
			defer wg.Done()
			settings := lease.LeaseSettings{
				LeaseName:     "shared",
				OwnerName:     fmt.Sprintf("node-%d", i),
				LeaseDuration: 0,
			}
			l := provider.GetLease(settings)
			<-start // release all goroutines simultaneously
			got, err := l.Acquire(context.Background(), nil)
			require.NoError(t, err)
			if got {
				winners.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	assert.EqualValues(t, 1, winners.Load(), "exactly one owner must win the race")
}

// TestMemoryLease_TTLExpiration verifies that the lease becomes available
// after LeaseDuration elapses, even without an explicit Release.
func TestMemoryLease_TTLExpiration(t *testing.T) {
	provider := lease.NewMemoryLeaseProvider()
	clock := &fakeClock{now: time.Unix(0, 0)}
	provider.SetClock(clock.Now)

	a := provider.GetLease(lease.LeaseSettings{
		LeaseName:     "ttl-shared",
		OwnerName:     "owner-A",
		LeaseDuration: 50 * time.Millisecond,
	})
	b := provider.GetLease(lease.LeaseSettings{
		LeaseName:     "ttl-shared",
		OwnerName:     "owner-B",
		LeaseDuration: 50 * time.Millisecond,
	})

	ctx := context.Background()
	gotA, err := a.Acquire(ctx, nil)
	require.NoError(t, err)
	require.True(t, gotA)

	// Before TTL elapses, B must NOT be able to acquire.
	gotB, err := b.Acquire(ctx, nil)
	require.NoError(t, err)
	assert.False(t, gotB, "B must not preempt A while A's TTL is alive")
	assert.True(t, a.CheckLease(), "A still holds the lease")

	// Advance the clock past A's TTL — now B should be able to acquire.
	clock.advance(75 * time.Millisecond)
	assert.False(t, a.CheckLease(), "A's lease should have expired")

	gotB, err = b.Acquire(ctx, nil)
	require.NoError(t, err)
	assert.True(t, gotB, "B should acquire after A's TTL expired")
	assert.True(t, b.CheckLease(), "B holds the lease after TTL preemption")
}

// TestMemoryLease_LostCallback verifies that the onLeaseLost callback is
// invoked when another owner preempts an expired lease.
func TestMemoryLease_LostCallback(t *testing.T) {
	provider := lease.NewMemoryLeaseProvider()
	clock := &fakeClock{now: time.Unix(0, 0)}
	provider.SetClock(clock.Now)

	a := provider.GetLease(lease.LeaseSettings{
		LeaseName:     "callback-shared",
		OwnerName:     "owner-A",
		LeaseDuration: 10 * time.Millisecond,
	})
	b := provider.GetLease(lease.LeaseSettings{
		LeaseName:     "callback-shared",
		OwnerName:     "owner-B",
		LeaseDuration: 10 * time.Millisecond,
	})

	lost := make(chan error, 1)
	gotA, err := a.Acquire(context.Background(), func(err error) { lost <- err })
	require.NoError(t, err)
	require.True(t, gotA)

	clock.advance(20 * time.Millisecond)
	gotB, err := b.Acquire(context.Background(), nil)
	require.NoError(t, err)
	require.True(t, gotB)

	select {
	case err := <-lost:
		assert.Error(t, err, "lost callback should report a non-nil error")
	case <-time.After(time.Second):
		t.Fatal("onLeaseLost was not invoked within 1s after preemption")
	}
}

// TestMemoryLease_RenewalRefreshesTTL verifies that re-Acquire by the same
// owner refreshes the TTL window.
func TestMemoryLease_RenewalRefreshesTTL(t *testing.T) {
	provider := lease.NewMemoryLeaseProvider()
	clock := &fakeClock{now: time.Unix(0, 0)}
	provider.SetClock(clock.Now)

	settings := lease.LeaseSettings{
		LeaseName:     "renewal",
		OwnerName:     "owner-A",
		LeaseDuration: 100 * time.Millisecond,
	}
	l := provider.GetLease(settings)

	ctx := context.Background()
	got, err := l.Acquire(ctx, nil)
	require.NoError(t, err)
	require.True(t, got)

	// Advance most of the way through the TTL, renew, then advance past
	// the original expiry.  The lease should still be held because the
	// renewal pushed the expiry forward.
	clock.advance(80 * time.Millisecond)
	got, err = l.Acquire(ctx, nil)
	require.NoError(t, err)
	require.True(t, got, "renewal must succeed for the same owner")

	clock.advance(50 * time.Millisecond) // 130ms total but renewed at 80ms
	assert.True(t, l.CheckLease(), "renewed lease must still be held")
}

// fakeClock is a controllable clock for TTL tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}
