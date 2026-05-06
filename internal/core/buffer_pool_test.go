/*
 * buffer_pool_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"sync"
	"testing"
)

// TestBufferPool_GetPutRoundtrip checks the basic checkout/return cycle:
// a buffer fetched by Get and returned via Put must be available for the
// next Get without triggering a fresh allocation.
func TestBufferPool_GetPutRoundtrip(t *testing.T) {
	const bufferSize = 256 * 1024
	p := NewBufferPool(4, bufferSize)

	first := p.Get()
	if len(first) != bufferSize {
		t.Fatalf("Get len=%d want %d", len(first), bufferSize)
	}
	if cap(first) != bufferSize {
		t.Fatalf("Get cap=%d want %d", cap(first), bufferSize)
	}
	if got := p.AllocCount(); got != 1 {
		t.Fatalf("AllocCount after first Get = %d want 1", got)
	}

	p.Put(first)
	if got := p.Depth(); got != 1 {
		t.Fatalf("Depth after Put = %d want 1", got)
	}

	second := p.Get()
	if got := p.AllocCount(); got != 1 {
		t.Fatalf("AllocCount after second Get (cache hit) = %d want 1", got)
	}
	if &second[0] != &first[0] {
		t.Fatalf("Get returned a fresh buffer instead of the recycled one")
	}
}

// TestBufferPool_ExhaustionFallsBackToFreshAlloc ensures Get does not block
// when the pool is empty: it allocates a new buffer on the spot.
func TestBufferPool_ExhaustionFallsBackToFreshAlloc(t *testing.T) {
	p := NewBufferPool(2, 4096)

	bufs := make([][]byte, 0, 5)
	for i := 0; i < 5; i++ {
		bufs = append(bufs, p.Get())
	}
	if got := p.AllocCount(); got != 5 {
		t.Fatalf("AllocCount = %d want 5 (pool empty on every checkout)", got)
	}
	for _, b := range bufs {
		if len(b) != 4096 {
			t.Fatalf("buffer len=%d want 4096", len(b))
		}
	}
}

// TestBufferPool_PutOnFullPoolDrops asserts that Put returns gracefully when
// the pool is at capacity: the excess buffer is dropped rather than blocking.
func TestBufferPool_PutOnFullPoolDrops(t *testing.T) {
	p := NewBufferPool(2, 1024)

	// Fill the pool by issuing three checkouts then returning all three.
	a := p.Get()
	b := p.Get()
	c := p.Get()
	p.Put(a)
	p.Put(b)
	p.Put(c) // should not block; the third Put is dropped.

	if got := p.Depth(); got != 2 {
		t.Fatalf("Depth = %d want 2 (third Put must drop on full pool)", got)
	}
}

// TestBufferPool_PutRejectsWrongSize covers the safety check that prevents
// callers from polluting the pool with mis-sized buffers — a buffer that
// grew past bufferSize during a frame, or an under-sized scratch slice.
func TestBufferPool_PutRejectsWrongSize(t *testing.T) {
	p := NewBufferPool(2, 1024)

	// Over-sized.
	p.Put(make([]byte, 8192))
	if got := p.Depth(); got != 0 {
		t.Fatalf("Depth after over-sized Put = %d want 0", got)
	}

	// Under-sized.
	p.Put(make([]byte, 16))
	if got := p.Depth(); got != 0 {
		t.Fatalf("Depth after under-sized Put = %d want 0", got)
	}

	// Exact match — must accept.
	p.Put(make([]byte, 1024))
	if got := p.Depth(); got != 1 {
		t.Fatalf("Depth after correctly-sized Put = %d want 1", got)
	}
}

// TestBufferPool_PutNilSafe asserts Put tolerates nil/empty input.
func TestBufferPool_PutNilSafe(t *testing.T) {
	p := NewBufferPool(2, 1024)
	p.Put(nil) // must not panic
	if got := p.Depth(); got != 0 {
		t.Fatalf("Depth after Put(nil) = %d want 0", got)
	}
}

// TestBufferPool_NilReceiverSafe documents that Get/Put on a nil pool work
// without panicking. tcpArteryReadLoop call sites rely on this so that
// optional pool wiring need not nil-check at every checkout.
func TestBufferPool_NilReceiverSafe(t *testing.T) {
	var p *BufferPool
	buf := p.Get()
	if len(buf) == 0 {
		t.Fatalf("Get on nil pool returned empty slice")
	}
	p.Put(buf) // must not panic
	if p.Capacity() != 0 || p.Depth() != 0 || p.BufferSize() != 0 || p.AllocCount() != 0 {
		t.Fatalf("nil pool accessor returned a non-zero value")
	}
}

// TestBufferPool_PassThroughMode covers NewBufferPool(0, _) / NewBufferPool(_, 0)
// — the disabled-pool configuration. Get must still return a usable buffer
// and Put must drop everything (Depth stays at 0).
func TestBufferPool_PassThroughMode(t *testing.T) {
	cases := []struct {
		name       string
		poolSize   int
		bufferSize int
	}{
		{"poolSize=0", 0, 4096},
		{"bufferSize=0", 4, 0},
		{"both=0", 0, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := NewBufferPool(c.poolSize, c.bufferSize)
			buf := p.Get()
			if len(buf) == 0 {
				t.Fatalf("Get returned empty slice in pass-through mode")
			}
			p.Put(buf)
			if got := p.Depth(); got != 0 {
				t.Fatalf("pass-through Depth = %d want 0", got)
			}
		})
	}
}

// TestBufferPool_AllocCountUnderSteadyLoad confirms the cache-hit path:
// once the pool is warmed up, subsequent Get/Put cycles must serve from
// cached buffers rather than re-allocating. This is the property Phase 3
// exists to deliver.
func TestBufferPool_AllocCountUnderSteadyLoad(t *testing.T) {
	const poolSize = 8
	const iterations = 1000
	p := NewBufferPool(poolSize, 4096)

	// Warm up by issuing poolSize checkouts then returning them all.
	warm := make([][]byte, poolSize)
	for i := range warm {
		warm[i] = p.Get()
	}
	for _, b := range warm {
		p.Put(b)
	}
	allocAfterWarmup := p.AllocCount()
	if allocAfterWarmup != poolSize {
		t.Fatalf("AllocCount after warmup = %d want %d", allocAfterWarmup, poolSize)
	}

	// Steady-state Get/Put cycles must not allocate.
	for i := 0; i < iterations; i++ {
		buf := p.Get()
		p.Put(buf)
	}
	if got := p.AllocCount(); got != allocAfterWarmup {
		t.Fatalf("AllocCount after %d steady cycles = %d want %d (pool hits expected)",
			iterations, got, allocAfterWarmup)
	}
}

// TestBufferPool_ConcurrentGetPut runs many goroutines through the pool to
// exercise the channel-based concurrency path under -race.
func TestBufferPool_ConcurrentGetPut(t *testing.T) {
	const workers = 8
	const iterations = 500
	p := NewBufferPool(4, 1024)

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				buf := p.Get()
				if len(buf) != 1024 {
					t.Errorf("Get len=%d want 1024", len(buf))
					return
				}
				p.Put(buf)
			}
		}()
	}
	wg.Wait()

	// Pool capacity is 4; after the storm the depth must be in [0, 4].
	if d := p.Depth(); d > p.Capacity() {
		t.Fatalf("Depth %d > Capacity %d", d, p.Capacity())
	}
}

// TestBufferPool_BufferSizeAndCapacityAccessors exercises the introspection
// helpers used by the read-loop wiring.
func TestBufferPool_BufferSizeAndCapacityAccessors(t *testing.T) {
	p := NewBufferPool(16, 8192)
	if got := p.Capacity(); got != 16 {
		t.Fatalf("Capacity = %d want 16", got)
	}
	if got := p.BufferSize(); got != 8192 {
		t.Fatalf("BufferSize = %d want 8192", got)
	}
}
