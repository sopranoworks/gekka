/*
 * buffer_pool.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"sync/atomic"
)

// BufferPool is a fixed-cap pool of pre-allocated read buffers used by the
// Artery TCP receive path. It is the gekka analogue of Pekko's
// EnvelopeBufferPool: every checkout returns a buffer already sized to the
// stream's maximum-frame-size, eliminating per-frame allocations on hot
// receive loops while bounding the steady-state heap footprint.
//
// Two pools live on each NodeManager:
//
//   - the regular pool (streamId 1/2), sized to maximum-frame-size with
//     pekko.remote.artery.advanced.buffer-pool-size buffers in flight, and
//   - the large-stream pool (streamId 3), sized to maximum-large-frame-size
//     with pekko.remote.artery.advanced.large-buffer-pool-size buffers.
//
// The pools are independent: traffic on the large stream never drains the
// regular pool and vice versa.
//
// Semantics:
//
//   - Get() returns a buffer pre-sized to bufferSize. If the pool is empty
//     a fresh buffer is allocated (no blocking, no waiting). The returned
//     slice's len matches bufferSize so callers can re-slice without
//     bothering with cap.
//   - Put(buf) returns the buffer to the pool when there is room, otherwise
//     drops it for the GC. Buffers whose cap differs from the configured
//     bufferSize are rejected — sharing a pool across stream sizes would
//     defeat the pool's purpose. Put on a nil pool is a no-op so call sites
//     do not need to nil-check.
type BufferPool struct {
	pool       chan []byte
	bufferSize int
	// allocCount counts how many fresh allocations Get had to make
	// (i.e. checkouts that missed the pool). Used by tests to assert the
	// pool's caching behaviour. Atomic because Get may be called from
	// multiple read-loop goroutines.
	allocCount atomic.Int64
}

// NewBufferPool builds a pool with capacity poolSize, each buffer pre-sized
// to bufferSize bytes. Pre-allocation is lazy — the channel is empty at
// construction; the first Get call allocates the buffer it returns.
//
// poolSize <= 0 or bufferSize <= 0 yields a "pass-through" pool whose Get
// always allocates and whose Put always drops. This matches the semantics
// of the receive path before this phase landed: every checkout is a fresh
// allocation. It is convenient for callers that have not finished wiring
// the config knob and for tests that want to disable pooling entirely.
func NewBufferPool(poolSize, bufferSize int) *BufferPool {
	if poolSize <= 0 || bufferSize <= 0 {
		return &BufferPool{}
	}
	return &BufferPool{
		pool:       make(chan []byte, poolSize),
		bufferSize: bufferSize,
	}
}

// Get returns a buffer of length p.bufferSize. When the pool is empty (or
// disabled) a fresh buffer is allocated and the alloc counter is bumped.
func (p *BufferPool) Get() []byte {
	if p == nil {
		// Nil-pool callers cannot have a counter; pass-through behaviour
		// without bookkeeping. Match the legacy 64 KiB scratch size so
		// callers see no behaviour change when the pool is unwired.
		return make([]byte, 64*1024)
	}
	if p.pool == nil {
		// Pass-through (constructed with poolSize<=0 or bufferSize<=0).
		// Allocate a fresh buffer of the requested size.
		size := p.bufferSizeOrDefault()
		p.allocCount.Add(1)
		return make([]byte, size)
	}
	select {
	case buf := <-p.pool:
		// Hit. Buffers in the pool are always exactly bufferSize.
		return buf
	default:
		// Miss. Allocate fresh sized to bufferSize so the caller never has
		// to grow the slice mid-read.
		p.allocCount.Add(1)
		return make([]byte, p.bufferSize)
	}
}

// Put returns buf to the pool when (a) the pool is enabled, (b) buf has
// the configured capacity, and (c) the pool has room. Otherwise buf is
// dropped for GC. nil-buffer or nil-pool calls are no-ops.
func (p *BufferPool) Put(buf []byte) {
	if p == nil || p.pool == nil || buf == nil {
		return
	}
	if cap(buf) != p.bufferSize {
		// Reject mis-sized buffers. A buffer that grew past bufferSize
		// during a prior frame would silently inflate the pool's memory
		// budget; an under-sized buffer would cause a future caller to
		// re-allocate immediately. Either way: drop.
		return
	}
	// Reset slice len so the consumer always sees the full bufferSize on
	// checkout, regardless of how the previous owner re-sliced it.
	buf = buf[:p.bufferSize]
	select {
	case p.pool <- buf:
	default:
		// Pool full — let the GC reclaim.
	}
}

// Capacity returns the maximum number of buffers the pool will retain.
// Zero for a pass-through pool.
func (p *BufferPool) Capacity() int {
	if p == nil || p.pool == nil {
		return 0
	}
	return cap(p.pool)
}

// BufferSize returns the per-buffer length used by Get/Put.
func (p *BufferPool) BufferSize() int {
	if p == nil {
		return 0
	}
	return p.bufferSize
}

// Depth returns the number of buffers currently parked in the pool. Used
// by tests to assert pool occupancy. Not authoritative under concurrent
// Get/Put — it is a snapshot.
func (p *BufferPool) Depth() int {
	if p == nil || p.pool == nil {
		return 0
	}
	return len(p.pool)
}

// AllocCount returns the cumulative number of fresh allocations Get has
// made. Tests use this to confirm that under steady load the pool serves
// most requests from cached buffers rather than reallocating.
func (p *BufferPool) AllocCount() int64 {
	if p == nil {
		return 0
	}
	return p.allocCount.Load()
}

func (p *BufferPool) bufferSizeOrDefault() int {
	if p == nil || p.bufferSize <= 0 {
		// 64 KiB matches the historical tcpArteryReadLoop scratch size
		// before pooling landed. Keeping the same starting point means
		// pass-through mode is byte-for-byte equivalent to the pre-Phase-3
		// behaviour.
		return 64 * 1024
	}
	return p.bufferSize
}
