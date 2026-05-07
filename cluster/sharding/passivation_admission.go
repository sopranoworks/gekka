/*
 * passivation_admission.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"hash/fnv"
)

// Round-2 session 26 — F3 Passivation: admission filter (frequency sketch).
//
// W-TinyLFU's admission filter is a count-min sketch sized as a multiple of
// the active-entity-limit.  Each entity touch increments `depth` counters
// (one per hash row) and the estimated frequency of an entity is the
// minimum of those rows.  When the cumulative access count crosses the
// reset threshold (resetMultiplier × activeEntityLimit) every counter is
// halved, giving newer accesses a heavier vote — this is the "tiny" in
// TinyLFU.
//
// The implementation uses uint8 counters capped at 0x0F so each cell stores
// a 4-bit value packed into a byte.  Dropping to nibble packing would halve
// memory but requires bit-shifting on the hot path; for the active-entity
// budgets gekka targets (≤1M) the byte-per-cell layout is ~16 MiB at the
// default 4× multiplier, which is acceptable.
//
// The frequency sketch is constructed once when the composite passivation
// strategy boots (see passivation_composite.go) and is shared across all
// entities under the strategy.

const (
	// frequencySketchMaxCount is the default per-counter saturation point —
	// 4-bit cells, matching Pekko's reference.conf default. Sketches built
	// with a non-default counter-bits value override this on a per-instance
	// basis via countMinSketch.maxCount.
	frequencySketchMaxCount uint8 = 0x0F

	// frequencySketchMinWidth is the minimum width of each sketch row,
	// applied even when activeEntityLimit × widthMultiplier produces a
	// smaller value.  Prevents pathological collisions on tiny test
	// limits (active-entity-limit = 4 still gets 64 columns).
	frequencySketchMinWidth = 64

	// frequencySketchMinCounterBits / frequencySketchMaxCounterBits cap
	// the supported counter-bits range. uint8 cells make 8 the natural
	// ceiling; 2 is the smallest meaningful range (0..3) below which the
	// admission filter loses all discrimination.  Pekko documents
	// 2/4/8/16/32/64 — the upper three values clamp here without panicking
	// because gekka cannot store >8 bits per cell without doubling memory.
	frequencySketchMinCounterBits = 2
	frequencySketchMaxCounterBits = 8
)

// countMinSketch is a fixed-width count-min sketch that estimates per-key
// access frequency using `depth` independent hash rows.  width is rounded up
// to a power of two so a single bit-mask suffices for column lookup.
type countMinSketch struct {
	depth      int
	width      int
	mask       uint32
	counters   [][]uint8
	seeds      []uint32
	resetEvery uint64
	accesses   uint64
	// maxCount is the per-cell saturation ceiling derived from the
	// configured counter-bits.  Held per instance so different sketches
	// can run with different precision under a single binary.
	maxCount uint8
}

// resolveCounterBits clamps the operator-supplied counter-bits to the
// supported [frequencySketchMinCounterBits, frequencySketchMaxCounterBits]
// range.  Zero / negative input collapses to the Pekko default (4-bit) so
// existing call sites that don't yet thread the parameter keep their
// historical saturation behaviour.
func resolveCounterBits(bits int) int {
	if bits <= 0 {
		return 4
	}
	if bits < frequencySketchMinCounterBits {
		return frequencySketchMinCounterBits
	}
	if bits > frequencySketchMaxCounterBits {
		return frequencySketchMaxCounterBits
	}
	return bits
}

// newCountMinSketch constructs a sketch sized to (depth × width').  width is
// rounded up to the next power of two and clamped to frequencySketchMinWidth.
// resetEvery is the total access count after which every counter is halved;
// pass 0 to disable the reset.  counterBits selects the per-cell saturation
// cap as `(1 << bits) - 1`; values outside [2,8] clamp into range and 0
// falls back to the historical 4-bit default.
func newCountMinSketch(depth, width int, resetEvery uint64, counterBits int) *countMinSketch {
	if depth <= 0 {
		depth = 4
	}
	if width < frequencySketchMinWidth {
		width = frequencySketchMinWidth
	}
	w := 1
	for w < width {
		w <<= 1
	}
	bits := resolveCounterBits(counterBits)
	cs := &countMinSketch{
		depth:      depth,
		width:      w,
		mask:       uint32(w - 1),
		counters:   make([][]uint8, depth),
		seeds:      make([]uint32, depth),
		resetEvery: resetEvery,
		maxCount:   uint8((uint16(1) << uint(bits)) - 1),
	}
	for i := 0; i < depth; i++ {
		cs.counters[i] = make([]uint8, w)
		// Mix the seeds with two large odd constants so the rows hash
		// keys to genuinely independent columns.
		cs.seeds[i] = uint32(0x9747B28C) ^ uint32(i)*uint32(0x6F0B7B5D)
	}
	return cs
}

// Increment bumps every row's counter for key, applying the periodic reset
// when the cumulative access count crosses resetEvery.
func (cs *countMinSketch) Increment(key string) {
	for i := 0; i < cs.depth; i++ {
		idx := cs.hash(key, cs.seeds[i])
		if cs.counters[i][idx] < cs.maxCount {
			cs.counters[i][idx]++
		}
	}
	cs.accesses++
	if cs.resetEvery > 0 && cs.accesses >= cs.resetEvery {
		cs.reset()
		cs.accesses = 0
	}
}

// Estimate returns the conservative (minimum-of-rows) frequency for key.
func (cs *countMinSketch) Estimate(key string) uint8 {
	min := cs.maxCount
	for i := 0; i < cs.depth; i++ {
		idx := cs.hash(key, cs.seeds[i])
		if cs.counters[i][idx] < min {
			min = cs.counters[i][idx]
		}
	}
	return min
}

// reset halves every counter so newer accesses have heavier weight than
// long-stale ones.  Called automatically when accesses crosses resetEvery.
func (cs *countMinSketch) reset() {
	for i := 0; i < cs.depth; i++ {
		row := cs.counters[i]
		for j := range row {
			row[j] >>= 1
		}
	}
}

// hash computes the column index for key within a single sketch row.  Uses
// FNV-1a with the row seed prepended so each row sees a unique byte stream
// for the same key.
func (cs *countMinSketch) hash(key string, seed uint32) uint32 {
	h := fnv.New32a()
	var b [4]byte
	b[0] = byte(seed)
	b[1] = byte(seed >> 8)
	b[2] = byte(seed >> 16)
	b[3] = byte(seed >> 24)
	_, _ = h.Write(b[:])
	_, _ = h.Write([]byte(key))
	return h.Sum32() & cs.mask
}
