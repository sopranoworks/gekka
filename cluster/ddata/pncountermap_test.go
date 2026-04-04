/*
 * pncountermap_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------- basic operations ----------

func TestPNCounterMap_IncrementDecrement(t *testing.T) {
	m := NewPNCounterMap()
	m.Increment("n1", "hits", 5)
	m.Increment("n1", "hits", 3)
	m.Decrement("n1", "hits", 2)

	v, ok := m.GetValue("hits")
	require.True(t, ok)
	assert.Equal(t, int64(6), v)
}

func TestPNCounterMap_MultipleKeys(t *testing.T) {
	m := NewPNCounterMap()
	m.Increment("n1", "requests", 100)
	m.Increment("n1", "errors", 5)
	m.Decrement("n1", "requests", 10)

	v1, ok1 := m.GetValue("requests")
	v2, ok2 := m.GetValue("errors")
	require.True(t, ok1)
	require.True(t, ok2)
	assert.Equal(t, int64(90), v1)
	assert.Equal(t, int64(5), v2)
}

func TestPNCounterMap_GetAbsentKey(t *testing.T) {
	m := NewPNCounterMap()
	v, ok := m.GetValue("missing")
	assert.False(t, ok)
	assert.Equal(t, int64(0), v)
}

func TestPNCounterMap_Keys(t *testing.T) {
	m := NewPNCounterMap()
	m.Increment("n1", "a", 1)
	m.Increment("n1", "b", 2)
	m.Increment("n1", "c", 3)

	keys := m.Keys()
	sort.Strings(keys)
	assert.Equal(t, []string{"a", "b", "c"}, keys)
}

func TestPNCounterMap_NegativeValue(t *testing.T) {
	m := NewPNCounterMap()
	m.Increment("n1", "balance", 10)
	m.Decrement("n1", "balance", 15)

	v, ok := m.GetValue("balance")
	require.True(t, ok)
	assert.Equal(t, int64(-5), v)
}

// ---------- CRDT merge semantics ----------

func TestPNCounterMap_Commutativity(t *testing.T) {
	// m1.Merge(m2) must equal m2.Merge(m1) (convergent value)
	m1a := NewPNCounterMap()
	m1a.Increment("n1", "k", 10)
	m1a.Decrement("n1", "k", 3)

	m2a := NewPNCounterMap()
	m2a.Increment("n2", "k", 7)
	m2a.Decrement("n2", "k", 2)

	// copy for second merge order
	m1b := m1a.Copy().(*PNCounterMap)
	m2b := m2a.Copy().(*PNCounterMap)

	m1a.Merge(m2a)
	m2b.Merge(m1b)

	v1, _ := m1a.GetValue("k")
	v2, _ := m2b.GetValue("k")
	assert.Equal(t, v1, v2, "commutativity: merge order must not affect result")
}

func TestPNCounterMap_Associativity(t *testing.T) {
	// (m1.Merge(m2)).Merge(m3) == m1.Merge(m2.Merge(m3))
	build := func(node, key string, inc, dec uint64) *PNCounterMap {
		m := NewPNCounterMap()
		m.Increment(node, key, inc)
		m.Decrement(node, key, dec)
		return m
	}
	m1 := build("n1", "x", 10, 1)
	m2 := build("n2", "x", 20, 2)
	m3 := build("n3", "x", 30, 3)

	// (m1 ∪ m2) ∪ m3
	left := m1.Copy().(*PNCounterMap)
	left.Merge(m2.Copy().(*PNCounterMap))
	left.Merge(m3.Copy().(*PNCounterMap))

	// m1 ∪ (m2 ∪ m3)
	right := m2.Copy().(*PNCounterMap)
	right.Merge(m3.Copy().(*PNCounterMap))
	m1copy := m1.Copy().(*PNCounterMap)
	m1copy.Merge(right)

	vl, _ := left.GetValue("x")
	vr, _ := m1copy.GetValue("x")
	assert.Equal(t, vl, vr, "associativity: grouping must not affect result")
}

func TestPNCounterMap_Idempotency(t *testing.T) {
	m := NewPNCounterMap()
	m.Increment("n1", "k", 5)
	m.Decrement("n1", "k", 2)

	// Merging with itself must not change value
	m.Merge(m.Copy().(*PNCounterMap))
	m.Merge(m.Copy().(*PNCounterMap))

	v, ok := m.GetValue("k")
	require.True(t, ok)
	assert.Equal(t, int64(3), v, "idempotency: self-merge must be no-op")
}

func TestPNCounterMap_MultiNodeMerge(t *testing.T) {
	// 3 nodes each increment different keys; after full merge all nodes converge
	m1 := NewPNCounterMap()
	m1.Increment("n1", "a", 100)

	m2 := NewPNCounterMap()
	m2.Increment("n2", "b", 200)

	m3 := NewPNCounterMap()
	m3.Increment("n3", "c", 300)
	m3.Decrement("n3", "a", 10)

	// Full gossip: everyone merges everyone
	m1.Merge(m2.Copy().(*PNCounterMap))
	m1.Merge(m3.Copy().(*PNCounterMap))
	m2.Merge(m1.Copy().(*PNCounterMap))
	m3.Merge(m1.Copy().(*PNCounterMap))

	for _, m := range []*PNCounterMap{m1, m2, m3} {
		va, _ := m.GetValue("a")
		vb, _ := m.GetValue("b")
		vc, _ := m.GetValue("c")
		assert.Equal(t, int64(90), va, "a = 100 - 10")
		assert.Equal(t, int64(200), vb)
		assert.Equal(t, int64(300), vc)
	}
}

// ---------- concurrent access ----------

func TestPNCounterMap_ConcurrentIncrement(t *testing.T) {
	m := NewPNCounterMap()
	var wg sync.WaitGroup
	const goroutines = 50
	const delta = uint64(2)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			node := "n1"
			m.Increment(node, "counter", delta)
		}(i)
	}
	wg.Wait()

	v, ok := m.GetValue("counter")
	require.True(t, ok)
	assert.Equal(t, int64(goroutines)*int64(delta), v)
}

func TestPNCounterMap_ConcurrentMixedOps(t *testing.T) {
	m := NewPNCounterMap()
	var wg sync.WaitGroup

	for i := 0; i < 25; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			m.Increment("n1", "k", 1)
		}()
		go func() {
			defer wg.Done()
			m.Decrement("n2", "k", 1)
		}()
	}
	wg.Wait()

	// net value must be 0 (25 inc - 25 dec), no panic
	v, ok := m.GetValue("k")
	require.True(t, ok)
	assert.Equal(t, int64(0), v)
}

// ---------- Copy ----------

func TestPNCounterMap_CopyIsIndependent(t *testing.T) {
	original := NewPNCounterMap()
	original.Increment("n1", "x", 10)

	clone := original.Copy().(*PNCounterMap)
	clone.Increment("n1", "x", 5) // must not affect original

	orig, _ := original.GetValue("x")
	cloned, _ := clone.GetValue("x")
	assert.Equal(t, int64(10), orig)
	assert.Equal(t, int64(15), cloned)
}
