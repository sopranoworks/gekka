/*
 * ormap_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestORMap_Basic(t *testing.T) {
	node1 := "node1"
	node2 := "node2"
	m1 := NewORMap()
	m2 := NewORMap()

	c1 := NewPNCounter()
	c1.Increment(node1, 10)
	m1.Put(node1, "k1", c1)

	c2 := NewPNCounter()
	c2.Increment(node2, 20)
	m2.Put(node2, "k2", c2)

	m1.Merge(m2)

	v1, ok := m1.Get("k1")
	assert.True(t, ok)
	assert.Equal(t, int64(10), v1.(*PNCounter).Value())

	v2, ok := m1.Get("k2")
	assert.True(t, ok)
	assert.Equal(t, int64(20), v2.(*PNCounter).Value())

	m1.Remove("k1")
	_, ok = m1.Get("k1")
	assert.False(t, ok)
}

func TestPNCounterMap(t *testing.T) {
	node1 := "node1"
	node2 := "node2"
	m1 := NewPNCounterMap()
	m2 := NewPNCounterMap()

	m1.Increment(node1, "k1", 5)
	m2.Increment(node2, "k1", 10)
	m2.Decrement(node2, "k1", 2)

	m1.Merge(m2)

	v, ok := m1.Get("k1")
	assert.True(t, ok)
	assert.Equal(t, int64(13), v.(*PNCounter).Value())
}

func TestORMultiMap(t *testing.T) {
	node1 := "node1"
	node2 := "node2"
	m1 := NewORMultiMap()
	m2 := NewORMultiMap()

	m1.AddBinding(node1, "k1", "a")
	m1.AddBinding(node1, "k1", "b")
	m2.AddBinding(node2, "k1", "b")
	m2.AddBinding(node2, "k1", "c")

	m1.Merge(m2)

	v, ok := m1.Get("k1")
	assert.True(t, ok)
	elements := v.(*ORSet).Elements()
	assert.ElementsMatch(t, []string{"a", "b", "c"}, elements)

	m1.RemoveBinding(node1, "k1", "a")
	v, _ = m1.Get("k1")
	assert.ElementsMatch(t, []string{"b", "c"}, v.(*ORSet).Elements())
}

func TestLWWMap_MultiNode(t *testing.T) {
	node1 := "node1"
	node2 := "node2"
	m1 := NewLWWMap()
	m2 := NewLWWMap()

	m1.PutWithNodeID(node1, "k1", "v1-node1")
	m2.PutWithNodeID(node2, "k1", "v1-node2") // concurrent update to same key

	// Since we use wall-clock time in LWWRegister, one will win.
	// Let's force a deterministic merge by manually setting timestamps if needed,
	// but here we just test that it converges.

	m1.Merge(m2.Snapshot())
	m2.Merge(m1.Snapshot())

	v1, _ := m1.Get("k1")
	v2, _ := m2.Get("k1")
	assert.Equal(t, v1, v2)
}
