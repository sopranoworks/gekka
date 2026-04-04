/*
 * ormultimap_test.go
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

func TestORMultiMap_PutAndGet(t *testing.T) {
	m := NewORMultiMap()
	m.Put("n1", "fruits", "apple")
	m.Put("n1", "fruits", "banana")
	m.Put("n1", "vegs", "carrot")

	fruits := m.GetElements("fruits")
	sort.Strings(fruits)
	assert.Equal(t, []string{"apple", "banana"}, fruits)

	vegs := m.GetElements("vegs")
	assert.Equal(t, []string{"carrot"}, vegs)
}

func TestORMultiMap_RemoveElement(t *testing.T) {
	m := NewORMultiMap()
	m.Put("n1", "tags", "go")
	m.Put("n1", "tags", "crdt")
	m.Put("n1", "tags", "actor")

	m.Remove("n1", "tags", "crdt")

	elems := m.GetElements("tags")
	sort.Strings(elems)
	assert.Equal(t, []string{"actor", "go"}, elems)
}

func TestORMultiMap_RemoveLastElement_KeyGone(t *testing.T) {
	m := NewORMultiMap()
	m.Put("n1", "k", "only")
	m.Remove("n1", "k", "only")

	elems := m.GetElements("k")
	assert.Nil(t, elems, "key must be absent after last element removed")
}

func TestORMultiMap_GetAbsentKey(t *testing.T) {
	m := NewORMultiMap()
	assert.Nil(t, m.GetElements("missing"))
}

func TestORMultiMap_Keys(t *testing.T) {
	m := NewORMultiMap()
	m.Put("n1", "a", "v1")
	m.Put("n1", "b", "v2")
	m.Put("n1", "c", "v3")

	keys := m.Keys()
	sort.Strings(keys)
	assert.Equal(t, []string{"a", "b", "c"}, keys)
}

func TestORMultiMap_AddBindingAlias(t *testing.T) {
	// AddBinding and Put must be interchangeable
	m := NewORMultiMap()
	m.AddBinding("n1", "k", "x")
	m.Put("n1", "k", "y")

	elems := m.GetElements("k")
	sort.Strings(elems)
	assert.Equal(t, []string{"x", "y"}, elems)
}

// ---------- CRDT merge semantics ----------

func TestORMultiMap_Commutativity(t *testing.T) {
	m1a := NewORMultiMap()
	m1a.Put("n1", "k", "a")
	m1a.Put("n1", "k", "b")

	m2a := NewORMultiMap()
	m2a.Put("n2", "k", "b")
	m2a.Put("n2", "k", "c")

	m1b := m1a.Copy().(*ORMultiMap)
	m2b := m2a.Copy().(*ORMultiMap)

	m1a.Merge(m2a)
	m2b.Merge(m1b)

	e1 := m1a.GetElements("k")
	e2 := m2b.GetElements("k")
	sort.Strings(e1)
	sort.Strings(e2)
	assert.Equal(t, e1, e2, "commutativity: merge order must not affect result")
	assert.Equal(t, []string{"a", "b", "c"}, e1)
}

func TestORMultiMap_Associativity(t *testing.T) {
	m1 := NewORMultiMap()
	m1.Put("n1", "k", "a")

	m2 := NewORMultiMap()
	m2.Put("n2", "k", "b")

	m3 := NewORMultiMap()
	m3.Put("n3", "k", "c")

	// (m1 ∪ m2) ∪ m3
	left := m1.Copy().(*ORMultiMap)
	left.Merge(m2.Copy().(*ORMultiMap))
	left.Merge(m3.Copy().(*ORMultiMap))

	// m1 ∪ (m2 ∪ m3)
	right := m2.Copy().(*ORMultiMap)
	right.Merge(m3.Copy().(*ORMultiMap))
	m1c := m1.Copy().(*ORMultiMap)
	m1c.Merge(right)

	el := left.GetElements("k")
	er := m1c.GetElements("k")
	sort.Strings(el)
	sort.Strings(er)
	assert.Equal(t, el, er, "associativity: grouping must not affect result")
}

func TestORMultiMap_Idempotency(t *testing.T) {
	m := NewORMultiMap()
	m.Put("n1", "k", "x")
	m.Put("n1", "k", "y")

	m.Merge(m.Copy().(*ORMultiMap))
	m.Merge(m.Copy().(*ORMultiMap))

	elems := m.GetElements("k")
	sort.Strings(elems)
	assert.Equal(t, []string{"x", "y"}, elems, "idempotency: self-merge must be no-op")
}

func TestORMultiMap_AddWins(t *testing.T) {
	// Concurrent add and remove of same element: add-wins semantics
	m1 := NewORMultiMap()
	m1.Put("n1", "k", "elem")

	m2 := m1.Copy().(*ORMultiMap) // m2 starts from same state

	// n2 concurrently removes "elem"
	m2.Remove("n2", "k", "elem")
	// n1 concurrently re-adds "elem" (add must win over concurrent remove)
	m1.Put("n1", "k", "elem")

	m1.Merge(m2)

	elems := m1.GetElements("k")
	assert.Contains(t, elems, "elem", "add-wins: concurrent add must survive concurrent remove")
}

func TestORMultiMap_MultipleKeys_FullGossip(t *testing.T) {
	m1 := NewORMultiMap()
	m1.Put("n1", "colors", "red")
	m1.Put("n1", "sizes", "small")

	m2 := NewORMultiMap()
	m2.Put("n2", "colors", "blue")
	m2.Put("n2", "sizes", "large")

	m3 := NewORMultiMap()
	m3.Put("n3", "colors", "green")

	// Full gossip round
	m1.Merge(m2.Copy().(*ORMultiMap))
	m1.Merge(m3.Copy().(*ORMultiMap))
	m2.Merge(m1.Copy().(*ORMultiMap))
	m3.Merge(m1.Copy().(*ORMultiMap))

	for _, m := range []*ORMultiMap{m1, m2, m3} {
		colors := m.GetElements("colors")
		sort.Strings(colors)
		assert.Equal(t, []string{"blue", "green", "red"}, colors)

		sizes := m.GetElements("sizes")
		sort.Strings(sizes)
		assert.Equal(t, []string{"large", "small"}, sizes)
	}
}

// ---------- concurrent access ----------

func TestORMultiMap_ConcurrentPut(t *testing.T) {
	m := NewORMultiMap()
	var wg sync.WaitGroup
	const n = 50

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			elem := string(rune('a' + id%26))
			m.Put("n1", "key", elem)
		}(i)
	}
	wg.Wait()

	// All distinct elements must be present; no panic
	elems := m.GetElements("key")
	require.NotNil(t, elems)
}

func TestORMultiMap_ConcurrentPutRemove(t *testing.T) {
	m := NewORMultiMap()
	var wg sync.WaitGroup

	for i := 0; i < 30; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			m.Put("n1", "shared", "elem")
		}()
		go func() {
			defer wg.Done()
			m.Remove("n2", "shared", "elem")
		}()
	}
	wg.Wait()
	// Must not panic; final state depends on scheduling
	_ = m.GetElements("shared")
}

// ---------- Copy ----------

func TestORMultiMap_CopyIsIndependent(t *testing.T) {
	original := NewORMultiMap()
	original.Put("n1", "k", "x")

	clone := original.Copy().(*ORMultiMap)
	clone.Put("n1", "k", "y") // must not affect original

	orig := original.GetElements("k")
	cloned := clone.GetElements("k")

	assert.Equal(t, []string{"x"}, orig)
	sort.Strings(cloned)
	assert.Equal(t, []string{"x", "y"}, cloned)
}
