/*
 * advanced_ops_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGroupBy(t *testing.T) {
	src := FromSlice([]int{1, 2, 3, 4, 5, 6})
	
	substreams, err := RunWith(
		GroupBy(src, 2, func(n int) string {
			if n%2 == 0 { return "even" }
			return "odd"
		}),
		Collect[SubStream[int]](),
		SyncMaterializer{},
	)
	require.NoError(t, err)
	assert.Len(t, substreams, 2)

	for _, ss := range substreams {
		elems, err := RunWith(ss.Source, Collect[int](), SyncMaterializer{})
		require.NoError(t, err)
		if ss.Key == "even" {
			assert.Equal(t, []int{2, 4, 6}, elems)
		} else {
			assert.Equal(t, []int{1, 3, 5}, elems)
		}
	}
}

func TestFlattenMerge(t *testing.T) {
	s1 := FromSlice([]int{1, 2})
	s2 := FromSlice([]int{3, 4})
	src := FromSlice([]Source[int, NotUsed]{s1, s2})

	res, err := RunWith(FlattenMerge(src, 2), Collect[int](), SyncMaterializer{})
	require.NoError(t, err)
	assert.Len(t, res, 4)
	assert.ElementsMatch(t, []int{1, 2, 3, 4}, res)
}

func TestConflate(t *testing.T) {
	// Use a slow sink to trigger conflation
	src := FromSlice([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	
	// conflate sum
	c := Conflate(src, func(i int) int { return i }, func(s int, i int) int { return s + i })
	
	// To reliably test conflation in a sync materializer might be tricky
	// because it pulls as fast as it can.
	// But our conflateIterator has a chan 1, so it might work if we add a delay in pull.
	
	it, _ := c.factory()
	
	// First pull
	v, ok, err := it.next()
	require.NoError(t, err)
	assert.True(t, ok)
	
	// Since SyncMaterializer pulls sequentially, we might just get all elements summed or individually.
	// In this implementation, the conflate goroutine runs ahead.
	
	total := v
	for {
		v, ok, err = it.next()
		require.NoError(t, err)
		if !ok {
			break
		}
		total += v
	}
	assert.Equal(t, 55, total)
}
