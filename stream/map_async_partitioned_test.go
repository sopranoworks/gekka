/*
 * map_async_partitioned_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMapAsyncPartitioned_OrderedPerKey verifies per-partition ordering.
// Odd-keyed elements must appear in input order; same for even-keyed.
// Different keys may interleave.
func TestMapAsyncPartitioned_OrderedPerKey(t *testing.T) {
	src := stream.FromSlice([]int{1, 2, 3, 4, 5, 6})

	result, err := stream.RunWith(
		stream.MapAsyncPartitioned(src, 4,
			func(n int) string {
				if n%2 == 0 {
					return "even"
				}
				return "odd"
			},
			func(n int) <-chan int {
				ch := make(chan int, 1)
				// Reverse delay: higher number finishes sooner within same partition.
				go func() { time.Sleep(time.Duration(10-n) * time.Millisecond); ch <- n * 10 }()
				return ch
			},
		),
		stream.Collect[int](),
		stream.SyncMaterializer{},
	)
	require.NoError(t, err)

	var odds, evens []int
	for _, v := range result {
		if (v/10)%2 != 0 {
			odds = append(odds, v)
		} else {
			evens = append(evens, v)
		}
	}
	assert.Equal(t, []int{10, 30, 50}, odds, "odd partition must preserve input order")
	assert.Equal(t, []int{20, 40, 60}, evens, "even partition must preserve input order")
}

// TestMapAsyncPartitioned_SinglePartitionIsSequential verifies that a single
// partition key forces sequential processing (peak concurrency == 1).
func TestMapAsyncPartitioned_SinglePartitionIsSequential(t *testing.T) {
	var concurrent int64
	var peak int64

	src := stream.FromSlice([]int{1, 2, 3, 4, 5, 6, 7, 8})
	_, err := stream.RunWith(
		stream.MapAsyncPartitioned(src, 4,
			func(_ int) string { return "same" },
			func(n int) <-chan int {
				ch := make(chan int, 1)
				go func() {
					c := atomic.AddInt64(&concurrent, 1)
					for {
						p := atomic.LoadInt64(&peak)
						if c <= p || atomic.CompareAndSwapInt64(&peak, p, c) {
							break
						}
					}
					time.Sleep(5 * time.Millisecond)
					atomic.AddInt64(&concurrent, -1)
					ch <- n
				}()
				return ch
			},
		),
		stream.Collect[int](),
		stream.SyncMaterializer{},
	)
	require.NoError(t, err)
	assert.Equal(t, int64(1), atomic.LoadInt64(&peak),
		"single partition key must process sequentially")
}
