/*
 * source_ops.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

// Cycle creates an infinite [Source] that repeats elements in order, wrapping
// around when exhausted.  Panics if elements is empty.
//
// Use [Source.Take] to bound the output:
//
//	stream.Cycle([]int{1, 2, 3}).Take(7) // [1,2,3,1,2,3,1]
func Cycle[T any](elements []T) Source[T, NotUsed] {
	if len(elements) == 0 {
		panic("stream.Cycle: elements must not be empty")
	}
	cp := make([]T, len(elements))
	copy(cp, elements)
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			return &cycleIterator[T]{elems: cp}, NotUsed{}
		},
	}
}

type cycleIterator[T any] struct {
	elems []T
	pos   int
}

func (c *cycleIterator[T]) next() (T, bool, error) {
	v := c.elems[c.pos]
	c.pos = (c.pos + 1) % len(c.elems)
	return v, true, nil
}

// ZipWithIndex pairs each element from src with its 0-based index.
func ZipWithIndex[T any](src Source[T, NotUsed]) Source[Pair[T, int64], NotUsed] {
	return Source[Pair[T, int64], NotUsed]{
		factory: func() (iterator[Pair[T, int64]], NotUsed) {
			up, _ := src.factory()
			return &zipWithIndexIterator[T]{upstream: up}, NotUsed{}
		},
	}
}

type zipWithIndexIterator[T any] struct {
	upstream iterator[T]
	idx      int64
}

func (z *zipWithIndexIterator[T]) next() (Pair[T, int64], bool, error) {
	elem, ok, err := z.upstream.next()
	if !ok || err != nil {
		var zero Pair[T, int64]
		return zero, ok, err
	}
	p := Pair[T, int64]{First: elem, Second: z.idx}
	z.idx++
	return p, true, nil
}

// CombineStrategy selects how [Combine] merges multiple sources.
type CombineStrategy int

const (
	// CombineMerge merges sources concurrently (order is non-deterministic).
	CombineMerge CombineStrategy = iota
	// CombineConcat concatenates sources sequentially.
	CombineConcat
)

// Combine merges multiple sources into one using the given strategy.
//   - [CombineMerge]: concurrent merge (same as [Merge]).
//   - [CombineConcat]: sequential concatenation in order.
func Combine[T any](strategy CombineStrategy, sources ...Source[T, NotUsed]) Source[T, NotUsed] {
	if len(sources) == 0 {
		return FromSlice[T](nil)
	}
	switch strategy {
	case CombineConcat:
		result := sources[0]
		for _, s := range sources[1:] {
			result = Concat(result, s)
		}
		return result
	default: // CombineMerge
		return Merge(sources...)
	}
}
