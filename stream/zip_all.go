/*
 * zip_all.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

// ZipAll combines two sources element-wise into pairs, padding the shorter
// source with the given default value so that no elements are dropped. The
// stream completes when both sources are exhausted.
func ZipAll[T1, T2 any](
	s1 Source[T1, NotUsed],
	s2 Source[T2, NotUsed],
	pad1 T1,
	pad2 T2,
) Source[Pair[T1, T2], NotUsed] {
	return ZipAllWith(s1, s2, pad1, pad2, func(a T1, b T2) Pair[T1, T2] {
		return Pair[T1, T2]{First: a, Second: b}
	})
}

// ZipAllWith combines two sources element-wise using a custom combine function,
// padding the shorter source with the given default value. The stream completes
// when both sources are exhausted.
func ZipAllWith[T1, T2, Out any](
	s1 Source[T1, NotUsed],
	s2 Source[T2, NotUsed],
	pad1 T1,
	pad2 T2,
	combine func(T1, T2) Out,
) Source[Out, NotUsed] {
	return Source[Out, NotUsed]{
		factory: func() (iterator[Out], NotUsed) {
			left, _ := s1.factory()
			right, _ := s2.factory()
			return &zipAllIterator[T1, T2, Out]{
				left:    left,
				right:   right,
				pad1:    pad1,
				pad2:    pad2,
				combine: combine,
			}, NotUsed{}
		},
	}
}

type zipAllIterator[T1, T2, Out any] struct {
	left    iterator[T1]
	right   iterator[T2]
	pad1    T1
	pad2    T2
	combine func(T1, T2) Out
	done1   bool
	done2   bool
}

func (z *zipAllIterator[T1, T2, Out]) next() (Out, bool, error) {
	var zero Out

	if z.done1 && z.done2 {
		return zero, false, nil
	}

	var v1 T1
	var v2 T2

	if !z.done1 {
		elem, ok, err := z.left.next()
		if err != nil {
			return zero, false, err
		}
		if !ok {
			z.done1 = true
			v1 = z.pad1
		} else {
			v1 = elem
		}
	} else {
		v1 = z.pad1
	}

	if !z.done2 {
		elem, ok, err := z.right.next()
		if err != nil {
			return zero, false, err
		}
		if !ok {
			z.done2 = true
			v2 = z.pad2
		} else {
			v2 = elem
		}
	} else {
		v2 = z.pad2
	}

	// If both became done on this pull, stream is complete.
	if z.done1 && z.done2 {
		return zero, false, nil
	}

	return z.combine(v1, v2), true, nil
}
