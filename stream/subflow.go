/*
 * subflow.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

// SubFlow[T] wraps a keyed sub-stream grouping produced by [Source.GroupBy].
// It exposes operators that apply independently to each sub-stream, and
// [SubFlow.MergeSubstreams] to re-combine them into a single [Source].
type SubFlow[T any] struct {
	src           Source[SubStream[T], NotUsed]
	maxSubstreams int
}

// Filter applies pred to every element inside each sub-stream, retaining only
// elements for which pred returns true.
func (sf SubFlow[T]) Filter(pred func(T) bool) SubFlow[T] {
	return SubFlow[T]{
		src: Via(sf.src, Map[SubStream[T], SubStream[T]](func(ss SubStream[T]) SubStream[T] {
			return SubStream[T]{
				Key:    ss.Key,
				Source: Via(ss.Source, Filter[T](pred)),
			}
		})),
		maxSubstreams: sf.maxSubstreams,
	}
}

// MergeSubstreams re-merges all grouped sub-streams into a single [Source][T].
// Up to maxSubstreams sub-streams are consumed concurrently; elements from
// different sub-streams may interleave non-deterministically.
func (sf SubFlow[T]) MergeSubstreams() Source[T, NotUsed] {
	srcOfSources := Via(sf.src, Map[SubStream[T], Source[T, NotUsed]](func(ss SubStream[T]) Source[T, NotUsed] {
		return ss.Source
	}))
	return FlattenMerge(srcOfSources, sf.maxSubstreams)
}

// To connects this SubFlow to sink after merging all sub-streams.
// It is equivalent to sf.MergeSubstreams().To(sink).
func (sf SubFlow[T]) To(sink sinkConnector[T]) RunnableGraph[NotUsed] {
	return sf.MergeSubstreams().To(sink)
}
