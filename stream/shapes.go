/*
 * shapes.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

// Inlet[T] represents a typed input port on a stream stage.
type Inlet[T any] struct{}

// Outlet[T] represents a typed output port on a stream stage.
type Outlet[T any] struct{}

// Shape represents the set of input and output ports of a stream stage.
type Shape interface {
	Inlets() []any
	Outlets() []any
}

// SourceShape[T] has exactly one Outlet.
type SourceShape[T any] struct {
	Out Outlet[T]
}

func (s SourceShape[T]) Inlets() []any {
	return nil
}

func (s SourceShape[T]) Outlets() []any {
	return []any{s.Out}
}

// SinkShape[T] has exactly one Inlet.
type SinkShape[T any] struct {
	In Inlet[T]
}

func (s SinkShape[T]) Inlets() []any {
	return []any{s.In}
}

func (s SinkShape[T]) Outlets() []any {
	return nil
}

// FlowShape[In, Out] has one Inlet and one Outlet.
type FlowShape[In, Out any] struct {
	In  Inlet[In]
	Out Outlet[Out]
}

func (s FlowShape[In, Out]) Inlets() []any {
	return []any{s.In}
}

func (s FlowShape[In, Out]) Outlets() []any {
	return []any{s.Out}
}

// FanInShape[T] has multiple Inlets and one Outlet.
type FanInShape[T any] struct {
	Inlets_ []Inlet[T]
	Out     Outlet[T]
}

func (s FanInShape[T]) Inlets() []any {
	res := make([]any, len(s.Inlets_))
	for i, in := range s.Inlets_ {
		res[i] = in
	}
	return res
}

func (s FanInShape[T]) Outlets() []any {
	return []any{s.Out}
}

// FanOutShape[T] has one Inlet and multiple Outlets.
type FanOutShape[T any] struct {
	In       Inlet[T]
	Outlets_ []Outlet[T]
}

func (s FanOutShape[T]) Inlets() []any {
	return []any{s.In}
}

func (s FanOutShape[T]) Outlets() []any {
	res := make([]any, len(s.Outlets_))
	for i, out := range s.Outlets_ {
		res[i] = out
	}
	return res
}

// ClosedShape represents a shape with no ports, used for RunnableGraph.
type ClosedShape struct{}

func (s ClosedShape) Inlets() []any {
	return nil
}

func (s ClosedShape) Outlets() []any {
	return nil
}
