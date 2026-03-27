/*
 * shapes.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

// Port is the internal interface for all connection points.
type Port interface {
	setID(id int)
	getID() int
}

// Inlet[T] represents a typed input port on a stream stage.
type Inlet[T any] struct {
	id int
}

func (i *Inlet[T]) setID(id int) { i.id = id }
func (i *Inlet[T]) getID() int  { return i.id }

// Outlet[T] represents a typed output port on a stream stage.
type Outlet[T any] struct {
	id int
}

func (o *Outlet[T]) setID(id int) { o.id = id }
func (o *Outlet[T]) getID() int  { return o.id }

// Shape represents the set of input and output ports of a stream stage.
type Shape interface {
	Inlets() []Port
	Outlets() []Port
}

// SourceShape[T] has exactly one Outlet.
type SourceShape[T any] struct {
	Out *Outlet[T]
}

func (s SourceShape[T]) Inlets() []Port {
	return nil
}

func (s SourceShape[T]) Outlets() []Port {
	return []Port{s.Out}
}

// SinkShape[T] has exactly one Inlet.
type SinkShape[T any] struct {
	In *Inlet[T]
}

func (s SinkShape[T]) Inlets() []Port {
	return []Port{s.In}
}

func (s SinkShape[T]) Outlets() []Port {
	return nil
}

// FlowShape[In, Out] has one Inlet and one Outlet.
type FlowShape[In, Out any] struct {
	In  *Inlet[In]
	Out *Outlet[Out]
}

func (s FlowShape[In, Out]) Inlets() []Port {
	return []Port{s.In}
}

func (s FlowShape[In, Out]) Outlets() []Port {
	return []Port{s.Out}
}

// FanInShape[T] has multiple Inlets and one Outlet.
type FanInShape[T any] struct {
	Inlets_ []*Inlet[T]
	Out     *Outlet[T]
}

func (s FanInShape[T]) Inlets() []Port {
	res := make([]Port, len(s.Inlets_))
	for i, in := range s.Inlets_ {
		res[i] = in
	}
	return res
}

func (s FanInShape[T]) Outlets() []Port {
	return []Port{s.Out}
}

// FanOutShape[T] has one Inlet and multiple Outlets.
type FanOutShape[T any] struct {
	In       *Inlet[T]
	Outlets_ []*Outlet[T]
}

func (s FanOutShape[T]) Inlets() []Port {
	return []Port{s.In}
}

func (s FanOutShape[T]) Outlets() []Port {
	res := make([]Port, len(s.Outlets_))
	for i, out := range s.Outlets_ {
		res[i] = out
	}
	return res
}

// ClosedShape represents a shape with no ports, used for RunnableGraph.
type ClosedShape struct{}

func (s ClosedShape) Inlets() []Port {
	return nil
}

func (s ClosedShape) Outlets() []Port {
	return nil
}

// FanIn2Shape[T0, T1, Out] has two distinct Inlets and one Outlet.
type FanIn2Shape[T0, T1, Out any] struct {
	In0 *Inlet[T0]
	In1 *Inlet[T1]
	Out *Outlet[Out]
}

func (s FanIn2Shape[T0, T1, Out]) Inlets() []Port {
	return []Port{s.In0, s.In1}
}

func (s FanIn2Shape[T0, T1, Out]) Outlets() []Port {
	return []Port{s.Out}
}

