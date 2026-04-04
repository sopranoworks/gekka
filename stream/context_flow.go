/*
 * context_flow.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

// SourceWithContext pairs each emitted element with an opaque context value.
// The context is carried alongside every element through the pipeline and is
// accessible at the sink, making it easy to propagate metadata — such as Kafka
// offsets, HTTP request IDs, or W3C trace headers — without threading it
// through every operator manually.
//
// SourceWithContext mirrors Pekko's pekko.stream.scaladsl.SourceWithContext.
//
// Example — wrap a slice source with an index context:
//
//	src := stream.SourceWithContextOf(
//	    stream.FromSlice([]string{"a", "b", "c"}),
//	    func(i int, s string) int { return i },
//	)
//	// Each element arrives downstream as Pair[string, int]{First: "a", Second: 0}, …
type SourceWithContext[T, Ctx, Mat any] struct {
	inner Source[Pair[T, Ctx], Mat]
}

// AsSourceWithContext wraps src by calling ctxFor for each element to derive
// its context value.  The index parameter i is the zero-based position of the
// element in the stream.
//
// Because Go generics do not allow the Source type to carry a method that
// returns a different outer type, this is a package-level constructor.
func AsSourceWithContext[T, Ctx, Mat any](
	src Source[T, Mat],
	ctxFor func(i int, elem T) Ctx,
) SourceWithContext[T, Ctx, Mat] {
	return SourceWithContext[T, Ctx, Mat]{
		inner: Source[Pair[T, Ctx], Mat]{
			factory: func() (iterator[Pair[T, Ctx]], Mat) {
				up, mat := src.factory()
				return &contextSourceIterator[T, Ctx]{upstream: up, ctxFor: ctxFor}, mat
			},
		},
	}
}

// SourceWithContextFrom wraps an existing source of (element, context) Pairs.
// This is useful when the pairs are already produced by an upstream stage.
func SourceWithContextFrom[T, Ctx, Mat any](src Source[Pair[T, Ctx], Mat]) SourceWithContext[T, Ctx, Mat] {
	return SourceWithContext[T, Ctx, Mat]{inner: src}
}

// Map applies fn to each element, preserving the context unchanged.
func (s SourceWithContext[T, Ctx, Mat]) Map(fn func(T) T) SourceWithContext[T, Ctx, Mat] {
	return SourceWithContext[T, Ctx, Mat]{
		inner: Source[Pair[T, Ctx], Mat]{
			factory: func() (iterator[Pair[T, Ctx]], Mat) {
				up, mat := s.inner.factory()
				return &contextMapIterator[T, Ctx]{
					upstream: up,
					fn:       fn,
				}, mat
			},
		},
	}
}

// Filter keeps only elements for which pred returns true.  The context of
// dropped elements is also discarded.
func (s SourceWithContext[T, Ctx, Mat]) Filter(pred func(T) bool) SourceWithContext[T, Ctx, Mat] {
	return SourceWithContext[T, Ctx, Mat]{
		inner: Source[Pair[T, Ctx], Mat]{
			factory: func() (iterator[Pair[T, Ctx]], Mat) {
				up, mat := s.inner.factory()
				return &contextFilterIterator[T, Ctx]{upstream: up, pred: pred}, mat
			},
		},
	}
}

// AsSource returns the underlying Source[Pair[T, Ctx], Mat], which can be
// composed with standard Flow and Sink stages.
func (s SourceWithContext[T, Ctx, Mat]) AsSource() Source[Pair[T, Ctx], Mat] {
	return s.inner
}

// To connects this source to a context-aware sink.
//
// The returned RunnableGraph can be Run with any Materializer.
func (s SourceWithContext[T, Ctx, Mat]) To(sink Sink[Pair[T, Ctx], NotUsed]) RunnableGraph[Mat] {
	return s.inner.To(sink)
}

// ─── FlowWithContext ──────────────────────────────────────────────────────────

// FlowWithContext transforms elements while threading an opaque context value
// through the pipeline unchanged.  Each operator (Map, Filter, …) receives
// only the element; the context is never exposed to the operator function and
// is never lost.
//
// FlowWithContext mirrors Pekko's pekko.stream.scaladsl.FlowWithContext.
//
// Usage:
//
//	fwc := stream.NewFlowWithContext[string, int]().
//	    Map(strings.ToUpper).
//	    Filter(func(s string) bool { return s != "" })
//
//	// Convert to a plain Flow and compose with standard Via:
//	flow := fwc.ToFlow()
type FlowWithContext[T, Ctx any] struct {
	inner Flow[Pair[T, Ctx], Pair[T, Ctx], NotUsed]
}

// NewFlowWithContext returns an identity FlowWithContext that passes elements
// and their contexts through unchanged.
func NewFlowWithContext[T, Ctx any]() FlowWithContext[T, Ctx] {
	return FlowWithContext[T, Ctx]{
		inner: Flow[Pair[T, Ctx], Pair[T, Ctx], NotUsed]{
			attach: func(up iterator[Pair[T, Ctx]]) (iterator[Pair[T, Ctx]], NotUsed) {
				return up, NotUsed{}
			},
		},
	}
}

// Map applies fn to each element, preserving the context.
func (f FlowWithContext[T, Ctx]) Map(fn func(T) T) FlowWithContext[T, Ctx] {
	return FlowWithContext[T, Ctx]{
		inner: Flow[Pair[T, Ctx], Pair[T, Ctx], NotUsed]{
			attach: func(up iterator[Pair[T, Ctx]]) (iterator[Pair[T, Ctx]], NotUsed) {
				prev, _ := f.inner.attach(up)
				return &contextMapIterator[T, Ctx]{upstream: prev, fn: fn}, NotUsed{}
			},
		},
	}
}

// Filter keeps only elements for which pred returns true.
func (f FlowWithContext[T, Ctx]) Filter(pred func(T) bool) FlowWithContext[T, Ctx] {
	return FlowWithContext[T, Ctx]{
		inner: Flow[Pair[T, Ctx], Pair[T, Ctx], NotUsed]{
			attach: func(up iterator[Pair[T, Ctx]]) (iterator[Pair[T, Ctx]], NotUsed) {
				prev, _ := f.inner.attach(up)
				return &contextFilterIterator[T, Ctx]{upstream: prev, pred: pred}, NotUsed{}
			},
		},
	}
}

// ToFlow converts this FlowWithContext into a plain
// Flow[Pair[T,Ctx], Pair[T,Ctx], NotUsed] that can be used with standard Via
// and stream graph DSL.
func (f FlowWithContext[T, Ctx]) ToFlow() Flow[Pair[T, Ctx], Pair[T, Ctx], NotUsed] {
	return f.inner
}

// ─── Via with FlowWithContext ─────────────────────────────────────────────────

// ViaWithContext applies a FlowWithContext to a SourceWithContext, producing a
// new SourceWithContext.
func ViaWithContext[T, Ctx, Mat any](
	src SourceWithContext[T, Ctx, Mat],
	flow FlowWithContext[T, Ctx],
) SourceWithContext[T, Ctx, Mat] {
	return SourceWithContext[T, Ctx, Mat]{
		inner: Via(src.inner, flow.inner),
	}
}

// ─── internal iterators ────────────────────────────────────────────────────────

// contextSourceIterator pairs each upstream element with a derived context.
type contextSourceIterator[T, Ctx any] struct {
	upstream iterator[T]
	ctxFor   func(i int, elem T) Ctx
	index    int
}

func (c *contextSourceIterator[T, Ctx]) next() (Pair[T, Ctx], bool, error) {
	elem, ok, err := c.upstream.next()
	if !ok || err != nil {
		var zero Pair[T, Ctx]
		return zero, ok, err
	}
	ctx := c.ctxFor(c.index, elem)
	c.index++
	return Pair[T, Ctx]{First: elem, Second: ctx}, true, nil
}

// contextMapIterator applies fn to the element, leaving the context untouched.
type contextMapIterator[T, Ctx any] struct {
	upstream iterator[Pair[T, Ctx]]
	fn       func(T) T
}

func (c *contextMapIterator[T, Ctx]) next() (Pair[T, Ctx], bool, error) {
	p, ok, err := c.upstream.next()
	if !ok || err != nil {
		return p, ok, err
	}
	return Pair[T, Ctx]{First: c.fn(p.First), Second: p.Second}, true, nil
}

// contextFilterIterator drops elements for which pred returns false.
type contextFilterIterator[T, Ctx any] struct {
	upstream iterator[Pair[T, Ctx]]
	pred     func(T) bool
}

func (c *contextFilterIterator[T, Ctx]) next() (Pair[T, Ctx], bool, error) {
	for {
		p, ok, err := c.upstream.next()
		if !ok || err != nil {
			return p, ok, err
		}
		if c.pred(p.First) {
			return p, true, nil
		}
		// Drop this pair and pull the next.
	}
}
