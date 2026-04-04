/*
 * junctions.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import "sync"

// ─── BroadcastStage ───────────────────────────────────────────────────────

// BroadcastStage implements a Graph[FanOutShape[T], NotUsed] that emits
// every incoming element to all n Outlets.
type BroadcastStage[T any] struct {
	n int
}

// NewBroadcast creates a new BroadcastStage for routing one input to n outputs.
func NewBroadcast[T any](n int) *BroadcastStage[T] {
	return &BroadcastStage[T]{n: n}
}

func (b *BroadcastStage[T]) Shape() FanOutShape[T] {
	outlets := make([]*Outlet[T], b.n)
	for i := range outlets {
		outlets[i] = &Outlet[T]{}
	}
	return FanOutShape[T]{In: &Inlet[T]{}, Outlets_: outlets}
}

func (b *BroadcastStage[T]) materialize(m Materializer, shape Shape) materializedStage {
	s := shape.(FanOutShape[T])

	lazyOuts := make([]*lazyIterator[T], b.n)
	outIters := make(map[int]any)
	for i, out := range s.Outlets_ {
		lazyOuts[i] = &lazyIterator[T]{}
		outIters[out.id] = lazyOuts[i]
	}

	lazyIn := &lazyIterator[T]{}

	channels := make([]chan T, b.n)
	for i := range channels {
		channels[i] = make(chan T, DefaultAsyncBufSize)
	}
	sharedErr := newSharedError()

	runner := func() error {
		defer func() {
			for _, ch := range channels {
				close(ch)
			}
		}()
		for {
			elem, ok, err := lazyIn.next()
			if err != nil {
				sharedErr.fail(err)
				return err
			}
			if !ok {
				return nil
			}
			for _, ch := range channels {
				// Block if downstream is slow (back-pressure)
				select {
				case ch <- elem:
				case <-sharedErr.sig:
					return nil
				}
			}
		}
	}

	inConns := map[int]func(any){
		s.In.id: func(up any) {
			lazyIn.inner = up.(iterator[T])
			for i := range channels {
				lazyOuts[i].inner = &channelIterator[T]{ch: channels[i], err: sharedErr}
			}
		},
	}

	return materializedStage{
		outIters: outIters,
		inConns:  inConns,
		runners:  []func() error{runner},
	}
}

// ─── MergeStage ───────────────────────────────────────────────────────────

// MergeStage implements a Graph[FanInShape[T], NotUsed] that merges n Inlets
// concurrently into a single Outlet.
type MergeStage[T any] struct {
	n int
}

// NewMerge creates a new MergeStage for combining n inputs into one output.
func NewMerge[T any](n int) *MergeStage[T] {
	return &MergeStage[T]{n: n}
}

func (m *MergeStage[T]) Shape() FanInShape[T] {
	inlets := make([]*Inlet[T], m.n)
	for i := range inlets {
		inlets[i] = &Inlet[T]{}
	}
	return FanInShape[T]{Inlets_: inlets, Out: &Outlet[T]{}}
}

func (m *MergeStage[T]) materialize(_ Materializer, shape Shape) materializedStage {
	s := shape.(FanInShape[T])

	lazyIns := make([]*lazyIterator[T], m.n)
	inConns := make(map[int]func(any))
	for i, in := range s.Inlets_ {
		lazyIns[i] = &lazyIterator[T]{}
		idx := i
		inConns[in.id] = func(up any) {
			lazyIns[idx].inner = up.(iterator[T])
		}
	}

	lazyOut := &lazyIterator[T]{}
	outIters := map[int]any{s.Out.id: lazyOut}

	ch := make(chan T, m.n*DefaultAsyncBufSize)
	sharedErr := newSharedError()
	runners := make([]func() error, m.n)

	lazyOut.inner = &channelIterator[T]{ch: ch, err: sharedErr}

	var wg sync.WaitGroup
	wg.Add(m.n)

	// Close the output channel once all producers terminate.
	go func() {
		wg.Wait()
		close(ch)
	}()

	for i := 0; i < m.n; i++ {
		idx := i
		runners[idx] = func() error {
			defer wg.Done()
			it := lazyIns[idx]
			for {
				select {
				case <-sharedErr.sig:
					return nil
				default:
				}
				elem, ok, err := it.next() // Blocks pulling
				if err != nil {
					sharedErr.fail(err)
					return err
				}
				if !ok {
					return nil
				}
				select {
				case <-sharedErr.sig:
					return nil
				case ch <- elem:
				}
			}
		}
	}

	return materializedStage{
		outIters: outIters,
		inConns:  inConns,
		runners:  runners,
	}
}

// ─── ZipStage ─────────────────────────────────────────────────────────────

// ZipStage implements a Graph[FanIn2Shape[A, B, Pair[A, B]], NotUsed] that
// emits a Pair[A, B] once elements are available on both Inlet[A] and Inlet[B].
type ZipStage[A, B any] struct{}

// NewZip creates a new ZipStage connecting two different input types into a Pair.
func NewZip[A, B any]() *ZipStage[A, B] {
	return &ZipStage[A, B]{}
}

func (z *ZipStage[A, B]) Shape() FanIn2Shape[A, B, Pair[A, B]] {
	return FanIn2Shape[A, B, Pair[A, B]]{
		In0: &Inlet[A]{},
		In1: &Inlet[B]{},
		Out: &Outlet[Pair[A, B]]{},
	}
}

func (z *ZipStage[A, B]) materialize(_ Materializer, shape Shape) materializedStage {
	s := shape.(FanIn2Shape[A, B, Pair[A, B]])

	lazyIn0 := &lazyIterator[A]{}
	lazyIn1 := &lazyIterator[B]{}
	lazyOut := &lazyIterator[Pair[A, B]]{}

	inConns := map[int]func(any){
		s.In0.id: func(up any) { lazyIn0.inner = up.(iterator[A]) },
		s.In1.id: func(up any) { lazyIn1.inner = up.(iterator[B]) },
	}

	lazyOut.inner = &zipIterator[A, B, Pair[A, B]]{
		left:  lazyIn0,
		right: lazyIn1,
		combine: func(a A, b B) Pair[A, B] {
			return Pair[A, B]{First: a, Second: b}
		},
	}

	outIters := map[int]any{s.Out.id: lazyOut}

	return materializedStage{
		outIters: outIters,
		inConns:  inConns,
		runners:  nil, // pure pullback mechanics
	}
}

// ─── MergePrioritizedStage ────────────────────────────────────────────────

// MergePrioritizedStage merges n input streams using a weighted round-robin
// policy: an upstream with priority p[i] is preferred p[i] times for every
// p[j] times upstream j is preferred.
type MergePrioritizedStage[T any] struct {
	priorities []int
}

// NewMergePrioritized creates a merge junction that emits elements from
// multiple upstreams using weighted round-robin based on priorities.
//
// priorities[i] is the relative weight of Inlets_[i].  An upstream with
// priority 3 receives 3× as many pulls as an upstream with priority 1.
// When a preferred upstream is exhausted, elements from the remaining
// active upstreams are still forwarded.
func NewMergePrioritized[T any](priorities []int) *MergePrioritizedStage[T] {
	return &MergePrioritizedStage[T]{priorities: priorities}
}

func (m *MergePrioritizedStage[T]) Shape() FanInShape[T] {
	inlets := make([]*Inlet[T], len(m.priorities))
	for i := range inlets {
		inlets[i] = &Inlet[T]{}
	}
	return FanInShape[T]{Inlets_: inlets, Out: &Outlet[T]{}}
}

// buildPrioritySeq expands a priority slice into a flat weighted index
// sequence.  For example, [3, 1] → [0, 0, 0, 1].
func buildPrioritySeq(priorities []int) []int {
	var seq []int
	for i, p := range priorities {
		for j := 0; j < p; j++ {
			seq = append(seq, i)
		}
	}
	return seq
}

func (m *MergePrioritizedStage[T]) materialize(_ Materializer, shape Shape) materializedStage {
	s := shape.(FanInShape[T])
	n := len(m.priorities)

	lazyIns := make([]*lazyIterator[T], n)
	inConns := make(map[int]func(any))
	for i, in := range s.Inlets_ {
		lazyIns[i] = &lazyIterator[T]{}
		idx := i
		inConns[in.id] = func(up any) {
			lazyIns[idx].inner = up.(iterator[T])
		}
	}

	lazyOut := &lazyIterator[T]{}
	outIters := map[int]any{s.Out.id: lazyOut}

	outCh := make(chan T, n*DefaultAsyncBufSize)
	sharedErr := newSharedError()
	lazyOut.inner = &channelIterator[T]{ch: outCh, err: sharedErr}

	seq := buildPrioritySeq(m.priorities)
	seqLen := len(seq)

	runner := func() error {
		defer close(outCh)
		done := make([]bool, n)
		activeCnt := n
		seqIdx := 0

		for activeCnt > 0 {
			upIdx := seq[seqIdx]
			seqIdx = (seqIdx + 1) % seqLen

			if done[upIdx] {
				continue
			}

			elem, ok, err := lazyIns[upIdx].next()
			if err != nil {
				sharedErr.fail(err)
				return err
			}
			if !ok {
				done[upIdx] = true
				activeCnt--
				continue
			}

			select {
			case <-sharedErr.sig:
				return nil
			case outCh <- elem:
			}
		}
		return nil
	}

	return materializedStage{
		outIters: outIters,
		inConns:  inConns,
		runners:  []func() error{runner},
	}
}

// ─── UnzipWith2Stage ──────────────────────────────────────────────────────

// UnzipWith2Stage splits a single input stream into two typed output streams
// by applying an extractor function to each element.
type UnzipWith2Stage[In, A, B any] struct {
	f func(In) (A, B)
}

// NewUnzipWith2 creates an UnzipWith2Stage that applies f to each incoming
// element and routes the two results to Out0 (type A) and Out1 (type B).
func NewUnzipWith2[In, A, B any](f func(In) (A, B)) *UnzipWith2Stage[In, A, B] {
	return &UnzipWith2Stage[In, A, B]{f: f}
}

func (u *UnzipWith2Stage[In, A, B]) Shape() FanOut2Shape[In, A, B] {
	return FanOut2Shape[In, A, B]{
		In:   &Inlet[In]{},
		Out0: &Outlet[A]{},
		Out1: &Outlet[B]{},
	}
}

func (u *UnzipWith2Stage[In, A, B]) materialize(_ Materializer, shape Shape) materializedStage {
	s := shape.(FanOut2Shape[In, A, B])

	lazyIn := &lazyIterator[In]{}
	ch0 := make(chan A, DefaultAsyncBufSize)
	ch1 := make(chan B, DefaultAsyncBufSize)
	sharedErr := newSharedError()

	lazyOut0 := &lazyIterator[A]{}
	lazyOut1 := &lazyIterator[B]{}
	lazyOut0.inner = &channelIterator[A]{ch: ch0, err: sharedErr}
	lazyOut1.inner = &channelIterator[B]{ch: ch1, err: sharedErr}

	inConns := map[int]func(any){
		s.In.id: func(up any) {
			lazyIn.inner = up.(iterator[In])
		},
	}

	outIters := map[int]any{
		s.Out0.id: lazyOut0,
		s.Out1.id: lazyOut1,
	}

	runner := func() error {
		defer func() {
			close(ch0)
			close(ch1)
		}()
		for {
			elem, ok, err := lazyIn.next()
			if err != nil {
				sharedErr.fail(err)
				return err
			}
			if !ok {
				return nil
			}
			a, b := u.f(elem)
			select {
			case <-sharedErr.sig:
				return nil
			case ch0 <- a:
			}
			select {
			case <-sharedErr.sig:
				return nil
			case ch1 <- b:
			}
		}
	}

	return materializedStage{
		outIters: outIters,
		inConns:  inConns,
		runners:  []func() error{runner},
	}
}
