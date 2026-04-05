/*
 * hub.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"sync"
)

// ─── MergeHub ─────────────────────────────────────────────────────────────────

// mergeHub is the shared state of a MergeHub.
type mergeHub[T any] struct {
	ch   chan T
	done chan struct{}
}

// NewMergeHub creates a dynamic merge hub.  Multiple producers can connect
// their Sinks to the returned MergeHubSink; all elements flow through the
// shared buffer channel and are consumed by the single returned source.
//
// bufferSize controls the depth of the internal channel.  When full, producers
// block until the consumer drains elements.
//
// Mirroring Pekko's pekko.stream.scaladsl.MergeHub.
//
// Usage:
//
//	sinkRef, source := stream.NewMergeHub[string](64)
//	// Start the consumer.
//	go source.To(sink).Run(mat)
//	// Connect multiple producers.
//	producer1.To(sinkRef).Run(mat)
//	producer2.To(sinkRef).Run(mat)
func NewMergeHub[T any](bufferSize int) (Sink[T, NotUsed], Source[T, NotUsed]) {
	if bufferSize <= 0 {
		bufferSize = DefaultAsyncBufSize
	}
	hub := &mergeHub[T]{
		ch:   make(chan T, bufferSize),
		done: make(chan struct{}),
	}

	sink := Sink[T, NotUsed]{
		runWith: func(upstream iterator[T]) (NotUsed, error) {
			for {
				elem, ok, err := upstream.next()
				if err != nil || !ok {
					return NotUsed{}, err
				}
				select {
				case hub.ch <- elem:
				case <-hub.done:
					return NotUsed{}, nil
				}
			}
		},
	}

	source := Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			return &channelReadIterator[T]{ch: hub.ch, done: hub.done}, NotUsed{}
		},
	}

	return sink, source
}

// ─── BroadcastHub ─────────────────────────────────────────────────────────────

// broadcastHub is the shared state of a BroadcastHub.
type broadcastHub[T any] struct {
	mu          sync.Mutex
	subscribers []chan T
	done        bool
	// startCh is closed when the first subscriber registers, unblocking the
	// broadcast goroutine. This prevents the goroutine from consuming the
	// upstream before any consumer is ready.
	startCh chan struct{}
	startOnce sync.Once
}

func (h *broadcastHub[T]) subscribe(bufSize int) <-chan T {
	ch := make(chan T, bufSize)
	h.mu.Lock()
	if h.done {
		// Upstream already exhausted: deliver an immediately-closed channel.
		close(ch)
	} else {
		h.subscribers = append(h.subscribers, ch)
	}
	h.mu.Unlock()
	// Signal the broadcast goroutine to start consuming upstream.
	h.startOnce.Do(func() { close(h.startCh) })
	return ch
}

func (h *broadcastHub[T]) broadcast(elem T) {
	h.mu.Lock()
	subs := make([]chan T, len(h.subscribers))
	copy(subs, h.subscribers)
	h.mu.Unlock()
	for _, sub := range subs {
		select {
		case sub <- elem:
		default:
			// Slow consumer — drop to avoid blocking the broadcast goroutine.
		}
	}
}

func (h *broadcastHub[T]) closeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.done = true
	for _, sub := range h.subscribers {
		close(sub)
	}
	h.subscribers = nil
}

// NewBroadcastHub creates a hub that fans a single upstream source out to
// multiple downstream consumers.  Each consumer receives elements from the
// moment they subscribe onwards; elements published before subscription are
// not replayed.  Elements are dropped for consumers whose buffer is full.
//
// bufferSize controls the per-consumer channel depth.
//
// The returned Source can be materialised multiple times — each materialisation
// creates a new consumer subscribed from that point in time.
//
// Mirroring Pekko's pekko.stream.scaladsl.BroadcastHub.
func NewBroadcastHub[T any](src Source[T, NotUsed], bufferSize int) Source[T, NotUsed] {
	if bufferSize <= 0 {
		bufferSize = DefaultAsyncBufSize
	}

	hub := &broadcastHub[T]{startCh: make(chan struct{})}

	// Start the broadcast goroutine.  It waits for the first subscriber before
	// consuming from upstream, so that subscribers registered "before the
	// source starts" always receive the full stream.
	go func() {
		<-hub.startCh // wait for at least one subscriber
		up, _ := src.factory()
		for {
			elem, ok, err := up.next()
			if !ok || err != nil {
				hub.closeAll()
				return
			}
			hub.broadcast(elem)
		}
	}()

	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			ch := hub.subscribe(bufferSize)
			return &channelCloseIterator[T]{ch: ch}, NotUsed{}
		},
	}
}

// ─── PartitionHub ─────────────────────────────────────────────────────────────

// NewPartitionHub creates a hub that routes each element from a single
// upstream source to exactly one of nOutputs downstream consumers, chosen by
// calling partitioner(nOutputs, element).
//
//   - partitioner: returns an index in [0, nOutputs).  Must be deterministic.
//   - nOutputs: the number of output partitions.
//   - bufferSize: per-partition buffer depth.
//
// The returned slice contains nOutputs Sources; index i receives all elements
// for which partitioner returns i.
//
// Mirroring Pekko's pekko.stream.scaladsl.PartitionHub.
func NewPartitionHub[T any](
	src Source[T, NotUsed],
	partitioner func(nOutputs int, elem T) int,
	nOutputs int,
	bufferSize int,
) []Source[T, NotUsed] {
	if bufferSize <= 0 {
		bufferSize = DefaultAsyncBufSize
	}

	channels := make([]chan T, nOutputs)
	for i := range channels {
		channels[i] = make(chan T, bufferSize)
	}

	// Start the router goroutine immediately.
	go func() {
		up, _ := src.factory()
		for {
			elem, ok, err := up.next()
			if !ok || err != nil {
				for _, ch := range channels {
					close(ch)
				}
				return
			}
			idx := partitioner(nOutputs, elem)
			if idx < 0 {
				idx = 0
			}
			if idx >= nOutputs {
				idx = nOutputs - 1
			}
			channels[idx] <- elem
		}
	}()

	sources := make([]Source[T, NotUsed], nOutputs)
	for i, ch := range channels {
		ch := ch // capture
		sources[i] = Source[T, NotUsed]{
			factory: func() (iterator[T], NotUsed) {
				return &channelCloseIterator[T]{ch: ch}, NotUsed{}
			},
		}
	}
	return sources
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// channelReadIterator reads from a channel until it is closed or the done
// signal fires.
type channelReadIterator[T any] struct {
	ch   <-chan T
	done <-chan struct{}
}

func (c *channelReadIterator[T]) next() (T, bool, error) {
	select {
	case v, ok := <-c.ch:
		if !ok {
			var zero T
			return zero, false, nil
		}
		return v, true, nil
	case <-c.done:
		var zero T
		return zero, false, nil
	}
}

// channelCloseIterator reads from a channel that is closed to signal completion.
type channelCloseIterator[T any] struct {
	ch <-chan T
}

func (c *channelCloseIterator[T]) next() (T, bool, error) {
	v, ok := <-c.ch
	if !ok {
		var zero T
		return zero, false, nil
	}
	return v, true, nil
}
