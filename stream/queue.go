/*
 * queue.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"errors"
	"sync"
)

// ErrQueueClosed is returned by [SourceQueue.Offer] after [SourceQueue.Complete]
// or [SourceQueue.Fail] has been called.
var ErrQueueClosed = errors.New("source queue closed")

// SourceQueue is the materialized value of [Queue].  It allows elements to be
// pushed into the stream imperatively from outside the pipeline.
type SourceQueue[T any] interface {
	// Offer pushes elem into the queue.  When the buffer is full the overflow
	// strategy configured at construction time determines behaviour.
	Offer(elem T) error

	// Complete signals that no more elements will be offered.  The downstream
	// source completes normally after draining buffered elements.
	Complete()

	// Fail signals that the queue has failed with err.  The downstream source
	// will surface this error after draining buffered elements.
	Fail(err error)
}

// sourceQueue is the concrete implementation of [SourceQueue].
type sourceQueue[T any] struct {
	mu       sync.Mutex
	ch       chan T
	closed   bool
	failErr  error
	strategy OverflowStrategy
}

func (q *sourceQueue[T]) Offer(elem T) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return ErrQueueClosed
	}
	strategy := q.strategy
	q.mu.Unlock()

	switch strategy {
	case OverflowBackpressure:
		q.ch <- elem
		return nil
	case OverflowDropTail:
		select {
		case q.ch <- elem:
		default:
			// drop incoming element
		}
		return nil
	case OverflowDropHead:
		select {
		case q.ch <- elem:
		default:
			// evict oldest, then push new
			select {
			case <-q.ch:
			default:
			}
			select {
			case q.ch <- elem:
			default:
			}
		}
		return nil
	case OverflowFail:
		select {
		case q.ch <- elem:
			return nil
		default:
			return ErrBufferFull
		}
	default:
		q.ch <- elem
		return nil
	}
}

func (q *sourceQueue[T]) Complete() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		close(q.ch)
	}
}

func (q *sourceQueue[T]) Fail(err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		q.failErr = err
		close(q.ch)
	}
}

// Queue creates a [Source] that is fed imperatively via the materialized
// [SourceQueue].  bufferSize controls the internal channel capacity; strategy
// determines overflow behaviour when the buffer is full.
//
// The returned Source materializes to the SourceQueue, allowing the caller to
// push elements after the graph has been started:
//
//	src := stream.Queue[int](16, stream.OverflowDropTail)
//	q, _ := stream.RunWith(src, stream.Collect[int](), stream.SyncMaterializer{})
func Queue[T any](bufferSize int, strategy OverflowStrategy) Source[T, SourceQueue[T]] {
	return Source[T, SourceQueue[T]]{
		factory: func() (iterator[T], SourceQueue[T]) {
			ch := make(chan T, bufferSize)
			q := &sourceQueue[T]{
				ch:       ch,
				strategy: strategy,
			}
			iter := &queueIterator[T]{ch: ch, q: q}
			return iter, q
		},
	}
}

// queueIterator drains the channel backing a [sourceQueue].
type queueIterator[T any] struct {
	ch chan T
	q  *sourceQueue[T]
}

func (qi *queueIterator[T]) next() (T, bool, error) {
	elem, ok := <-qi.ch
	if !ok {
		var zero T
		qi.q.mu.Lock()
		err := qi.q.failErr
		qi.q.mu.Unlock()
		return zero, false, err
	}
	return elem, true, nil
}
