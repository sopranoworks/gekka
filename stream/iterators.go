/*
 * iterators.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"log"
	"sync"
	"time"
)

// ─── funcIterator ─────────────────────────────────────────────────────────

// funcIterator wraps a closure as an [iterator].
type funcIterator[T any] struct {
	pullFn func() (T, bool, error)
}

func (f *funcIterator[T]) next() (T, bool, error) { return f.pullFn() }

// sliceIterator emits elements from a slice, one per pull.
type sliceIterator[T any] struct {
	elems []T
	pos   int
}

// ─── repeatIterator ───────────────────────────────────────────────────────

// repeatIterator emits elem on every pull, never completing.
type repeatIterator[T any] struct{ elem T }

func (r *repeatIterator[T]) next() (T, bool, error) { return r.elem, true, nil }

func (s *sliceIterator[T]) next() (T, bool, error) {
	if s.pos >= len(s.elems) {
		var zero T
		return zero, false, nil
	}
	v := s.elems[s.pos]
	s.pos++
	return v, true, nil
}

// ─── mapIterator ──────────────────────────────────────────────────────────

type mapIterator[In, Out any] struct {
	upstream iterator[In]
	fn       func(In) Out
}

func (m *mapIterator[In, Out]) next() (Out, bool, error) {
	in, ok, err := m.upstream.next()
	if !ok || err != nil {
		var zero Out
		return zero, ok, err
	}
	return m.fn(in), true, nil
}

// ─── filterIterator ───────────────────────────────────────────────────────

type filterIterator[T any] struct {
	upstream iterator[T]
	pred     func(T) bool
}

func (f *filterIterator[T]) next() (T, bool, error) {
	for {
		elem, ok, err := f.upstream.next()
		if !ok || err != nil {
			return elem, ok, err
		}
		if f.pred(elem) {
			return elem, true, nil
		}
	}
}

// ─── takeIterator ─────────────────────────────────────────────────────────

type takeIterator[T any] struct {
	upstream iterator[T]
	n        int
	emitted  int
}

func (t *takeIterator[T]) next() (T, bool, error) {
	if t.emitted >= t.n {
		var zero T
		return zero, false, nil
	}
	elem, ok, err := t.upstream.next()
	if ok {
		t.emitted++
	}
	return elem, ok, err
}

// ─── mapEIterator ─────────────────────────────────────────────────────────

// mapEIterator applies an error-returning function to each upstream element.
// If fn returns an error the iterator propagates it so that an enclosing
// [supervisedIterator] can apply the configured [Decider].
type mapEIterator[In, Out any] struct {
	upstream iterator[In]
	fn       func(In) (Out, error)
}

func (m *mapEIterator[In, Out]) next() (Out, bool, error) {
	in, ok, err := m.upstream.next()
	if !ok || err != nil {
		var zero Out
		return zero, ok, err
	}
	out, err := m.fn(in)
	if err != nil {
		var zero Out
		return zero, false, err
	}
	return out, true, nil
}

// ─── bufferIterator ───────────────────────────────────────────────────────

// bufferIterator runs the upstream in a dedicated goroutine and stores
// elements in a bounded channel.  The overflow strategy determines what
// happens when the channel is full.
type bufferIterator[T any] struct {
	ch     chan T
	errCh  chan error
	stopCh chan struct{}
}

func newBufferIterator[T any](upstream iterator[T], size int, strategy OverflowStrategy) *bufferIterator[T] {
	b := &bufferIterator[T]{
		ch:     make(chan T, size),
		errCh:  make(chan error, 1),
		stopCh: make(chan struct{}),
	}
	go func() {
		defer close(b.ch)
		for {
			elem, ok, err := upstream.next()
			if err != nil {
				select {
				case b.errCh <- err:
				default:
				}
				return
			}
			if !ok {
				return
			}
			switch strategy {
			case OverflowBackpressure:
				// Block until the consumer makes room or the stream is stopped.
				select {
				case b.ch <- elem:
				case <-b.stopCh:
					return
				}
			case OverflowDropTail:
				// Non-blocking send: drop the incoming element if full.
				select {
				case b.ch <- elem:
				default:
				}
			case OverflowDropHead:
				// Non-blocking send; on overflow drain the oldest element first.
				// Single-producer invariant: no mutex needed.
				select {
				case b.ch <- elem:
				default:
					select {
					case <-b.ch: // discard oldest
					default:
					}
					b.ch <- elem // guaranteed safe: we just freed a slot
				}
			case OverflowFail:
				select {
				case b.ch <- elem:
				default:
					select {
					case b.errCh <- ErrBufferFull:
					default:
					}
					return
				}
			}
		}
	}()
	return b
}

func (b *bufferIterator[T]) next() (T, bool, error) {
	// Fast path: surface a pending error without blocking.
	select {
	case err := <-b.errCh:
		var zero T
		return zero, false, err
	default:
	}
	select {
	case err := <-b.errCh:
		var zero T
		return zero, false, err
	case v, ok := <-b.ch:
		if !ok {
			// Channel closed — check for a final error.
			select {
			case err := <-b.errCh:
				var zero T
				return zero, false, err
			default:
				var zero T
				return zero, false, nil
			}
		}
		return v, true, nil
	}
}

// ─── throttleIterator ─────────────────────────────────────────────────────

// throttleIterator limits throughput to at most `elements` per `per` duration
// using a synchronous token-bucket algorithm.  Up to `burst` tokens may
// accumulate, allowing short bursts above the steady-state rate.
type throttleIterator[T any] struct {
	upstream  iterator[T]
	interval  time.Duration // per / elements
	last      time.Time     // wall-clock time of the last token bucket update
	burst     int           // maximum token accumulation
	available int           // tokens currently available
}

func newThrottleIterator[T any](upstream iterator[T], elements int, per time.Duration, burst int) *throttleIterator[T] {
	if burst <= 0 {
		burst = elements
	}
	return &throttleIterator[T]{
		upstream:  upstream,
		interval:  per / time.Duration(elements),
		last:      time.Now(),
		burst:     burst,
		available: burst, // start with a full bucket
	}
}

func (t *throttleIterator[T]) next() (T, bool, error) {
	// Pull from upstream first: if the source is exhausted we must not consume
	// a token (which would cause an unnecessary sleep on the terminal pull).
	elem, ok, err := t.upstream.next()
	if !ok || err != nil {
		return elem, ok, err
	}

	// We have a real element — replenish tokens and wait if the bucket is empty.
	elapsed := time.Since(t.last)
	if newTokens := int(elapsed / t.interval); newTokens > 0 {
		t.available = min(t.available+newTokens, t.burst)
		t.last = t.last.Add(time.Duration(newTokens) * t.interval)
	}

	if t.available > 0 {
		t.available--
	} else {
		sleepUntil := t.last.Add(t.interval)
		if d := time.Until(sleepUntil); d > 0 {
			time.Sleep(d)
		}
		t.last = sleepUntil
	}
	return elem, true, nil
}

// ─── delayIterator ────────────────────────────────────────────────────────

// delayIterator introduces a fixed pause after each element before returning
// it to the downstream stage, creating an inter-element delay.
type delayIterator[T any] struct {
	upstream iterator[T]
	delay    time.Duration
}

func (d *delayIterator[T]) next() (T, bool, error) {
	elem, ok, err := d.upstream.next()
	if ok && err == nil {
		time.Sleep(d.delay)
	}
	return elem, ok, err
}

// ─── errorIterator ────────────────────────────────────────────────────────

// errorIterator immediately fails with an error on the first pull.
type errorIterator[T any] struct {
	err error
}

func (e *errorIterator[T]) next() (T, bool, error) {
	var zero T
	return zero, false, e.err
}

// ─── groupedIterator ──────────────────────────────────────────────────────

// groupedIterator batches upstream elements into slices of exactly n elements.
// The last batch may be smaller if the upstream is exhausted before n elements
// are accumulated.  An empty upstream produces no batches.
type groupedIterator[T any] struct {
	upstream iterator[T]
	n        int
	done     bool
}

func (g *groupedIterator[T]) next() ([]T, bool, error) {
	if g.done {
		return nil, false, nil
	}
	batch := make([]T, 0, g.n)
	for len(batch) < g.n {
		elem, ok, err := g.upstream.next()
		if err != nil {
			return nil, false, err
		}
		if !ok {
			g.done = true
			break
		}
		batch = append(batch, elem)
	}
	if len(batch) == 0 {
		return nil, false, nil
	}
	return batch, true, nil
}

// ─── groupedWithinIterator ────────────────────────────────────────────────

// groupedWithinIterator emits a batch whenever the batch size reaches n OR the
// wall-clock timeout d expires, whichever comes first.  An empty batch at
// timer expiry is suppressed — no zero-length slice is ever emitted.
//
// Two goroutines are used internally:
//   - reader: pulls elements from the (blocking) upstream iterator and
//     forwards them on elemCh.
//   - batcher: selects on elemCh and the timer; builds and emits batches.
type groupedWithinIterator[T any] struct {
	ch    <-chan []T
	errCh <-chan error
}

func newGroupedWithinIterator[T any](upstream iterator[T], n int, d time.Duration) *groupedWithinIterator[T] {
	outCh := make(chan []T, 8)
	errCh := make(chan error, 1)

	go func() {
		defer close(outCh)

		// elemCh bridges the blocking upstream.next() to the batcher's select.
		elemCh := make(chan T, n)

		// Reader goroutine: pulls elements and puts them on elemCh.
		go func() {
			defer close(elemCh)
			for {
				elem, ok, err := upstream.next()
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				if !ok {
					return
				}
				elemCh <- elem
			}
		}()

		// Batcher: collect elements until count or time window is reached.
		timer := time.NewTimer(d)
		defer timer.Stop()
		batch := make([]T, 0, n)

		flush := func() {
			if len(batch) == 0 {
				return
			}
			cp := make([]T, len(batch))
			copy(cp, batch)
			outCh <- cp
			batch = batch[:0]
		}

		resetTimer := func() {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(d)
		}

		for {
			select {
			case elem, ok := <-elemCh:
				if !ok {
					// Reader exited (upstream done or error).
					flush()
					return
				}
				batch = append(batch, elem)
				if len(batch) >= n {
					flush()
					resetTimer()
				}
			case <-timer.C:
				flush() // no-op if batch is empty
				timer.Reset(d)
			}
		}
	}()

	return &groupedWithinIterator[T]{ch: outCh, errCh: errCh}
}

func (g *groupedWithinIterator[T]) next() ([]T, bool, error) {
	// Fast path: surface a pending error before blocking.
	select {
	case err := <-g.errCh:
		return nil, false, err
	default:
	}
	select {
	case err := <-g.errCh:
		return nil, false, err
	case batch, ok := <-g.ch:
		if !ok {
			// Channel closed — check for a final error.
			select {
			case err := <-g.errCh:
				return nil, false, err
			default:
				return nil, false, nil
			}
		}
		return batch, true, nil
	}
}

// ─── concatIterator ───────────────────────────────────────────────────────

// concatIterator drains left completely before starting right.
type concatIterator[T any] struct {
	left    iterator[T]
	right   iterator[T]
	onRight bool
}

func (c *concatIterator[T]) next() (T, bool, error) {
	if !c.onRight {
		elem, ok, err := c.left.next()
		if err != nil {
			return elem, false, err
		}
		if ok {
			return elem, true, nil
		}
		// Left exhausted — switch to right.
		c.onRight = true
	}
	return c.right.next()
}

// ─── zipIterator ──────────────────────────────────────────────────────────

// zipIterator pairs elements from two upstream iterators element-by-element.
// It completes when either upstream is exhausted and propagates errors from
// either side immediately.
type zipIterator[T1, T2, Out any] struct {
	left    iterator[T1]
	right   iterator[T2]
	combine func(T1, T2) Out
}

func (z *zipIterator[T1, T2, Out]) next() (Out, bool, error) {
	l, ok, err := z.left.next()
	if !ok || err != nil {
		var zero Out
		return zero, ok, err
	}
	r, ok, err := z.right.next()
	if !ok || err != nil {
		var zero Out
		return zero, ok, err
	}
	return z.combine(l, r), true, nil
}

// ─── logIterator ──────────────────────────────────────────────────────────

// logIterator logs each element that passes through using the standard logger.
type logIterator[T any] struct {
	upstream iterator[T]
	name     string
}

func (l *logIterator[T]) next() (T, bool, error) {
	elem, ok, err := l.upstream.next()
	if ok && err == nil {
		log.Printf("[stream] %s: %v", l.name, elem)
	}
	return elem, ok, err
}

// ─── mapAsyncIterator ─────────────────────────────────────────────────────

// mapAsyncIterator runs fn concurrently on up to parallelism upstream elements
// and emits results in the original input order.
//
// On each pull it eagerly refills the in-flight queue up to capacity, then
// blocks on the oldest pending channel (head of queue).  If the head channel
// is closed without sending a value the element is silently dropped and the
// next pending result is awaited.
type mapAsyncIterator[In, Out any] struct {
	upstream    iterator[In]
	fn          func(In) <-chan Out
	parallelism int
	pending     []<-chan Out // ordered queue of in-flight result channels
	done        bool
}

func (m *mapAsyncIterator[In, Out]) next() (Out, bool, error) {
	for {
		// Eagerly fill the in-flight queue up to capacity.
		for !m.done && len(m.pending) < m.parallelism {
			elem, ok, err := m.upstream.next()
			if err != nil {
				var zero Out
				return zero, false, err
			}
			if !ok {
				m.done = true
				break
			}
			m.pending = append(m.pending, m.fn(elem))
		}

		if len(m.pending) == 0 {
			// All upstream elements consumed and all results delivered.
			var zero Out
			return zero, false, nil
		}

		// Wait for the oldest in-flight operation (preserves order).
		ch := m.pending[0]
		m.pending = m.pending[1:]

		val, ok := <-ch
		if ok {
			return val, true, nil
		}
		// Channel closed without a value — drop this element and try next.
	}
}

// ─── statefulMapConcatIterator ────────────────────────────────────────────

// statefulMapConcatIterator applies a state-bearing function to each upstream
// element.  The function may return zero or more output elements; they are
// buffered internally and emitted one by one before the next upstream element
// is pulled.
type statefulMapConcatIterator[In, S, Out any] struct {
	upstream iterator[In]
	state    S
	fn       func(S, In) (S, []Out)
	buf      []Out
}

func (s *statefulMapConcatIterator[In, S, Out]) next() (Out, bool, error) {
	for {
		// Drain any buffered outputs before pulling from upstream.
		if len(s.buf) > 0 {
			v := s.buf[0]
			s.buf = s.buf[1:]
			return v, true, nil
		}

		elem, ok, err := s.upstream.next()
		if !ok || err != nil {
			var zero Out
			return zero, ok, err
		}

		newState, outputs := s.fn(s.state, elem)
		s.state = newState
		s.buf = outputs
		// Loop: drain buf on the next iteration.
	}
}

// ─── filterMapIterator ────────────────────────────────────────────────────

// filterMapIterator applies a partial function to each upstream element.
// Elements for which the function returns false are silently dropped.
type filterMapIterator[In, Out any] struct {
	upstream iterator[In]
	pf       func(In) (Out, bool)
}

func (f *filterMapIterator[In, Out]) next() (Out, bool, error) {
	for {
		elem, ok, err := f.upstream.next()
		if !ok || err != nil {
			var zero Out
			return zero, ok, err
		}
		out, keep := f.pf(elem)
		if keep {
			return out, true, nil
		}
		// Drop and pull the next element.
	}
}

// ─── groupBy helpers ──────────────────────────────────────────────────────

// newGroupByIterator pre-collects all elements from upstream, groups them by
// key, and returns an iterator that emits one SubStream per distinct key in
// insertion order.  If upstream fails or maxSubstreams is exceeded the
// returned iterator immediately propagates the error.
func newGroupByIterator[T any](upstream iterator[T], maxSubstreams int, key func(T) string) iterator[SubStream[T]] {
	groups := make(map[string][]T)
	var order []string

	for {
		elem, ok, err := upstream.next()
		if err != nil {
			return &errorIterator[SubStream[T]]{err: err}
		}
		if !ok {
			break
		}
		k := key(elem)
		if _, exists := groups[k]; !exists {
			if len(groups) >= maxSubstreams {
				return &errorIterator[SubStream[T]]{err: ErrTooManySubstreams}
			}
			order = append(order, k)
		}
		groups[k] = append(groups[k], elem)
	}

	substreams := make([]SubStream[T], len(order))
	for i, k := range order {
		substreams[i] = SubStream[T]{Key: k, Source: FromSlice(groups[k])}
	}
	return &sliceIterator[SubStream[T]]{elems: substreams}
}

// newGroupByStreamingIterator creates a streaming GroupBy iterator that routes
// elements to per-key buffered channels without pre-collecting the upstream.
// Sub-streams are emitted as new keys are first observed; back-pressure from
// sub-stream consumers propagates to the upstream because the goroutine blocks
// when a per-key channel is full.
//
// Unlike [newGroupByIterator] this implementation never buffers all elements
// in memory simultaneously; it is suitable for large or unbounded upstreams.
func newGroupByStreamingIterator[T any](upstream iterator[T], maxSubstreams int, key func(T) string) iterator[SubStream[T]] {
	subCh := make(chan SubStream[T], maxSubstreams)
	sharedErr := newSharedError()

	go func() {
		defer close(subCh)

		groups := make(map[string]chan T)

		for {
			// Honour error/cancellation before pulling.
			select {
			case <-sharedErr.sig:
				return
			default:
			}

			elem, ok, err := upstream.next()
			if err != nil {
				sharedErr.fail(err)
				return
			}
			if !ok {
				// Upstream exhausted — close every per-key channel so sub-stream
				// consumers see a normal completion.
				for _, ch := range groups {
					close(ch)
				}
				return
			}

			k := key(elem)
			ch, exists := groups[k]
			if !exists {
				if len(groups) >= maxSubstreams {
					sharedErr.fail(ErrTooManySubstreams)
					return
				}
				newCh := make(chan T, DefaultAsyncBufSize)
				groups[k] = newCh
				ch = newCh

				// Build the SubStream with a factory that reads from the channel.
				// newCh is captured by value so each closure references its own
				// channel, independent of future loop iterations.
				sub := SubStream[T]{
					Key: k,
					Source: Source[T, NotUsed]{
						factory: func() (iterator[T], NotUsed) {
							return &channelIterator[T]{ch: newCh, err: sharedErr}, NotUsed{}
						},
					},
				}
				select {
				case subCh <- sub:
				case <-sharedErr.sig:
					return
				}
			}

			// Route element to the per-key channel.  This is the back-pressure
			// point: if the channel is full the goroutine blocks, preventing the
			// upstream from advancing until the consumer drains the sub-stream.
			select {
			case ch <- elem:
			case <-sharedErr.sig:
				return
			}
		}
	}()

	return &channelIterator[SubStream[T]]{ch: subCh, err: sharedErr}
}

// ─── flattenMergeIterator ──────────────────────────────────────────────────

type flattenMergeIterator[T any] struct {
	ch    chan T
	errCh chan error
}

func newFlattenMergeIterator[T any](upstream iterator[Source[T, NotUsed]], breadth int) *flattenMergeIterator[T] {
	f := &flattenMergeIterator[T]{
		ch:    make(chan T, breadth*DefaultAsyncBufSize),
		errCh: make(chan error, 1),
	}

	go func() {
		defer close(f.ch)
		var wg sync.WaitGroup
		semaphore := make(chan struct{}, breadth)

		for {
			src, ok, err := upstream.next()
			if err != nil {
				select {
				case f.errCh <- err:
				default:
				}
				return
			}
			if !ok {
				break
			}

			semaphore <- struct{}{}
			wg.Add(1)
			go func(s Source[T, NotUsed]) {
				defer wg.Done()
				defer func() { <-semaphore }()

				it, _ := s.factory()
				for {
					elem, ok, err := it.next()
					if err != nil {
						select {
						case f.errCh <- err:
						default:
						}
						return
					}
					if !ok {
						break
					}
					f.ch <- elem
				}
			}(src)
		}
		wg.Wait()
	}()

	return f
}

func (f *flattenMergeIterator[T]) next() (T, bool, error) {
	select {
	case err := <-f.errCh:
		var zero T
		return zero, false, err
	default:
	}
	select {
	case err := <-f.errCh:
		var zero T
		return zero, false, err
	case v, ok := <-f.ch:
		if !ok {
			select {
			case err := <-f.errCh:
				var zero T
				return zero, false, err
			default:
				var zero T
				return zero, false, nil
			}
		}
		return v, true, nil
	}
}

// ─── conflateIterator ──────────────────────────────────────────────────────

type conflateIterator[In, S any] struct {
	ch    chan S
	errCh chan error
}

func newConflateIterator[In, S any](upstream iterator[In], seed func(In) S, aggregate func(S, In) S) *conflateIterator[In, S] {
	c := &conflateIterator[In, S]{
		ch:    make(chan S, 1),
		errCh: make(chan error, 1),
	}

	go func() {
		defer close(c.ch)
		var current S
		var hasValue bool

		for {
			elem, ok, err := upstream.next()
			if err != nil {
				select {
				case c.errCh <- err:
				default:
				}
				return
			}
			if !ok {
				if hasValue {
					c.ch <- current
				}
				return
			}

			if !hasValue {
				current = seed(elem)
				hasValue = true
			} else {
				current = aggregate(current, elem)
			}

			// Try to send the conflated value if downstream is ready.
			// This implements the non-blocking "compaction" behavior.
			select {
			case c.ch <- current:
				hasValue = false
			default:
				// Downstream not ready, continue aggregating.
			}
		}
	}()

	return c
}

func (c *conflateIterator[In, S]) next() (S, bool, error) {
	select {
	case err := <-c.errCh:
		var zero S
		return zero, false, err
	default:
	}
	select {
	case err := <-c.errCh:
		var zero S
		return zero, false, err
	case v, ok := <-c.ch:
		if !ok {
			select {
			case err := <-c.errCh:
				var zero S
				return zero, false, err
			default:
				var zero S
				return zero, false, nil
			}
		}
		return v, true, nil
	}
}
