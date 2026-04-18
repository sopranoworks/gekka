/*
 * zip_latest.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import "sync"

// ZipLatest combines two sources by pairing the latest element from each side.
// No pair is emitted until both sources have produced at least one element.
// When either source updates, a new pair is emitted using the latest values
// from both sides. The stream completes when both sources are exhausted.
func ZipLatest[T1, T2 any](
	s1 Source[T1, NotUsed],
	s2 Source[T2, NotUsed],
) Source[Pair[T1, T2], NotUsed] {
	return Source[Pair[T1, T2], NotUsed]{
		factory: func() (iterator[Pair[T1, T2]], NotUsed) {
			left, _ := s1.factory()
			right, _ := s2.factory()
			return &zipLatestIterator[T1, T2]{
				left:  left,
				right: right,
				ch1:   make(chan T1, 1),
				ch2:   make(chan T2, 1),
				errCh: make(chan error, 2),
				done:  make(chan struct{}),
			}, NotUsed{}
		},
	}
}

// CombineLatest is an alias for [ZipLatest].
func CombineLatest[T1, T2 any](
	s1 Source[T1, NotUsed],
	s2 Source[T2, NotUsed],
) Source[Pair[T1, T2], NotUsed] {
	return ZipLatest(s1, s2)
}

type zipLatestIterator[T1, T2 any] struct {
	left  iterator[T1]
	right iterator[T2]
	ch1   chan T1
	ch2   chan T2
	errCh chan error
	done  chan struct{}

	once      sync.Once
	closeOnce sync.Once
	last1     T1
	last2     T2
	has1      bool
	has2      bool
	done1     bool
	done2     bool
}

func (z *zipLatestIterator[T1, T2]) closeDone() {
	z.closeOnce.Do(func() { close(z.done) })
}

func (z *zipLatestIterator[T1, T2]) startOnce() {
	z.once.Do(func() {
		go func() {
			defer close(z.ch1)
			for {
				v, ok, err := z.left.next()
				if err != nil {
					z.errCh <- err
					return
				}
				if !ok {
					return
				}
				select {
				case z.ch1 <- v:
				case <-z.done:
					return
				}
			}
		}()
		go func() {
			defer close(z.ch2)
			for {
				v, ok, err := z.right.next()
				if err != nil {
					z.errCh <- err
					return
				}
				if !ok {
					return
				}
				select {
				case z.ch2 <- v:
				case <-z.done:
					return
				}
			}
		}()
	})
}

func (z *zipLatestIterator[T1, T2]) next() (Pair[T1, T2], bool, error) {
	z.startOnce()

	for {
		if z.done1 && z.done2 {
			// Drain any pending error before declaring normal completion.
			select {
			case err := <-z.errCh:
				z.closeDone()
				return Pair[T1, T2]{}, false, err
			default:
			}
			z.closeDone()
			return Pair[T1, T2]{}, false, nil
		}

		select {
		case v, ok := <-z.ch1:
			if !ok {
				z.done1 = true
				// Non-blocking drain: an error may already be queued.
				select {
				case err := <-z.errCh:
					z.closeDone()
					return Pair[T1, T2]{}, false, err
				default:
				}
				continue
			}
			z.last1 = v
			z.has1 = true
		case v, ok := <-z.ch2:
			if !ok {
				z.done2 = true
				// Non-blocking drain: an error may already be queued.
				select {
				case err := <-z.errCh:
					z.closeDone()
					return Pair[T1, T2]{}, false, err
				default:
				}
				continue
			}
			z.last2 = v
			z.has2 = true
		case err := <-z.errCh:
			z.closeDone()
			return Pair[T1, T2]{}, false, err
		}

		if z.has1 && z.has2 {
			return Pair[T1, T2]{First: z.last1, Second: z.last2}, true, nil
		}
	}
}
