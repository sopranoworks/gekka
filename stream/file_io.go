/*
 * file_io.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"io"
	"os"
	"sync"
)

// ─── Future ───────────────────────────────────────────────────────────────

// Future[T] is a single-assignment promise that delivers a value (or error)
// exactly once.  Callers block on [Future.Get] until the value is available.
type Future[T any] struct {
	once  sync.Once
	value T
	err   error
	done  chan struct{}
}

func newFuture[T any]() *Future[T] {
	return &Future[T]{done: make(chan struct{})}
}

// complete resolves the future.  Only the first call has any effect.
func (f *Future[T]) complete(v T, err error) {
	f.once.Do(func() {
		f.value = v
		f.err = err
		close(f.done)
	})
}

// Get blocks until the future is resolved and returns its value and error.
func (f *Future[T]) Get() (T, error) {
	<-f.done
	return f.value, f.err
}

// ─── fileReadIterator ─────────────────────────────────────────────────────

type fileReadIterator struct {
	file      *os.File
	chunkSize int
	total     int64
	future    *Future[int64]
	done      bool
}

func (it *fileReadIterator) next() ([]byte, bool, error) {
	if it.done {
		var zero []byte
		return zero, false, nil
	}
	buf := make([]byte, it.chunkSize)
	n, err := it.file.Read(buf)
	if n > 0 {
		it.total += int64(n)
		return buf[:n], true, nil
	}
	it.done = true
	it.file.Close()
	if err == io.EOF {
		it.future.complete(it.total, nil)
		var zero []byte
		return zero, false, nil
	}
	it.future.complete(it.total, err)
	var zero []byte
	return zero, false, err
}

// ─── SourceFromFile ───────────────────────────────────────────────────────

// SourceFromFile returns a Source that reads path in chunks of chunkSize bytes
// and emits each chunk as a []byte element.  It also returns a [Future][int64]
// that is resolved with the total number of bytes read when the source
// completes (or with an error if reading fails).
//
// Example:
//
//	src, bytesFut := stream.SourceFromFile("/var/log/app.log", 4096)
//	_, err := stream.RunWith(stream.Via(src, processFlow), stream.Discard[[]byte](), m)
//	total, _ := bytesFut.Get()
func SourceFromFile(path string, chunkSize int) (Source[[]byte, NotUsed], *Future[int64]) {
	fut := newFuture[int64]()
	src := Source[[]byte, NotUsed]{
		factory: func() (iterator[[]byte], NotUsed) {
			f, err := os.Open(path)
			if err != nil {
				fut.complete(0, err)
				return &errorIterator[[]byte]{err: err}, NotUsed{}
			}
			return &fileReadIterator{file: f, chunkSize: chunkSize, future: fut}, NotUsed{}
		},
	}
	return src, fut
}

// ─── SinkToFile ───────────────────────────────────────────────────────────

// SinkToFile returns a Sink that writes each incoming []byte chunk to path,
// creating or truncating the file as needed.  The materialized [Future][int64]
// is resolved with the total number of bytes written when the stream
// completes, or with an error on failure.
//
// Example:
//
//	sink := stream.SinkToFile("/tmp/out.bin")
//	fut, err := stream.RunWith(src, sink, m)
//	total, writeErr := fut.Get()
func SinkToFile(path string) Sink[[]byte, *Future[int64]] {
	return Sink[[]byte, *Future[int64]]{
		runWith: func(upstream iterator[[]byte]) (*Future[int64], error) {
			fut := newFuture[int64]()
			f, err := os.Create(path)
			if err != nil {
				fut.complete(0, err)
				return fut, err
			}
			defer f.Close()

			total := int64(0)
			for {
				chunk, ok, pullErr := upstream.next()
				if pullErr != nil {
					fut.complete(total, pullErr)
					return fut, pullErr
				}
				if !ok {
					break
				}
				n, writeErr := f.Write(chunk)
				total += int64(n)
				if writeErr != nil {
					fut.complete(total, writeErr)
					return fut, writeErr
				}
			}
			fut.complete(total, nil)
			return fut, nil
		},
	}
}
