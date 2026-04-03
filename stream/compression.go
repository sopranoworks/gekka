/*
 * compression.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"fmt"
	"io"
)

// Compression provides Flow operators for gzip and deflate encoding/decoding of
// byte streams, mirroring Pekko's pekko.stream.scaladsl.Compression object.
//
// All operators work on Flow[[]byte, []byte, NotUsed] and compose naturally
// with Framing, TCP, and file I/O stages.
var Compression compressionOps

type compressionOps struct{}

// DefaultCompressionLevel is passed to Gzip and Deflate when the caller does
// not want to specify an explicit compression level.
const DefaultCompressionLevel = gzip.DefaultCompression

// Gzip returns a Flow that gzip-compresses each byte slice emitted by
// upstream. The compressor is flushed (sync-flush) after every upstream
// element so that the receiver can decompress incrementally.
//
//   - level: gzip.DefaultCompression, gzip.BestSpeed, gzip.BestCompression,
//     or any value in 1–9.
//
// This mirrors Pekko's Compression.gzip.
func (compressionOps) Gzip(level int) Flow[[]byte, []byte, NotUsed] {
	return Flow[[]byte, []byte, NotUsed]{
		attach: func(up iterator[[]byte]) (iterator[[]byte], NotUsed) {
			it := &gzipEncodeIter{upstream: up, level: level}
			it.init()
			return it, NotUsed{}
		},
	}
}

// Gunzip returns a Flow that gzip-decompresses a byte stream. The entire
// upstream is accumulated before decompression; the decompressed output is
// then emitted in chunks of at most maxBytesPerChunk bytes.
//
// This mirrors Pekko's Compression.gunzip.
func (compressionOps) Gunzip(maxBytesPerChunk int) Flow[[]byte, []byte, NotUsed] {
	if maxBytesPerChunk <= 0 {
		maxBytesPerChunk = 64 * 1024
	}
	return Flow[[]byte, []byte, NotUsed]{
		attach: func(up iterator[[]byte]) (iterator[[]byte], NotUsed) {
			return &gunzipIter{upstream: up, maxChunk: maxBytesPerChunk}, NotUsed{}
		},
	}
}

// Deflate returns a Flow that deflate-compresses each byte slice emitted by
// upstream. The compressor is flushed (sync-flush) after every upstream
// element.
//
// This mirrors Pekko's Compression.deflate.
func (compressionOps) Deflate(level int) Flow[[]byte, []byte, NotUsed] {
	return Flow[[]byte, []byte, NotUsed]{
		attach: func(up iterator[[]byte]) (iterator[[]byte], NotUsed) {
			it := &deflateEncodeIter{upstream: up, level: level}
			it.init()
			return it, NotUsed{}
		},
	}
}

// Inflate returns a Flow that deflate-decompresses a byte stream. The entire
// upstream is accumulated before decompression; the decompressed output is
// then emitted in chunks of at most maxBytesPerChunk bytes.
//
// This mirrors Pekko's Compression.inflate.
func (compressionOps) Inflate(maxBytesPerChunk int) Flow[[]byte, []byte, NotUsed] {
	if maxBytesPerChunk <= 0 {
		maxBytesPerChunk = 64 * 1024
	}
	return Flow[[]byte, []byte, NotUsed]{
		attach: func(up iterator[[]byte]) (iterator[[]byte], NotUsed) {
			return &inflateIter{upstream: up, maxChunk: maxBytesPerChunk}, NotUsed{}
		},
	}
}

// ── gzipEncodeIter ────────────────────────────────────────────────────────────

type gzipEncodeIter struct {
	upstream iterator[[]byte]
	level    int
	buf      bytes.Buffer
	gzw      *gzip.Writer
	done     bool
	err      error
}

func (g *gzipEncodeIter) init() {
	var err error
	g.gzw, err = gzip.NewWriterLevel(&g.buf, g.level)
	if err != nil {
		// Fall back to default compression on bad level.
		g.gzw, _ = gzip.NewWriterLevel(&g.buf, gzip.DefaultCompression)
	}
}

func (g *gzipEncodeIter) next() ([]byte, bool, error) {
	for {
		if g.done {
			return nil, false, g.err
		}

		chunk, ok, err := g.upstream.next()
		if !ok {
			// Upstream exhausted: close the gzip writer to emit the footer.
			g.done = true
			g.err = err
			if closeErr := g.gzw.Close(); closeErr != nil {
				return nil, false, closeErr
			}
			if g.buf.Len() > 0 {
				out := g.buf.Bytes()
				result := make([]byte, len(out))
				copy(result, out)
				g.buf.Reset()
				return result, true, nil
			}
			return nil, false, err
		}

		if _, writeErr := g.gzw.Write(chunk); writeErr != nil {
			return nil, false, fmt.Errorf("compression: gzip encode: %w", writeErr)
		}
		if flushErr := g.gzw.Flush(); flushErr != nil {
			return nil, false, fmt.Errorf("compression: gzip flush: %w", flushErr)
		}

		if g.buf.Len() > 0 {
			out := g.buf.Bytes()
			result := make([]byte, len(out))
			copy(result, out)
			g.buf.Reset()
			return result, true, nil
		}
		// Empty chunk produced no compressed bytes — pull another.
	}
}

// ── gunzipIter ────────────────────────────────────────────────────────────────

type gunzipIter struct {
	upstream    iterator[[]byte]
	maxChunk    int
	initialized bool
	data        []byte
	pos         int
}

func (g *gunzipIter) next() ([]byte, bool, error) {
	if !g.initialized {
		g.initialized = true
		var raw bytes.Buffer
		for {
			chunk, ok, err := g.upstream.next()
			if !ok {
				if err != nil {
					return nil, false, err
				}
				break
			}
			raw.Write(chunk)
		}
		gzr, err := gzip.NewReader(&raw)
		if err != nil {
			return nil, false, fmt.Errorf("compression: gunzip: %w", err)
		}
		var decompressed bytes.Buffer
		if _, copyErr := io.Copy(&decompressed, gzr); copyErr != nil {
			return nil, false, fmt.Errorf("compression: gunzip decompress: %w", copyErr)
		}
		gzr.Close()
		g.data = decompressed.Bytes()
	}

	if g.pos >= len(g.data) {
		return nil, false, nil
	}
	end := g.pos + g.maxChunk
	if end > len(g.data) {
		end = len(g.data)
	}
	out := make([]byte, end-g.pos)
	copy(out, g.data[g.pos:end])
	g.pos = end
	return out, true, nil
}

// ── deflateEncodeIter ─────────────────────────────────────────────────────────

type deflateEncodeIter struct {
	upstream iterator[[]byte]
	level    int
	buf      bytes.Buffer
	fw       *flate.Writer
	done     bool
	err      error
}

func (d *deflateEncodeIter) init() {
	var err error
	d.fw, err = flate.NewWriter(&d.buf, d.level)
	if err != nil {
		d.fw, _ = flate.NewWriter(&d.buf, flate.DefaultCompression)
	}
}

func (d *deflateEncodeIter) next() ([]byte, bool, error) {
	for {
		if d.done {
			return nil, false, d.err
		}

		chunk, ok, err := d.upstream.next()
		if !ok {
			d.done = true
			d.err = err
			if closeErr := d.fw.Close(); closeErr != nil {
				return nil, false, closeErr
			}
			if d.buf.Len() > 0 {
				out := d.buf.Bytes()
				result := make([]byte, len(out))
				copy(result, out)
				d.buf.Reset()
				return result, true, nil
			}
			return nil, false, err
		}

		if _, writeErr := d.fw.Write(chunk); writeErr != nil {
			return nil, false, fmt.Errorf("compression: deflate encode: %w", writeErr)
		}
		if flushErr := d.fw.Flush(); flushErr != nil {
			return nil, false, fmt.Errorf("compression: deflate flush: %w", flushErr)
		}

		if d.buf.Len() > 0 {
			out := d.buf.Bytes()
			result := make([]byte, len(out))
			copy(result, out)
			d.buf.Reset()
			return result, true, nil
		}
	}
}

// ── inflateIter ───────────────────────────────────────────────────────────────

type inflateIter struct {
	upstream    iterator[[]byte]
	maxChunk    int
	initialized bool
	data        []byte
	pos         int
}

func (d *inflateIter) next() ([]byte, bool, error) {
	if !d.initialized {
		d.initialized = true
		var raw bytes.Buffer
		for {
			chunk, ok, err := d.upstream.next()
			if !ok {
				if err != nil {
					return nil, false, err
				}
				break
			}
			raw.Write(chunk)
		}
		r := flate.NewReader(&raw)
		var decompressed bytes.Buffer
		if _, copyErr := io.Copy(&decompressed, r); copyErr != nil {
			return nil, false, fmt.Errorf("compression: inflate decompress: %w", copyErr)
		}
		r.Close()
		d.data = decompressed.Bytes()
	}

	if d.pos >= len(d.data) {
		return nil, false, nil
	}
	end := d.pos + d.maxChunk
	if end > len(d.data) {
		end = len(d.data)
	}
	out := make([]byte, end-d.pos)
	copy(out, d.data[d.pos:end])
	d.pos = end
	return out, true, nil
}
