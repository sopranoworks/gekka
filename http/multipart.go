// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http

import (
	"io"
	nethttp "net/http"

	"github.com/sopranoworks/gekka/stream"
)

// FileUpload extracts an uploaded file from a multipart/form-data request.
// The file content is provided to inner as a stream.Source[[]byte] of 32 KiB chunks —
// the file is never fully buffered in memory.
// Responds 400 when fieldName is missing or the request is not multipart.
func FileUpload(fieldName string, inner func(filename string, body stream.Source[[]byte, stream.NotUsed], ctx *RequestContext)) Route {
	return func(ctx *RequestContext) {
		if err := ctx.Request.ParseMultipartForm(32 << 20); err != nil {
			nethttp.Error(ctx.Writer, "multipart parse error: "+err.Error(), nethttp.StatusBadRequest)
			ctx.Completed = true
			return
		}
		f, fh, err := ctx.Request.FormFile(fieldName)
		if err != nil {
			nethttp.Error(ctx.Writer, "missing field: "+fieldName, nethttp.StatusBadRequest)
			ctx.Completed = true
			return
		}
		defer f.Close() //nolint:errcheck
		src := fileSource(f)
		inner(fh.Filename, src, ctx)
	}
}

// fileSource wraps an io.ReadCloser as a stream.Source[[]byte] of 32 KiB chunks.
func fileSource(rc io.ReadCloser) stream.Source[[]byte, stream.NotUsed] {
	const chunkSize = 32 * 1024
	buf := make([]byte, chunkSize)
	var closed bool
	return stream.FromIteratorFunc(func() ([]byte, bool, error) {
		if closed {
			return nil, false, nil
		}
		n, err := rc.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			return chunk, true, nil
		}
		if err == io.EOF {
			rc.Close() //nolint:errcheck
			closed = true
			return nil, false, nil
		}
		return nil, false, err
	})
}

// FormDataField extracts a single text field from a multipart/form-data request.
// Responds 400 when fieldName is not present.
func FormDataField(fieldName string, inner func(value string, ctx *RequestContext)) Route {
	return func(ctx *RequestContext) {
		if err := ctx.Request.ParseMultipartForm(32 << 20); err != nil {
			nethttp.Error(ctx.Writer, "multipart parse error: "+err.Error(), nethttp.StatusBadRequest)
			ctx.Completed = true
			return
		}
		vals, ok := ctx.Request.MultipartForm.Value[fieldName]
		if !ok || len(vals) == 0 {
			nethttp.Error(ctx.Writer, "missing field: "+fieldName, nethttp.StatusBadRequest)
			ctx.Completed = true
			return
		}
		inner(vals[0], ctx)
	}
}
