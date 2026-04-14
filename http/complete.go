// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http

import (
	"encoding/json"
	"fmt"
	nethttp "net/http"
	"strconv"
	"strings"

	"github.com/sopranoworks/gekka/stream"
)

// Complete writes a response. Body handling:
//   - string → text/plain; charset=utf-8
//   - []byte → application/octet-stream
//   - nil → no body, just status code
//   - any other type → JSON via encoding/json, application/json
func Complete(status int, body any) Route {
	return func(ctx *RequestContext) {
		ctx.Completed = true
		switch v := body.(type) {
		case string:
			ctx.Writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
			ctx.Writer.WriteHeader(status)
			_, _ = ctx.Writer.Write([]byte(v))
		case []byte:
			ctx.Writer.Header().Set("Content-Type", "application/octet-stream")
			ctx.Writer.WriteHeader(status)
			_, _ = ctx.Writer.Write(v)
		case nil:
			ctx.Writer.WriteHeader(status)
		default:
			ctx.Writer.Header().Set("Content-Type", "application/json")
			ctx.Writer.WriteHeader(status)
			enc := json.NewEncoder(ctx.Writer)
			_ = enc.Encode(v)
		}
	}
}

// CompleteWith writes a response with explicit content type and raw bytes.
func CompleteWith(status int, contentType string, body []byte) Route {
	return func(ctx *RequestContext) {
		ctx.Completed = true
		ctx.Writer.Header().Set("Content-Type", contentType)
		ctx.Writer.WriteHeader(status)
		_, _ = ctx.Writer.Write(body)
	}
}

// Redirect sends an HTTP redirect.
func Redirect(url string, status int) Route {
	return func(ctx *RequestContext) {
		ctx.Completed = true
		nethttp.Redirect(ctx.Writer, ctx.Request, url, status)
	}
}

// Reject explicitly rejects — signals "try next route".
func Reject() Route {
	return func(ctx *RequestContext) {
		ctx.Rejected = true
	}
}

// ServerSentEvent is a single SSE frame (W3C SSE spec).
type ServerSentEvent struct {
	ID        string // optional; "id: <ID>\n"
	EventType string // optional; "event: <type>\n"
	Data      string // required; multi-line data is split into multiple "data:" lines
	Retry     int    // optional retry ms; 0 = not sent
}

// format serialises the event as an SSE wire frame ending with a blank line.
func (e ServerSentEvent) format() string {
	var b strings.Builder
	if e.ID != "" {
		fmt.Fprintf(&b, "id: %s\n", e.ID)
	}
	if e.EventType != "" {
		fmt.Fprintf(&b, "event: %s\n", e.EventType)
	}
	if e.Retry > 0 {
		fmt.Fprintf(&b, "retry: %s\n", strconv.Itoa(e.Retry))
	}
	for _, line := range strings.Split(e.Data, "\n") {
		fmt.Fprintf(&b, "data: %s\n", line)
	}
	b.WriteByte('\n')
	return b.String()
}

// SSESource streams ServerSentEvents from src as a text/event-stream HTTP response.
func SSESource[Mat any](src stream.Source[ServerSentEvent, Mat]) Route {
	return func(ctx *RequestContext) {
		w := ctx.Writer
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(nethttp.StatusOK)
		flusher, canFlush := w.(nethttp.Flusher)

		_, _ = stream.RunWith(
			src,
			stream.SinkFromFunc(func(pull func() (ServerSentEvent, bool, error)) error {
				for {
					evt, ok, err := pull()
					if err != nil || !ok {
						return err
					}
					if _, werr := fmt.Fprint(w, evt.format()); werr != nil {
						// Client disconnected — treat as clean completion from server's perspective.
						return nil
					}
					if canFlush {
						flusher.Flush()
					}
				}
			}),
			stream.SyncMaterializer{},
		)
		ctx.Completed = true
	}
}
