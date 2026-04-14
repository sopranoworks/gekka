// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http_test

import (
	"bufio"
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	ghttp "github.com/sopranoworks/gekka/http"
	"github.com/sopranoworks/gekka/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSE_StreamEvents(t *testing.T) {
	events := []ghttp.ServerSentEvent{
		{Data: "hello"},
		{Data: "world", EventType: "greet"},
		{ID: "3", Data: "done"},
	}
	route := ghttp.Path("/events", ghttp.Get(ghttp.SSESource(stream.FromSlice(events))))
	srv, err := ghttp.NewServer(":0", route)
	require.NoError(t, err)
	defer srv.Shutdown(context.Background()) //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", "http://"+srv.Addr+"/events", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	scanner := bufio.NewScanner(resp.Body)
	var collected []string
	for scanner.Scan() {
		line := scanner.Text()
		collected = append(collected, line)
		if strings.Contains(line, "data: done") {
			break
		}
	}
	joined := strings.Join(collected, "\n")
	assert.Contains(t, joined, "data: hello")
	assert.Contains(t, joined, "event: greet")
	assert.Contains(t, joined, "data: world")
	assert.Contains(t, joined, "id: 3")
	assert.Contains(t, joined, "data: done")

	// Each event must be terminated by a blank line (W3C SSE wire format).
	assert.Contains(t, joined, "\n\n", "SSE events must be separated by blank lines")
}
