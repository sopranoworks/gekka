// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	ghttp "github.com/sopranoworks/gekka/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebSocket_Echo(t *testing.T) {
	// Server: echo all incoming messages back.
	echoHandler := func(msg ghttp.WSMessage) (ghttp.WSMessage, bool) {
		return msg, true // (response, keep-alive)
	}
	route := ghttp.Path("/ws", ghttp.HandleWebSocketMessages(echoHandler))

	srv, err := ghttp.NewServer(":0", route)
	require.NoError(t, err)
	defer srv.Shutdown(context.Background()) //nolint:errcheck

	u := url.URL{Scheme: "ws", Host: srv.Addr, Path: "/ws"}
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	require.NoError(t, err)
	defer conn.Close()

	err = conn.WriteMessage(websocket.TextMessage, []byte("hello"))
	require.NoError(t, err)

	conn.SetReadDeadline(time.Now().Add(2 * time.Second)) //nolint:errcheck
	mt, data, err := conn.ReadMessage()
	require.NoError(t, err)
	assert.Equal(t, websocket.TextMessage, mt)
	assert.Equal(t, "hello", string(data))
}
