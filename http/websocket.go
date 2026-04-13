// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package http

import (
	nethttp "net/http"

	"github.com/gorilla/websocket"
)

// WSMessage is a WebSocket message (text or binary).
type WSMessage struct {
	// MessageType is websocket.TextMessage (1) or websocket.BinaryMessage (2).
	MessageType int
	Data        []byte
}

var wsUpgrader = websocket.Upgrader{
	// Allow all origins by default (caller can override via custom upgrader).
	CheckOrigin: func(r *nethttp.Request) bool { return true },
}

// HandleWebSocketMessages upgrades the HTTP connection to a WebSocket and
// invokes handler for each incoming message. If handler returns false, the
// connection is closed. Rejects non-WebSocket requests.
//
// handler signature: func(msg WSMessage) (response WSMessage, keepAlive bool)
func HandleWebSocketMessages(handler func(WSMessage) (WSMessage, bool)) Route {
	return func(ctx *RequestContext) {
		conn, err := wsUpgrader.Upgrade(ctx.Writer, ctx.Request, nil)
		if err != nil {
			// Upgrade() already wrote the error response.
			ctx.Completed = true
			return
		}
		ctx.Completed = true
		defer conn.Close()

		for {
			mt, data, err := conn.ReadMessage()
			if err != nil {
				return // client closed or read error
			}
			resp, keepAlive := handler(WSMessage{MessageType: mt, Data: data})
			if err := conn.WriteMessage(resp.MessageType, resp.Data); err != nil {
				return
			}
			if !keepAlive {
				return
			}
		}
	}
}

// HandleWebSocketMessagesAsync is like HandleWebSocketMessages but handler runs
// in a goroutine per connection and receives/sends via channels, enabling
// streaming use-cases (matches pekko-http's Flow[Message, Message] model).
func HandleWebSocketMessagesAsync(
	handler func(in <-chan WSMessage, out chan<- WSMessage),
) Route {
	return func(ctx *RequestContext) {
		conn, err := wsUpgrader.Upgrade(ctx.Writer, ctx.Request, nil)
		if err != nil {
			ctx.Completed = true
			return
		}
		ctx.Completed = true

		in := make(chan WSMessage, 16)
		out := make(chan WSMessage, 16)

		go handler(in, out)

		// Writer goroutine.
		go func() {
			for msg := range out {
				if err := conn.WriteMessage(msg.MessageType, msg.Data); err != nil {
					conn.Close()
					return
				}
			}
		}()

		// Reader (main goroutine, blocks until connection closes).
		defer close(in)
		for {
			mt, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			in <- WSMessage{MessageType: mt, Data: data}
		}
	}
}
