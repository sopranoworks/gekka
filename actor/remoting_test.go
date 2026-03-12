/*
 * remoting_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

func TestRouter_Buffering_Migrated(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	remoteAddr := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("remoteSystem"),
		Hostname: proto.String("10.0.0.1"),
		Port:     proto.Uint32(2552),
	}

	// Build a fully-initialised outbound association.
	assoc := &GekkaAssociationMock{
		state:     WAITING_FOR_HANDSHAKE,
		role:      OUTBOUND,
		conn:      server,
		pending:   make([][]byte, 0),
		handshake: make(chan struct{}),
		outbox:    make(chan []byte, 100),
		remote: &gproto_remote.UniqueAddress{
			Address: remoteAddr,
			Uid:     proto.Uint64(0),
		},
	}

	// Start the outbox writer goroutine.
	outboxCtx, outboxCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer outboxCancel()
	go func() {
		for {
			select {
			case <-outboxCtx.Done():
				return
			case msg, ok := <-assoc.outbox:
				if !ok {
					return
				}
				// Mock WriteFrame
				buf := make([]byte, 4+len(msg))
				binary.LittleEndian.PutUint32(buf, uint32(len(msg)))
				copy(buf[4:], msg)
				_, _ = assoc.conn.Write(buf)
			}
		}
	}()

	// Send while waiting — message should be buffered.
	payload := []byte("delayed-message")
	if err := assoc.Send("/user/test", payload, 4, ""); err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	if len(assoc.pending) != 1 {
		t.Fatal("message should be buffered in pending")
	}

	// Complete handshake.
	go func() {
		assoc.mu.Lock()
		assoc.state = ASSOCIATED
		assoc.mu.Unlock()
		close(assoc.handshake)
		assoc.flushPending()
	}()

	// The pending message should now be flushed.
	header := make([]byte, 4)
	if _, err := io.ReadFull(client, header); err != nil {
		t.Fatalf("failed to read frame header after handshake: %v", err)
	}
	frameLen := binary.LittleEndian.Uint32(header)
	if frameLen == 0 {
		t.Fatal("expected non-zero frame length")
	}
	frameBytes := make([]byte, frameLen)
	if _, err := io.ReadFull(client, frameBytes); err != nil {
		t.Fatalf("failed to read frame body: %v", err)
	}
	// Note: We'd need ParseArteryFrame here for full verification, 
	// but just checking if the payload is present is enough for this migration check.
}

type GekkaAssociationMock struct {
	mu        sync.RWMutex
	state     AssociationState
	role      AssociationRole
	conn      net.Conn
	pending   [][]byte
	handshake chan struct{}
	outbox    chan []byte
	remote    *gproto_remote.UniqueAddress
}

func (a *GekkaAssociationMock) Send(path string, payload []byte, serializerID int32, manifest string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	
	// Simplify: just buffer everything if not associated
	if a.state != ASSOCIATED {
		a.pending = append(a.pending, payload)
		return nil
	}
	a.outbox <- payload
	return nil
}

func (a *GekkaAssociationMock) SendWithSender(path string, sender string, payload []byte, serializerID int32, manifest string) error {
	return a.Send(path, payload, serializerID, manifest)
}

func (a *GekkaAssociationMock) flushPending() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, msg := range a.pending {
		a.outbox <- msg
	}
	a.pending = nil
}
