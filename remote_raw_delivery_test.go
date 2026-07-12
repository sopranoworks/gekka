/*
 * remote_raw_delivery_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
)

// rawEchoActor captures every message Receive is called with, and — if the
// payload is []byte — echoes it back to the sender as an ack. It mirrors the
// EchoActor pattern used by examples/remote-direct, examples/basic and
// examples/aeron-transport.
type rawEchoActor struct {
	actor.BaseActor
	received chan any
}

func (a *rawEchoActor) Receive(msg any) {
	a.received <- msg
	if b, ok := msg.([]byte); ok {
		if s := a.Sender(); s != nil {
			s.Tell(append([]byte("Ack: "), b...))
		}
	}
}

// TestRemoteRawBytesDelivery_ArrivesAsByteSlice guards the raw-bytes (serializer
// id 4) delivery contract for RegisterActor/ActorOf-registered actors: a
// message sent as []byte across a real Artery TCP connection between two
// local Cluster nodes must arrive at Receive as a plain []byte, with the
// sender reachable via BaseActor.Sender() — not wrapped in *IncomingMessage.
//
// examples/remote-direct/main.go once assumed the opposite (*IncomingMessage
// wrapping) and silently dropped every remote message because of it; this
// test exercises the exact code shape those examples use so that contract
// can't drift again without a build/test failure.
func TestRemoteRawBytesDelivery_ArrivesAsByteSlice(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nodeA, err := NewCluster(ClusterConfig{SystemName: "RawDeliveryTest", Host: "127.0.0.1", Port: 2570})
	if err != nil {
		t.Fatalf("NewCluster nodeA: %v", err)
	}
	defer func() { _ = nodeA.Shutdown() }()

	echo := &rawEchoActor{BaseActor: actor.NewBaseActor(), received: make(chan any, 1)}
	if _, err := nodeA.System.ActorOf(actor.Props{New: func() actor.Actor { return echo }}, "rawEcho"); err != nil {
		t.Fatalf("ActorOf echo: %v", err)
	}

	nodeB, err := NewCluster(ClusterConfig{SystemName: "RawDeliveryTest", Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("NewCluster nodeB: %v", err)
	}
	defer func() { _ = nodeB.Shutdown() }()

	ack := &rawEchoActor{BaseActor: actor.NewBaseActor(), received: make(chan any, 1)}
	ackRef, err := nodeB.System.ActorOf(actor.Props{New: func() actor.Actor { return ack }}, "ack")
	if err != nil {
		t.Fatalf("ActorOf ack: %v", err)
	}

	remoteRef, err := nodeB.ActorSelection("pekko://RawDeliveryTest@127.0.0.1:2570/user/rawEcho").Resolve(ctx)
	if err != nil {
		t.Fatalf("Resolve remote echo: %v", err)
	}

	remoteRef.Tell([]byte("hello"), ackRef)

	select {
	case msg := <-echo.received:
		if _, ok := msg.(*IncomingMessage); ok {
			t.Fatalf("echo actor received *IncomingMessage — raw-bytes delivery contract regressed; want []byte")
		}
		b, ok := msg.([]byte)
		if !ok {
			t.Fatalf("echo actor received %T, want []byte", msg)
		}
		if string(b) != "hello" {
			t.Fatalf("echo actor received %q, want %q", b, "hello")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("echo actor did not receive message within 10s")
	}

	select {
	case msg := <-ack.received:
		b, ok := msg.([]byte)
		if !ok {
			t.Fatalf("ack actor received %T, want []byte", msg)
		}
		if string(b) != "Ack: hello" {
			t.Fatalf("ack actor received %q, want %q", b, "Ack: hello")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ack actor did not receive reply within 10s")
	}
}
