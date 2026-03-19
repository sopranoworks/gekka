//go:build integration

/*
 * integration_delivery_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package gekka — E2E integration tests for Reliable Delivery interop with Pekko.
//
// Two sub-tests verify both directions of the Reliable Delivery flow:
//
//   - GoProducer_ScalaConsumer: Go's ProducerController sends N messages to
//     Scala's ConsumerController at /user/scalaConsumer.  Success is confirmed
//     when Scala prints PEKKO_DELIVERY_RECEIVED:<text> for each message.
//
//   - ScalaProducer_GoConsumer: Scala's ProducerController sends 10 messages to
//     Go's ConsumerController at /user/goConsumer.  Success is confirmed when
//     Go's consumer actor receives all Delivery messages and Scala prints
//     PEKKO_DELIVERY_PRODUCER_NEXT:<n> for each one.
//
// Run with:
//
//	go test -v -tags integration -run TestDeliveryInterop -timeout 300s .
package gekka

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed/delivery"
)

// TestDeliveryInterop is the top-level Reliable Delivery E2E test.
// It starts PekkoIntegrationNode, initialises a Gekka cluster on port 2553,
// waits for the handshake, then runs two delivery direction sub-tests.
func TestDeliveryInterop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	t.Cleanup(cancel)

	// ── 1. Start Scala node ────────────────────────────────────────────────
	sig := startPekkoIntegrationNode(t, ctx)

	log.Println("[WAIT] Waiting for PEKKO_NODE_READY...")
	select {
	case <-sig.ready:
		log.Println("[SCALA] PekkoIntegrationNode is ready.")
	case <-ctx.Done():
		t.Fatalf("Scala node did not print PEKKO_NODE_READY within timeout")
	}

	// ── 2. Initialize Gekka cluster on port 2553 ───────────────────────────
	selfAddr := actor.Address{
		Protocol: "pekko",
		System:   "GekkaSystem",
		Host:     "127.0.0.1",
		Port:     2553,
	}
	node, err := NewCluster(ClusterConfig{Address: selfAddr})
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	t.Cleanup(func() { node.Shutdown() })
	log.Printf("[GO] Cluster node listening at %s", node.Addr())

	// Register the Reliable Delivery serializer so the router can encode/decode
	// SequencedMessage, Request, Ack, Resend, and RegisterConsumer.
	node.RegisterSerializer(delivery.NewSerializer())

	// ── 3. Join and wait for handshake ────────────────────────────────────
	log.Println("[GO] Joining GekkaSystem at 127.0.0.1:2552...")
	if err := node.Join("127.0.0.1", 2552); err != nil {
		t.Fatalf("Join: %v", err)
	}

	log.Println("[GO] Waiting for Artery handshake with 127.0.0.1:2552...")
	if err := node.WaitForHandshake(ctx, "127.0.0.1", 2552); err != nil {
		t.Fatalf("WaitForHandshake: %v", err)
	}
	log.Println("[GO] Handshake established.")

	// Allow a short settling time for cluster gossip.
	time.Sleep(2 * time.Second)

	// ── Sub-tests ─────────────────────────────────────────────────────────

	t.Run("GoProducer_ScalaConsumer", func(t *testing.T) {
		testGoProducerScalaConsumer(t, ctx, node, sig)
	})

	t.Run("ScalaProducer_GoConsumer", func(t *testing.T) {
		testScalaProducerGoConsumer(t, ctx, node, sig)
	})
}

// ---------------------------------------------------------------------------
// Sub-test: GoProducer_ScalaConsumer
// ---------------------------------------------------------------------------

// testGoProducerScalaConsumer spawns a Go ProducerController at /user/goProducer
// targeting Scala's ConsumerController at /user/scalaConsumer on port 2552.
// It sends msgCount messages and waits for Scala to print
// PEKKO_DELIVERY_RECEIVED:<text> for each one.
func testGoProducerScalaConsumer(t *testing.T, ctx context.Context, node *Cluster, sig *scalaSignals) {
	t.Helper()

	const msgCount = 5
	scalaConsumerPath := "pekko://GekkaSystem@127.0.0.1:2552/user/scalaConsumer"

	// Spawn Go's ProducerController at /user/goProducer.
	pc := delivery.NewProducerController("go-producer", scalaConsumerPath, delivery.DefaultWindowSize)
	goProducerRef, err := node.System.ActorOf(Props{New: func() actor.Actor {
		return actor.NewTypedActor[any](pc)
	}}, "goProducer")
	if err != nil {
		t.Fatalf("ActorOf goProducer: %v", err)
	}
	log.Printf("[GO→SCALA] ProducerController spawned at %s", goProducerRef.Path())

	// Wait for Scala's ConsumerController to send RegisterConsumer to Go's
	// ProducerController (Scala resolves /user/goProducer and registers).
	// Give Scala up to 30s to resolve Go's actor path.
	log.Println("[GO→SCALA] Waiting for PEKKO_DELIVERY_CONSUMER_READY...")
	select {
	case <-sig.deliveryConsumerReady:
		log.Println("[GO→SCALA] Scala ConsumerController is ready.")
	case <-time.After(30 * time.Second):
		t.Fatalf("PEKKO_DELIVERY_CONSUMER_READY not seen within 30s")
	}

	// Send msgCount messages via the ProducerController.
	for i := 0; i < msgCount; i++ {
		text := fmt.Sprintf("go-delivery-%d", i)
		goProducerRef.Tell(delivery.SendMessage{
			Payload:      []byte(text),
			SerializerID: 4, // ByteArraySerializer
		})
		log.Printf("[GO→SCALA] Enqueued message %d: %q", i, text)
	}

	// Collect PEKKO_DELIVERY_RECEIVED signals from Scala.
	received := make(map[string]bool)
	deadline := time.After(60 * time.Second)
	for len(received) < msgCount {
		select {
		case payload := <-sig.deliveryReceived:
			log.Printf("[GO→SCALA] Scala received: %q", payload)
			received[payload] = true
		case <-deadline:
			t.Fatalf("GoProducer_ScalaConsumer: only %d/%d messages received within 60s", len(received), msgCount)
		}
	}

	// Verify all expected messages were received.
	for i := 0; i < msgCount; i++ {
		want := fmt.Sprintf("go-delivery-%d", i)
		if !received[want] {
			t.Errorf("GoProducer_ScalaConsumer: missing message %q", want)
		}
	}
	log.Printf("[GO→SCALA] PASS — all %d messages delivered to Scala", msgCount)
}

// ---------------------------------------------------------------------------
// Sub-test: ScalaProducer_GoConsumer
// ---------------------------------------------------------------------------

// deliveryConsumerActor is a simple actor that receives Delivery messages from
// the ConsumerController, sends Confirmed back, and signals the test via a channel.
type deliveryConsumerActor struct {
	actor.BaseActor
	received chan string
}

func (a *deliveryConsumerActor) Receive(msg any) {
	switch m := msg.(type) {
	case delivery.Delivery:
		text := string(m.Msg.EnclosedMessage)
		log.Printf("[SCALA→GO] Delivery seqNr=%d payload=%q", m.SeqNr, text)
		// Confirm to the ConsumerController.
		m.ConfirmTo.Tell(delivery.Confirmed{SeqNr: m.SeqNr}, a.Self())
		select {
		case a.received <- text:
		default:
		}
	default:
		log.Printf("[SCALA→GO] deliveryConsumerActor: unhandled %T", msg)
	}
}

// testScalaProducerGoConsumer spawns Go's ConsumerController at /user/goConsumer
// and sends it a ConsumerStart pointing to Scala's ProducerController at
// /user/scalaProducer on port 2552.  It waits for all 10 messages that
// Scala's ReliableProducerBehavior sends, and verifies Scala prints
// PEKKO_DELIVERY_PRODUCER_NEXT:<n> for each.
func testScalaProducerGoConsumer(t *testing.T, ctx context.Context, node *Cluster, sig *scalaSignals) {
	t.Helper()

	const totalMessages = 10
	scalaProducerPath := "pekko://GekkaSystem@127.0.0.1:2552/user/scalaProducer"

	// Spawn the application consumer actor that will receive Delivery messages.
	receivedCh := make(chan string, totalMessages)
	appConsumerRef, err := node.System.ActorOf(Props{New: func() actor.Actor {
		a := &deliveryConsumerActor{received: receivedCh}
		a.BaseActor = actor.NewBaseActor()
		return a
	}}, "goDeliveryConsumerApp")
	if err != nil {
		t.Fatalf("ActorOf goDeliveryConsumerApp: %v", err)
	}
	log.Printf("[SCALA→GO] App consumer spawned at %s", appConsumerRef.Path())

	// Spawn Go's ConsumerController at /user/goConsumer.
	cc := delivery.NewConsumerController(appConsumerRef, delivery.DefaultWindowSize)
	goConsumerRef, err := node.System.ActorOf(Props{New: func() actor.Actor {
		return actor.NewTypedActor[any](cc)
	}}, "goConsumer")
	if err != nil {
		t.Fatalf("ActorOf goConsumer: %v", err)
	}
	log.Printf("[SCALA→GO] ConsumerController spawned at %s", goConsumerRef.Path())

	// Trigger registration with Scala's ProducerController.
	goConsumerRef.Tell(delivery.ConsumerStart{ProducerPath: scalaProducerPath})
	log.Printf("[SCALA→GO] Sent ConsumerStart targeting %s", scalaProducerPath)

	// Collect all delivered messages from the app consumer.
	received := make(map[string]bool)
	deadline := time.After(60 * time.Second)
	for len(received) < totalMessages {
		select {
		case text := <-receivedCh:
			log.Printf("[SCALA→GO] App consumer received: %q", text)
			received[text] = true
		case <-deadline:
			t.Fatalf("ScalaProducer_GoConsumer: only %d/%d messages received by Go within 60s",
				len(received), totalMessages)
		}
	}

	// Verify Scala also logged PEKKO_DELIVERY_PRODUCER_NEXT for each index.
	nextSeen := make(map[int]bool)
	collectDeadline := time.After(10 * time.Second)
collectLoop:
	for len(nextSeen) < totalMessages {
		select {
		case n := <-sig.deliveryProducerNext:
			nextSeen[n] = true
		case <-collectDeadline:
			break collectLoop
		}
	}

	for i := 0; i < totalMessages; i++ {
		if !nextSeen[i] {
			t.Errorf("ScalaProducer_GoConsumer: missing PEKKO_DELIVERY_PRODUCER_NEXT:%d", i)
		}
	}

	log.Printf("[SCALA→GO] PASS — all %d messages delivered to Go", totalMessages)
}
