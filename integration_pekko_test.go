//go:build integration

/*
 * integration_pekko_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package gekka — E2E integration tests against a live Scala/Pekko node.
//
// The test harness starts com.example.PekkoIntegrationNode via sbt and waits
// for the "PEKKO_NODE_READY" signal before running any sub-test.
//
// Run with:
//
//	go test -v -tags integration -run TestPekkoIntegrationNode -timeout 300s .
package gekka

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster/pubsub"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
)

// scalaSignals collects the named signals emitted by PekkoIntegrationNode on
// stdout so each sub-test can wait for the event it cares about.
type scalaSignals struct {
	ready          chan struct{} // closed when "PEKKO_NODE_READY" is seen
	pubsubReceived chan string   // carries the payload of every "PEKKO_PUBSUB_RECEIVED:<msg>" line
}

// startPekkoIntegrationNode launches com.example.PekkoIntegrationNode via sbt,
// registers a t.Cleanup to kill the process, starts a scanner goroutine that
// routes stdout lines to the returned scalaSignals channels, and returns once
// the process has started (before "PEKKO_NODE_READY" appears).
func startPekkoIntegrationNode(t *testing.T, ctx context.Context) *scalaSignals {
	t.Helper()

	cmd := exec.CommandContext(ctx, "sbt", "runMain com.example.PekkoIntegrationNode")
	cmd.Dir = "scala-server"

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	cmd.Stderr = cmd.Stdout // merge stderr into the same scanner

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start sbt: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
			cmd.Wait() //nolint:errcheck
		}
	})

	sig := &scalaSignals{
		ready:          make(chan struct{}),
		pubsubReceived: make(chan string, 16),
	}

	go func() {
		scanner := bufio.NewScanner(stdout)
		readyOnce := false
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("[SCALA] %s\n", line)

			if !readyOnce && strings.Contains(line, "PEKKO_NODE_READY") {
				readyOnce = true
				close(sig.ready)
			}
			if strings.HasPrefix(line, "PEKKO_PUBSUB_RECEIVED:") {
				payload := strings.TrimPrefix(line, "PEKKO_PUBSUB_RECEIVED:")
				select {
				case sig.pubsubReceived <- payload:
				default:
				}
			}
		}
	}()

	return sig
}

// TestPekkoIntegrationNode is the top-level E2E test.  It starts one
// PekkoIntegrationNode, initialises a Gekka cluster on port 2553, waits for
// the Artery handshake, then runs three independent sub-tests:
//
//   - RemoteAsk        — request/response to /user/echo
//   - PubSubBridge     — Go→Scala publish on the "bridge" topic
//   - ClusterMembership — Scala node visible as Up in Go's gossip view
func TestPekkoIntegrationNode(t *testing.T) {
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

	// Register an OnMessage handler for the Ask sub-test (replies go here
	// automatically via the pending-reply map, but the handler also captures
	// any unexpected messages for debugging).
	node.OnMessage(func(_ context.Context, msg *IncomingMessage) error {
		log.Printf("[GO] OnMessage: sid=%d manifest=%q len=%d", msg.SerializerId, msg.Manifest, len(msg.Payload))
		return nil
	})

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

	// Allow a short settling time so gossip can propagate cluster state.
	time.Sleep(2 * time.Second)

	// ── Sub-tests ─────────────────────────────────────────────────────────

	t.Run("RemoteAsk", func(t *testing.T) {
		testRemoteAsk(t, ctx, node)
	})

	t.Run("PubSubBridge", func(t *testing.T) {
		testPubSubBridge(t, ctx, node, sig)
	})

	t.Run("ClusterMembership", func(t *testing.T) {
		testClusterMembership(t, node)
	})
}

// ---------------------------------------------------------------------------
// Sub-test: RemoteAsk
// ---------------------------------------------------------------------------

// testRemoteAsk sends a message to the Scala EchoActor at /user/echo using
// the Ask pattern and asserts the "Echo: <msg>" response.
func testRemoteAsk(t *testing.T, ctx context.Context, node *Cluster) {
	t.Helper()

	askCtx, askCancel := context.WithTimeout(ctx, 30*time.Second)
	defer askCancel()

	scalaEcho := actor.Address{
		Protocol: "pekko",
		System:   "GekkaSystem",
		Host:     "127.0.0.1",
		Port:     2552,
	}.WithRoot("user").Child("echo")

	msg := []byte("Hello from Gekka")
	log.Printf("[ASK] Sending Ask to %s with msg %q", scalaEcho, msg)

	reply, err := node.Ask(askCtx, scalaEcho, msg)
	if err != nil {
		t.Fatalf("Ask: %v", err)
	}
	if reply.SerializerId != 4 {
		t.Errorf("reply SerializerId = %d, want 4 (ByteArraySerializer)", reply.SerializerId)
	}

	got := string(reply.Payload)
	want := "Echo: Hello from Gekka"
	if got != want {
		t.Errorf("Ask reply = %q, want %q", got, want)
	} else {
		log.Printf("[ASK] PASS — got %q", got)
	}
}

// ---------------------------------------------------------------------------
// Sub-test: PubSubBridge
// ---------------------------------------------------------------------------

// testPubSubBridge publishes a message from Go to the Scala DistributedPubSub
// "bridge" topic and verifies the BridgeSubscriber prints
// "PEKKO_PUBSUB_RECEIVED:<payload>" on stdout.
func testPubSubBridge(t *testing.T, ctx context.Context, node *Cluster, sig *scalaSignals) {
	t.Helper()

	// Build the Publish envelope using gekka's PubSubSerializer.
	ser := &pubsub.PubSubSerializer{}
	const topic = "bridge"
	msgPayload := []byte("hello from gekka")

	pubBytes, err := ser.EncodePublish(topic, msgPayload, 4 /* ByteArraySerializer */, "")
	if err != nil {
		t.Fatalf("EncodePublish: %v", err)
	}

	// Resolve the association to the Scala mediator (already established via Join).
	assoc, ok := node.nm.GetAssociationByHost("127.0.0.1", 2552)
	if !ok {
		t.Fatalf("no association to 127.0.0.1:2552 — WaitForHandshake should have ensured this")
	}

	// The DistributedPubSubMediator lives at /system/distributedPubSubMediator.
	mediatorPath := fmt.Sprintf("pekko://GekkaSystem@127.0.0.1:2552/system/distributedPubSubMediator")

	log.Printf("[PUBSUB] Publishing %q to topic %q via %s", msgPayload, topic, mediatorPath)
	if err := assoc.Send(mediatorPath, pubBytes, pubsub.PubSubSerializerID, pubsub.PublishManifest); err != nil {
		t.Fatalf("assoc.Send Publish: %v", err)
	}

	// Wait for the Scala subscriber to echo the received message on stdout.
	select {
	case received := <-sig.pubsubReceived:
		want := string(msgPayload)
		if received != want {
			t.Errorf("PubSubBridge: received %q, want %q", received, want)
		} else {
			log.Printf("[PUBSUB] PASS — received %q", received)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("PubSubBridge: no PEKKO_PUBSUB_RECEIVED within 30s")
	}
}

// ---------------------------------------------------------------------------
// Sub-test: ClusterMembership
// ---------------------------------------------------------------------------

// testClusterMembership asserts that:
//  1. The Scala seed node (127.0.0.1:2552) appears in Go's gossip state.
//  2. It has MemberStatus_Up.
//  3. There are at least 2 Up members (Scala + Go).
func testClusterMembership(t *testing.T, node *Cluster) {
	t.Helper()

	const timeout = 30 * time.Second
	deadline := time.After(timeout)

	for {
		state := node.cm.GetState()
		members := state.GetMembers()
		allAddrs := state.GetAllAddresses()

		upCount := 0
		scalaUp := false

		for _, m := range members {
			if m.GetStatus() != gproto_cluster.MemberStatus_Up {
				continue
			}
			upCount++
			idx := int(m.GetAddressIndex())
			if idx < len(allAddrs) {
				if allAddrs[idx].GetAddress().GetPort() == 2552 {
					scalaUp = true
				}
			}
		}

		if upCount >= 2 && scalaUp {
			log.Printf("[MEMBERSHIP] PASS — %d Up members, Scala (2552) is Up", upCount)
			return
		}

		select {
		case <-deadline:
			t.Fatalf("ClusterMembership: after %v: upCount=%d scalaUp=%v", timeout, upCount, scalaUp)
		case <-time.After(500 * time.Millisecond):
		}
	}
}
