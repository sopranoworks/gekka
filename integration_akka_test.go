//go:build integration

/*
 * integration_akka_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package gekka — E2E integration tests against a live Scala/Akka 2.6.x node.
//
// The test harness starts com.example.AkkaIntegrationNode via sbt and waits
// for the "AKKA_NODE_READY" signal before running any sub-test.
//
// Run with:
//
//	go test -v -tags integration -run TestAkkaIntegrationNode -timeout 300s .
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

// akkaSignals collects the named signals emitted by AkkaIntegrationNode on
// stdout so each sub-test can wait for the event it cares about.
type akkaSignals struct {
	ready          chan struct{} // closed when "AKKA_NODE_READY" is seen
	pubsubReceived chan string   // carries the payload of every "AKKA_PUBSUB_RECEIVED:<msg>" line
}

// startAkkaIntegrationNode launches com.example.AkkaIntegrationNode via sbt,
// registers a t.Cleanup to kill the process, starts a scanner goroutine that
// routes stdout lines to the returned akkaSignals channels, and returns once
// the process has started (before "AKKA_NODE_READY" appears).
func startAkkaIntegrationNode(t *testing.T, ctx context.Context) *akkaSignals {
	t.Helper()

	cmd := exec.CommandContext(ctx, "sbt", "akkaServer/runMain com.example.AkkaIntegrationNode")
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

	sig := &akkaSignals{
		ready:          make(chan struct{}),
		pubsubReceived: make(chan string, 16),
	}

	go func() {
		scanner := bufio.NewScanner(stdout)
		readyOnce := false
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("[SCALA] %s\n", line)

			if !readyOnce && strings.Contains(line, "AKKA_NODE_READY") {
				readyOnce = true
				close(sig.ready)
			}
			if strings.HasPrefix(line, "AKKA_PUBSUB_RECEIVED:") {
				payload := strings.TrimPrefix(line, "AKKA_PUBSUB_RECEIVED:")
				select {
				case sig.pubsubReceived <- payload:
				default:
				}
			}
		}
	}()

	return sig
}

// TestAkkaIntegrationNode is the top-level E2E test against Akka 2.6.x.
// It starts one AkkaIntegrationNode, initialises a Gekka cluster on port 2555,
// waits for the Artery handshake, then runs three independent sub-tests:
//
//   - RemoteAsk         — request/response to /user/echo
//   - PubSubBridge      — Go→Scala publish on the "bridge" topic
//   - ClusterMembership — Scala node visible as Up in Go's gossip view
func TestAkkaIntegrationNode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	t.Cleanup(cancel)

	// ── 1. Start Scala/Akka node ───────────────────────────────────────────
	sig := startAkkaIntegrationNode(t, ctx)

	log.Println("[WAIT] Waiting for AKKA_NODE_READY...")
	select {
	case <-sig.ready:
		log.Println("[SCALA] AkkaIntegrationNode is ready.")
	case <-ctx.Done():
		t.Fatalf("Akka node did not print AKKA_NODE_READY within timeout")
	}

	// ── 2. Initialize Gekka cluster on port 2555 ───────────────────────────
	selfAddr := actor.Address{
		Protocol: "akka",
		System:   "GekkaSystem",
		Host:     "127.0.0.1",
		Port:     2555,
	}
	node, err := NewCluster(ClusterConfig{Address: selfAddr})
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	t.Cleanup(func() { node.Shutdown() })
	log.Printf("[GO] Cluster node listening at %s", node.Addr())

	node.OnMessage(func(_ context.Context, msg *IncomingMessage) error {
		log.Printf("[GO] OnMessage: sid=%d manifest=%q len=%d", msg.SerializerId, msg.Manifest, len(msg.Payload))
		return nil
	})

	// ── 3. Join and wait for handshake ────────────────────────────────────
	log.Println("[GO] Joining GekkaSystem at 127.0.0.1:2554...")
	if err := node.Join("127.0.0.1", 2554); err != nil {
		t.Fatalf("Join: %v", err)
	}

	log.Println("[GO] Waiting for Artery handshake with 127.0.0.1:2554...")
	if err := node.WaitForHandshake(ctx, "127.0.0.1", 2554); err != nil {
		t.Fatalf("WaitForHandshake: %v", err)
	}
	log.Println("[GO] Handshake established.")

	// Allow a short settling time so gossip can propagate cluster state.
	time.Sleep(2 * time.Second)

	// ── Sub-tests ─────────────────────────────────────────────────────────

	t.Run("RemoteAsk", func(t *testing.T) {
		testAkkaRemoteAsk(t, ctx, node)
	})

	t.Run("PubSubBridge", func(t *testing.T) {
		testAkkaPubSubBridge(t, ctx, node, sig)
	})

	t.Run("ClusterMembership", func(t *testing.T) {
		testAkkaClusterMembership(t, node)
	})
}

// ---------------------------------------------------------------------------
// Sub-test: RemoteAsk
// ---------------------------------------------------------------------------

func testAkkaRemoteAsk(t *testing.T, ctx context.Context, node *Cluster) {
	t.Helper()

	askCtx, askCancel := context.WithTimeout(ctx, 30*time.Second)
	defer askCancel()

	akkaEcho := actor.Address{
		Protocol: "akka",
		System:   "GekkaSystem",
		Host:     "127.0.0.1",
		Port:     2554,
	}.WithRoot("user").Child("echo")

	msg := []byte("Hello from Gekka")
	log.Printf("[ASK] Sending Ask to %s with msg %q", akkaEcho, msg)

	reply, err := node.Ask(askCtx, akkaEcho, msg)
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

func testAkkaPubSubBridge(t *testing.T, ctx context.Context, node *Cluster, sig *akkaSignals) {
	t.Helper()

	ser := &pubsub.PubSubSerializer{}
	const topic = "bridge"
	msgPayload := []byte("hello from gekka")

	pubBytes, err := ser.EncodePublish(topic, msgPayload, 4 /* ByteArraySerializer */, "")
	if err != nil {
		t.Fatalf("EncodePublish: %v", err)
	}

	assoc, ok := node.nm.GetAssociationByHost("127.0.0.1", 2554)
	if !ok {
		t.Fatalf("no association to 127.0.0.1:2554 — WaitForHandshake should have ensured this")
	}

	mediatorPath := fmt.Sprintf("akka://GekkaSystem@127.0.0.1:2554/system/distributedPubSubMediator")

	log.Printf("[PUBSUB] Publishing %q to topic %q via %s", msgPayload, topic, mediatorPath)
	if err := assoc.Send(mediatorPath, pubBytes, pubsub.PubSubSerializerID, pubsub.PublishManifest); err != nil {
		t.Fatalf("assoc.Send Publish: %v", err)
	}

	select {
	case received := <-sig.pubsubReceived:
		want := string(msgPayload)
		if received != want {
			t.Errorf("PubSubBridge: received %q, want %q", received, want)
		} else {
			log.Printf("[PUBSUB] PASS — received %q", received)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("PubSubBridge: no AKKA_PUBSUB_RECEIVED within 30s")
	}
}

// ---------------------------------------------------------------------------
// Sub-test: ClusterMembership
// ---------------------------------------------------------------------------

func testAkkaClusterMembership(t *testing.T, node *Cluster) {
	t.Helper()

	const timeout = 30 * time.Second
	deadline := time.After(timeout)

	for {
		state := node.cm.GetState()
		members := state.GetMembers()
		allAddrs := state.GetAllAddresses()

		upCount := 0
		akkaUp := false

		for _, m := range members {
			if m.GetStatus() != gproto_cluster.MemberStatus_Up {
				continue
			}
			upCount++
			idx := int(m.GetAddressIndex())
			if idx < len(allAddrs) {
				if allAddrs[idx].GetAddress().GetPort() == 2554 {
					akkaUp = true
				}
			}
		}

		if upCount >= 2 && akkaUp {
			log.Printf("[MEMBERSHIP] PASS — %d Up members, Akka (2554) is Up", upCount)
			return
		}

		select {
		case <-deadline:
			t.Fatalf("ClusterMembership: after %v: upCount=%d akkaUp=%v", timeout, upCount, akkaUp)
		case <-time.After(500 * time.Millisecond):
		}
	}
}
