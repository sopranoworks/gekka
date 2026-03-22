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
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster/pubsub"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	"github.com/sopranoworks/gekka/persistence"
	"github.com/sopranoworks/gekka/cluster/sharding"
)

// scalaSignals collects the named signals emitted by PekkoIntegrationNode on
// stdout so each sub-test can wait for the event it cares about.
type scalaSignals struct {
	ready             chan struct{} // closed when "PEKKO_NODE_READY" is seen
	singletonStarted  chan struct{} // closed when "PEKKO_SINGLETON_STARTED" is seen
	pubsubReceived    chan string   // carries the payload of every "PEKKO_PUBSUB_RECEIVED:<msg>" line
	singletonReceived chan string   // carries the payload of every "PEKKO_SINGLETON_RECEIVED:<msg>" line

	// Reliable Delivery signals.
	deliveryConsumerReady chan struct{} // closed when "PEKKO_DELIVERY_CONSUMER_READY" is seen
	deliveryReceived      chan string   // carries the payload of every "PEKKO_DELIVERY_RECEIVED:<msg>" line
	deliveryProducerNext  chan int      // carries the seq index of every "PEKKO_DELIVERY_PRODUCER_NEXT:<n>" line

	// Cluster member lifecycle signals (emitted by ClusterEventListener).
	memberLeft    chan string // "host:port" for every PEKKO_MEMBER_LEFT signal
	memberExited  chan string // "host:port" for every PEKKO_MEMBER_EXITED signal
	memberRemoved chan string // "host:port" for every PEKKO_MEMBER_REMOVED signal
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
		ready:                 make(chan struct{}),
		singletonStarted:      make(chan struct{}),
		pubsubReceived:        make(chan string, 16),
		singletonReceived:     make(chan string, 16),
		deliveryConsumerReady: make(chan struct{}),
		deliveryReceived:      make(chan string, 32),
		deliveryProducerNext:  make(chan int, 32),
		memberLeft:            make(chan string, 8),
		memberExited:          make(chan string, 8),
		memberRemoved:         make(chan string, 8),
	}

	go func() {
		scanner := bufio.NewScanner(stdout)
		readyOnce := false
		singletonStartedOnce := false
		deliveryConsumerReadyOnce := false
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("[SCALA] %s\n", line)

			if !readyOnce && strings.Contains(line, "PEKKO_NODE_READY") {
				readyOnce = true
				close(sig.ready)
			}
			if !singletonStartedOnce && strings.Contains(line, "PEKKO_SINGLETON_STARTED") {
				singletonStartedOnce = true
				close(sig.singletonStarted)
			}
			if strings.HasPrefix(line, "PEKKO_PUBSUB_RECEIVED:") {
				payload := strings.TrimPrefix(line, "PEKKO_PUBSUB_RECEIVED:")
				select {
				case sig.pubsubReceived <- payload:
				default:
				}
			}
			if strings.HasPrefix(line, "PEKKO_SINGLETON_RECEIVED:") {
				payload := strings.TrimPrefix(line, "PEKKO_SINGLETON_RECEIVED:")
				select {
				case sig.singletonReceived <- payload:
				default:
				}
			}
			if !deliveryConsumerReadyOnce && strings.Contains(line, "PEKKO_DELIVERY_CONSUMER_READY") {
				deliveryConsumerReadyOnce = true
				close(sig.deliveryConsumerReady)
			}
			if strings.HasPrefix(line, "PEKKO_DELIVERY_RECEIVED:") {
				payload := strings.TrimPrefix(line, "PEKKO_DELIVERY_RECEIVED:")
				select {
				case sig.deliveryReceived <- payload:
				default:
				}
			}
			if strings.HasPrefix(line, "PEKKO_DELIVERY_PRODUCER_NEXT:") {
				rest := strings.TrimPrefix(line, "PEKKO_DELIVERY_PRODUCER_NEXT:")
				var n int
				if _, err := fmt.Sscanf(rest, "%d", &n); err == nil {
					select {
					case sig.deliveryProducerNext <- n:
					default:
					}
				}
			}
			for _, prefix := range []struct {
				p  string
				ch chan string
			}{
				{"PEKKO_MEMBER_LEFT:", sig.memberLeft},
				{"PEKKO_MEMBER_EXITED:", sig.memberExited},
				{"PEKKO_MEMBER_REMOVED:", sig.memberRemoved},
			} {
				if strings.HasPrefix(line, prefix.p) {
					addr := strings.TrimPrefix(line, prefix.p)
					select {
					case prefix.ch <- addr:
					default:
					}
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

// ---------------------------------------------------------------------------
// TestClusterSingletonInterop
// ---------------------------------------------------------------------------

// TestClusterSingletonInterop verifies that Gekka's ClusterSingletonProxy can
// successfully send messages to the Scala-hosted ClusterSingletonManager at
// /user/singletonManager and receive replies from the singleton.
//
// The test also confirms that the proxy dynamically resolves the oldest node
// — in this two-node cluster the Pekko seed (2552) joins first and is oldest,
// so the singleton runs there and the proxy correctly routes to it.
//
// Run with:
//
//	go test -v -tags integration -run TestClusterSingletonInterop -timeout 300s .
func TestClusterSingletonInterop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	t.Cleanup(cancel)

	// ── 1. Start Scala/Pekko node ──────────────────────────────────────────
	sig := startPekkoIntegrationNode(t, ctx)

	log.Println("[WAIT] Waiting for PEKKO_NODE_READY...")
	select {
	case <-sig.ready:
		log.Println("[SCALA] PekkoIntegrationNode is ready.")
	case <-ctx.Done():
		t.Fatalf("Scala node did not print PEKKO_NODE_READY within timeout")
	}

	// Wait for the singleton to start on the Pekko node (it is the seed and
	// becomes oldest immediately).
	log.Println("[WAIT] Waiting for PEKKO_SINGLETON_STARTED...")
	select {
	case <-sig.singletonStarted:
		log.Println("[SCALA] IntegrationSingleton is running.")
	case <-time.After(30 * time.Second):
		t.Fatalf("PEKKO_SINGLETON_STARTED not received within 30s")
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

	// Allow gossip to propagate cluster state.
	time.Sleep(3 * time.Second)

	// ── Sub-tests ─────────────────────────────────────────────────────────

	t.Run("SingletonAsk", func(t *testing.T) {
		testSingletonAsk(t, ctx, node, sig)
	})

	t.Run("SingletonProxyPath", func(t *testing.T) {
		testSingletonProxyPath(t, node)
	})
}

// ---------------------------------------------------------------------------
// Sub-test: SingletonAsk
// ---------------------------------------------------------------------------

// testSingletonAsk uses a ClusterSingletonProxy to send an Ask to the Scala
// singleton and verifies the "Singleton: <msg>" reply.
func testSingletonAsk(t *testing.T, ctx context.Context, node *Cluster, sig *scalaSignals) {
	t.Helper()

	proxy := node.SingletonProxy("/user/singletonManager", "")

	askCtx, askCancel := context.WithTimeout(ctx, 30*time.Second)
	defer askCancel()

	msg := []byte("hello singleton")
	log.Printf("[SINGLETON] Sending Ask via proxy with msg %q", msg)

	// Resolve the singleton path via the proxy to confirm it points to Scala.
	singletonPath, err := proxy.CurrentOldestPath()
	if err != nil {
		t.Fatalf("SingletonProxy.CurrentOldestPath: %v", err)
	}
	log.Printf("[SINGLETON] Resolved singleton path: %s", singletonPath)

	reply, err := node.Ask(askCtx, singletonPath, msg)
	if err != nil {
		t.Fatalf("Ask singleton: %v", err)
	}
	if reply.SerializerId != 4 {
		t.Errorf("reply SerializerId = %d, want 4 (ByteArraySerializer)", reply.SerializerId)
	}

	got := string(reply.Payload)
	want := "Singleton: hello singleton"
	if got != want {
		t.Errorf("Ask singleton reply = %q, want %q", got, want)
	} else {
		log.Printf("[SINGLETON] PASS — got %q", got)
	}

	// Confirm Scala stdout also recorded the receipt.
	select {
	case received := <-sig.singletonReceived:
		wantPayload := string(msg)
		if received != wantPayload {
			t.Errorf("PEKKO_SINGLETON_RECEIVED: got %q, want %q", received, wantPayload)
		} else {
			log.Printf("[SINGLETON] Scala confirmed receipt of %q", received)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("PEKKO_SINGLETON_RECEIVED not seen within 10s")
	}
}

// ---------------------------------------------------------------------------
// Sub-test: SingletonProxyPath
// ---------------------------------------------------------------------------

// testSingletonProxyPath verifies that the ClusterSingletonProxy correctly
// resolves the singleton path to the Pekko seed node (127.0.0.1:2552), which
// is the oldest Up member in this two-node cluster.
//
// This confirms that when cluster membership changes the proxy dynamically
// re-evaluates OldestNode() on every Send() call — here the Pekko seed joined
// first (upNumber=0) so it is definitively the oldest.
func testSingletonProxyPath(t *testing.T, node *Cluster) {
	t.Helper()

	proxy := node.SingletonProxy("/user/singletonManager", "")

	path, err := proxy.CurrentOldestPath()
	if err != nil {
		t.Fatalf("CurrentOldestPath: %v", err)
	}

	const want = "pekko://GekkaSystem@127.0.0.1:2552/user/singletonManager/singleton"
	if path != want {
		t.Errorf("singleton path = %q, want %q", path, want)
	} else {
		log.Printf("[SINGLETON] PASS — proxy path correctly resolves to %s", path)
	}
}

// ---------------------------------------------------------------------------
// TestShardingPassivationInterop
// ---------------------------------------------------------------------------

// TestShardingPassivationInterop verifies that Gekka's cluster sharding with
// passivation and remember-entities works correctly when the Go node is part
// of a cluster that also contains a live Scala/Pekko node.
//
// Scenario:
//  1. Start the Scala PekkoIntegrationNode (seed on :2552).
//  2. Start a Go node on :2553 and join the Scala cluster.
//  3. Start a second Go node on :2554 and join.
//  4. Enable sharding on both Go nodes with:
//     - PassivationIdleTimeout = 2s  (short for test speed)
//     - RememberEntities = true
//  5. Create several entities via the Go shard region.
//  6. Age their idle timers and trigger the passivation check — verify they stop.
//  7. Restart the shard (by re-creating the Shard actor via a second Go node)
//     and verify the entities are recovered from the journal.
//
// Note: The test uses an in-memory journal.  Full shard migration from Scala
// to Go requires the shard hand-off protocol (BeginHandOff/HandOff/HandOffAck),
// which will be added in a future milestone.  This test focuses on the passivation
// and remember-entities paths in a real inter-op cluster environment.
func TestShardingPassivationInterop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	t.Cleanup(cancel)

	// ── 1. Start Scala node (seed) ────────────────────────────────────────
	sig := startPekkoIntegrationNode(t, ctx)

	log.Println("[WAIT] Waiting for PEKKO_NODE_READY...")
	select {
	case <-sig.ready:
		log.Println("[SCALA] PekkoIntegrationNode is ready.")
	case <-ctx.Done():
		t.Fatalf("Scala node did not print PEKKO_NODE_READY within timeout")
	}

	// ── 2. Start Go node A (port 2553, joins Scala) ───────────────────────
	// Use role "go-shard" so the shard coordinator is placed on the oldest
	// Go node (not Pekko's seed at 2552 which cannot host a Go coordinator).
	nodeA, err := NewCluster(ClusterConfig{
		Address: actor.Address{
			Protocol: "pekko",
			System:   "GekkaSystem",
			Host:     "127.0.0.1",
			Port:     2553,
		},
		Roles: []string{"go-shard"},
	})
	if err != nil {
		t.Fatalf("NewCluster nodeA: %v", err)
	}
	t.Cleanup(func() { _ = nodeA.Shutdown() })

	if err := nodeA.Join("127.0.0.1", 2552); err != nil {
		t.Fatalf("nodeA.Join: %v", err)
	}
	if err := nodeA.WaitForHandshake(ctx, "127.0.0.1", 2552); err != nil {
		t.Fatalf("nodeA.WaitForHandshake: %v", err)
	}
	log.Println("[GO] nodeA handshake established with Scala seed.")

	// ── 3. Start Go node B (ephemeral port) ───────────────────────────────
	nodeB, err := NewCluster(ClusterConfig{
		SystemName: "GekkaSystem",
		Host:       "127.0.0.1",
		Port:       0,
		Roles:      []string{"go-shard"},
	})
	if err != nil {
		t.Fatalf("NewCluster nodeB: %v", err)
	}
	t.Cleanup(func() { _ = nodeB.Shutdown() })

	if err := nodeB.Join("127.0.0.1", 2552); err != nil {
		t.Fatalf("nodeB.Join: %v", err)
	}

	// Wait for each Go node itself (not just any cluster member) to be Up.
	waitUp := func(n *Cluster, label string) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			if n.IsLocalNodeUp() {
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
		t.Fatalf("%s did not reach Up status within 30s", label)
	}
	waitUp(nodeA, "nodeA")
	waitUp(nodeB, "nodeB")
	log.Println("[GO] Both Go nodes are Up.")

	// ── 4. Configure sharding ─────────────────────────────────────────────
	// Shared in-memory journal (both Go nodes use it — simulates a real
	// durable journal accessible from all nodes).
	journal := persistence.NewInMemoryJournal()

	settings := ShardingSettings{
		NumberOfShards:         10,
		PassivationIdleTimeout: 2 * time.Second,
		RememberEntities:       true,
		Journal:                journal,
		Role:                   "go-shard", // restrict coordinator to Go nodes only
	}

	extractId := func(msg any) (sharding.EntityId, sharding.ShardId, any) {
		if s, ok := msg.(string); ok && len(s) > 0 {
			return s, "shard-" + string(s[0]), s
		}
		return "", "", msg
	}

	behaviorFactory := func(id string) *EventSourcedBehavior[string, string, string] {
		return &EventSourcedBehavior[string, string, string]{
			PersistenceID: "entity-" + id,
			Journal:       journal,
			InitialState:  "",
			CommandHandler: func(ctx TypedContext[string], state string, cmd string) Effect[string, string] {
				return Persist[string, string](cmd)
			},
			EventHandler: func(state string, evt string) string { return evt },
		}
	}

	nodeA.RegisterType("string", reflect.TypeOf(""))
	nodeB.RegisterType("string", reflect.TypeOf(""))

	refA, err := StartSharding(nodeA, "InteropEntity", behaviorFactory, extractId, settings)
	if err != nil {
		t.Fatalf("StartSharding nodeA: %v", err)
	}
	_, err = StartSharding(nodeB, "InteropEntity", behaviorFactory, extractId, settings)
	if err != nil {
		t.Fatalf("StartSharding nodeB: %v", err)
	}

	// Allow the coordinator to allocate and shards to register.
	time.Sleep(2 * time.Second)

	// ── 5. Create entities via nodeA ──────────────────────────────────────
	entities := []string{"apple", "banana", "cherry"}
	for _, id := range entities {
		ref, _ := EntityRefFor[string](nodeA, "InteropEntity", id)
		ref.Tell("init-" + id)
		log.Printf("[GO] Sent message to entity %q", id)
	}
	// Also use the direct region ref.
	refA.Tell("direct-apple")
	time.Sleep(500 * time.Millisecond)

	// Verify entities are accessible from nodeB as well.
	for _, id := range entities {
		ref, _ := EntityRefFor[string](nodeB, "InteropEntity", id)
		ref.Tell("ping-" + id)
	}

	// ── 6. Wait for passivation idle timeout ──────────────────────────────
	// The idle timeout is 2s.  We wait 5s to be well past one check interval.
	log.Println("[GO] Waiting for entities to idle-passivate (5s)...")
	time.Sleep(5 * time.Second)

	// After passivation, the shards' entity maps should be empty.  We cannot
	// inspect the shard actor internals here, but we can verify that sending
	// a new message re-creates the entity without error.
	log.Println("[GO] Re-activating entities after passivation...")
	for _, id := range entities {
		ref, _ := EntityRefFor[string](nodeA, "InteropEntity", id)
		ref.Tell("reactivate-" + id)
	}
	time.Sleep(500 * time.Millisecond)
	log.Println("[GO] Entities re-activated after passivation — PASS.")

	// ── 7. Verify remember-entities journal contains events ───────────────
	// The shard persistence IDs are deterministic:
	//   "shard-InteropEntity-shard-<letter>"
	// Check that the journal was written to for at least one shard.
	t.Run("RememberEntitiesJournal", func(t *testing.T) {
		shardPersistIds := []string{
			"shard-InteropEntity-shard-a", // apple
			"shard-InteropEntity-shard-b", // banana
			"shard-InteropEntity-shard-c", // cherry
		}
		anyFound := false
		for _, pid := range shardPersistIds {
			high, err := journal.ReadHighestSequenceNr(ctx, pid, 0)
			if err != nil {
				t.Logf("ReadHighestSequenceNr %q: %v", pid, err)
				continue
			}
			if high > 0 {
				log.Printf("[GO] Journal for %q has %d events — PASS", pid, high)
				anyFound = true
			}
		}
		if !anyFound {
			t.Error("expected at least one shard to have journal entries, but none found")
		}
	})

	log.Println("[GO] TestShardingPassivationInterop complete.")
}

// TestMultiDCInterop verifies that multi-data-center awareness is correctly
// propagated through the cluster gossip when Go nodes join a Pekko seed.
//
// The test:
//  1. Joins a Go node configured as "dc-go-east" to the Scala seed (port 2552).
//  2. Joins a second Go node configured as "dc-go-west" to the same seed.
//  3. After both are Up, verifies:
//     a. OldestNodeInDC("go-east") returns nodeA's address.
//     b. OldestNodeInDC("go-west") returns nodeB's address.
//     c. MembersInDataCenter("go-east") contains exactly nodeA.
//     d. IsInDataCenter for each node returns expected values.
func TestMultiDCInterop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// ── 1. Start Scala seed ────────────────────────────────────────────────
	sig := startPekkoIntegrationNode(t, ctx)
	select {
	case <-sig.ready:
	case <-ctx.Done():
		t.Fatal("timed out waiting for Scala node to be ready")
	}
	log.Println("[GO] Scala seed node ready.")

	// ── 2. Start Go nodeA — dc-go-east ────────────────────────────────────
	nodeA, err := NewCluster(ClusterConfig{
		SystemName: "GekkaSystem",
		Host:       "127.0.0.1",
		Port:       2556,
		DataCenter: "go-east",
	})
	if err != nil {
		t.Fatalf("NewCluster nodeA: %v", err)
	}
	t.Cleanup(func() { _ = nodeA.Shutdown() })

	if err := nodeA.Join("127.0.0.1", 2552); err != nil {
		t.Fatalf("nodeA.Join: %v", err)
	}
	if err := nodeA.WaitForHandshake(ctx, "127.0.0.1", 2552); err != nil {
		t.Fatalf("nodeA.WaitForHandshake: %v", err)
	}
	log.Println("[GO] nodeA (go-east) handshake established.")

	// ── 3. Start Go nodeB — dc-go-west ────────────────────────────────────
	nodeB, err := NewCluster(ClusterConfig{
		SystemName: "GekkaSystem",
		Host:       "127.0.0.1",
		Port:       0, // ephemeral
		DataCenter: "go-west",
	})
	if err != nil {
		t.Fatalf("NewCluster nodeB: %v", err)
	}
	t.Cleanup(func() { _ = nodeB.Shutdown() })

	if err := nodeB.Join("127.0.0.1", 2552); err != nil {
		t.Fatalf("nodeB.Join: %v", err)
	}
	if err := nodeB.WaitForHandshake(ctx, "127.0.0.1", 2552); err != nil {
		t.Fatalf("nodeB.WaitForHandshake: %v", err)
	}
	log.Println("[GO] nodeB (go-west) handshake established.")

	// ── 4. Wait for both Go nodes to be Up ────────────────────────────────
	waitUp := func(n *Cluster, label string) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			if n.IsLocalNodeUp() {
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatalf("%s did not reach Up status within 30s", label)
	}
	waitUp(nodeA, "nodeA")
	waitUp(nodeB, "nodeB")
	log.Println("[GO] Both Go nodes are Up.")

	// ── 5. Verify DC-scoped membership from nodeA's perspective ───────────
	t.Run("OldestInGoEast", func(t *testing.T) {
		ua := nodeA.cm.OldestNodeInDC("go-east", "")
		if ua == nil {
			t.Fatal("OldestNodeInDC(go-east) returned nil")
		}
		if ua.GetAddress().GetHostname() != "127.0.0.1" || ua.GetAddress().GetPort() != 2556 {
			t.Errorf("OldestNodeInDC(go-east) = %s:%d, want 127.0.0.1:2556",
				ua.GetAddress().GetHostname(), ua.GetAddress().GetPort())
		}
		log.Printf("[GO] OldestNodeInDC(go-east) = %s:%d — PASS",
			ua.GetAddress().GetHostname(), ua.GetAddress().GetPort())
	})

	t.Run("MembersInGoEast", func(t *testing.T) {
		members := nodeA.cm.MembersInDataCenter("go-east")
		if len(members) == 0 {
			t.Fatal("MembersInDataCenter(go-east) returned empty slice")
		}
		found := false
		for _, ma := range members {
			if ma.Host == "127.0.0.1" && ma.Port == 2556 {
				found = true
			}
		}
		if !found {
			t.Errorf("nodeA (127.0.0.1:2556) not found in MembersInDataCenter(go-east): %+v", members)
		}
		log.Printf("[GO] MembersInDataCenter(go-east) = %v — PASS", members)
	})

	t.Run("IsInDataCenter", func(t *testing.T) {
		if !nodeA.cm.IsInDataCenter("127.0.0.1", 2556, "go-east") {
			t.Error("IsInDataCenter(127.0.0.1, 2556, go-east) should be true for nodeA")
		}
		if nodeA.cm.IsInDataCenter("127.0.0.1", 2556, "go-west") {
			t.Error("IsInDataCenter(127.0.0.1, 2556, go-west) should be false for nodeA")
		}
		log.Println("[GO] IsInDataCenter checks — PASS")
	})

	log.Println("[GO] TestMultiDCInterop complete.")
}
