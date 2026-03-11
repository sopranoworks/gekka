//go:build integration

/*
 * integration_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster"
)

// remoteSystem constructs an actor.Address for the Scala test server.
func remoteSystem(system, host string, port int) actor.Address {
	return actor.Address{Protocol: "pekko", System: system, Host: host, Port: port}
}

func TestIntegration_PekkoServer(t *testing.T) {
	// 1. [STARTING SERVER]
	log.Printf("[STARTING SERVER] Initializing Scala Pekko server...")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sbt", "runMain com.example.PekkoServer")
	cmd.Dir = "../scala-server"

	stdout, _ := cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start sbt: %v", err)
	}
	defer func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
	}()

	ready := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(stdout)
		readyOnce := false
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("[SCALA] %s\n", line)
			if !readyOnce && strings.Contains(line, "--- PEKKO SERVER READY ---") {
				readyOnce = true
				close(ready)
			}
		}
	}()

	select {
	case <-ready:
		log.Printf("[SERVER READY] Scala server is listening.")
	case <-ctx.Done():
		t.Fatalf("server failed to start within timeout. Check sbt logs.")
	}

	// 2. Spawn Go node (port 0 = OS-assigned)
	node, err := Spawn(NodeConfig{SystemName: "GoClient", Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	defer node.Shutdown()
	t.Logf("Go client is listening on %s", node.Addr())

	// Channel to receive the reply
	replyChan := make(chan string, 1)
	node.OnMessage(func(ctx context.Context, msg *IncomingMessage) error {
		if msg.SerializerId == 4 { // ByteArraySerializer
			reply := string(msg.Payload)
			log.Printf("[RECEIVING] Received reply: %s", reply)
			replyChan <- reply
		}
		return nil
	})

	// 3. [HANDSHAKE] Initiate connection using typed actor path
	remote := remoteSystem("RemoteSystem", "127.0.0.1", 2552)
	echoPath := remote.WithRoot("user").Child("echo")

	log.Printf("[HANDSHAKE] Initiating handshake with %s", echoPath)
	go func() {
		if err := node.Send(ctx, echoPath, []byte("probe")); err != nil {
			log.Printf("IntegrationTest: probe send error: %v", err)
		}
	}()

	log.Printf("[WAITING] Waiting for Artery handshake...")
	if err := node.WaitForHandshake(ctx, "127.0.0.1", 2552); err != nil {
		t.Fatalf("[TIMEOUT] %v", err)
	}
	log.Printf("[ASSOCIATED] Handshake successful.")

	// 4. [SENDING] Send using the typed path
	log.Printf("[SENDING] Sending 'Hello from Go' to %s", echoPath)
	if err := node.Send(ctx, echoPath, []byte("Hello from Go")); err != nil {
		t.Fatalf("failed to send message: %v", err)
	}

	// 5. [RECEIVING]
	log.Printf("[RECEIVING] Waiting for echo response...")
	select {
	case reply := <-replyChan:
		expected := "Echo: Hello from Go"
		if reply != expected {
			t.Errorf("[FAILURE] Expected %q, got %q (hex: %x)", expected, reply, reply)
		} else {
			log.Printf("[SUCCESS] Integration test passed!")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("[TIMEOUT] Reply not received within 30s")
	}
}

func TestClusterSingletonProxy(t *testing.T) {
	// 1. Start Scala ClusterSingletonServer
	log.Printf("[STARTING] Initializing Scala ClusterSingletonServer...")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sbt", "runMain com.example.ClusterSingletonServer")
	cmd.Dir = "../scala-server"
	stdout, _ := cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start sbt: %v", err)
	}
	defer func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
	}()

	ready := make(chan struct{})
	goNodeUpChan := make(chan struct{}, 1)

	go func() {
		scanner := bufio.NewScanner(stdout)
		readyOnce := false
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("[SCALA] %s\n", line)
			if !readyOnce && strings.Contains(line, "--- SINGLETON SERVER READY ---") {
				readyOnce = true
				close(ready)
			}
			if strings.Contains(line, "--- GO NODE UP ---") {
				select {
				case goNodeUpChan <- struct{}{}:
				default:
				}
			}
		}
	}()

	select {
	case <-ready:
		log.Printf("[SERVER READY] Singleton server is listening.")
	case <-ctx.Done():
		t.Fatalf("server failed to start within timeout")
	}

	// 2. Spawn Go node using typed Address
	selfAddr := actor.Address{Protocol: "pekko", System: "ClusterSystem", Host: "127.0.0.1", Port: 2553}
	node, err := Spawn(NodeConfig{Address: selfAddr})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	defer node.Shutdown()

	replyChan := make(chan string, 1)
	node.OnMessage(func(ctx context.Context, msg *IncomingMessage) error {
		if msg.SerializerId == 4 {
			reply := string(msg.Payload)
			log.Printf("[RECEIVING] Singleton reply: %s", reply)
			replyChan <- reply
		}
		return nil
	})

	// 3. Join cluster
	log.Printf("[JOINING] Connecting to Scala singleton server at 127.0.0.1:2552")
	if err := node.Join("127.0.0.1", 2552); err != nil {
		t.Fatalf("Join: %v", err)
	}

	// 4. Wait for Scala to report Go node as UP
	log.Printf("[WAITING UP] Waiting for Scala to report Go node as UP...")
	select {
	case <-goNodeUpChan:
		log.Printf("[CONFIRMED UP] Go node is UP in the cluster.")
	case <-time.After(30 * time.Second):
		t.Fatalf("[TIMEOUT] Go node did not become UP within 30s")
	}

	// 5. Send via ClusterSingletonProxy
	proxy := node.SingletonProxy("/user/singletonManager", "")
	path, err := proxy.CurrentOldestPath()
	if err != nil {
		t.Fatalf("proxy could not resolve oldest node: %v", err)
	}
	log.Printf("[PROXY] Resolved singleton path: %s", path)

	msgBytes := []byte("Hello Singleton")
	log.Printf("[SENDING] Sending %q via ClusterSingletonProxy...", string(msgBytes))
	if err := proxy.Send(ctx, msgBytes); err != nil {
		t.Fatalf("proxy.Send failed: %v", err)
	}

	// 6. Wait for Ack reply
	select {
	case reply := <-replyChan:
		expected := "Ack: Hello Singleton"
		if reply != expected {
			t.Errorf("[FAILURE] Expected %q, got %q", expected, reply)
		} else {
			log.Printf("[SUCCESS] ClusterSingletonProxy test passed!")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("[TIMEOUT] No reply from singleton within 15s")
	}
}

func TestClusterJoinLeave(t *testing.T) {
	// 1. [STARTING SERVER]
	log.Printf("[STARTING SERVER] Initializing Scala Cluster Seed Node...")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sbt", "runMain com.example.ClusterSeedNode")
	cmd.Dir = "../scala-server"

	stdout, _ := cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start sbt: %v", err)
	}
	defer func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
	}()

	ready := make(chan struct{})
	goNodeUpChan := make(chan struct{}, 1)
	goNodeUnreachableChan := make(chan struct{}, 1)
	goNodeRemovedChan := make(chan struct{}, 1)

	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("[SCALA] %s\n", line)
			if strings.Contains(line, "--- SEED NODE READY ---") {
				close(ready)
			}
			if strings.Contains(line, "--- GO NODE UP ---") {
				select {
				case goNodeUpChan <- struct{}{}:
				default:
				}
			}
			if strings.Contains(line, "--- GO NODE UNREACHABLE ---") {
				select {
				case goNodeUnreachableChan <- struct{}{}:
				default:
				}
			}
			if strings.Contains(line, "--- GO NODE REMOVED ---") {
				select {
				case goNodeRemovedChan <- struct{}{}:
				default:
				}
			}
		}
	}()

	select {
	case <-ready:
		log.Printf("[SERVER READY] Scala seed node is listening.")
	case <-ctx.Done():
		t.Fatalf("server failed to start within timeout. Check sbt logs.")
	}

	// 2. Spawn Go node using typed Address
	selfAddr := actor.Address{Protocol: "pekko", System: "ClusterSystem", Host: "127.0.0.1", Port: 2553}
	node, err := Spawn(NodeConfig{Address: selfAddr})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	defer node.Shutdown()

	self := node.SelfAddress()
	log.Printf("[SELF] %s", self)

	// 3. [JOINING]
	log.Printf("[JOINING] Connecting to Scala seed node at 127.0.0.1:2552")
	if err := node.Join("127.0.0.1", 2552); err != nil {
		t.Fatalf("Join: %v", err)
	}

	// 4. [VERIFY UP]
	log.Printf("[WAITING UP] Waiting for Scala to report Go node as UP...")
	select {
	case <-goNodeUpChan:
		log.Printf("[CONFIRMED UP] Scala seed node has marked Go node as UP.")
	case <-time.After(30 * time.Second):
		t.Fatalf("[TIMEOUT] Scala seed node did not mark Go node as UP within 30s")
	}

	// 5. [FAILING]
	log.Printf("[FAILING] Halting heartbeat to simulate unreachability...")
	node.StopHeartbeat()

	log.Printf("[WAITING UNREACHABLE] Waiting for Scala to report Go node as UNREACHABLE...")
	select {
	case <-goNodeUnreachableChan:
		log.Printf("[CONFIRMED UNREACHABLE] Scala seed node detected Go node failure.")
	case <-time.After(40 * time.Second):
		t.Fatalf("[TIMEOUT] Scala seed node did not detect Go node failure within 40s")
	}

	// 6. [LEAVING]
	log.Printf("[RESTORING] Resuming heartbeat...")
	node.StartHeartbeat()
	time.Sleep(2 * time.Second)

	log.Printf("[LEAVING] Sending Leave command...")
	if err := node.Leave(); err != nil {
		t.Fatalf("Leave: %v", err)
	}

	log.Printf("[WAITING REMOVED] Waiting for Scala to report Go node as REMOVED...")
	select {
	case <-goNodeRemovedChan:
		log.Printf("[CONFIRMED REMOVED] Scala seed node successfully removed Go node.")
	case <-time.After(30 * time.Second):
		t.Fatalf("[TIMEOUT] Scala seed node did not remove Go node within 30s")
	}

	log.Printf("[SUCCESS] Dynamic Cluster Joining and Leaving test passed!")
}

func TestDistributedData(t *testing.T) {
	// 1. Start Scala DistributedDataServer
	log.Printf("[STARTING] Initializing Scala DistributedDataServer...")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sbt", "runMain com.example.DistributedDataServer")
	cmd.Dir = "../scala-server"
	stdout, _ := cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start sbt: %v", err)
	}
	defer func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
	}()

	ready := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(stdout)
		readyOnce := false
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("[SCALA] %s\n", line)
			if !readyOnce && strings.Contains(line, "--- DDATA SERVER READY ---") {
				readyOnce = true
				close(ready)
			}
		}
	}()

	select {
	case <-ready:
		log.Printf("[SERVER READY] DistributedDataServer is up.")
	case <-ctx.Done():
		t.Fatalf("server failed to start within timeout")
	}

	// 2. Spawn Go node using typed Address
	selfAddr := actor.Address{Protocol: "pekko", System: "ClusterSystem", Host: "127.0.0.1", Port: 2553}
	node, err := Spawn(NodeConfig{Address: selfAddr})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	defer node.Shutdown()

	// 3. Configure Replicator before joining.
	//    Build the peer path with the typed API.
	repl := node.Replicator()
	seed := actor.Address{Protocol: "pekko", System: "ClusterSystem", Host: "127.0.0.1", Port: 2552}
	peerPath := seed.WithRoot("user").Child("goReplicator")
	repl.AddPeer(peerPath.String())

	node.OnMessage(func(ctx context.Context, msg *IncomingMessage) error {
		if len(msg.Payload) > 0 && msg.Payload[0] == '{' {
			if err := repl.HandleIncoming(msg.Payload); err != nil {
				log.Printf("repl.HandleIncoming error: %v", err)
			}
		}
		return nil
	})

	// 4. Join cluster
	log.Printf("[JOINING] Connecting to Scala server at %s", seed)
	if err := node.Join(seed.Host, uint32(seed.Port)); err != nil {
		t.Fatalf("Join: %v", err)
	}

	log.Printf("[WAITING] Waiting for Artery handshake with %s...", seed)
	if err := node.WaitForHandshake(ctx, seed.Host, uint32(seed.Port)); err != nil {
		t.Fatalf("WaitForHandshake: %v", err)
	}
	log.Printf("[ASSOCIATED] Connected to %s.", seed)
	time.Sleep(500 * time.Millisecond)

	// 5. Update CRDTs and start gossip
	log.Printf("[GO→SCALA] Incrementing GCounter 'hits' by 7")
	repl.IncrementCounter("hits", 7, WriteLocal)

	log.Printf("[GO→SCALA] Adding 'golang' and 'pekko' to ORSet 'tags'")
	repl.AddToSet("tags", "golang", WriteLocal)
	repl.AddToSet("tags", "pekko", WriteLocal)

	repl.Start(ctx)
	defer repl.Stop()

	log.Printf("[WAITING] Waiting for gossip propagation (%v × 3 cycles)...", repl.GossipInterval)
	time.Sleep(repl.GossipInterval*3 + 500*time.Millisecond)

	// 6. Verify local CRDT state
	if v := repl.GCounter("hits").Value(); v != 7 {
		t.Errorf("GCounter 'hits': expected 7, got %d", v)
	} else {
		log.Printf("[OK] GCounter 'hits' = %d", v)
	}

	elems := repl.ORSet("tags").Elements()
	elemSet := make(map[string]bool, len(elems))
	for _, e := range elems {
		elemSet[e] = true
	}
	if !elemSet["golang"] || !elemSet["pekko"] {
		t.Errorf("ORSet 'tags': expected {golang, pekko}, got %v", elems)
	} else {
		log.Printf("[OK] ORSet 'tags' = %v", elems)
	}

	log.Printf("[SUCCESS] DistributedData (CRDT) test passed!")
}

func TestClusterFailureRecovery(t *testing.T) {
	// 1. Start Scala 2-node cluster with extended stable-after (60s) so the
	//    recovery window is large enough for the test to observe unreachability
	//    and then resume heartbeats before SBR downs the node.
	log.Printf("[STARTING] Initializing Scala 2-node cluster (recovery config)...")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sbt", "runMain com.example.MultiNodeClusterRecovery")
	cmd.Dir = "../scala-server"
	stdout, _ := cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start sbt: %v", err)
	}
	defer func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
	}()

	ready := make(chan struct{})
	clusterSize4Chan := make(chan struct{}, 1)
	unreachableChan := make(chan struct{}, 1)
	reachableChan := make(chan struct{}, 1)

	go func() {
		scanner := bufio.NewScanner(stdout)
		readyOnce := false
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("[SCALA] %s\n", line)
			if !readyOnce && strings.Contains(line, "--- MULTI-NODE CLUSTER READY ---") {
				readyOnce = true
				close(ready)
			}
			if strings.Contains(line, "(Total Up: 4)") {
				select {
				case clusterSize4Chan <- struct{}{}:
				default:
				}
			}
			if strings.Contains(line, "[MULTI] Unreachable:") {
				select {
				case unreachableChan <- struct{}{}:
				default:
				}
			}
			if strings.Contains(line, "[MULTI] cluster.ReachableMember") {
				select {
				case reachableChan <- struct{}{}:
				default:
				}
			}
		}
	}()

	select {
	case <-ready:
		log.Printf("[READY] Scala cluster (2 nodes) is up.")
	case <-ctx.Done():
		t.Fatalf("Scala cluster failed to start within timeout")
	}

	// 2. Spawn Go Node 1
	node1, err := Spawn(NodeConfig{SystemName: "ClusterSystem", Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("Spawn Node 1: %v", err)
	}
	defer node1.Shutdown()

	replyChan := make(chan []byte, 4)
	node1.OnMessage(func(ctx context.Context, msg *IncomingMessage) error {
		if msg.SerializerId == 4 {
			log.Printf("[RECEIVING NODE 1] %q", msg.Payload)
			select {
			case replyChan <- msg.Payload:
			default:
			}
		}
		return nil
	})

	log.Printf("[JOINING] Node 1 joining at %s...", node1.Addr())
	if err := node1.Join("127.0.0.1", 2552); err != nil {
		t.Fatalf("Node 1 Join: %v", err)
	}

	// 3. Spawn Go Node 2
	node2, err := Spawn(NodeConfig{SystemName: "ClusterSystem", Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("Spawn Node 2: %v", err)
	}
	defer node2.Shutdown()

	node2.OnMessage(func(ctx context.Context, msg *IncomingMessage) error {
		log.Printf("[RECEIVING NODE 2] serializerId=%d payload=%q", msg.SerializerId, msg.Payload)
		return nil
	})

	log.Printf("[JOINING] Node 2 joining at %s...", node2.Addr())
	if err := node2.Join("127.0.0.1", 2552); err != nil {
		t.Fatalf("Node 2 Join: %v", err)
	}

	// 4. Wait for all 4 members to be Up
	log.Printf("[WAITING] Waiting for all 4 nodes to be Up...")
	select {
	case <-clusterSize4Chan:
		log.Printf("[CONVERGENCE] All 4 nodes are Up.")
	case <-time.After(60 * time.Second):
		t.Fatalf("[TIMEOUT] Cluster did not reach size 4 within 60s")
	}

	// 5. Stop heartbeat on Node 1 — simulate node failure
	log.Printf("[FAILING] Stopping Node 1 heartbeat to simulate failure...")
	node1.StopHeartbeat()

	// 6. Wait for Scala to detect Node 1 as Unreachable
	// acceptable-heartbeat-pause = 15s, so this fires ~15s after StopHeartbeat.
	log.Printf("[WAITING] Waiting for Scala to detect an Unreachable member (up to 30s)...")
	select {
	case <-unreachableChan:
		log.Printf("[UNREACHABLE] Scala cluster detected an unreachable member.")
	case <-time.After(30 * time.Second):
		t.Fatalf("[TIMEOUT] Scala cluster did not detect unreachable node within 30s")
	}

	// 7. Verify the remaining cluster continues to function while Node 1 is unreachable.
	log.Printf("[VERIFY] Node 2 sending to echo on 2552 while Node 1 is unreachable...")
	echoPath2552 := remoteSystem("ClusterSystem", "127.0.0.1", 2552).WithRoot("user").Child("echo")
	if err := node2.Send(ctx, echoPath2552, []byte("node2 alive check")); err != nil {
		t.Fatalf("[VERIFY] Node 2 send failed while Node 1 unreachable: %v", err)
	}
	log.Printf("[VERIFY] Node 2 send succeeded — remaining 3-node cluster is operational.")

	// 8. Resume heartbeat on Node 1 — simulate recovery.
	// SBR stable-after = 60s, so we have ~45s remaining before SBR downs the node.
	log.Printf("[RECOVERING] Resuming Node 1 heartbeat...")
	node1.StartHeartbeat()

	// 9. Wait for Scala to confirm Node 1 is Reachable again.
	// Pekko's phi-accrual failure detector clears within a few heartbeat intervals
	// (~1s each), so this typically fires within 5–10s of resuming.
	log.Printf("[WAITING] Waiting for Scala to confirm Node 1 is Reachable (up to 30s)...")
	select {
	case <-reachableChan:
		log.Printf("[REACHABLE] Scala cluster confirmed member is reachable again.")
	case <-time.After(30 * time.Second):
		t.Fatalf("[TIMEOUT] Scala cluster did not confirm reachability within 30s")
	}

	// 10. Verify Node 1 can send messages immediately after recovery.
	log.Printf("[VERIFY] Node 1 sending to echo on 2552 after recovery...")
	if err := node1.Send(ctx, echoPath2552, []byte("node1 recovered")); err != nil {
		t.Fatalf("[VERIFY] Node 1 send after recovery failed: %v", err)
	}
	select {
	case reply := <-replyChan:
		expected := "Echo: node1 recovered"
		if string(reply) != expected {
			t.Errorf("[FAILURE] Expected %q, got %q", expected, string(reply))
		} else {
			log.Printf("[SUCCESS] Node 1 received echo reply after recovery: %q", string(reply))
		}
	case <-time.After(15 * time.Second):
		t.Fatal("[TIMEOUT] Node 1 did not receive echo reply within 15s after recovery")
	}

	log.Printf("[SUCCESS] TestClusterFailureRecovery passed!")
}

func TestClusterChurn(t *testing.T) {
	const iterations = 5

	// 1. Start Scala 2-node cluster. We use MultiNodeCluster (stable-after=5s) because
	//    GoNode-A leaves explicitly (Leave), not via heartbeat failure, so SBR is not
	//    involved in the graceful Leaving → Removed lifecycle.
	log.Printf("[STARTING] Initializing Scala 2-node cluster for churn test...")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sbt", "runMain com.example.MultiNodeCluster")
	cmd.Dir = "../scala-server"
	stdout, _ := cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start sbt: %v", err)
	}
	defer func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
	}()

	// nodeUpChan fires each time the cluster reaches size 3 (GoNode-A joined).
	// nodeRemovedChan fires each time a cluster.MemberRemoved event is logged (GoNode-A left).
	// Both are buffered generously so the scanner goroutine never blocks.
	ready := make(chan struct{})
	nodeUpChan := make(chan struct{}, iterations+2)
	nodeRemovedChan := make(chan struct{}, iterations+2)

	go func() {
		scanner := bufio.NewScanner(stdout)
		readyOnce := false
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("[SCALA] %s\n", line)
			if !readyOnce && strings.Contains(line, "--- MULTI-NODE CLUSTER READY ---") {
				readyOnce = true
				close(ready)
			}
			// "(Total Up: 3)" means one Go node is Up alongside the 2 Scala nodes.
			if strings.Contains(line, "(Total Up: 3)") {
				select {
				case nodeUpChan <- struct{}{}:
				default:
				}
			}
			// "[MULTI] cluster.MemberRemoved" means a member completed the Leave lifecycle.
			if strings.Contains(line, "[MULTI] cluster.MemberRemoved") {
				select {
				case nodeRemovedChan <- struct{}{}:
				default:
				}
			}
		}
	}()

	select {
	case <-ready:
		log.Printf("[READY] Scala cluster (2 nodes) is up.")
	case <-ctx.Done():
		t.Fatalf("Scala cluster failed to start within timeout")
	}

	// 2. Spawn GoNode-B for continuous traffic. It is not a cluster member; it
	//    just maintains an Artery connection to the echo actor on port 2552.
	nodeB, err := Spawn(NodeConfig{SystemName: "ClusterSystem", Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("Spawn GoNode-B: %v", err)
	}
	defer nodeB.Shutdown()

	nodeB.OnMessage(func(ctx context.Context, msg *IncomingMessage) error {
		// Echo replies arrive here; just log them.
		log.Printf("[TRAFFIC] GoNode-B echo reply: %q", msg.Payload)
		return nil
	})

	echoPath := remoteSystem("ClusterSystem", "127.0.0.1", 2552).WithRoot("user").Child("echo")

	// Trigger the outbound connection, then wait until the Artery handshake completes
	// so that the traffic loop starts with a live association.
	go func() {
		if err := nodeB.Send(ctx, echoPath, []byte("probe")); err != nil {
			log.Printf("[TRAFFIC] GoNode-B probe error: %v", err)
		}
	}()
	if err := nodeB.WaitForHandshake(ctx, "127.0.0.1", 2552); err != nil {
		t.Fatalf("GoNode-B handshake: %v", err)
	}
	log.Printf("[TRAFFIC] GoNode-B connected to 2552; starting continuous traffic.")

	// trafficErrChan collects any send errors observed during the churn loop.
	trafficErrChan := make(chan error, 1000)
	stopTraffic := make(chan struct{})
	trafficDone := make(chan struct{})

	go func() {
		defer close(trafficDone)
		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := nodeB.Send(ctx, echoPath, []byte("churn-probe")); err != nil {
					log.Printf("[TRAFFIC] GoNode-B send error: %v", err)
					select {
					case trafficErrChan <- err:
					default:
					}
				}
			case <-stopTraffic:
				return
			}
		}
	}()

	// 3. Churn loop: GoNode-A joins and immediately leaves 5 times.
	for i := 0; i < iterations; i++ {
		log.Printf("[CHURN] Iteration %d/%d — spawning GoNode-A...", i+1, iterations)

		nodeA, err := Spawn(NodeConfig{SystemName: "ClusterSystem", Host: "127.0.0.1", Port: 0})
		if err != nil {
			close(stopTraffic)
			t.Fatalf("[CHURN] Iteration %d: Spawn GoNode-A: %v", i+1, err)
		}

		if err := nodeA.Join("127.0.0.1", 2552); err != nil {
			nodeA.Shutdown()
			close(stopTraffic)
			t.Fatalf("[CHURN] Iteration %d: Join: %v", i+1, err)
		}

		// Wait for Scala to confirm GoNode-A is Up (cluster size → 3).
		select {
		case <-nodeUpChan:
			log.Printf("[CHURN] Iteration %d: GoNode-A is Up (cluster size 3).", i+1)
		case <-time.After(30 * time.Second):
			nodeA.Shutdown()
			close(stopTraffic)
			t.Fatalf("[CHURN] Iteration %d: GoNode-A did not become Up within 30s", i+1)
		}

		// Leave immediately — triggers the graceful Leaving → Exiting → Removed cycle.
		log.Printf("[CHURN] Iteration %d — GoNode-A leaving...", i+1)
		if err := nodeA.Leave(); err != nil {
			nodeA.Shutdown()
			close(stopTraffic)
			t.Fatalf("[CHURN] Iteration %d: Leave: %v", i+1, err)
		}

		// Wait for Scala to confirm GoNode-A is Removed (cluster size → 2).
		select {
		case <-nodeRemovedChan:
			log.Printf("[CHURN] Iteration %d: GoNode-A removed (cluster size 2).", i+1)
		case <-time.After(30 * time.Second):
			nodeA.Shutdown()
			close(stopTraffic)
			t.Fatalf("[CHURN] Iteration %d: GoNode-A was not removed within 30s", i+1)
		}

		nodeA.Shutdown()
		log.Printf("[CHURN] Iteration %d complete.", i+1)
	}

	// 4. Stop traffic goroutine and assess results.
	close(stopTraffic)
	<-trafficDone

	if errs := len(trafficErrChan); errs > 0 {
		t.Errorf("[FAILURE] GoNode-B traffic had %d send error(s) during churn — cluster was not stable", errs)
	} else {
		log.Printf("[SUCCESS] GoNode-B traffic was uninterrupted across all %d churn iterations.", iterations)
	}

	log.Printf("[SUCCESS] TestClusterChurn passed!")
}

// eventForwarderActor simply pipes all received cluster.ClusterDomainEvent values to a channel.
type eventForwarderActor struct {
	actor.BaseActor
	ch chan<- cluster.ClusterDomainEvent
}

func (a *eventForwarderActor) Receive(msg any) {
	if evt, ok := msg.(cluster.ClusterDomainEvent); ok {
		a.ch <- evt
	}
}

// waitForUpMembers blocks until node's cluster gossip contains at least
// expected Up members, or until timeout elapses.
//
// Uses the ClusterEvent subscription API (push-based) rather than polling, so
// it reacts immediately to each cluster.MemberUp / cluster.MemberRemoved transition.
func waitForUpMembers(node *GekkaNode, expected int, timeout time.Duration) error {
	// Fast path: already satisfied — no need to subscribe at all.
	if countUpMembers(node) >= expected {
		return nil
	}

	// Subscribe to membership transitions that can change the Up count.
	ch := make(chan cluster.ClusterDomainEvent, 32)
	forwarderRef, _ := node.System.ActorOf(Props{
		New: func() actor.Actor {
			return &eventForwarderActor{
				BaseActor: actor.NewBaseActor(),
				ch:        ch,
			}
		},
	}, "waitUpSubscriber")

	node.Subscribe(forwarderRef, cluster.EventMemberUp, cluster.EventMemberRemoved)
	defer node.Unsubscribe(forwarderRef)
	defer node.System.Stop(forwarderRef)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		// Re-evaluate after every event (or on first entry).
		if countUpMembers(node) >= expected {
			return nil
		}
		select {
		case <-ch:
			// A membership transition occurred; loop back and re-count.
		case <-timer.C:
			return fmt.Errorf("timeout: wanted %d Up members, have %d",
				expected, countUpMembers(node))
		}
	}
}

// countUpMembers returns the number of members in Up status in node's current
// gossip view. Used by waitForUpMembers.
func countUpMembers(node *GekkaNode) int {
	count := 0
	for _, m := range node.cm.GetState().GetMembers() {
		if m.GetStatus() == cluster.MemberStatus_Up {
			count++
		}
	}
	return count
}

func TestCluster_GoDominantMixed(t *testing.T) {
	// Go-Seed uses a fixed low port so it deterministically wins leader election
	// (DetermineLeader sorts by host:port; 2550 < any OS-assigned ephemeral port).
	const goSeedPort = uint32(2550)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// ── Step 1: Start Go-Seed ──────────────────────────────────────────────
	log.Printf("[SEED] Spawning Go-Seed on port %d...", goSeedPort)
	goSeed, err := Spawn(NodeConfig{SystemName: "ClusterSystem", Host: "127.0.0.1", Port: goSeedPort})
	if err != nil {
		t.Fatalf("Spawn Go-Seed: %v", err)
	}
	defer goSeed.Shutdown()

	// Self-join: router no-ops the InitJoin to self, gossip loop starts.
	if err := goSeed.Join("127.0.0.1", goSeedPort); err != nil {
		t.Fatalf("Go-Seed self-join: %v", err)
	}
	log.Printf("[SEED] Go-Seed running at %s", goSeed.Addr())

	// ── Step 2: Start Scala node joining Go-Seed ───────────────────────────
	log.Printf("[SCALA] Starting Scala node to join Go-Seed...")
	scalaCmd := exec.CommandContext(ctx, "sbt",
		fmt.Sprintf("runMain com.example.ScalaClusterNode 127.0.0.1 %d", goSeedPort))
	scalaCmd.Dir = "../scala-server"
	scalaStdout, _ := scalaCmd.StdoutPipe()
	scalaStdin, _ := scalaCmd.StdinPipe()
	scalaCmd.Stderr = scalaCmd.Stdout

	if err := scalaCmd.Start(); err != nil {
		t.Fatalf("failed to start ScalaClusterNode: %v", err)
	}
	defer func() {
		// Best-effort graceful leave; kill ensures cleanup regardless.
		scalaStdin.Write([]byte("leave\n"))
		time.Sleep(2 * time.Second)
		if scalaCmd.Process != nil {
			scalaCmd.Process.Kill()
		}
	}()

	scalaReady := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(scalaStdout)
		readyOnce := false
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("[SCALA] %s\n", line)
			if !readyOnce && strings.Contains(line, "--- SCALA NODE STARTED ---") {
				readyOnce = true
				close(scalaReady)
			}
		}
	}()

	select {
	case <-scalaReady:
		log.Printf("[SCALA] Scala node started.")
	case <-ctx.Done():
		t.Fatalf("Scala node failed to start within timeout")
	}

	// ── Step 3: Spawn Go-2, Go-3, Go-4 ────────────────────────────────────
	var goNodes [3]*GekkaNode
	names := []string{"Go-2", "Go-3", "Go-4"}
	for i := range goNodes {
		n, err := Spawn(NodeConfig{SystemName: "ClusterSystem", Host: "127.0.0.1", Port: 0})
		if err != nil {
			t.Fatalf("Spawn %s: %v", names[i], err)
		}
		defer n.Shutdown()
		goNodes[i] = n
		if err := n.Join("127.0.0.1", goSeedPort); err != nil {
			t.Fatalf("%s Join: %v", names[i], err)
		}
		log.Printf("[JOIN] %s joining at %s", names[i], n.Addr())
	}
	go2, go3, go4 := goNodes[0], goNodes[1], goNodes[2]

	// ── Step 4: Wait for all 5 nodes to be Up in Go-Seed's gossip ─────────
	log.Printf("[WAITING] Waiting for all 5 members to be Up...")
	if err := waitForUpMembers(goSeed, 5, 90*time.Second); err != nil {
		t.Fatalf("[CONVERGENCE] %v", err)
	}
	log.Printf("[CONVERGENCE] 5-node mixed cluster (Go×4, Scala×1) is Up.")

	// Verify Go-Seed is the effective leader (lowest port → first in sorted order).
	leader := goSeed.cm.DetermineLeader()
	if leader == nil {
		t.Fatalf("[LEADER] No leader found in 5-node cluster")
	}
	if leader.GetAddress().GetPort() != goSeedPort {
		t.Errorf("[LEADER] Expected Go-Seed (port=%d) as leader, got port=%d",
			goSeedPort, leader.GetAddress().GetPort())
	} else {
		log.Printf("[LEADER] Go-Seed (port=%d) is correctly the cluster leader.", goSeedPort)
	}

	// ── Stress: Simultaneous departure of Go-3 and Scala ──────────────────
	log.Printf("[DEPARTURE] Triggering simultaneous departure: Go-3 + Scala...")
	go func() {
		if err := go3.Leave(); err != nil {
			log.Printf("[DEPARTURE] Go-3 Leave error: %v", err)
		}
	}()
	go func() {
		if _, err := scalaStdin.Write([]byte("leave\n")); err != nil {
			log.Printf("[DEPARTURE] Scala leave write error: %v", err)
		}
	}()

	// ── Wait for cluster to converge to 3 Up nodes ─────────────────────────
	log.Printf("[WAITING] Waiting for cluster to settle at 3 Up members (Go-Seed, Go-2, Go-4)...")
	if err := waitForUpMembers(goSeed, 3, 90*time.Second); err != nil {
		t.Fatalf("[CONVERGENCE] After simultaneous departure: %v", err)
	}

	// Verify the correct 3 remain: Go-Seed + Go-2 + Go-4 should all be Up.
	state := goSeed.cm.GetState()
	upCount := 0
	for _, m := range state.GetMembers() {
		if m.GetStatus() == cluster.MemberStatus_Up {
			upCount++
		}
	}
	if upCount != 3 {
		t.Errorf("[FAILURE] Expected exactly 3 Up members after departure, got %d", upCount)
	} else {
		log.Printf("[CONVERGENCE] Cluster settled correctly at %d Up members.", upCount)
	}

	// Go-2 and Go-4 should still be able to communicate (no send errors).
	echoPath := remoteSystem("ClusterSystem", "127.0.0.1", int(goSeedPort)).WithRoot("user").Child("echo")
	if err := go2.Send(ctx, echoPath, []byte("go2-post-departure")); err != nil {
		t.Errorf("[VERIFY] Go-2 send to Go-Seed echo failed: %v", err)
	}
	if err := go4.Send(ctx, echoPath, []byte("go4-post-departure")); err != nil {
		t.Errorf("[VERIFY] Go-4 send to Go-Seed echo failed: %v", err)
	}

	log.Printf("[SUCCESS] TestCluster_GoDominantMixed passed!")
}

// HexDump outputs a formatted hex dump of the data.
func HexDump(data []byte) {
	fmt.Printf("%s\n", hex.Dump(data))
}

// Helper for troubleshooting
func verboseLogFrame(data []byte) {
	log.Printf("Frame size: %d", len(data))
	HexDump(data)
}

func TestMultiNodeDynamicJoin(t *testing.T) {
	// 1. [STARTING SCALA CLUSTER]
	log.Printf("[STARTING] Initializing Scala 2-node cluster...")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sbt", "runMain com.example.MultiNodeCluster")
	cmd.Dir = "../scala-server"
	stdout, _ := cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start sbt: %v", err)
	}
	defer func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
	}()

	ready := make(chan struct{})
	clusterSize3Chan := make(chan struct{}, 1)
	clusterSize4Chan := make(chan struct{}, 1)

	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("[SCALA] %s\n", line)
			if strings.Contains(line, "--- MULTI-NODE CLUSTER READY ---") {
				close(ready)
			}
			if strings.Contains(line, "(Total Up: 3)") {
				select {
				case clusterSize3Chan <- struct{}{}:
				default:
				}
			}
			if strings.Contains(line, "(Total Up: 4)") {
				select {
				case clusterSize4Chan <- struct{}{}:
				default:
				}
			}
		}
	}()

	select {
	case <-ready:
		log.Printf("[READY] Scala cluster (2 nodes) is up.")
	case <-ctx.Done():
		t.Fatalf("Scala cluster failed to start within timeout")
	}

	// 2. Spawn Go Node 1
	node1, err := Spawn(NodeConfig{SystemName: "ClusterSystem", Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("Spawn Node 1: %v", err)
	}
	defer node1.Shutdown()

	node1.OnMessage(func(ctx context.Context, msg *IncomingMessage) error {
		log.Printf("[RECEIVING NODE 1] serializerId=%d manifest=%q payload=%q", msg.SerializerId, msg.Manifest, msg.Payload)
		return nil
	})

	log.Printf("[JOINING] Node 1 joining...")
	if err := node1.Join("127.0.0.1", 2552); err != nil {
		t.Fatalf("Node 1 Join: %v", err)
	}

	// Wait for size 3
	select {
	case <-clusterSize3Chan:
		log.Printf("[CONVERGENCE] Cluster size 3 reached.")
	case <-time.After(30 * time.Second):
		t.Fatalf("[TIMEOUT] Cluster size 3 not reached")
	}

	// 3. Spawn Go Node 2
	node2, err := Spawn(NodeConfig{SystemName: "ClusterSystem", Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("Spawn Node 2: %v", err)
	}
	defer node2.Shutdown()

	node2.OnMessage(func(ctx context.Context, msg *IncomingMessage) error {
		log.Printf("[RECEIVING NODE 2] serializerId=%d manifest=%q payload=%q", msg.SerializerId, msg.Manifest, msg.Payload)
		return nil
	})

	log.Printf("[JOINING] Node 2 joining...")
	if err := node2.Join("127.0.0.1", 2552); err != nil {
		t.Fatalf("Node 2 Join: %v", err)
	}

	// 4. Verification of size 4
	select {
	case <-clusterSize4Chan:
		log.Printf("[CONVERGENCE] Cluster size 4 reached.")
	case <-time.After(40 * time.Second):
		t.Fatalf("[TIMEOUT] Cluster size 4 not reached")
	}

	// 5. Exchange messages
	log.Printf("[SENDING] Node 1 sending bytes to Scala echo on 2552...")
	echoPath1 := remoteSystem("ClusterSystem", "127.0.0.1", 2552).WithRoot("user").Child("echo")
	if err := node1.Send(ctx, echoPath1, []byte("hello from go-node-1")); err != nil {
		t.Fatalf("Node 1 Send failed: %v", err)
	}

	log.Printf("[SENDING] Node 2 sending bytes to Scala echo on 2554...")
	echoPath2 := remoteSystem("ClusterSystem", "127.0.0.1", 2554).WithRoot("user").Child("echo")
	if err := node2.Send(ctx, echoPath2, []byte("hello from go-node-2")); err != nil {
		t.Fatalf("Node 2 Send failed: %v", err)
	}

	// Allow some time for processing
	time.Sleep(5 * time.Second)
	log.Printf("[SUCCESS] Multi-node integration test complete.")
}

// TestCluster_CRDT_Consistency_Under_Failure verifies that a GCounter CRDT
// retains the expected total (100) across all surviving Go nodes after cluster
// instability: Go-Seed heartbeat pause followed by Go-4 graceful leave.
//
// Cluster composition: Go-Seed (port 2550) + Go-2 + Go-3 + Go-4 (OS ports) + 1 Scala node.
// CRDT increments: goSeed=10, go2=20, go3=30, go4=40  → expectedTotal=100.
//
// Timeline:
//
//	Phase 0 — 5 nodes Up, all-to-all replicator gossip for 3 s
//	Phase 1 — goSeed.StopHeartbeat() for 3 s
//	Phase 2 — go4.Leave() + stop go4 replicator, wait 2 s
//	Phase 3 — goSeed.StartHeartbeat()
//	Phase 4 — wait for 4 Up members (goSeed+go2+go3+Scala)
//	Verify  — goSeed, go2, go3 each see counter == 100
func TestCluster_CRDT_Consistency_Under_Failure(t *testing.T) {
	const goSeedPort = uint32(2550)
	const expectedTotal = uint64(100)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// ── Step 1: Go-Seed (self-join) ────────────────────────────────────────
	log.Printf("[CRDT] Spawning Go-Seed on port %d...", goSeedPort)
	goSeed, err := Spawn(NodeConfig{SystemName: "ClusterSystem", Host: "127.0.0.1", Port: goSeedPort})
	if err != nil {
		t.Fatalf("Spawn Go-Seed: %v", err)
	}
	defer goSeed.Shutdown()
	if err := goSeed.Join("127.0.0.1", goSeedPort); err != nil {
		t.Fatalf("Go-Seed self-join: %v", err)
	}
	log.Printf("[CRDT] Go-Seed running at %s", goSeed.Addr())

	// ── Step 2: Scala node joining Go-Seed ─────────────────────────────────
	log.Printf("[CRDT] Starting Scala node to join Go-Seed...")
	scalaCmd := exec.CommandContext(ctx, "sbt",
		fmt.Sprintf("runMain com.example.ScalaClusterNode 127.0.0.1 %d", goSeedPort))
	scalaCmd.Dir = "../scala-server"
	scalaStdout, _ := scalaCmd.StdoutPipe()
	scalaStdin, _ := scalaCmd.StdinPipe()
	scalaCmd.Stderr = scalaCmd.Stdout
	if err := scalaCmd.Start(); err != nil {
		t.Fatalf("start ScalaClusterNode: %v", err)
	}
	defer func() {
		scalaStdin.Write([]byte("leave\n"))
		time.Sleep(2 * time.Second)
		if scalaCmd.Process != nil {
			scalaCmd.Process.Kill()
		}
	}()

	scalaReady := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(scalaStdout)
		readyOnce := false
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("[SCALA] %s\n", line)
			if !readyOnce && strings.Contains(line, "--- SCALA NODE STARTED ---") {
				readyOnce = true
				close(scalaReady)
			}
		}
	}()

	select {
	case <-scalaReady:
		log.Printf("[CRDT] Scala node started.")
	case <-ctx.Done():
		t.Fatalf("Scala node failed to start within timeout")
	}

	// ── Step 3: Spawn go2, go3, go4 and join Go-Seed ───────────────────────
	goNodes := make([]*GekkaNode, 3)
	nodeNames := []string{"Go-2", "Go-3", "Go-4"}
	for i := range goNodes {
		n, err := Spawn(NodeConfig{SystemName: "ClusterSystem", Host: "127.0.0.1", Port: 0})
		if err != nil {
			t.Fatalf("Spawn %s: %v", nodeNames[i], err)
		}
		defer n.Shutdown()
		goNodes[i] = n
		if err := n.Join("127.0.0.1", goSeedPort); err != nil {
			t.Fatalf("%s Join: %v", nodeNames[i], err)
		}
		log.Printf("[CRDT] %s joined at %s", nodeNames[i], n.Addr())
	}
	go2, go3, go4 := goNodes[0], goNodes[1], goNodes[2]

	// ── Step 4: Wait for all 5 nodes Up ────────────────────────────────────
	log.Printf("[CRDT] Waiting for 5 Up members...")
	if err := waitForUpMembers(goSeed, 5, 90*time.Second); err != nil {
		t.Fatalf("[CRDT] Initial convergence: %v", err)
	}
	log.Printf("[CRDT] 5-node mixed cluster (Go×4 + Scala×1) is Up.")

	// ── Step 5: Wire all-to-all Replicator mesh ─────────────────────────────
	allNodes := []*GekkaNode{goSeed, go2, go3, go4}
	allRepls := make([]*Replicator, 4)
	for i, n := range allNodes {
		allRepls[i] = n.Replicator()
		allRepls[i].GossipInterval = 500 * time.Millisecond
	}

	// Build peer paths using the actual bound ports.
	for i, n := range allNodes {
		for j, peer := range allNodes {
			if i == j {
				continue
			}
			path := fmt.Sprintf("pekko://ClusterSystem@127.0.0.1:%d/user/replicator", peer.Port())
			allRepls[i].AddPeer(path)
		}
		// Route raw JSON gossip messages to this node's replicator.
		repl := allRepls[i]
		n.OnMessage(func(ctx context.Context, msg *IncomingMessage) error {
			if len(msg.Payload) > 0 && msg.Payload[0] == '{' {
				if err := repl.HandleIncoming(msg.Payload); err != nil {
					log.Printf("[CRDT] HandleIncoming error: %v", err)
				}
			}
			return nil
		})
	}

	// Apply fixed increments: 10+20+30+40 = 100.
	allRepls[0].IncrementCounter("visits", 10, WriteLocal) // goSeed
	allRepls[1].IncrementCounter("visits", 20, WriteLocal) // go2
	allRepls[2].IncrementCounter("visits", 30, WriteLocal) // go3
	allRepls[3].IncrementCounter("visits", 40, WriteLocal) // go4

	// Start gossip on all 4 Go nodes.
	for _, r := range allRepls {
		r.Start(ctx)
	}
	defer allRepls[0].Stop()
	defer allRepls[1].Stop()
	defer allRepls[2].Stop()
	// allRepls[3] is stopped explicitly after go4 leaves.

	// ── Phase 0: Initial gossip convergence ────────────────────────────────
	log.Printf("[CRDT] Phase 0: Waiting 3s for initial gossip convergence...")
	time.Sleep(3 * time.Second)

	// ── Phase 1: Stop goSeed heartbeat (simulates seed instability) ─────────
	log.Printf("[CRDT] Phase 1: Pausing Go-Seed heartbeat for 3s...")
	goSeed.StopHeartbeat()
	time.Sleep(3 * time.Second)

	// ── Phase 2: go4 leaves the cluster ────────────────────────────────────
	log.Printf("[CRDT] Phase 2: Go-4 leaving cluster...")
	if err := go4.Leave(); err != nil {
		log.Printf("[CRDT] go4.Leave error (non-fatal): %v", err)
	}
	allRepls[3].Stop() // halt go4's gossip after departure
	time.Sleep(2 * time.Second)

	// ── Phase 3: Restore goSeed heartbeat ──────────────────────────────────
	log.Printf("[CRDT] Phase 3: Restoring Go-Seed heartbeat...")
	goSeed.StartHeartbeat()

	// ── Phase 4: Wait for 4 Up members (goSeed + go2 + go3 + Scala) ────────
	log.Printf("[CRDT] Phase 4: Waiting for cluster to recover to 4 Up members...")
	if err := waitForUpMembers(goSeed, 4, 60*time.Second); err != nil {
		t.Fatalf("[CRDT] Recovery convergence: %v", err)
	}
	log.Printf("[CRDT] Cluster recovered to 4 Up members.")

	// Give surviving nodes one more gossip round to fully converge.
	time.Sleep(3 * time.Second)

	// ── Verify CRDT consistency on all surviving Go nodes ───────────────────
	survivedNodes := []*GekkaNode{goSeed, go2, go3}
	survivedRepls := allRepls[:3]
	survivedNames := []string{"Go-Seed", "Go-2", "Go-3"}
	allOK := true
	for i, n := range survivedNodes {
		val := survivedRepls[i].GCounter("visits").Value()
		if val != expectedTotal {
			t.Errorf("[CRDT] %s (%s): counter=%d, want %d",
				survivedNames[i], n.SelfAddress(), val, expectedTotal)
			allOK = false
		} else {
			log.Printf("[CRDT] %s (%s): counter=%d — OK", survivedNames[i], n.SelfAddress(), val)
		}
	}
	if allOK {
		log.Printf("[SUCCESS] TestCluster_CRDT_Consistency_Under_Failure passed!")
	}
}

// TestAsk_PekkoEcho verifies the Ask (request-response) pattern:
// Go sends a message to Pekko's EchoActor with a temporary sender path, and
// blocks until the reply arrives in that temporary mailbox.
func TestAsk_PekkoEcho(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Start Scala PekkoServer (RemoteSystem@127.0.0.1:2552 with /user/echo).
	cmd := exec.CommandContext(ctx, "sbt", "runMain com.example.PekkoServer")
	cmd.Dir = "../scala-server"
	stdout, _ := cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sbt: %v", err)
	}
	defer func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
	}()

	ready := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(stdout)
		readyOnce := false
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("[SCALA] %s\n", line)
			if !readyOnce && strings.Contains(line, "--- PEKKO SERVER READY ---") {
				readyOnce = true
				close(ready)
			}
		}
	}()

	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatalf("server failed to start within timeout")
	}

	// Spawn Go node with OS-assigned port.
	node, err := Spawn(NodeConfig{SystemName: "GoClient", Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	defer node.Shutdown()
	t.Logf("Go node listening on %s", node.Addr())

	// Initiate Artery handshake by sending a probe.
	remote := remoteSystem("RemoteSystem", "127.0.0.1", 2552)
	echoPath := remote.WithRoot("user").Child("echo")

	go func() {
		if err := node.Send(ctx, echoPath, []byte("probe")); err != nil {
			log.Printf("probe send error: %v", err)
		}
	}()

	if err := node.WaitForHandshake(ctx, "127.0.0.1", 2552); err != nil {
		t.Fatalf("WaitForHandshake: %v", err)
	}
	t.Logf("Artery handshake complete")

	// ── Ask ──────────────────────────────────────────────────────────────────
	// Give Pekko a moment to open its return connection to us.
	time.Sleep(1 * time.Second)

	askCtx, askCancel := context.WithTimeout(ctx, 10*time.Second)
	defer askCancel()

	reply, err := node.Ask(askCtx, echoPath, []byte("Hello Ask"))
	if err != nil {
		t.Fatalf("Ask: %v", err)
	}

	got := string(reply.Payload)
	want := "Echo: Hello Ask"
	if got != want {
		t.Errorf("Ask reply: got %q, want %q (hex: %x)", got, want, reply.Payload)
	} else {
		t.Logf("[SUCCESS] Ask reply: %q", got)
	}

	// Second Ask to verify the temp path is properly cleaned up and a new one works.
	askCtx2, askCancel2 := context.WithTimeout(ctx, 10*time.Second)
	defer askCancel2()

	reply2, err := node.Ask(askCtx2, echoPath, []byte("Second Ask"))
	if err != nil {
		t.Fatalf("Ask (2nd): %v", err)
	}
	got2 := string(reply2.Payload)
	want2 := "Echo: Second Ask"
	if got2 != want2 {
		t.Errorf("Ask (2nd) reply: got %q, want %q", got2, want2)
	} else {
		t.Logf("[SUCCESS] Ask (2nd) reply: %q", got2)
	}

}
