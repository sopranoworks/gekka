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

	"gekka/gekka/actor"
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

// HexDump outputs a formatted hex dump of the data.
func HexDump(data []byte) {
	fmt.Printf("%s\n", hex.Dump(data))
}

// Helper for troubleshooting
func verboseLogFrame(data []byte) {
	log.Printf("Frame size: %d", len(data))
	HexDump(data)
}
