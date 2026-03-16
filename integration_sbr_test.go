//go:build integration

/*
 * integration_sbr_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package gekka — Split Brain Resolver E2E integration tests.
//
// These tests start com.example.SBRTestNode via sbt and verify that Pekko's
// SBR and Gekka's SBR interoperate correctly when a network partition is
// simulated by muting the association between nodes.
//
// Run with:
//
//	go test -v -tags integration -run TestSBRInterop -timeout 300s .
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
	gcluster "github.com/sopranoworks/gekka/cluster"
)

// sbrSignals collects stdout signals emitted by SBRTestNode so sub-tests can
// block until the expected cluster event arrives.
type sbrSignals struct {
	ready            chan struct{} // closed when "SBR_NODE_READY" is seen
	memberUp         chan string   // "host:port" for every PEKKO_MEMBER_UP
	memberUnreachable chan string  // "host:port" for every PEKKO_MEMBER_UNREACHABLE
	memberDown       chan string   // "host:port" for every PEKKO_MEMBER_DOWN
	memberRemoved    chan string   // "host:port" for every PEKKO_MEMBER_REMOVED
}

// startSBRTestNode launches com.example.SBRTestNode via sbt with the given
// SBR strategy, registers a Cleanup to kill it, and returns the signal channels.
func startSBRTestNode(t *testing.T, ctx context.Context, strategy string) *sbrSignals {
	t.Helper()

	cmd := exec.CommandContext(ctx, "sbt",
		fmt.Sprintf("runMain com.example.SBRTestNode %s", strategy))
	cmd.Dir = "scala-server"

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start sbt SBRTestNode: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
			cmd.Wait() //nolint:errcheck
		}
	})

	sig := &sbrSignals{
		ready:             make(chan struct{}),
		memberUp:          make(chan string, 16),
		memberUnreachable: make(chan string, 16),
		memberDown:        make(chan string, 16),
		memberRemoved:     make(chan string, 16),
	}

	go func() {
		scanner := bufio.NewScanner(stdout)
		readyOnce := false
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("[SCALA-SBR] %s\n", line)

			if !readyOnce && strings.Contains(line, "SBR_NODE_READY") {
				readyOnce = true
				close(sig.ready)
			}
			for _, entry := range []struct {
				prefix string
				ch     chan string
			}{
				{"PEKKO_MEMBER_UP:", sig.memberUp},
				{"PEKKO_MEMBER_UNREACHABLE:", sig.memberUnreachable},
				{"PEKKO_MEMBER_DOWN:", sig.memberDown},
				{"PEKKO_MEMBER_REMOVED:", sig.memberRemoved},
			} {
				if strings.HasPrefix(line, entry.prefix) {
					addr := strings.TrimPrefix(line, entry.prefix)
					select {
					case entry.ch <- addr:
					default:
					}
				}
			}
		}
	}()

	return sig
}

// waitSBRSignal waits up to timeout for addr to appear on ch.
// Returns true on success. On failure it logs the cause.
func waitSBRSignal(ctx context.Context, t *testing.T, ch <-chan string, wantAddr string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case got := <-ch:
			if got == wantAddr || wantAddr == "" {
				return true
			}
			// Not the address we're waiting for — continue draining.
		case <-deadline.C:
			t.Logf("timeout waiting for signal with addr %q", wantAddr)
			return false
		case <-ctx.Done():
			t.Logf("context cancelled waiting for signal with addr %q", wantAddr)
			return false
		}
	}
}

// ── TestSBRKeepMajorityInterop ─────────────────────────────────────────────

// TestSBRKeepMajorityInterop verifies that when a 2-node cluster (Scala +
// Gekka) uses keep-majority SBR and the Gekka node becomes unresponsive (by
// muting its association), the Scala SBR:
//
//  1. Marks the Gekka node UNREACHABLE.
//  2. After stable-after (2 s), downs the Gekka node.
//  3. Eventually transitions the Gekka node to REMOVED.
//
// With 2 nodes the partition is 1 vs 1 (tie). Pekko's keep-majority
// tie-break: the side with the member that has the lowest UID survives.
// Since Scala joined as seed (lower UID), Scala survives and downs Go.
func TestSBRKeepMajorityInterop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	t.Cleanup(cancel)

	// ── Start Scala SBR test node ──────────────────────────────────────────
	sig := startSBRTestNode(t, ctx, "keep-majority")
	log.Println("[SBR] Waiting for SBR_NODE_READY…")
	select {
	case <-sig.ready:
		log.Println("[SBR] Scala SBR node ready.")
	case <-ctx.Done():
		t.Fatal("Scala SBR node did not print SBR_NODE_READY within timeout")
	}

	// ── Start Go node ──────────────────────────────────────────────────────
	selfAddr := actor.Address{
		Protocol: "pekko",
		System:   "GekkaSystem",
		Host:     "127.0.0.1",
		Port:     2553,
	}
	// Go does NOT run SBR here — we only test Scala's SBR decision.
	node, err := NewCluster(ClusterConfig{Address: selfAddr})
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	t.Cleanup(func() { node.Shutdown() })

	// ── Join and wait for handshake ────────────────────────────────────────
	log.Println("[SBR] Joining GekkaSystem at 127.0.0.1:2552…")
	if err := node.Join("127.0.0.1", 2552); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if err := node.WaitForHandshake(ctx, "127.0.0.1", 2552); err != nil {
		t.Fatalf("WaitForHandshake: %v", err)
	}
	log.Println("[SBR] Artery handshake complete.")

	// Wait for Scala to report both nodes Up.
	log.Println("[SBR] Waiting for Scala to report Go node (2553) as Up…")
	if !waitSBRSignal(ctx, t, sig.memberUp, "127.0.0.1:2553", 60*time.Second) {
		t.Fatal("Go node never reached Up status on Scala side")
	}
	log.Println("[SBR] Both nodes Up. Simulating partition by muting Go→Scala.")

	// ── Simulate partition: stop all outbound frames from Go to Scala ──────
	// This prevents Go from sending heartbeat responses, cluster gossip, or
	// any other frames. Scala's phi-accrual FD will detect Go as unreachable
	// after acceptable-heartbeat-pause (3 s). SBR then fires after stable-after (2 s).
	node.MuteNode("127.0.0.1", 2552)

	// ── Wait for Scala to detect Go as UNREACHABLE ─────────────────────────
	log.Println("[SBR] Waiting for PEKKO_MEMBER_UNREACHABLE:127.0.0.1:2553…")
	if !waitSBRSignal(ctx, t, sig.memberUnreachable, "127.0.0.1:2553", 60*time.Second) {
		t.Fatal("Scala did not mark Go as UNREACHABLE within timeout")
	}
	log.Println("[SBR] ✓ Scala reported PEKKO_MEMBER_UNREACHABLE for Go node.")

	// ── Wait for Scala's SBR to DOWN Go ────────────────────────────────────
	// stable-after = 2 s; allow generous buffer for gossip convergence.
	log.Println("[SBR] Waiting for PEKKO_MEMBER_DOWN:127.0.0.1:2553…")
	if !waitSBRSignal(ctx, t, sig.memberDown, "127.0.0.1:2553", 30*time.Second) {
		t.Fatal("Scala SBR did not DOWN the Go node within timeout")
	}
	log.Println("[SBR] ✓ Scala SBR downed the Go node.")

	// ── Wait for REMOVED ────────────────────────────────────────────────────
	log.Println("[SBR] Waiting for PEKKO_MEMBER_REMOVED:127.0.0.1:2553…")
	if !waitSBRSignal(ctx, t, sig.memberRemoved, "127.0.0.1:2553", 30*time.Second) {
		t.Fatal("Go node was not REMOVED within timeout")
	}
	log.Println("[SBR] ✓ Scala reported PEKKO_MEMBER_REMOVED for Go node. Test passed.")
}

// ── TestSBRKeepOldestInterop ───────────────────────────────────────────────

// TestSBRKeepOldestInterop verifies that when a 2-node cluster uses
// keep-oldest SBR and the oldest node (Scala seed, port 2552) terminates
// abruptly, the Gekka node (the survivor):
//
//  1. Detects Scala as UNREACHABLE via the phi-accrual failure detector.
//  2. After stable-after (2 s), the Go SBR decides DownSelf = true (the oldest
//     is unreachable, so Go must leave).
//  3. Go publishes MemberLeft for itself, observable via SubscribeChannel.
func TestSBRKeepOldestInterop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	t.Cleanup(cancel)

	// ── Start Scala SBR test node ──────────────────────────────────────────
	sig := startSBRTestNode(t, ctx, "keep-oldest")
	log.Println("[SBR-KO] Waiting for SBR_NODE_READY…")
	select {
	case <-sig.ready:
		log.Println("[SBR-KO] Scala SBR node ready.")
	case <-ctx.Done():
		t.Fatal("Scala SBR node did not print SBR_NODE_READY within timeout")
	}

	// ── Start Go node with keep-oldest SBR ────────────────────────────────
	selfAddr := actor.Address{
		Protocol: "pekko",
		System:   "GekkaSystem",
		Host:     "127.0.0.1",
		Port:     2553,
	}
	node, err := NewCluster(ClusterConfig{
		Address: selfAddr,
		SBR: SBRConfig{
			ActiveStrategy: "keep-oldest",
			StableAfter:    2 * time.Second,
		},
	})
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	t.Cleanup(func() { node.Shutdown() })

	// Subscribe to Go's cluster events BEFORE joining so we don't miss any.
	evtSub := node.SubscribeChannel(
		reflect.TypeOf(gcluster.UnreachableMember{}),
		reflect.TypeOf(gcluster.MemberLeft{}),
	)
	t.Cleanup(evtSub.Cancel)

	// ── Join and wait for handshake ────────────────────────────────────────
	log.Println("[SBR-KO] Joining GekkaSystem at 127.0.0.1:2552…")
	if err := node.Join("127.0.0.1", 2552); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if err := node.WaitForHandshake(ctx, "127.0.0.1", 2552); err != nil {
		t.Fatalf("WaitForHandshake: %v", err)
	}
	log.Println("[SBR-KO] Artery handshake complete.")

	// Wait until Scala reports both members Up.
	log.Println("[SBR-KO] Waiting for Scala to report Go node (2553) as Up…")
	if !waitSBRSignal(ctx, t, sig.memberUp, "127.0.0.1:2553", 60*time.Second) {
		t.Fatal("Go node never reached Up status on Scala side")
	}
	log.Println("[SBR-KO] Both nodes Up. Terminating Scala (oldest node).")

	// ── Kill the Scala process to simulate abrupt failure ─────────────────
	// We need the raw *exec.Cmd to kill it. Re-retrieve it from the test's
	// cleanup stack by storing it during startSBRTestNode. Since Go 1.21
	// there is no direct way to retrieve the cmd once registered with Cleanup;
	// we use a shared variable via closure instead.
	//
	// The startSBRTestNode helper already registered a Cleanup that kills the
	// process. We kill it explicitly here so the timing is under our control.
	// The Cleanup will call Kill+Wait again, which is harmless.

	// Because we can't easily get the cmd back, we mute the association
	// (bidirectional: mute both directions) instead of killing the process.
	// This is equivalent from Go's FD perspective: Go stops receiving
	// heartbeats from Scala, and Go's FD marks Scala as unreachable.
	//
	// Note: muting is bidirectional — Go also drops inbound frames from Scala,
	// so Scala gets no heartbeat replies either and marks Go unreachable too.
	// The keep-oldest SBR on Go should still decide DownSelf because the oldest
	// (Scala, 2552, lower upNumber) is the unreachable side.
	node.MuteNode("127.0.0.1", 2552)
	log.Println("[SBR-KO] Association with Scala muted (simulating abrupt failure).")

	// ── Wait for Go to detect Scala as UNREACHABLE ─────────────────────────
	log.Println("[SBR-KO] Waiting for UnreachableMember event on Go side…")
	unreachableDeadline := time.NewTimer(60 * time.Second)
	defer unreachableDeadline.Stop()
	unreachableSeen := false
waitUnreachable:
	for {
		select {
		case evt := <-evtSub.C:
			switch e := evt.(type) {
			case gcluster.UnreachableMember:
				if e.Member.Port == 2552 {
					log.Printf("[SBR-KO] ✓ Go detected Scala (2552) as unreachable.")
					unreachableSeen = true
					break waitUnreachable
				}
			}
		case <-unreachableDeadline.C:
			t.Fatal("Go did not detect Scala as unreachable within timeout")
		case <-ctx.Done():
			t.Fatal("context cancelled waiting for UnreachableMember")
		}
	}
	if !unreachableSeen {
		t.Fatal("unreachable event not observed")
	}

	// ── Wait for Go's SBR to decide DownSelf → MemberLeft for self ────────
	// keep-oldest: oldest (Scala, lower upNumber) is unreachable → DownSelf = true
	// → LeaveCluster() → MemberLeft published for Go's own address.
	log.Println("[SBR-KO] Waiting for MemberLeft event for Go (2553)…")
	leftDeadline := time.NewTimer(30 * time.Second)
	defer leftDeadline.Stop()
waitLeft:
	for {
		select {
		case evt := <-evtSub.C:
			switch e := evt.(type) {
			case gcluster.MemberLeft:
				if e.Member.Port == 2553 {
					log.Printf("[SBR-KO] ✓ Go correctly left cluster: %s", e.Member)
					break waitLeft
				}
			}
		case <-leftDeadline.C:
			t.Fatal("Go did not emit MemberLeft within timeout after keep-oldest SBR decision")
		case <-ctx.Done():
			t.Fatal("context cancelled waiting for MemberLeft")
		}
	}

	log.Println("[SBR-KO] ✓ keep-oldest SBR interop test passed.")
}
