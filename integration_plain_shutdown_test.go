//go:build integration

/*
 * integration_plain_shutdown_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package gekka — E2E integration test asserting that the plain
// Cluster.Shutdown() entry point drives the cluster through the standard
// Leaving → Exiting → Removed lifecycle, identical to GracefulShutdown.
//
// Without this guarantee the dashboard (which terminates via the plain
// Shutdown path) is downed by the seed's failure detector instead of being
// removed gracefully, which produces user-visible "Up → Down" transitions
// inside the cluster view.
//
// Run with:
//
//	go test -v -tags integration -run TestPlainShutdownInterop -timeout 300s .
package gekka

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
)

// TestPlainShutdownInterop joins a Scala/Pekko cluster from a Gekka node,
// triggers the plain Cluster.Shutdown() (NOT GracefulShutdown), and verifies
// that the Scala side still observes the full Up → Leaving → Exiting → Removed
// sequence.  Issue 1 of the 2026-05-16 dashboard self-down spec: plain
// Shutdown must run the CoordinatedShutdown phases before tearing down the
// transport so that cluster-leave can deliver a Leave message to the seed.
func TestPlainShutdownInterop(t *testing.T) {
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

	// ── 2. Initialize Gekka cluster on port 2554 (2553 owned by the other shutdown test) ──
	selfAddr := actor.Address{
		Protocol: "pekko",
		System:   "GekkaSystem",
		Host:     "127.0.0.1",
		Port:     2554,
	}
	node, err := NewCluster(ClusterConfig{Address: selfAddr})
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	log.Printf("[GO] Cluster node listening at %s", node.Addr())

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

	// Let gossip propagate so the Scala node sees us as Up before shutdown.
	time.Sleep(5 * time.Second)

	// ── 4. Trigger plain Shutdown (NOT GracefulShutdown) ──────────────────
	goAddr := fmt.Sprintf("127.0.0.1:%d", selfAddr.Port)
	log.Println("[GO] Triggering plain Shutdown...")
	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- node.Shutdown()
	}()

	// ── 5. Verify membership transitions on the Scala side ─────────────────
	//
	// A correctly-leaving node emits Leaving → Exiting → Removed.  A node
	// that just disconnects (no Leave message) shows Up → Down → Removed
	// to the seed.  The latter is what Issue 1 produces.
	t.Run("MemberLeft", func(t *testing.T) {
		log.Printf("[SHUTDOWN] Waiting for PEKKO_MEMBER_LEFT:%s from Scala...", goAddr)
		if !waitForScalaSignal(ctx, t, sig.memberLeft, goAddr, true, 60*time.Second) {
			t.Errorf("PEKKO_MEMBER_LEFT:%s not seen within 60s — plain Shutdown did not emit Leave", goAddr)
		}
	})

	t.Run("MemberExited", func(t *testing.T) {
		log.Printf("[SHUTDOWN] Waiting for PEKKO_MEMBER_EXITED:%s from Scala...", goAddr)
		if !waitForScalaSignal(ctx, t, sig.memberExited, goAddr, true, 60*time.Second) {
			t.Errorf("PEKKO_MEMBER_EXITED:%s not seen within 60s", goAddr)
		}
	})

	t.Run("MemberRemoved", func(t *testing.T) {
		log.Printf("[SHUTDOWN] Waiting for PEKKO_MEMBER_REMOVED:%s from Scala...", goAddr)
		if !waitForScalaSignal(ctx, t, sig.memberRemoved, goAddr, true, 60*time.Second) {
			t.Errorf("PEKKO_MEMBER_REMOVED:%s not seen within 60s", goAddr)
		}
	})

	// ── 6. Shutdown must complete after Removed ──────────────────────────
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Logf("[SHUTDOWN] Shutdown completed with: %v", err)
		} else {
			log.Println("[SHUTDOWN] Shutdown completed successfully.")
		}
	case <-time.After(90 * time.Second):
		t.Error("Shutdown did not return within 90s after MemberRemoved")
	}
}
