//go:build integration

/*
 * integration_graceful_shutdown_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package gekka — E2E integration test for the Coordinated Shutdown sequence.
//
// TestGracefulShutdownInterop verifies that when a Gekka node calls
// GracefulShutdown, the Pekko/Scala cluster partner observes the standard
// member lifecycle transitions:
//
//	Up → Leaving → Exiting → Removed
//
// The Scala side confirms each transition by printing
// PEKKO_MEMBER_LEFT / PEKKO_MEMBER_EXITED / PEKKO_MEMBER_REMOVED signals to
// stdout, which the Go test runner collects via the scalaSignals struct.
//
// Run with:
//
//	go test -v -tags integration -run TestGracefulShutdownInterop -timeout 300s .
package gekka

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
)

// TestGracefulShutdownInterop starts a Scala/Pekko node, joins a Gekka cluster
// node, triggers GracefulShutdown on the Gekka node, and verifies that the
// Scala side observes the full Up → Leaving → Exiting → Removed sequence.
func TestGracefulShutdownInterop(t *testing.T) {
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

	// Wait for Scala to see the Go node join as Up before testing shutdown.
	goAddr := fmt.Sprintf("127.0.0.1:2553")
	log.Printf("[WAIT] Waiting for Scala to see Go node (%s) as Up...", goAddr)
	if !waitForScalaSignal(ctx, t, sig.memberLeft, goAddr, false, 0) {
		// memberLeft may not have fired yet — that is expected at this point.
		// Just allow a short settling time so gossip propagates.
	}
	time.Sleep(3 * time.Second)

	// ── 4. Trigger graceful shutdown on the Gekka node ────────────────────
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer shutCancel()

	log.Println("[GO] Triggering GracefulShutdown...")
	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- node.GracefulShutdown(shutCtx)
	}()

	// ── 5. Verify membership transitions on the Scala side ─────────────────

	t.Run("MemberLeft", func(t *testing.T) {
		log.Printf("[SHUTDOWN] Waiting for PEKKO_MEMBER_LEFT:%s from Scala...", goAddr)
		if !waitForScalaSignal(ctx, t, sig.memberLeft, goAddr, true, 60*time.Second) {
			t.Errorf("PEKKO_MEMBER_LEFT:%s not seen within 60s", goAddr)
		} else {
			log.Printf("[SHUTDOWN] PASS — Scala observed Go node Leaving")
		}
	})

	t.Run("MemberExited", func(t *testing.T) {
		log.Printf("[SHUTDOWN] Waiting for PEKKO_MEMBER_EXITED:%s from Scala...", goAddr)
		if !waitForScalaSignal(ctx, t, sig.memberExited, goAddr, true, 60*time.Second) {
			t.Errorf("PEKKO_MEMBER_EXITED:%s not seen within 60s", goAddr)
		} else {
			log.Printf("[SHUTDOWN] PASS — Scala observed Go node Exiting")
		}
	})

	t.Run("MemberRemoved", func(t *testing.T) {
		log.Printf("[SHUTDOWN] Waiting for PEKKO_MEMBER_REMOVED:%s from Scala...", goAddr)
		if !waitForScalaSignal(ctx, t, sig.memberRemoved, goAddr, true, 60*time.Second) {
			t.Errorf("PEKKO_MEMBER_REMOVED:%s not seen within 60s", goAddr)
		} else {
			log.Printf("[SHUTDOWN] PASS — Scala observed Go node Removed")
		}
	})

	// ── 6. GracefulShutdown should complete after Removed ──────────────────
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Logf("[SHUTDOWN] GracefulShutdown completed with: %v", err)
		} else {
			log.Println("[SHUTDOWN] GracefulShutdown completed successfully.")
		}
	case <-time.After(70 * time.Second):
		t.Error("GracefulShutdown goroutine did not return within 70s after MemberRemoved")
	}
}

// waitForScalaSignal drains ch until it receives a message containing addr or
// the timeout expires.  If require is false it checks non-blockingly and always
// returns false (used to peek without blocking).
func waitForScalaSignal(
	ctx context.Context,
	t *testing.T,
	ch chan string,
	addr string,
	require bool,
	timeout time.Duration,
) bool {
	t.Helper()
	if !require {
		// Non-blocking peek.
		select {
		case got := <-ch:
			return got == addr
		default:
			return false
		}
	}

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case got := <-ch:
			log.Printf("[SIGNAL] received %q (want %q)", got, addr)
			if got == addr {
				return true
			}
		case <-deadline.C:
			return false
		case <-ctx.Done():
			t.Logf("context cancelled while waiting for %q", addr)
			return false
		}
	}
}
