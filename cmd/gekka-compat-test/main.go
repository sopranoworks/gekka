/*
 * main.go — gekka-compat-test
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 *
 * Artery compatibility test binary.
 *
 * Starts a Gekka node, joins an Akka 2.6 cluster, waits for the Artery
 * handshake to complete, then confirms cluster membership via the
 * Management HTTP API.
 *
 * Exit codes:
 *   0 — handshake + MemberUp + management API all confirmed
 *   1 — any failure
 *
 * Signals printed to stdout (read by GekkaCompatSpec.scala):
 *   GEKKA_ARTERY_ASSOCIATED  — Artery TCP handshake complete
 *   GEKKA_MEMBER_UP:<addr>   — this node appeared as MemberUp in gossip
 *   CLUSTER_MEMBERS:<json>   — /cluster/members response body
 *   COMPAT_TEST_PASSED       — all checks passed; safe to kill the process
 *
 * Usage:
 *   gekka-compat-test \
 *     --system    GekkaSystem \
 *     --seed-host 127.0.0.1   \
 *     --seed-port 2551         \
 *     --port      2552         \
 *     --mgmt-port 8558
 */
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster"
	"github.com/sopranoworks/gekka/internal/core"
)

func main() {
	systemName := flag.String("system", "GekkaSystem", "Akka actor system name shared by all cluster members")
	seedHost := flag.String("seed-host", "127.0.0.1", "Akka seed node hostname")
	seedPort := flag.Int("seed-port", 2551, "Akka seed node port")
	localPort := flag.Int("port", 2552, "Local Artery TCP port")
	mgmtPort := flag.Int("mgmt-port", 8558, "Management HTTP port to expose and query")
	timeout := flag.Duration("timeout", 60*time.Second, "Maximum time to wait for cluster membership")
	flag.Parse()

	cfg := gekka.ClusterConfig{
		SystemName: *systemName,
		Host:       "127.0.0.1",
		Port:       uint32(*localPort),
		Provider:   gekka.ProviderAkka,
		SeedNodes: []actor.Address{
			{
				Protocol: "akka",
				System:   *systemName,
				Host:     *seedHost,
				Port:     *seedPort,
			},
		},
		Management: core.ManagementConfig{
			Enabled:             true,
			Hostname:            "127.0.0.1",
			Port:                *mgmtPort,
			HealthChecksEnabled: true,
		},
	}

	node, err := gekka.NewCluster(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewCluster: %v\n", err)
		os.Exit(1)
	}
	defer node.Shutdown()

	// Subscribe to cluster domain events so we can detect our own MemberUp.
	memberUpCh := make(chan cluster.MemberUp, 4)
	watcherRef, err := node.System.ActorOf(gekka.Props{
		New: func() actor.Actor {
			return &memberWatcher{BaseActor: actor.NewBaseActor(), ch: memberUpCh}
		},
	}, "compatWatcher")
	if err != nil {
		fmt.Fprintf(os.Stderr, "ActorOf: %v\n", err)
		os.Exit(1)
	}
	node.Subscribe(watcherRef)
	defer node.Unsubscribe(watcherRef)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	// ── Join the cluster ──────────────────────────────────────────────────────
	proto := "akka"
	if cfg.Provider == gekka.ProviderPekko {
		proto = "pekko"
	}
	log.Printf("gekka-compat-test: joining %s://%s@%s:%d via seed %s:%d",
		proto, *systemName, cfg.Host, *localPort, *seedHost, *seedPort)

	if err := node.JoinSeeds(); err != nil {
		fmt.Fprintf(os.Stderr, "JoinSeeds: %v\n", err)
		os.Exit(1)
	}

	// ── Wait for Artery handshake ─────────────────────────────────────────────
	if err := node.WaitForHandshake(ctx, *seedHost, uint32(*seedPort)); err != nil {
		fmt.Fprintf(os.Stderr, "WaitForHandshake: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("GEKKA_ARTERY_ASSOCIATED")

	// ── Wait for MemberUp ─────────────────────────────────────────────────────
	select {
	case evt := <-memberUpCh:
		fmt.Printf("GEKKA_MEMBER_UP:%s\n", evt.Member.String())
	case <-ctx.Done():
		fmt.Fprintln(os.Stderr, "timeout waiting for MemberUp")
		os.Exit(1)
	}

	// ── Query management API ──────────────────────────────────────────────────
	time.Sleep(500 * time.Millisecond)
	mgmtURL := fmt.Sprintf("http://127.0.0.1:%d/cluster/members", *mgmtPort)
	resp, err := http.Get(mgmtURL) //nolint:gosec
	if err != nil {
		fmt.Fprintf(os.Stderr, "GET %s: %v\n", mgmtURL, err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "GET %s: HTTP %d\n", mgmtURL, resp.StatusCode)
		os.Exit(1)
	}

	var membersPayload any
	if err := json.NewDecoder(resp.Body).Decode(&membersPayload); err != nil {
		fmt.Fprintf(os.Stderr, "decode members: %v\n", err)
		os.Exit(1)
	}
	membersJSON, _ := json.MarshalIndent(membersPayload, "", "  ")
	fmt.Printf("CLUSTER_MEMBERS:%s\n", membersJSON)
	fmt.Println("COMPAT_TEST_PASSED")

	// Keep the management server alive until the test runner sends SIGTERM/SIGINT
	// (Scala's proc.destroy()). The Scala test queries http://127.0.0.1:8558/cluster/members
	// after a 1.5 s sleep, so we must not exit before it does.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh
}

// memberWatcher is a lightweight actor that forwards MemberUp events to the
// main goroutine via a buffered channel.
type memberWatcher struct {
	actor.BaseActor
	ch chan cluster.MemberUp
}

func (a *memberWatcher) Receive(msg any) {
	if evt, ok := msg.(cluster.MemberUp); ok {
		select {
		case a.ch <- evt:
		default:
		}
	}
}
