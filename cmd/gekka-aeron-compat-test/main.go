/*
 * main.go — gekka-aeron-compat-test
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 *
 * Standalone Aeron-UDP compatibility test binary.
 *
 * Starts a Gekka node using the native Aeron-wire-compatible UDP transport,
 * joins an Akka 2.6 cluster, verifies membership and Ping/Pong delivery,
 * then serves the management HTTP API until terminated.
 *
 * This binary is discovered by AeronClusterSpec.scala via:
 *   bin/gekka-aeron-compat-test   (built with: go build -o bin/gekka-aeron-compat-test ./cmd/gekka-aeron-compat-test)
 *
 * Exit codes:
 *   0 — all checks passed
 *   1 — any failure
 *
 * Supported flags:
 *   --transport   aeron-udp (always used; accepted for compatibility with
 *                 the combined gekka-compat-test invocation syntax)
 *   --system      Akka ActorSystem name  (default: GekkaSystem)
 *   --seed-host   seed node hostname     (default: 127.0.0.1)
 *   --seed-port   seed node port         (default: 2551)
 *   --port        local UDP port         (default: 2563)
 *   --mgmt-port   management HTTP port   (default: 8568)
 *   --echo-target full Akka actor path for Ping/Pong test (optional)
 *   --timeout     overall timeout        (default: 120s)
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

// echoActor increments a counter and responds to specific steps in the echo chain.
type echoActor struct {
	actor.BaseActor
	count int
}

func (a *echoActor) Receive(msg any) {
	switch m := msg.(type) {
	case string:
		a.count++
		fmt.Printf("STATUS: ECHO_RECEIVED:%s (count=%d)\n", m, a.count)
		if m == "Step-2" {
			fmt.Println("STATUS: ECHO_STEP_2_RECEIVED")
			a.Sender().Tell("Step-3", a.Self())
		} else if m == "Step-4" {
			fmt.Println("STATUS: ECHO_STEP_4_RECEIVED")
		}
	}
}

// memberWatcherActor forwards cluster domain events to a channel.
type memberWatcherActor struct {
	actor.BaseActor
	ch chan any
}

func (a *memberWatcherActor) Receive(msg any) {
	switch msg.(type) {
	case cluster.MemberUp, cluster.UnreachableMember, cluster.MemberDowned, cluster.MemberRemoved:
		select {
		case a.ch <- msg:
		default:
		}
	}
}

func main() {
	_ = flag.String("transport", "aeron-udp", "transport (always aeron-udp for this binary)")
	systemName := flag.String("system", "GekkaSystem", "Akka ActorSystem name")
	seedHost := flag.String("seed-host", "127.0.0.1", "seed node hostname")
	seedPort := flag.Int("seed-port", 2551, "seed node port")
	localPort := flag.Int("port", 2563, "local Aeron UDP port")
	mgmtPort := flag.Int("mgmt-port", 8568, "management HTTP port")
	echoTarget := flag.String("echo-target", "", "actor path for Ping/Pong echo test")
	timeout := flag.Duration("timeout", 120*time.Second, "overall timeout")
	flag.Parse()

	cfg := gekka.ClusterConfig{
		SystemName: *systemName,
		Host:       "127.0.0.1",
		Port:       uint32(*localPort),
		Provider:   gekka.ProviderAkka,
		Transport:  "aeron-udp",
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
		fmt.Fprintf(os.Stderr, "aeron: NewCluster: %v\n", err)
		os.Exit(1)
	}
	defer node.Shutdown()

	eventCh := make(chan any, 16)
	watcherRef, err := node.System.ActorOf(gekka.Props{
		New: func() actor.Actor {
			return &memberWatcherActor{BaseActor: actor.NewBaseActor(), ch: eventCh}
		},
	}, "aeronWatcher")
	if err != nil {
		fmt.Fprintf(os.Stderr, "aeron: ActorOf watcher: %v\n", err)
		os.Exit(1)
	}
	node.Subscribe(watcherRef,
		cluster.EventMemberUp,
		cluster.EventUnreachableMember,
		cluster.EventMemberDowned,
		cluster.EventMemberRemoved,
	)
	defer node.Unsubscribe(watcherRef)

	echoRef, err := node.System.ActorOf(gekka.Props{
		New: func() actor.Actor {
			return &echoActor{BaseActor: actor.NewBaseActor()}
		},
	}, "echo")
	if err != nil {
		fmt.Fprintf(os.Stderr, "aeron: ActorOf echo: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	log.Printf("aeron-compat-test: joining akka://%s@127.0.0.1:%d via seed %s:%d (aeron-udp)",
		*systemName, *localPort, *seedHost, *seedPort)

	if err := node.JoinSeeds(); err != nil {
		fmt.Fprintf(os.Stderr, "aeron: JoinSeeds: %v\n", err)
		os.Exit(1)
	}

	if err := node.WaitForHandshake(ctx, *seedHost, uint32(*seedPort)); err != nil {
		fmt.Printf("FAIL: AERON_HANDSHAKE_TIMEOUT %s:%d\n", *seedHost, *seedPort)
		os.Exit(1)
	}
	fmt.Println("STATUS: ARTERY_ASSOCIATED")

	for {
		select {
		case evt := <-eventCh:
			switch e := evt.(type) {
			case cluster.MemberUp:
				if e.Member.Host == cfg.Host && e.Member.Port == cfg.Port {
					fmt.Printf("STATUS: MEMBER_UP:%s\n", e.Member.String())
					goto memberUp
				}
			case cluster.UnreachableMember:
				fmt.Printf("FAIL: AERON_UNREACHABLE %s\n", e.Member.String())
				os.Exit(1)
			case cluster.MemberDowned:
				fmt.Printf("FAIL: AERON_DOWN %s\n", e.Member.String())
				os.Exit(1)
			case cluster.MemberRemoved:
				if e.Member.Host == cfg.Host && e.Member.Port == cfg.Port {
					fmt.Printf("FAIL: AERON_REMOVED %s\n", e.Member.String())
					os.Exit(1)
				}
			}
		case <-ctx.Done():
			fmt.Println("FAIL: AERON_TIMEOUT waiting for MemberUp")
			os.Exit(1)
		}
	}

memberUp:
	if *echoTarget != "" {
		targetRef, err := node.ActorSelection(*echoTarget).Resolve(context.Background())
		if err != nil {
			fmt.Printf("FAIL: AERON_ECHO_RESOLVE %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("STATUS: ECHO_STARTING to %s\n", *echoTarget)
		targetRef.Tell("Step-1", echoRef)
	}

	time.Sleep(500 * time.Millisecond)
	mgmtURL := fmt.Sprintf("http://127.0.0.1:%d/cluster/members", *mgmtPort)
	resp, err := http.Get(mgmtURL) //nolint:gosec
	if err != nil {
		fmt.Printf("FAIL: AERON_MGMT_GET %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("FAIL: AERON_MGMT_HTTP %d\n", resp.StatusCode)
		os.Exit(1)
	}
	var payload any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		fmt.Printf("FAIL: AERON_MGMT_DECODE %v\n", err)
		os.Exit(1)
	}
	membersJSON, _ := json.MarshalIndent(payload, "", "  ")
	fmt.Printf("CLUSTER_MEMBERS:%s\n", membersJSON)
	fmt.Println("STATUS: COMPAT_TEST_PASSED")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh
}
