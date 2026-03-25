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

// EchoActor increments a counter and responds to specific steps in the echo chain.
type EchoActor struct {
        actor.BaseActor
        count int
}

func (a *EchoActor) Receive(msg any) {
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

func main() {
        systemName := flag.String("system", "GekkaSystem", "Akka actor system name shared by all cluster members")
        seedHost := flag.String("seed-host", "127.0.0.1", "Akka seed node hostname")
        seedPort := flag.Int("seed-port", 2551, "Akka seed node port")
        localPort := flag.Int("port", 2552, "Local Artery TCP port")
        mgmtPort := flag.Int("mgmt-port", 8558, "Management HTTP port to expose and query")
        echoTarget := flag.String("echo-target", "", "Full Akka actor path of the target EchoActor (e.g. akka://GekkaSystem@127.0.0.1:2551/user/echo)")
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

        // Subscribe to cluster domain events so we can detect our own MemberUp
        // and detect failures (unreachable/down/removed).
        eventCh := make(chan any, 16)
        watcherRef, err := node.System.ActorOf(gekka.Props{
                New: func() actor.Actor {
                        return &memberWatcher{BaseActor: actor.NewBaseActor(), ch: eventCh}
                },
        }, "compatWatcher")
        if err != nil {
                fmt.Fprintf(os.Stderr, "ActorOf: %v\n", err)
                os.Exit(1)
        }
        node.Subscribe(watcherRef,
                cluster.EventMemberUp,
                cluster.EventUnreachableMember,
                cluster.EventMemberDowned,
                cluster.EventMemberRemoved,
        )
        defer node.Unsubscribe(watcherRef)

        // Spawn EchoActor
        echoRef, err := node.System.ActorOf(gekka.Props{
                New: func() actor.Actor {
                        return &EchoActor{BaseActor: actor.NewBaseActor()}
                },
        }, "echo")
        if err != nil {
                fmt.Fprintf(os.Stderr, "ActorOf echo: %v\n", err)
                os.Exit(1)
        }

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
                fmt.Printf("FAIL: HANDSHAKE_TIMEOUT %s:%d\n", *seedHost, *seedPort)
                os.Exit(1)
        }
        fmt.Println("STATUS: ARTERY_ASSOCIATED")

        // ── Wait for MemberUp ─────────────────────────────────────────────────────
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
                                fmt.Printf("FAIL: CLUSTER_UNREACHABLE %s\n", e.Member.String())
                                os.Exit(1)
                        case cluster.MemberDowned:
                                fmt.Printf("FAIL: CLUSTER_MEMBER_DOWN %s\n", e.Member.String())
                                os.Exit(1)
                        case cluster.MemberRemoved:
                                if e.Member.Host == cfg.Host && e.Member.Port == cfg.Port {
                                        fmt.Printf("FAIL: LOCAL_MEMBER_REMOVED %s\n", e.Member.String())
                                        os.Exit(1)
                                }
                        }
                case <-ctx.Done():
                        fmt.Println("FAIL: TIMEOUT")
                        os.Exit(1)
                }
        }

memberUp:
        // ── Start Echo Chain ──────────────────────────────────────────────────────
        if *echoTarget != "" {
                targetRef, err := node.ActorSelection(*echoTarget).Resolve(context.Background())
                if err != nil {
                        fmt.Printf("FAIL: ECHO_TARGET_RESOLVE_ERROR %v\n", err)
                        os.Exit(1)
                }
                fmt.Printf("STATUS: ECHO_STARTING to %s\n", *echoTarget)
                targetRef.Tell("Step-1", echoRef)
        }

        // ── Query management API ──────────────────────────────────────────────────
        time.Sleep(500 * time.Millisecond)

	mgmtURL := fmt.Sprintf("http://127.0.0.1:%d/cluster/members", *mgmtPort)
	resp, err := http.Get(mgmtURL) //nolint:gosec
	if err != nil {
		fmt.Printf("FAIL: MGMT_API_GET_ERROR %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("FAIL: MGMT_API_HTTP_ERROR %d\n", resp.StatusCode)
		os.Exit(1)
	}

	var membersPayload any
	if err := json.NewDecoder(resp.Body).Decode(&membersPayload); err != nil {
		fmt.Printf("FAIL: MGMT_API_DECODE_ERROR %v\n", err)
		os.Exit(1)
	}
	membersJSON, _ := json.MarshalIndent(membersPayload, "", "  ")
	fmt.Printf("CLUSTER_MEMBERS:%s\n", membersJSON)
	fmt.Println("STATUS: COMPAT_TEST_PASSED")

	// Keep the management server alive until the test runner sends SIGTERM/SIGINT
	// (Scala's proc.destroy()).
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh
}

// memberWatcher is a lightweight actor that forwards cluster events to the
// main goroutine via a buffered channel.
type memberWatcher struct {
	actor.BaseActor
	ch chan any
}

func (a *memberWatcher) Receive(msg any) {
	switch msg.(type) {
	case cluster.MemberUp, cluster.UnreachableMember, cluster.MemberDowned, cluster.MemberRemoved:
		select {
		case a.ch <- msg:
		default:
		}
	}
}
