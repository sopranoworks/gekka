// cmd/showcase-gekka/main.go — gekka node binary for gekka_showcase_test.
// SPDX-License-Identifier: MIT
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	gekka "github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/internal/core"
	"github.com/sopranoworks/gekka/logger"
	jcbor "github.com/sopranoworks/gekka/serialization/jackson-cbor"
	"reflect"
)

func main() {
	systemName := flag.String("system", "ShowcaseCluster", "actor system name")
	nodeLabel := flag.String("node", "", "node label, e.g. g1")
	port := flag.Int("port", 0, "remoting port")
	mgmtPort := flag.Int("mgmt-port", 0, "management HTTP port")
	seeds := flag.String("seeds", "", "comma-separated seed list host:port,host:port")
	rolesCSV := flag.String("roles", "showcase-member", "comma-separated cluster roles")
	peersCSV := flag.String("peers", "", "comma-separated peer URIs (e.g. pekko://ShowcaseCluster@127.0.0.1:2552); empty disables TellSender")
	verbose := flag.Bool("verbose", false, "enable verbose cluster logging (gossip/heartbeat/phi debug lines)")
	flag.Parse()

	if *nodeLabel == "" || *port == 0 || *mgmtPort == 0 || *seeds == "" {
		fmt.Fprintln(os.Stderr, "usage: showcase-gekka --node g1 --port 2541 --mgmt-port 9541 --seeds host:port,... [--peers uri,uri]")
		os.Exit(2)
	}

	roles := strings.Split(*rolesCSV, ",")

	cfg := gekka.ClusterConfig{
		LogInfoVerbose: *verbose,
		SystemName:     *systemName,
		Address: actor.Address{
			Protocol: "pekko",
			System:   *systemName,
			Host:     "127.0.0.1",
			Port:     *port,
		},
		Roles: roles,
		// Failure-detector parity with the JVM showcase members
		// (test/showcase/scala application.conf sets
		// pekko.cluster.failure-detector.acceptable-heartbeat-pause = 20s):
		// every node in one cluster must tolerate the same heartbeat
		// silence, or the strictest node's φ flags a healthy peer first
		// and feeds the JVM SBR.
		FailureDetector: gekka.FailureDetectorConfig{
			AcceptableHeartbeatPause: 20 * time.Second,
		},
		Management: core.ManagementConfig{
			Enabled:  true,
			Hostname: "127.0.0.1",
			Port:     *mgmtPort,
		},
	}

	cluster, err := gekka.NewCluster(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewCluster: %v\n", err)
		os.Exit(1)
	}

	if *verbose {
		// LogInfoVerbose gates Debug-level log calls; the logger's default
		// level (INFO) would silently drop them without this.  Must run
		// AFTER NewCluster: its config load re-installs the logger with the
		// configured (INFO) level, overwriting any earlier Set.
		logger.MainLevelVar().Set(slog.LevelDebug)
		logger.StdoutLevelVar().Set(slog.LevelDebug)
	}

	// Register the Pekko jackson-cbor serializer at the same ID
	// (33) Pekko's reference.conf class-defines. Both peers see the same
	// value because both load Pekko's reference.conf; gekka mirrors it via
	// jcbor.DefaultID. Then register each showcase Go type so the serializer
	// can decode inbound Scala-emitted bytes and emit outbound Go-side
	// values with the right JVM manifest.
	jcborSer := jcbor.New(jcbor.DefaultID)
	RegisterShowcaseTypes(jcborSer)
	cluster.RegisterSerializerByValue(jcborSer)
	registry := cluster.Serialization()
	for manifest, goType := range map[string]reflect.Type{
		JVMEchoEnvelope:       reflect.TypeOf((*EchoEnvelope)(nil)),
		JVMAskEnvelope:        reflect.TypeOf((*AskEnvelope)(nil)),
		JVMShowcaseEchoCustom: reflect.TypeOf((*ShowcaseEchoCustom)(nil)),
		JVMSystemMessagePing:  reflect.TypeOf((*SystemMessagePing)(nil)),
		JVMPing:               reflect.TypeOf((*Ping)(nil)),
		JVMPong:               reflect.TypeOf((*Pong)(nil)),
	} {
		registry.RegisterManifest(manifest, goType, jcbor.DefaultID)
	}

	// Join the first seed.
	first := strings.SplitN(strings.Split(*seeds, ",")[0], ":", 2)
	if len(first) != 2 {
		fmt.Fprintf(os.Stderr, "bad --seeds: %q\n", *seeds)
		os.Exit(2)
	}
	seedHost := first[0]
	var seedPort int
	if _, err := fmt.Sscanf(first[1], "%d", &seedPort); err != nil {
		fmt.Fprintf(os.Stderr, "bad seed port in %q: %v\n", *seeds, err)
		os.Exit(2)
	}
	if err := cluster.Join(seedHost, uint32(seedPort)); err != nil {
		fmt.Fprintf(os.Stderr, "Join: %v\n", err)
		os.Exit(1)
	}

	// Spawn the EchoActor before waiting for self-Up so the receiver side
	// of FT1 is online the moment cluster membership transitions to Up.
	if _, err := cluster.System.ActorOf(gekka.Props{
		New: func() actor.Actor {
			return &EchoActor{BaseActor: actor.NewBaseActor()}
		},
	}, "echo"); err != nil {
		fmt.Fprintf(os.Stderr, "ActorOf echo: %v\n", err)
		os.Exit(1)
	}

	// Spawn the AskActor (FT2 receiver) before waiting for self-Up so the
	// ask-pattern responder is online as soon as membership reaches Up.
	if _, err := cluster.System.ActorOf(gekka.Props{
		New: func() actor.Actor {
			return &AskActor{BaseActor: actor.NewBaseActor()}
		},
	}, "ask"); err != nil {
		fmt.Fprintf(os.Stderr, "ActorOf ask: %v\n", err)
		os.Exit(1)
	}

	// Wait until the cluster reports this node Up.
	waitCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for {
		if cluster.IsLocalNodeUp() {
			break
		}
		select {
		case <-waitCtx.Done():
			fmt.Fprintln(os.Stderr, "timed out waiting for self-Up")
			os.Exit(1)
		case <-time.After(200 * time.Millisecond):
		}
	}

	// Steady-state anchor (spec §4 / §5.4.2): the traffic actors' ERROR
	// accounting is scoped to the strict Gate-2 window, whose per-node
	// approximation is the first local observation of the full expected
	// membership Up. Traffic itself starts immediately (spec revision-2:
	// "DO NOT delay or constrain FT1/2/3").
	peers := parsePeers(*peersCSV)
	anchor := &steadyAnchor{}
	go watchAnchor(anchor, clusterUpCount(cluster), len(peers)+1, nil)

	// Spawn the TellSenderActor only when at least one peer is configured.
	if len(peers) > 0 {
		self := cluster.SelfAddress()
		origin := fmt.Sprintf("%s://%s@%s:%d",
			self.Protocol, self.System, self.Host, self.Port)
		var sender *TellSenderActor
		if _, err := cluster.System.ActorOf(gekka.Props{
			New: func() actor.Actor {
				sender = NewTellSenderActor(cluster, peers, origin, anchor)
				return sender
			},
		}, "tellSender"); err != nil {
			fmt.Fprintf(os.Stderr, "ActorOf tellSender: %v\n", err)
			os.Exit(1)
		}
		if sender != nil {
			sender.StartTickers()
		}

		// Spawn the AskSenderActor (FT2 requester) only when peers are
		// configured. It mirrors the TellSender wiring but uses
		// cluster.Ask for request-reply with a 5-second per-call timeout.
		var asker *AskSenderActor
		if _, err := cluster.System.ActorOf(gekka.Props{
			New: func() actor.Actor {
				asker = NewAskSenderActor(cluster, peers, origin, anchor)
				return asker
			},
		}, "askSender"); err != nil {
			fmt.Fprintf(os.Stderr, "ActorOf askSender: %v\n", err)
			os.Exit(1)
		}
		if asker != nil {
			asker.StartTickers()
		}
	}

	// FT3: Distributed Data CRDT actors. Runs unconditionally — even on a
	// peerless solo node — because the local Replicator gossip loop is always
	// active and convergence with future peers happens automatically once the
	// cluster membership reaches Up.
	startDdata(cluster, *nodeLabel)

	// FT4: spawn a ClusterSingletonManager for this node's own role. The
	// manager is responsible for hosting the actual ShowcaseSingletonActor
	// when this node becomes the oldest member with role=singleton-<label>.
	// The manager is spawned UNCONDITIONALLY (no role-membership pre-check)
	// because the gekka SingletonManager itself queries cluster.OldestNode
	// and only spawns the child when isLocalOldest() is true; if this node
	// does not carry the matching role the manager will simply never elect.
	ownRole := "singleton-" + *nodeLabel
	singletonMgr := cluster.SingletonManager(gekka.Props{
		New: func() actor.Actor {
			return &ShowcaseSingletonActor{BaseActor: actor.NewBaseActor()}
		},
	}, ownRole)
	if _, err := cluster.System.ActorOf(gekka.Props{
		New: func() actor.Actor { return singletonMgr },
	}, "singleton-manager-"+ownRole); err != nil {
		fmt.Fprintf(os.Stderr, "ActorOf singleton-manager-%s: %v\n", ownRole, err)
		os.Exit(1)
	}

	// FT4 ClientActor: every node pings every singleton role on a 4s tick,
	// with a 30s warmup grace (anchored at the local all-members-Up
	// observation per spec §5.4.2) that suppresses unestablished-role
	// timeouts.
	startClient(cluster, *nodeLabel, anchor)

	fmt.Printf("--- SHOWCASE NODE READY: %s ---\n", *nodeLabel)
	os.Stdout.Sync()

	// Block on SIGTERM/SIGINT.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh
	_ = cluster.Shutdown()
}

// parsePeers splits a comma-separated peer list, trims whitespace, and
// returns the non-empty entries. An empty or whitespace-only input yields
// an empty slice.
func parsePeers(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
