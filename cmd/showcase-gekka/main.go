// cmd/showcase-gekka/main.go — gekka node binary for gekka_showcase_test.
// SPDX-License-Identifier: MIT
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	gekka "github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/internal/core"
)

func main() {
	systemName := flag.String("system", "ShowcaseCluster", "actor system name")
	nodeLabel := flag.String("node", "", "node label, e.g. g1")
	port := flag.Int("port", 0, "remoting port")
	mgmtPort := flag.Int("mgmt-port", 0, "management HTTP port")
	seeds := flag.String("seeds", "", "comma-separated seed list host:port,host:port")
	rolesCSV := flag.String("roles", "showcase-member", "comma-separated cluster roles")
	peersCSV := flag.String("peers", "", "comma-separated peer URIs (e.g. pekko://ShowcaseCluster@127.0.0.1:2552); empty disables TellSender")
	flag.Parse()

	if *nodeLabel == "" || *port == 0 || *mgmtPort == 0 || *seeds == "" {
		fmt.Fprintln(os.Stderr, "usage: showcase-gekka --node g1 --port 2541 --mgmt-port 9541 --seeds host:port,... [--peers uri,uri]")
		os.Exit(2)
	}

	roles := strings.Split(*rolesCSV, ",")

	cfg := gekka.ClusterConfig{
		SystemName: *systemName,
		Address: actor.Address{
			Protocol: "pekko",
			System:   *systemName,
			Host:     "127.0.0.1",
			Port:     *port,
		},
		Roles: roles,
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

	// Spawn the TellSenderActor only when at least one peer is configured.
	peers := parsePeers(*peersCSV)
	if len(peers) > 0 {
		self := cluster.SelfAddress()
		origin := fmt.Sprintf("%s://%s@%s:%d",
			self.Protocol, self.System, self.Host, self.Port)
		var sender *TellSenderActor
		if _, err := cluster.System.ActorOf(gekka.Props{
			New: func() actor.Actor {
				sender = NewTellSenderActor(cluster, peers, origin)
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
				asker = NewAskSenderActor(cluster, peers, origin)
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
