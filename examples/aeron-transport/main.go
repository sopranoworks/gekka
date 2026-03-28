// Package main is the "aeron-transport" Gekka example.
//
// It demonstrates joining a hybrid Go/JVM cluster using the native Aeron UDP
// transport.  The Go node:
//
//  1. Loads HOCON configuration (transport = aeron-udp) from application.conf.
//  2. Spawns a local EchoActor at /user/echo.
//  3. Joins the Akka/Pekko seed node via Aeron UDP and waits for the handshake.
//  4. Subscribes to cluster membership events.
//  5. Sends periodic Ping messages to the seed node's /user/echo actor and
//     prints the replies.
//  6. Leaves gracefully on SIGINT / SIGTERM.
//
// Transport details
// ─────────────────
// When transport = aeron-udp:
//   • The Gekka node speaks the Aeron 1.30.0 wire protocol over UDP.
//   • Three logical Artery streams are multiplexed over a single UDP port:
//       Stream 1 — Control  (handshake, cluster messages, heartbeats)
//       Stream 2 — Ordinary (user-level actor messages)
//       Stream 3 — Large    (fragmented messages)
//   • Reliability is achieved via NACK-based retransmission and Status Message
//     (SM) flow-control frames — no JVM Media Driver is required.
//
// See docs/PROTOCOL.md §"Aeron (UDP) Framing" for the full wire-format reference.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster"
)

// ── Actor definitions ─────────────────────────────────────────────────────────

// echoActor receives messages (local or remote) and logs them.
// When a remote IncomingMessage arrives it replies with "Ack: <payload>".
type echoActor struct {
	actor.BaseActor
}

func (a *echoActor) Receive(msg any) {
	switch m := msg.(type) {
	case *gekka.IncomingMessage:
		text := string(m.Payload)
		a.Log().Info("received", "serializerId", m.SerializerId, "payload", text)
		if s := a.Sender(); s != nil && s.Path() != "" {
			reply := []byte("Ack: " + text)
			a.Log().Info("replying", "to", s.Path())
			s.Tell(reply, a.Self())
		}
	default:
		a.Log().Info("local message", "msg", m)
	}
}

// clusterEventActor logs cluster membership transitions.
type clusterEventActor struct {
	actor.BaseActor
}

func (a *clusterEventActor) Receive(msg any) {
	evt, ok := msg.(cluster.ClusterDomainEvent)
	if !ok {
		return
	}
	switch e := evt.(type) {
	case cluster.MemberUp:
		a.Log().Info("MemberUp", "member", e.Member)
	case cluster.MemberLeft:
		a.Log().Info("MemberLeft", "member", e.Member)
	case cluster.MemberExited:
		a.Log().Info("MemberExited", "member", e.Member)
	case cluster.MemberRemoved:
		a.Log().Info("MemberRemoved", "member", e.Member)
	case cluster.UnreachableMember:
		a.Log().Warn("UnreachableMember", "member", e.Member)
	case cluster.ReachableMember:
		a.Log().Info("ReachableMember", "member", e.Member)
	}
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── Step 1: Spawn from HOCON config (transport = aeron-udp) ───────────
	node, err := gekka.NewClusterFromConfig("application.conf")
	if err != nil {
		log.Fatalf("NewClusterFromConfig: %v", err)
	}
	defer node.Shutdown()

	log.Printf("[aeron] Gekka node  : %s  (port %d)", node.Addr(), node.Port())
	log.Printf("[aeron] Actor system: %s", node.SelfAddress().System)

	seeds := node.Seeds()
	if len(seeds) == 0 {
		log.Fatal("no seed-nodes configured in application.conf")
	}
	seed := seeds[0]

	// ── Step 2: Spawn local echo actor ────────────────────────────────────
	localRef, err := node.System.ActorOf(gekka.Props{
		New: func() actor.Actor { return &echoActor{BaseActor: actor.NewBaseActor()} },
	}, "echo")
	if err != nil {
		log.Fatalf("ActorOf echo: %v", err)
	}
	log.Printf("[aeron] local echo actor: %s", localRef)

	// Resolve remote echo actor path (lazy — TCP/UDP connection established on first use).
	echoPath := seed.WithRoot("user").Child("echo")
	remoteRef, err := node.ActorSelection(echoPath.String()).Resolve(ctx)
	if err != nil {
		log.Fatalf("ActorSelection.Resolve: %v", err)
	}
	log.Printf("[aeron] remote echo actor: %s", remoteRef)

	// ── Step 3: Join via Aeron UDP ────────────────────────────────────────
	log.Printf("[aeron] joining cluster via seed %s …", seed)
	if err := node.JoinSeeds(); err != nil {
		log.Fatalf("JoinSeeds: %v", err)
	}
	if err := node.WaitForHandshake(ctx, seed.Host, uint32(seed.Port)); err != nil {
		log.Fatalf("WaitForHandshake: %v", err)
	}
	log.Printf("[aeron] Aeron UDP handshake complete — joined cluster.")

	// ── Step 4: Subscribe to cluster events ──────────────────────────────
	watcherRef, err := node.System.ActorOf(gekka.Props{
		New: func() actor.Actor { return &clusterEventActor{BaseActor: actor.NewBaseActor()} },
	}, "clusterWatcher")
	if err != nil {
		log.Fatalf("ActorOf watcher: %v", err)
	}
	node.Subscribe(watcherRef)
	defer node.Unsubscribe(watcherRef)

	// ── Step 5: Send periodic pings ───────────────────────────────────────
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	sendPing := func() {
		payload := []byte(fmt.Sprintf("Ping at %s", time.Now().Format(time.TimeOnly)))
		log.Printf("[aeron] -> Tell %q  →  %s", payload, remoteRef)
		remoteRef.Tell(payload, localRef)
	}

	askPing := func() {
		askCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		reply, err := remoteRef.(gekka.ActorRef).Ask(askCtx, []byte("Ping"))
		if err != nil {
			log.Printf("[aeron] Ask error: %v", err)
			return
		}
		switch v := reply.(type) {
		case []byte:
			log.Printf("[aeron] <- Ask reply: %q", v)
		default:
			log.Printf("[aeron] <- Ask reply: %v", v)
		}
	}

	sendPing()
	askPing()

	tick := 0
	for {
		select {
		case <-ticker.C:
			tick++
			if tick%2 == 0 {
				askPing()
			} else {
				sendPing()
			}

		case <-ctx.Done():
			log.Println("[aeron] signal received — leaving cluster …")
			if err := node.Leave(); err != nil {
				log.Printf("[aeron] Leave error: %v", err)
			}
			time.Sleep(2 * time.Second)
			return
		}
	}
}
