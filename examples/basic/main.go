// Package main is the "basic" Gekka example.
//
// It demonstrates the core v0.1.0+ API patterns in a single file:
//
//  1. Load HOCON configuration with SpawnFromConfig.
//  2. Create a typed Actor via node.System.ActorOf (Pekko-style actorOf pattern).
//  3. Join the cluster via JoinSeeds.
//  4. Deliver messages with ActorRef.Tell and ActorRef.Ask.
//  5. Reply to the sender using BaseActor.Sender() and BaseActor.Self().
//  6. Subscribe to cluster membership events via an actor.
//  7. Graceful leave on SIGINT / SIGTERM.
//
// Run alongside a Pekko seed node — see README.md.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gekka/gekka"
	"gekka/gekka/actor"
)

// ── Actor definitions ─────────────────────────────────────────────────────────

// clusterWatcherActor receives and logs cluster domain events.
// It is subscribed to the node's cluster event stream via node.Subscribe.
type clusterWatcherActor struct {
	actor.BaseActor
}

func (a *clusterWatcherActor) Receive(msg any) {
	if evt, ok := msg.(gekka.ClusterDomainEvent); ok {
		switch e := evt.(type) {
		case gekka.MemberUp:
			a.Log().Info("MemberUp", "member", e.Member)
		case gekka.MemberLeft:
			a.Log().Info("MemberLeft", "member", e.Member)
		case gekka.MemberExited:
			a.Log().Info("MemberExited", "member", e.Member)
		case gekka.MemberRemoved:
			a.Log().Info("MemberRemoved", "member", e.Member)
		case gekka.UnreachableMember:
			a.Log().Warn("Unreachable", "member", e.Member)
		case gekka.ReachableMember:
			a.Log().Info("Reachable", "member", e.Member)
		}
	}
}

// EchoActor receives messages from the remote Pekko echo actor and replies
// using the location-transparent Sender() / Self() API.
//
// Key patterns demonstrated:
//   - a.Sender() — the ActorRef of whoever sent this message (local or remote).
//   - a.Self()   — this actor's own ActorRef, used as the sender of replies.
//   - Tell(msg, a.Self()) — sends a reply so the remote actor knows who to
//     respond to next (mirrors Pekko's sender() / self pattern).
type EchoActor struct {
	actor.BaseActor
}

// Receive is called once per message in a dedicated goroutine.
// Messages from Artery arrive as *gekka.IncomingMessage; direct local Tell
// calls arrive as their original type.
func (a *EchoActor) Receive(msg any) {
	switch m := msg.(type) {
	case *gekka.IncomingMessage:
		text := string(m.Payload)
		a.Log().Info("received", "serializerId", m.SerializerId, "payload", text)

		// Reply to whoever sent the message.  If the sender is known (non-nil),
		// use Tell with a.Self() so the remote side can identify the reply origin.
		// This mirrors Pekko's:  sender() ! "Ack: " + text
		if s := a.Sender(); s != nil && s.Path() != "" {
			reply := []byte("Ack: " + text)
			a.Log().Info("replying", "payload", string(reply))
			s.Tell(reply, a.Self())
		}

	default:
		// Local Tell without an IncomingMessage wrapper (e.g. from tests or
		// internal actors).  Sender() is still set if Tell was called with a
		// sender argument.
		a.Log().Info("local message", "msg", m)
	}
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	// Respect SIGINT (Ctrl-C) and SIGTERM so the node leaves gracefully.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── Step 1: Spawn ─────────────────────────────────────────────────────
	node, err := gekka.SpawnFromConfig("application.conf")
	if err != nil {
		log.Fatalf("SpawnFromConfig: %v", err)
	}
	defer node.Shutdown()

	self := node.SelfAddress()
	log.Printf("[gekka] node listening on  %s (port %d)", node.Addr(), node.Port())
	log.Printf("[gekka] actor-system name: %s", self.System)

	// Resolve the seed address from config.
	seeds := node.Seeds()
	if len(seeds) == 0 {
		log.Fatal("no seed-nodes configured in application.conf")
	}
	seed := seeds[0]

	// ── Step 2: Create the local EchoActor with node.System.ActorOf ───────
	//
	// ActorOf uses Props to instantiate the actor, registers it at /user/echo,
	// starts its goroutine, and returns a location-transparent ActorRef.
	// SpawnActor automatically injects Self() so the actor can use it inside
	// Receive without capturing a reference externally.
	localRef, err := node.System.ActorOf(gekka.Props{
		New: func() actor.Actor {
			return &EchoActor{BaseActor: actor.NewBaseActor()}
		},
	}, "echo")
	if err != nil {
		log.Fatalf("ActorOf: %v", err)
	}
	log.Printf("[gekka] local echo actor:  %s", localRef)

	// Build an ActorRef for the remote Pekko echo actor via ActorSelection.
	// For remote URIs, Resolve returns immediately — the TCP connection is
	// established lazily on the first Tell or Ask.
	echoPath := seed.WithRoot("user").Child("echo")
	remoteRef, err := node.ActorSelection(echoPath.String()).Resolve(ctx)
	if err != nil {
		log.Fatalf("ActorSelection.Resolve: %v", err)
	}
	log.Printf("[gekka] remote echo actor: %s", remoteRef)

	// ── Step 3: Join the cluster ──────────────────────────────────────────
	log.Printf("[gekka] joining cluster via seed %s …", seed)
	if err := node.JoinSeeds(); err != nil {
		log.Fatalf("JoinSeeds: %v", err)
	}
	if err := node.WaitForHandshake(ctx, seed.Host, uint32(seed.Port)); err != nil {
		log.Fatalf("WaitForHandshake: %v", err)
	}
	log.Printf("[gekka] Artery handshake complete — joined cluster.")

	// ── Step 4: Subscribe to cluster events via an actor ──────────────────
	//
	// Using an actor for event delivery (instead of a channel) means the
	// handler runs in its own goroutine and benefits from the same sequential
	// mailbox semantics as any other actor.
	watcherRef, err := node.System.ActorOf(gekka.Props{
		New: func() actor.Actor {
			return &clusterWatcherActor{BaseActor: actor.NewBaseActor()}
		},
	}, "clusterWatcher")
	if err != nil {
		log.Fatalf("ActorOf watcher: %v", err)
	}
	node.Subscribe(watcherRef)
	defer node.Unsubscribe(watcherRef)

	// ── Step 5: Send messages periodically ───────────────────────────────
	//
	// sendPing — fire-and-forget Tell.  Pass localRef as sender so that the
	// remote Pekko echo actor's reply is routed back to our local /user/echo.
	//
	// askPing  — request-reply Ask (uses an ephemeral temp actor internally;
	// the Sender in the remote actor will be the temp path, not localRef).
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	sendPing := func() {
		payload := []byte(fmt.Sprintf("Ping at %s", time.Now().Format(time.TimeOnly)))
		log.Printf("[gekka] -> Tell %q  →  %s  (sender=%s)", payload, remoteRef, localRef)
		// Tell with localRef as sender: remote actor's Sender() returns localRef.
		remoteRef.Tell(payload, localRef)
	}

	askPing := func() {
		askCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		reply, err := remoteRef.Ask(askCtx, []byte("Ping"))
		if err != nil {
			log.Printf("[gekka] Ask error: %v", err)
			return
		}
		switch v := reply.(type) {
		case []byte:
			log.Printf("[gekka] <- Ask reply: %q", v)
		default:
			log.Printf("[gekka] <- Ask reply: %v", v)
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

		// ── Step 6: Graceful shutdown ──────────────────────────────────
		case <-ctx.Done():
			log.Println("[gekka] signal received — leaving cluster …")
			if err := node.Leave(); err != nil {
				log.Printf("[gekka] Leave error: %v", err)
			}
			time.Sleep(2 * time.Second)
			return
		}
	}
}
