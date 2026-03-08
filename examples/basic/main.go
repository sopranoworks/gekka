// Package main is the "basic" Gekka example.
//
// It demonstrates the core v0.1.0+ API patterns in a single file:
//
//  1. Load HOCON configuration with SpawnFromConfig.
//  2. Create a typed Actor via node.System.ActorOf (Pekko-style actorOf pattern).
//  3. Join the cluster via JoinSeeds.
//  4. Deliver messages with ActorRef.Tell and ActorRef.Ask.
//  5. Subscribe to cluster membership events.
//  6. Graceful leave on SIGINT / SIGTERM.
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

// ── Actor definition ──────────────────────────────────────────────────────────

// EchoActor responds to "Ping" messages with "Pong".
//
// Embed actor.BaseActor to get the mailbox channel for free; implement
// Receive to handle each inbound message.
type EchoActor struct {
	actor.BaseActor
	echoRef gekka.ActorRef // ref to the remote echo actor
}

// Receive is called once per message in a dedicated goroutine.
// Messages are typed as *gekka.IncomingMessage because that is what
// GekkaNode.RegisterActor pushes into the mailbox.
func (a *EchoActor) Receive(msg any) {
	incoming, ok := msg.(*gekka.IncomingMessage)
	if !ok {
		return
	}

	text := string(incoming.Payload)
	log.Printf("[actor] <- received (serializerId=%d): %q", incoming.SerializerId, text)

	if text == "Ping" {
		log.Printf("[actor] -> replying Pong to %s", a.echoRef)
		a.echoRef.Tell([]byte("Pong"))
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

	// Build an ActorRef for the remote echo actor via ActorSelection.
	// For remote URIs, Resolve returns immediately — the TCP connection is
	// established lazily on the first Tell or Ask.
	echoPath := seed.WithRoot("user").Child("echo")
	echoRef, err := node.ActorSelection(echoPath.String()).Resolve(ctx)
	if err != nil {
		log.Fatalf("ActorSelection.Resolve: %v", err)
	}
	log.Printf("[gekka] remote echo actor: %s", echoRef)

	// ── Step 2: Create a local actor with node.System.ActorOf ────────────
	//
	// ActorOf uses Props to instantiate the actor, registers it at
	// /user/<name>, starts its goroutine, and returns a location-transparent
	// ActorRef.  Dependencies are captured by the Props.New closure.
	localRef, err := node.System.ActorOf(gekka.Props{
		New: func() actor.Actor {
			return &EchoActor{
				BaseActor: actor.NewBaseActor(),
				echoRef:   echoRef,
			}
		},
	}, "echo")
	if err != nil {
		log.Fatalf("ActorOf: %v", err)
	}
	log.Printf("[gekka] local echo actor:  %s", localRef)

	// ── Step 3: Join the cluster ──────────────────────────────────────────
	log.Printf("[gekka] joining cluster via seed %s …", seed)
	if err := node.JoinSeeds(); err != nil {
		log.Fatalf("JoinSeeds: %v", err)
	}
	if err := node.WaitForHandshake(ctx, seed.Host, uint32(seed.Port)); err != nil {
		log.Fatalf("WaitForHandshake: %v", err)
	}
	log.Printf("[gekka] Artery handshake complete — joined cluster.")

	// ── Step 4: Subscribe to cluster membership events ────────────────────
	events := make(chan gekka.ClusterDomainEvent, 16)
	node.Subscribe(events)
	defer node.Unsubscribe(events)

	go func() {
		for {
			select {
			case evt, ok := <-events:
				if !ok {
					return
				}
				switch e := evt.(type) {
				case gekka.MemberUp:
					log.Printf("[cluster] MemberUp:      %s", e.Member)
				case gekka.MemberLeft:
					log.Printf("[cluster] MemberLeft:    %s", e.Member)
				case gekka.MemberExited:
					log.Printf("[cluster] MemberExited:  %s", e.Member)
				case gekka.MemberRemoved:
					log.Printf("[cluster] MemberRemoved: %s", e.Member)
				case gekka.UnreachableMember:
					log.Printf("[cluster] Unreachable:   %s", e.Member)
				case gekka.ReachableMember:
					log.Printf("[cluster] Reachable:     %s", e.Member)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// ── Step 5: Send messages periodically ───────────────────────────────
	//
	// Alternate between fire-and-forget Tell and request-reply Ask so both
	// patterns are visible in the log.
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	sendPing := func() {
		payload := []byte(fmt.Sprintf("Ping at %s", time.Now().Format(time.TimeOnly)))
		log.Printf("[gekka] -> Tell %q  →  %s", payload, echoRef)
		echoRef.Tell(payload)
	}

	askPing := func() {
		askCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		reply, err := echoRef.Ask(askCtx, []byte("Ping"))
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
