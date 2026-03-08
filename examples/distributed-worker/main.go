// Package main demonstrates Cluster Singleton routing via Gekka's
// ClusterSingletonProxy, including automatic failover.
//
// Pattern:
//
//  1. SpawnFromConfig — load application.conf (3-node cluster config).
//  2. node.System.ActorOf — create a local AckActor to receive replies.
//  3. JoinSeeds — connect to both Scala seed nodes.
//  4. SingletonProxy — route jobs to /user/singletonManager/singleton on
//     the current oldest node; re-resolves automatically on every Send.
//  5. Periodic dispatch — send a new Job every 2 s; print the singleton's
//     current location so failover is visible in the log.
//
// Run alongside two Pekko ClusterSingletonManager nodes — see README.md.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gekka/gekka"
	"gekka/gekka/actor"
)

// Job is sent to the cluster singleton for processing.
// It is JSON-encoded and delivered as raw bytes (Pekko ByteArraySerializer, ID 4).
type Job struct {
	ID      string `json:"id"`
	Payload string `json:"payload"`
	SentAt  string `json:"sent_at"`
}

// Ack is the expected JSON reply from the Scala singleton actor.
// The schema matches ClusterSingletonServer.scala in the test suite.
type Ack struct {
	JobID   string `json:"job_id"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// ── AckActor ──────────────────────────────────────────────────────────────────

// AckActor handles incoming acknowledgement messages from the Scala singleton.
// Embed actor.BaseActor to get the channel-based mailbox for free.
type AckActor struct {
	actor.BaseActor
}

// Receive is called once per inbound message in the actor's goroutine.
func (a *AckActor) Receive(msg any) {
	incoming, ok := msg.(*gekka.IncomingMessage)
	if !ok {
		return
	}
	var ack Ack
	if err := json.Unmarshal(incoming.Payload, &ack); err == nil && ack.JobID != "" {
		log.Printf("[worker] <- ack  job_id=%q  status=%q  msg=%q",
			ack.JobID, ack.Status, ack.Message)
	} else {
		log.Printf("[worker] <- received (serializerId=%d): %q",
			incoming.SerializerId, incoming.Payload)
	}
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── Step 1: Spawn ─────────────────────────────────────────────────────
	//
	// SpawnFromConfig reads application.conf, resolves hostname/port, and
	// starts the Artery TCP listener.  Port 0 lets the OS pick a free port.
	node, err := gekka.SpawnFromConfig("application.conf")
	if err != nil {
		log.Fatalf("SpawnFromConfig: %v", err)
	}
	defer node.Shutdown()

	log.Printf("[worker] listening on %s", node.Addr())

	seeds := node.Seeds()
	if len(seeds) == 0 {
		log.Fatal("[worker] no seed-nodes configured in application.conf")
	}
	seed := seeds[0]

	// ── Step 2: Register a local AckActor with node.System.ActorOf ────────
	//
	// The Scala singleton actor replies to the Artery sender address embedded
	// in every frame.  Creating a local actor here gives the singleton a
	// stable /user/ackReceiver path to reply to.
	_, err = node.System.ActorOf(gekka.Props{
		New: func() actor.Actor {
			return &AckActor{BaseActor: actor.NewBaseActor()}
		},
	}, "ackReceiver")
	if err != nil {
		log.Fatalf("ActorOf: %v", err)
	}

	// ── Step 3: Join the cluster ─────────────────────────────────────────
	//
	// JoinSeeds contacts the first seed listed in application.conf.
	// WaitForHandshake blocks until the Artery TCP association is ASSOCIATED.
	log.Printf("[worker] joining cluster via %s …", seed)
	if err := node.JoinSeeds(); err != nil {
		log.Fatalf("JoinSeeds: %v", err)
	}
	if err := node.WaitForHandshake(ctx, seed.Host, uint32(seed.Port)); err != nil {
		log.Fatalf("WaitForHandshake: %v", err)
	}
	log.Printf("[worker] joined cluster. Waiting for membership to converge …")

	// ── Step 4: Create a ClusterSingletonProxy ───────────────────────────
	//
	// SingletonProxy("/user/singletonManager", "") routes to the actor at
	//   pekko://ClusterSystem@<oldest-host>:<oldest-port>/user/singletonManager/singleton
	//
	// The second argument is an optional cluster role filter ("" = any role).
	// On every Send the proxy re-queries OldestNode() from the gossip state,
	// so if the oldest node leaves or crashes the next Send automatically
	// targets the new oldest node — no retry logic needed here.
	proxy := node.SingletonProxy("/user/singletonManager", "")

	// ── Step 5: Dispatch jobs periodically ───────────────────────────────
	//
	// Every 2 s:
	//  a) Print where the singleton currently lives (failover is visible here).
	//  b) Send a JSON-encoded Job to the singleton.
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	jobCounter := 0
	for {
		select {
		case <-ticker.C:
			jobCounter++

			// Resolve the current singleton actor path.
			// If the oldest node changes between ticks — e.g. because you
			// stopped seed-1 — this path will point to seed-2 next time.
			singletonPath, err := proxy.CurrentOldestPath()
			if err != nil {
				// Not yet converged (membership gossip still propagating).
				log.Printf("[worker] singleton not elected yet: %v", err)
				continue
			}
			log.Printf("[worker] singleton → %s", singletonPath)

			// Encode and dispatch the job.
			job := Job{
				ID:      fmt.Sprintf("job-%04d", jobCounter),
				Payload: fmt.Sprintf("process-batch-%d", jobCounter),
				SentAt:  time.Now().Format(time.RFC3339),
			}
			data, err := json.Marshal(job)
			if err != nil {
				log.Printf("[worker] marshal error: %v", err)
				continue
			}

			// proxy.Send re-resolves OldestNode() on each call.
			// If the singleton has migrated to a different node, the next
			// Send reaches the new host without any application changes.
			if err := proxy.Send(ctx, data); err != nil {
				log.Printf("[worker] send error: %v", err)
			} else {
				log.Printf("[worker] → dispatched %s", job.ID)
			}

		// ── Step 6: Graceful shutdown ─────────────────────────────────
		//
		// Leave broadcasts a Leave message so the Pekko SBR can cleanly
		// transition this node through Leaving → Exiting → Removed.
		case <-ctx.Done():
			log.Println("[worker] signal received — leaving cluster …")
			if err := node.Leave(); err != nil {
				log.Printf("[worker] Leave error: %v", err)
			}
			// Give the Leave message time to propagate before Shutdown.
			time.Sleep(2 * time.Second)
			return
		}
	}
}
