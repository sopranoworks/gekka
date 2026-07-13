// cmd/showcase-gekka/tell_sender.go — Gekka-side TellSenderActor for FT1
// of the gekka_showcase_test plan.
//
// TellSenderActor ticks every 4 seconds, sends an EchoEnvelope("SEND") to
// every peer's /user/echo, records the in-flight request in `pending`, and
// expects a REPLY back within 10 seconds. A separate sweep loop scans the
// pending map every 1 second and emits an ERROR log for entries that have
// exceeded the timeout.
//
// SPDX-License-Identifier: MIT
package main

import (
	"fmt"
	"strings"
	"sync"
	"time"

	gekka "github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/logger"
)

const (
	tellTickInterval = 4 * time.Second
	tellTimeout      = 10 * time.Second
	tellSweepEvery   = 1 * time.Second
)

// tickToken is delivered to the actor itself by the tick goroutine.
type tickToken struct{}

// sweepToken is delivered to the actor itself by the sweep goroutine.
type sweepToken struct{}

// pendingEntry records an in-flight request awaiting a REPLY.
type pendingEntry struct {
	peer        string
	payloadKind string
	sentAt      time.Time
}

// TellSenderActor periodically sends EchoEnvelope messages to peer EchoActors
// and tracks request/response latency, emitting ERROR logs on timeouts.
type TellSenderActor struct {
	actor.BaseActor

	cluster *gekka.Cluster
	peers   []string // full pekko URIs, e.g. "pekko://ShowcaseCluster@127.0.0.1:2552"
	origin  string   // self-address string used as Originator in envelopes
	// anchor scopes ERROR accounting to the strict Gate-2 window: echoes
	// SENT before the local all-members-Up observation are setup-phase
	// traffic and their timeouts must not ERROR (spec §4). See anchor.go.
	anchor *steadyAnchor

	mu       sync.Mutex
	nextSeq  int64
	pending  map[int64]pendingEntry
	stopOnce sync.Once
	stopCh   chan struct{}
	started  bool
}

// NewTellSenderActor constructs a TellSenderActor. The actor is inert until
// StartTickers is invoked after it has been registered via ActorOf.
func NewTellSenderActor(cluster *gekka.Cluster, peers []string, origin string, anchor *steadyAnchor) *TellSenderActor {
	return &TellSenderActor{
		BaseActor: actor.NewBaseActor(),
		cluster:   cluster,
		peers:     peers,
		origin:    origin,
		anchor:    anchor,
		pending:   make(map[int64]pendingEntry),
		stopCh:    make(chan struct{}),
	}
}

// StartTickers spawns the tick + sweep goroutines. It is safe to call once;
// subsequent calls are no-ops.
func (a *TellSenderActor) StartTickers() {
	a.mu.Lock()
	if a.started {
		a.mu.Unlock()
		return
	}
	a.started = true
	a.mu.Unlock()

	go a.tickLoop()
	go a.sweepLoop()
}

// PostStop signals both background goroutines to exit. Safe to call multiple
// times.
func (a *TellSenderActor) PostStop() {
	a.stopOnce.Do(func() { close(a.stopCh) })
}

// tickLoop drives outbound sends by Telling tickToken to the actor itself.
func (a *TellSenderActor) tickLoop() {
	t := time.NewTicker(tellTickInterval)
	defer t.Stop()
	for {
		select {
		case <-a.stopCh:
			return
		case <-t.C:
			if self := a.Self(); self != nil {
				self.Tell(tickToken{}, self)
			}
		}
	}
}

// sweepLoop scans the pending map for entries past tellTimeout.
func (a *TellSenderActor) sweepLoop() {
	t := time.NewTicker(tellSweepEvery)
	defer t.Stop()
	for {
		select {
		case <-a.stopCh:
			return
		case <-t.C:
			if self := a.Self(); self != nil {
				self.Tell(sweepToken{}, self)
			}
		}
	}
}

// Receive handles tick / sweep / reply events. Tokens drive the outbound
// send and timeout sweep; *EchoEnvelope replies clear the pending entry.
func (a *TellSenderActor) Receive(msg any) {
	switch m := msg.(type) {
	case tickToken:
		a.sendOnce()

	case sweepToken:
		a.sweep()

	case *EchoEnvelope:
		if m.Direction != "REPLY" {
			logger.Default().Error("TellSender: non-REPLY envelope",
				"direction", m.Direction,
				"seq", m.SeqNo,
				"originator", m.Originator)
			return
		}
		a.mu.Lock()
		entry, ok := a.pending[m.SeqNo]
		if ok {
			delete(a.pending, m.SeqNo)
		}
		a.mu.Unlock()
		if !ok {
			logger.Default().Error("TellSender: REPLY for unknown seq",
				"seq", m.SeqNo,
				"originator", m.Originator)
			return
		}
		latency := time.Since(entry.sentAt)
		logger.Default().Info("TellSender: REPLY",
			"seq", m.SeqNo,
			"peer", entry.peer,
			"payloadKind", entry.payloadKind,
			"latencyMs", latency.Milliseconds())

	default:
		logger.Default().Error("TellSender: unexpected message",
			"type", fmtType(msg))
	}
}

// sendOnce rotates the payload kind and sends one EchoEnvelope to every peer.
func (a *TellSenderActor) sendOnce() {
	if len(a.peers) == 0 {
		return
	}
	self := a.Self()
	if self == nil {
		return
	}

	// Allocate a unique seq per (tick, peer). The pending map is keyed by
	// seq alone, so reusing one seq across N peers (the prior shape) made
	// the last peer's pendingEntry overwrite the others — surfaces as
	// "REPLY for unknown seq" floods once cross-language replies actually
	// flow (which they didn't before Phase 3's JacksonCborSerializer wiring).
	for _, peer := range a.peers {
		a.mu.Lock()
		a.nextSeq++
		seq := a.nextSeq
		a.mu.Unlock()

		kind := payloadKindForSeq(seq)
		payload := payloadValueForKind(kind, seq, a.origin)
		env := NewEchoEnvelope(seq, a.origin, "SEND", kind, payload)
		path := peerEchoPath(peer)

		a.mu.Lock()
		a.pending[seq] = pendingEntry{
			peer:        peer,
			payloadKind: kind,
			sentAt:      time.Now(),
		}
		a.mu.Unlock()

		// Resolve via ActorSelection so the reply finds its way back to
		// this actor via Sender(); fall back to a path-only Send so the
		// outbound message still leaves the node when the remote actor is
		// not locally registered.
		sel := a.cluster.ActorSelection(path)
		ref, err := sel.Resolve(a.cluster.Context())
		if err != nil {
			if !a.anchor.countsForStrictWindow(time.Now()) {
				logger.Default().Debug("TellSender: setup-phase resolve failure (not counted)",
					"peer", peer, "path", path, "err", err.Error())
				continue
			}
			logger.Default().Error("TellSender: resolve peer failed",
				"peer", peer, "path", path, "err", err.Error())
			continue
		}
		ref.Tell(env, self)
	}
}

// sweep emits ERROR logs for pending entries that have exceeded tellTimeout
// and removes them from the pending map.
func (a *TellSenderActor) sweep() {
	now := time.Now()
	a.mu.Lock()
	expired := make([]int64, 0)
	for seq, entry := range a.pending {
		if now.Sub(entry.sentAt) >= tellTimeout {
			expired = append(expired, seq)
		}
	}
	type expEntry struct {
		seq   int64
		entry pendingEntry
	}
	exp := make([]expEntry, 0, len(expired))
	for _, seq := range expired {
		exp = append(exp, expEntry{seq: seq, entry: a.pending[seq]})
		delete(a.pending, seq)
	}
	a.mu.Unlock()

	for _, e := range exp {
		// Setup-phase sends (pre-anchor) expire silently — spec §4: the
		// strict window begins at Gate 1 PASS, and a peer that had not
		// even booted when the envelope left cannot fail the run.
		if !a.anchor.countsForStrictWindow(e.entry.sentAt) {
			logger.Default().Debug("TellSender: setup-phase echo miss (not counted)",
				"seq", e.seq,
				"peer", e.entry.peer,
				"payloadKind", e.entry.payloadKind)
			continue
		}
		logger.Default().Error("TellSender: echo timeout",
			"seq", e.seq,
			"peer", e.entry.peer,
			"payloadKind", e.entry.payloadKind,
			"ageMs", now.Sub(e.entry.sentAt).Milliseconds())
	}
}

// payloadKindForSeq rotates through the four payload kinds the plan calls
// for: string, long, system, custom.
func payloadKindForSeq(seq int64) string {
	switch seq % 4 {
	case 1:
		return "string"
	case 2:
		return "long"
	case 3:
		return "system"
	default:
		return "custom"
	}
}

// payloadValueForKind returns a representative payload value for a given
// payload kind. Each kind matches the Scala-side TellSenderActor.scala:50-55
// rotation so cross-language echo round-trips exercise the same wire shapes
// in both directions.
func payloadValueForKind(kind string, seq int64, originator string) interface{} {
	switch kind {
	case "string":
		return fmt.Sprintf("hello-%d-%s", seq, originator)
	case "long":
		return seq
	case "system":
		return NewSystemMessagePing(seq, originator)
	case "custom":
		bytes := make([]byte, 16)
		for i := range bytes {
			bytes[i] = 0x42
		}
		return NewShowcaseEchoCustom(seq, originator, bytes)
	default:
		return seq
	}
}

// peerEchoPath builds the full /user/echo URI for a peer base URL like
// "pekko://ShowcaseCluster@127.0.0.1:2552".
func peerEchoPath(peer string) string {
	p := strings.TrimRight(peer, "/")
	return p + "/user/echo"
}
