// cmd/showcase-gekka/ask_actor.go — Gekka-side AskActor + AskSenderActor for
// FT2 of the gekka_showcase_test plan.
//
// AskActor is the receiver side: it accepts a SEND-direction AskEnvelope and
// replies with the same envelope flipped to REPLY direction, delivered to
// whichever Sender() the inbound message resolved to. This is the same shape
// as EchoActor (FT1), only it participates in the ask-pattern half of the
// showcase.
//
// AskSenderActor is the requester side: every 4 seconds it issues a
// cluster.Ask against each configured peer's /user/ask path with a 5 second
// per-request timeout. Failures (timeout, no peer, transport error) are
// surfaced as structured ERROR logs so the smoke test catches regressions.
//
// SPDX-License-Identifier: MIT
package main

import (
	"context"
	"strings"
	"sync"
	"time"

	gekka "github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/logger"
)

// AskActor handles the receiver half of FT2. It accepts a SEND-direction
// AskEnvelope, flips Direction to REPLY, and Tells it back to the Sender.
// Replies travel via the Sender() reference so the ask-pattern caller's temp
// reply path receives them.
//
// AskEnvelope is defined in envelope.go alongside the other showcase wire
// types. The Phase 3 JacksonCborSerializer registration in main.go makes
// Scala-emitted jackson-cbor frames decode directly into *AskEnvelope.
type AskActor struct {
	actor.BaseActor
}

// Receive handles ask-pattern requests.
func (a *AskActor) Receive(msg any) {
	switch m := msg.(type) {
	case *AskEnvelope:
		if m.Direction != "SEND" {
			logger.Default().Error("AskActor: non-SEND envelope",
				"direction", m.Direction,
				"seq", m.SeqNo,
				"originator", m.Originator)
			return
		}
		reply := *m
		reply.Direction = "REPLY"
		if s := a.Sender(); s != nil && s.Path() != "" {
			s.Tell(&reply, a.Self())
			return
		}
		logger.Default().Error("AskActor: no sender on SEND envelope",
			"seq", m.SeqNo,
			"originator", m.Originator)

	default:
		logger.Default().Error("AskActor: unexpected message",
			"type", fmtType(msg))
	}
}

const (
	// askInterval is the cadence at which AskSenderActor fires one
	// ask-pattern request per configured peer.
	askInterval = 4 * time.Second
	// askTimeout is the per-request budget for a single cluster.Ask call.
	askTimeout = 5 * time.Second
)

// AskSenderActor drives the requester side of FT2. On each tick it issues
// one cluster.Ask per peer. Each Ask is dispatched in its own goroutine so
// the actor mailbox is not blocked while waiting for the reply / timeout.
type AskSenderActor struct {
	actor.BaseActor

	cluster *gekka.Cluster
	peers   []string // full pekko URIs (e.g. "pekko://ShowcaseCluster@127.0.0.1:2552")
	origin  string   // self-address string used as Originator in envelopes
	// anchor scopes ERROR accounting to the strict Gate-2 window (spec §4);
	// asks issued before the local all-members-Up observation are
	// setup-phase traffic and their timeouts must not ERROR. See anchor.go.
	anchor *steadyAnchor

	mu       sync.Mutex
	nextSeq  int64
	stopOnce sync.Once
	stopCh   chan struct{}
	started  bool
}

// NewAskSenderActor constructs an AskSenderActor. The actor is inert until
// StartTickers is invoked after registration.
func NewAskSenderActor(cluster *gekka.Cluster, peers []string, origin string, anchor *steadyAnchor) *AskSenderActor {
	return &AskSenderActor{
		BaseActor: actor.NewBaseActor(),
		cluster:   cluster,
		peers:     peers,
		origin:    origin,
		anchor:    anchor,
		stopCh:    make(chan struct{}),
	}
}

// askTickToken is the self-tell delivered by the tick goroutine.
type askTickToken struct{}

// StartTickers launches the tick goroutine. Safe to call once; subsequent
// calls are no-ops.
func (a *AskSenderActor) StartTickers() {
	a.mu.Lock()
	if a.started {
		a.mu.Unlock()
		return
	}
	a.started = true
	a.mu.Unlock()

	go a.tickLoop()
}

// PostStop closes the stop channel so the tick goroutine exits cleanly.
func (a *AskSenderActor) PostStop() {
	a.stopOnce.Do(func() { close(a.stopCh) })
}

// tickLoop self-tells askTickToken on the configured cadence.
func (a *AskSenderActor) tickLoop() {
	t := time.NewTicker(askInterval)
	defer t.Stop()
	for {
		select {
		case <-a.stopCh:
			return
		case <-t.C:
			if self := a.Self(); self != nil {
				self.Tell(askTickToken{}, self)
			}
		}
	}
}

// Receive handles tick events; reply handling lives in the per-request
// goroutine because cluster.Ask returns its reply synchronously.
func (a *AskSenderActor) Receive(msg any) {
	switch msg.(type) {
	case askTickToken:
		a.tickRound()
	default:
		logger.Default().Error("AskSender: unexpected message",
			"type", fmtType(msg))
	}
}

// tickRound issues one Ask per peer using the rotating payload-kind scheme
// shared with TellSender (string / long / system / custom).
func (a *AskSenderActor) tickRound() {
	if len(a.peers) == 0 {
		return
	}

	for _, peer := range a.peers {
		a.mu.Lock()
		a.nextSeq++
		seq := a.nextSeq
		a.mu.Unlock()

		kind := payloadKindForSeq(seq)
		payload := payloadValueForKind(kind, seq, a.origin)
		env := NewAskEnvelope(seq, a.origin, "SEND", kind, payload)
		path := peerAskPath(peer)

		go a.doAsk(seq, peer, kind, path, env)
	}
}

// doAsk performs a single cluster.Ask and logs the outcome. It runs on its
// own goroutine so the actor mailbox is never blocked on a remote reply.
func (a *AskSenderActor) doAsk(seq int64, peer, kind, path string, env *AskEnvelope) {
	ctx, cancel := context.WithTimeout(context.Background(), askTimeout)
	defer cancel()

	start := time.Now()
	reply, err := a.cluster.Ask(ctx, path, env)
	latency := time.Since(start)
	if err != nil {
		// Setup-phase asks (sent pre-anchor) fail silently — spec §4:
		// the strict window begins at Gate 1 PASS; a peer that had not
		// booted yet cannot fail the run.
		if !a.anchor.countsForStrictWindow(start) {
			logger.Default().Debug("AskSender: setup-phase ask miss (not counted)",
				"peer", peer, "seq", seq, "payloadKind", kind)
			return
		}
		logger.Default().Error("AskSender: ask timeout/error",
			"peer", peer,
			"seq", seq,
			"payloadKind", kind,
			"latencyMs", latency.Milliseconds(),
			"err", err.Error())
		return
	}

	if reply == nil {
		if !a.anchor.countsForStrictWindow(start) {
			logger.Default().Debug("AskSender: setup-phase nil reply (not counted)",
				"peer", peer, "seq", seq, "payloadKind", kind)
			return
		}
		logger.Default().Error("AskSender: nil reply",
			"peer", peer, "seq", seq, "payloadKind", kind)
		return
	}
	replyEnv, ok := reply.DeserializedMessage.(*AskEnvelope)
	if !ok {
		logger.Default().Error("AskSender: unexpected reply type",
			"peer", peer,
			"seq", seq,
			"payloadKind", kind,
			"got", fmtType(reply.DeserializedMessage))
		return
	}
	if replyEnv.Direction != "REPLY" {
		logger.Default().Error("AskSender: non-REPLY direction",
			"peer", peer, "seq", replyEnv.SeqNo, "direction", replyEnv.Direction)
		return
	}
	logger.Default().Info("AskSender: REPLY",
		"seq", replyEnv.SeqNo,
		"peer", peer,
		"payloadKind", replyEnv.PayloadKind,
		"latencyMs", latency.Milliseconds())
}

// peerAskPath builds the full /user/ask URI for a peer base URL like
// "pekko://ShowcaseCluster@127.0.0.1:2552".
func peerAskPath(peer string) string {
	p := strings.TrimRight(peer, "/")
	return p + "/user/ask"
}
