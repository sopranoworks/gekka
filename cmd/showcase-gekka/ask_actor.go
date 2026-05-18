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

// AskEnvelope mirrors the Scala case class used by the ask-pattern showcase.
//
// As with EchoEnvelope, the cross-language CBOR codec gap (Scala's
// jackson-cbor serializer is not yet registered on the Gekka side) means that
// a Scala-originated AskEnvelope arrives as a raw *gekka.IncomingMessage; the
// architectural concern is tracked in the plan and surfaced explicitly by the
// receiver below, not silently dropped.
type AskEnvelope struct {
	SeqNo       int64       `json:"seqNo"`
	Originator  string      `json:"originator"`
	Direction   string      `json:"direction"`
	PayloadKind string      `json:"payloadKind"`
	Payload     interface{} `json:"payload"`
}

// AskActor handles the receiver half of FT2. It accepts a SEND-direction
// AskEnvelope, flips Direction to REPLY, and Tells it back to the Sender.
// Replies travel via the Sender() reference so the ask-pattern caller's temp
// reply path receives them.
type AskActor struct {
	actor.BaseActor
}

// Receive handles ask-pattern requests and surfaces raw remote deliveries
// (no codec) as structured errors.
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

	case *gekka.IncomingMessage:
		// Raw remote delivery — the deserializer for this serializer ID is
		// not registered on the Gekka side. Surface a structured error so
		// the smoke test catches it; do not crash the actor.
		logger.Default().Error("AskActor: unsupported remote envelope",
			"serializerId", m.SerializerId,
			"manifest", m.Manifest,
			"payloadLen", len(m.Payload),
			"recipient", m.RecipientPath)

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

	mu       sync.Mutex
	nextSeq  int64
	stopOnce sync.Once
	stopCh   chan struct{}
	started  bool
}

// NewAskSenderActor constructs an AskSenderActor. The actor is inert until
// StartTickers is invoked after registration.
func NewAskSenderActor(cluster *gekka.Cluster, peers []string, origin string) *AskSenderActor {
	return &AskSenderActor{
		BaseActor: actor.NewBaseActor(),
		cluster:   cluster,
		peers:     peers,
		origin:    origin,
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
		payload := payloadValueForKind(kind, seq)
		env := &AskEnvelope{
			SeqNo:       seq,
			Originator:  a.origin,
			Direction:   "SEND",
			PayloadKind: kind,
			Payload:     payload,
		}
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
		logger.Default().Error("AskSender: ask timeout/error",
			"peer", peer,
			"seq", seq,
			"payloadKind", kind,
			"latencyMs", latency.Milliseconds(),
			"err", err.Error())
		return
	}

	// Local replies arrive as the deserialized payload; remote replies
	// (without a registered CBOR codec) arrive as raw bytes. Both cases
	// are logged distinctly so the smoke test can tell them apart.
	if reply == nil {
		logger.Default().Error("AskSender: nil reply",
			"peer", peer, "seq", seq, "payloadKind", kind)
		return
	}
	if env, ok := reply.DeserializedMessage.(*AskEnvelope); ok {
		if env.Direction != "REPLY" {
			logger.Default().Error("AskSender: non-REPLY direction",
				"peer", peer, "seq", env.SeqNo, "direction", env.Direction)
			return
		}
		logger.Default().Info("AskSender: REPLY",
			"seq", env.SeqNo,
			"peer", peer,
			"payloadKind", env.PayloadKind,
			"latencyMs", latency.Milliseconds())
		return
	}
	// Remote raw-bytes reply: same cross-language codec gap as the Tell
	// path. Log as ERROR so the architectural concern is visible.
	logger.Default().Error("AskSender: unsupported remote reply",
		"peer", peer,
		"seq", seq,
		"payloadKind", kind,
		"serializerId", reply.SerializerId,
		"manifest", reply.Manifest,
		"payloadLen", len(reply.Payload))
}

// peerAskPath builds the full /user/ask URI for a peer base URL like
// "pekko://ShowcaseCluster@127.0.0.1:2552".
func peerAskPath(peer string) string {
	p := strings.TrimRight(peer, "/")
	return p + "/user/ask"
}
