/*
 * test_kit.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package testkit provides a general-purpose Actor TestKit for the Gekka
// actor framework.
//
// The central component is [TestProbe], a lightweight actor reference that
// records every message it receives and exposes time-aware assertion methods.
// Probes are created via [GekkaTestKit.NewProbe] and can be passed to any
// actor as a sender or target, just like a real actor reference.
//
// # Typical usage
//
//	tk    := testkit.NewTestKit(system)         // wrap your ActorSystem
//	probe := tk.NewProbe(t)                     // create a probe
//
//	// Pass probe.Ref() as the sender so the actor under test can reply.
//	actorRef.Tell("ping", probe.Ref())
//
//	// Assert the expected reply within 1 second.
//	probe.ExpectMsg("pong", time.Second)
//
//	// Assert no stray message arrives in the next 50 ms.
//	probe.ExpectNoMsg(50 * time.Millisecond)
//
// # Bridging existing test helpers
//
// If you currently use [actor.FunctionalMockRef] as an ad-hoc probe, replace it
// with [TestProbe]: it provides the same actor.Ref interface but adds structured
// assertions, timeout support, and sender tracking for Reply.
//
//	// Before (manual channel + FunctionalMockRef):
//	ch  := make(chan any, 1)
//	ref := actor.NewFunctionalMockRef("/temp/mock", func(m any) { ch <- m })
//	actor.Tell("ping", ref)
//	got := <-ch
//
//	// After (TestProbe):
//	probe := tk.NewProbe(t)
//	actor.Tell("ping", probe.Ref())
//	probe.ExpectMsg("ping", time.Second)
package testkit

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
)

// ─── GekkaTestKit ─────────────────────────────────────────────────────────────

// GekkaTestKit wraps an [actor.ActorContext] (e.g. a [gekka.ActorSystem]) and
// provides probe creation and other test utilities.
//
// System may be nil when only [TestProbe] is needed and no actors need to be
// spawned through the kit itself.
type GekkaTestKit struct {
	// System is the ActorContext used to spawn actors under test.
	// May be nil when the kit is used solely for TestProbe assertions.
	System actor.ActorContext

	// useCallingThread, when true, overrides Props.Dispatcher to
	// DispatcherCallingThread for every ActorOf call made through this kit.
	useCallingThread bool
}

// NewTestKit creates a GekkaTestKit backed by system.
// Pass nil when only probes are needed.
func NewTestKit(system actor.ActorContext) *GekkaTestKit {
	return &GekkaTestKit{System: system}
}

// WithCallingThreadDispatcher returns a new GekkaTestKit that forces all actors
// spawned via [GekkaTestKit.ActorOf] to use the CallingThreadDispatcher, making
// message delivery synchronous and deterministic in tests.
func (tk *GekkaTestKit) WithCallingThreadDispatcher() *GekkaTestKit {
	return &GekkaTestKit{System: tk.System, useCallingThread: true}
}

// ActorOf creates an actor through the kit's System, applying any kit-level
// options (e.g. WithCallingThreadDispatcher) to the props before delegating.
func (tk *GekkaTestKit) ActorOf(props actor.Props, name string) (actor.Ref, error) {
	if tk.useCallingThread {
		props.Dispatcher = actor.DispatcherCallingThread
	}
	return tk.System.ActorOf(props, name)
}

// NewProbe creates and returns a new [TestProbe].
// The probe is immediately ready to receive messages; it does not need to be
// registered with the actor system.
func (tk *GekkaTestKit) NewProbe(t testing.TB) *TestProbe {
	t.Helper()
	id := probeSeq.Add(1)
	return &TestProbe{
		path:    fmt.Sprintf("testkit://TestSystem/temp/probe-%d", id),
		mailbox: make(chan probeEnvelope, 512),
		t:       t,
	}
}

// probeSeq provides unique sequential IDs for probe paths.
var probeSeq atomic.Int64

// ─── probeEnvelope ────────────────────────────────────────────────────────────

// probeEnvelope is the internal record stored by TestProbe for each received
// message. It mirrors [actor.Envelope] but lives only within the testkit.
type probeEnvelope struct {
	msg    any
	sender actor.Ref
}

// ─── TestProbe ────────────────────────────────────────────────────────────────

// TestProbe is a lightweight [actor.Ref] that records every message it
// receives and provides time-aware assertion methods.
//
// Create probes with [GekkaTestKit.NewProbe].
type TestProbe struct {
	path    string
	mailbox chan probeEnvelope
	t       testing.TB

	lastMu  sync.Mutex
	lastEnv *probeEnvelope
}

// ── actor.Ref implementation ──────────────────────────────────────────────

// Tell captures msg (and the optional sender) into the probe's internal
// mailbox.  It satisfies [actor.Ref] so the probe can be used wherever an
// actor reference is accepted.
//
// If msg is an [actor.Envelope] (delivered by the local actor runtime when
// an explicit sender is present), Tell transparently unwraps it so that
// [ExpectMsg] always sees the raw payload.
func (p *TestProbe) Tell(msg any, sender ...actor.Ref) {
	var s actor.Ref
	if len(sender) > 0 && sender[0] != nil {
		s = sender[0]
	}

	env := probeEnvelope{sender: s}
	// Transparently unwrap actor.Envelope produced by the local actor runtime.
	if e, ok := msg.(actor.Envelope); ok {
		env.msg = e.Payload
		if s == nil && e.Sender != nil {
			env.sender = e.Sender
		}
	} else {
		env.msg = msg
	}

	select {
	case p.mailbox <- env:
	default:
		// Mailbox full — drop silently, analogous to Pekko dead-letters.
	}
}

// Path returns the unique actor-path URI assigned to this probe.
func (p *TestProbe) Path() string { return p.path }

// Ref returns the probe itself as an [actor.Ref].
// It is equivalent to a cast and exists for readability at the call site:
//
//	actorRef.Tell("ping", probe.Ref())
func (p *TestProbe) Ref() actor.Ref { return p }

// ── Assertions ────────────────────────────────────────────────────────────

// ExpectMsg blocks until the next message received by the probe equals msg
// (via [reflect.DeepEqual]) or timeout elapses.
//
// Any message that does not match immediately fails the test; no retries are
// attempted.  If the test should tolerate other messages, drain them manually
// before calling ExpectMsg.
//
// On match the received message is recorded as the "last message" so that a
// subsequent [Reply] call sends to the right sender.
func (p *TestProbe) ExpectMsg(msg any, timeout time.Duration) {
	p.t.Helper()
	select {
	case env := <-p.mailbox:
		if !reflect.DeepEqual(env.msg, msg) {
			p.t.Fatalf("testkit: ExpectMsg: got %v (%T), want %v (%T)",
				env.msg, env.msg, msg, msg)
		}
		p.setLast(&env)
	case <-time.After(timeout):
		p.t.Fatalf("testkit: ExpectMsg: no message matching %v (%T) within %s",
			msg, msg, timeout)
	}
}

// ExpectNoMsg asserts that no message arrives during duration.
// Fails the test immediately if any message is received during the interval.
func (p *TestProbe) ExpectNoMsg(duration time.Duration) {
	p.t.Helper()
	select {
	case env := <-p.mailbox:
		p.t.Fatalf("testkit: ExpectNoMsg: unexpected message %v (%T) within %s",
			env.msg, env.msg, duration)
	case <-time.After(duration):
		// Good: no message arrived.
	}
}

// Reply delivers msg to the sender of the last message received by this probe.
//
// It fails the test when:
//   - No message has been received yet ([ExpectMsg] was not called).
//   - The last message was sent without a sender ([actor.NoSender]).
func (p *TestProbe) Reply(msg any) {
	p.t.Helper()
	last := p.getLast()
	if last == nil {
		p.t.Fatal("testkit: Reply: no message has been received yet (call ExpectMsg first)")
		return
	}
	if last.sender == nil {
		p.t.Fatal("testkit: Reply: last message had no sender (was actor.NoSender)")
		return
	}
	last.sender.Tell(msg, p)
}

// ── Helpers ───────────────────────────────────────────────────────────────

func (p *TestProbe) setLast(env *probeEnvelope) {
	p.lastMu.Lock()
	defer p.lastMu.Unlock()
	cp := *env
	p.lastEnv = &cp
}

func (p *TestProbe) getLast() *probeEnvelope {
	p.lastMu.Lock()
	defer p.lastMu.Unlock()
	return p.lastEnv
}

// ─── Generic helpers ──────────────────────────────────────────────────────────

// ExpectMsgType blocks until a message of type T arrives on probe or timeout
// elapses.  It returns the typed message for further inspection.
//
// Any message of a different type immediately fails the test.
//
//	reply := testkit.ExpectMsgType[MyReply](probe, time.Second)
//	require.Equal(t, "ok", reply.Status)
func ExpectMsgType[T any](p *TestProbe, timeout time.Duration) T {
	p.t.Helper()
	select {
	case env := <-p.mailbox:
		v, ok := env.msg.(T)
		if !ok {
			var zero T
			p.t.Fatalf("testkit: ExpectMsgType: got %v (%T), want type %T",
				env.msg, env.msg, zero)
			panic("unreachable") // t.Fatalf calls runtime.Goexit
		}
		p.setLast(&env)
		return v
	case <-time.After(timeout):
		var zero T
		p.t.Fatalf("testkit: ExpectMsgType: no message of type %T within %s", zero, timeout)
		panic("unreachable")
	}
}
