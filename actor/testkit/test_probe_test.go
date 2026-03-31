/*
 * test_probe_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package testkit_test

import (
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/testkit"
	"github.com/stretchr/testify/require"
)

// ─── actors under test ────────────────────────────────────────────────────────

// pingPongActor replies "pong" to every "ping" it receives, using the sender
// reference set by the actor runtime (BaseActor.Sender).
type pingPongActor struct {
	actor.BaseActor
}

func (a *pingPongActor) Receive(msg any) {
	if msg == "ping" && a.Sender() != nil {
		a.Sender().Tell("pong", a.Self())
	}
}

// captureActor records all messages it receives into a channel.
// It replies "pong" to "ping" and forwards every other message to inbox.
type captureActor struct {
	actor.BaseActor
	inbox chan<- any
}

func (a *captureActor) Receive(msg any) {
	switch msg {
	case "ping":
		if a.Sender() != nil {
			a.Sender().Tell("pong", a.Self())
		}
	default:
		select {
		case a.inbox <- msg:
		default:
		}
	}
}

// startActor wires a into a FunctionalMockRef self reference and calls
// actor.Start. Returns the self reference so tests can deliver messages.
func startActor(a actor.Actor, selfPath string) actor.Ref {
	selfRef := actor.NewFunctionalMockRef(selfPath, func(m any) {
		select {
		case a.Mailbox() <- m:
		default:
		}
	})
	if s, ok := any(a).(interface{ SetSelf(actor.Ref) }); ok {
		s.SetSelf(selfRef)
	}
	actor.Start(a)
	return selfRef
}

// ─── tests ────────────────────────────────────────────────────────────────────

// TestTestProbe_ReceiveFromActor verifies that a TestProbe can receive a message
// sent by a standard actor in response to a Tell.
func TestTestProbe_ReceiveFromActor(t *testing.T) {
	tk := testkit.NewTestKit(nil)
	probe := tk.NewProbe(t)

	pp := &pingPongActor{BaseActor: actor.NewBaseActor()}
	ppRef := startActor(pp, "/user/pingpong")

	// Deliver "ping" via Envelope so the runtime sets pp.Sender() = probe.
	ppRef.Tell(actor.Envelope{Payload: "ping", Sender: probe.Ref()})

	// Actor replies "pong" → probe must receive it within 1 s.
	probe.ExpectMsg("pong", time.Second)
}

// TestTestProbe_Reply verifies the full cycle:
//  1. Standard actor receives "ping" with probe as sender → replies "pong".
//  2. probe.ExpectMsg captures the "pong" and records the actor's self as last sender.
//  3. probe.Reply("ack") delivers "ack" back to the actor.
//  4. The actor captures "ack" in its inbox channel.
func TestTestProbe_Reply(t *testing.T) {
	tk := testkit.NewTestKit(nil)
	probe := tk.NewProbe(t)

	inbox := make(chan any, 4)
	act := &captureActor{
		BaseActor: actor.NewBaseActor(),
		inbox:     inbox,
	}
	actRef := startActor(act, "/user/capture")

	// 1. Probe asks actor to say "pong".
	actRef.Tell(actor.Envelope{Payload: "ping", Sender: probe.Ref()})
	probe.ExpectMsg("pong", time.Second)
	// lastSender == actRef (actor's self)

	// 2. Probe replies "ack" to the actor.
	probe.Reply("ack")

	// 3. Actor's Receive captures "ack" in inbox (default case).
	select {
	case got := <-inbox:
		require.Equal(t, "ack", got)
	case <-time.After(time.Second):
		t.Fatal("testkit: Reply did not deliver message to actor within 1 s")
	}
}

// TestTestProbe_ExpectNoMsg verifies that ExpectNoMsg passes when no message
// arrives during the given interval.
func TestTestProbe_ExpectNoMsg(t *testing.T) {
	tk := testkit.NewTestKit(nil)
	probe := tk.NewProbe(t)

	probe.ExpectNoMsg(50 * time.Millisecond)
}

// TestTestProbe_ExpectNoMsg_AfterSilence verifies that ExpectNoMsg passes when
// an actor ignores the delivered message (sends no reply to probe).
func TestTestProbe_ExpectNoMsg_AfterSilence(t *testing.T) {
	tk := testkit.NewTestKit(nil)
	probe := tk.NewProbe(t)

	pp := &pingPongActor{BaseActor: actor.NewBaseActor()}
	ppRef := startActor(pp, "/user/pingpong2")

	// "hello" is not "ping" so the actor never replies to probe.
	ppRef.Tell(actor.Envelope{Payload: "hello", Sender: probe.Ref()})

	probe.ExpectNoMsg(100 * time.Millisecond)
}

// TestExpectMsgType verifies the generic ExpectMsgType helper for typed messages.
func TestExpectMsgType(t *testing.T) {
	type MyReply struct{ Status string }

	tk := testkit.NewTestKit(nil)
	probe := tk.NewProbe(t)

	// Simulate an actor pushing a typed reply directly to the probe.
	probe.Tell(MyReply{Status: "ok"})

	got := testkit.ExpectMsgType[MyReply](probe, time.Second)
	require.Equal(t, "ok", got.Status)
}

// TestTestProbe_MultipleProbes verifies that two independent probes do not
// share state or message queues.
func TestTestProbe_MultipleProbes(t *testing.T) {
	tk := testkit.NewTestKit(nil)
	probe1 := tk.NewProbe(t)
	probe2 := tk.NewProbe(t)

	require.NotEqual(t, probe1.Path(), probe2.Path(), "probes must have unique paths")

	probe1.Tell("msg-for-one")
	probe2.Tell("msg-for-two")

	probe1.ExpectMsg("msg-for-one", time.Second)
	probe2.ExpectMsg("msg-for-two", time.Second)

	probe1.ExpectNoMsg(30 * time.Millisecond)
	probe2.ExpectNoMsg(30 * time.Millisecond)
}
