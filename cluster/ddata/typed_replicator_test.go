/*
 * typed_replicator_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/cluster/ddata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Helpers ───────────────────────────────────────────────────────────────

// fakeTypedContext provides a minimal TypedContext[Cmd] for testing.
type fakeTypedContext[Cmd any] struct {
	selfRef typed.TypedActorRef[Cmd]
	logger  *slog.Logger
}

func newFakeTypedContext[Cmd any](selfRef typed.TypedActorRef[Cmd]) *fakeTypedContext[Cmd] {
	return &fakeTypedContext[Cmd]{
		selfRef: selfRef,
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func (c *fakeTypedContext[Cmd]) Self() typed.TypedActorRef[Cmd]  { return c.selfRef }
func (c *fakeTypedContext[Cmd]) System() actor.ActorContext       { return nil }
func (c *fakeTypedContext[Cmd]) Log() *slog.Logger               { return c.logger }
func (c *fakeTypedContext[Cmd]) Watch(actor.Ref)                 {}
func (c *fakeTypedContext[Cmd]) Unwatch(actor.Ref)               {}
func (c *fakeTypedContext[Cmd]) Stop(actor.Ref)                  {}
func (c *fakeTypedContext[Cmd]) Passivate()                      {}
func (c *fakeTypedContext[Cmd]) Sender() actor.Ref               { return nil }
func (c *fakeTypedContext[Cmd]) Spawn(any, string) (actor.Ref, error) {
	return nil, nil
}
func (c *fakeTypedContext[Cmd]) SpawnAnonymous(any) (actor.Ref, error) {
	return nil, nil
}
func (c *fakeTypedContext[Cmd]) SystemActorOf(any, string) (actor.Ref, error) {
	return nil, nil
}
func (c *fakeTypedContext[Cmd]) Ask(actor.Ref, func(actor.Ref) any, func(any, error) Cmd) {}
func (c *fakeTypedContext[Cmd]) Timers() typed.TimerScheduler[Cmd]                        { return nil }
func (c *fakeTypedContext[Cmd]) Stash() typed.StashBuffer[Cmd]                            { return nil }

// chanRef collects messages sent to it via a channel.
type chanRef[T any] struct {
	ch chan T
}

func newChanRef[T any](buf int) *chanRef[T] { return &chanRef[T]{ch: make(chan T, buf)} }

func (r *chanRef[T]) Tell(msg any, _ ...actor.Ref) {
	if m, ok := msg.(T); ok {
		r.ch <- m
	}
}
func (r *chanRef[T]) Path() string { return "/test/chanRef" }

// newFakeCtx creates a fakeTypedContext backed by a chanRef.
func newFakeCtx[Cmd any](buf int) (typed.TypedContext[Cmd], *chanRef[Cmd]) {
	ref := newChanRef[Cmd](buf)
	ctx := newFakeTypedContext[Cmd](typed.NewTypedActorRef[Cmd](ref))
	return ctx, ref
}

// ── Tests ─────────────────────────────────────────────────────────────────

// TestTypedReplicatorAdapter_AskGet verifies that AskGet delivers a response
// containing the current CRDT value to the actor.
func TestTypedReplicatorAdapter_AskGet(t *testing.T) {
	repl := ddata.NewReplicator("node1", nil)

	// Pre-populate a GCounter.
	c := repl.GCounter("hits")
	c.Increment("node1", 42)

	type Cmd struct {
		Resp ddata.ReplicatorResponse[*ddata.GCounter]
	}

	ctx, ref := newFakeCtx[Cmd](1)
	adapter := ddata.NewTypedReplicatorAdapter(ctx, repl,
		func(r ddata.ReplicatorResponse[*ddata.GCounter]) Cmd { return Cmd{Resp: r} })

	adapter.AskGet("hits", ddata.GCounterGetter)

	select {
	case cmd := <-ref.ch:
		assert.True(t, cmd.Resp.Found)
		assert.Equal(t, "hits", cmd.Resp.Key)
		assert.Equal(t, uint64(42), cmd.Resp.Data.Value())
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for AskGet response")
	}
}

// TestTypedReplicatorAdapter_AskGet_Missing verifies AskGet with a key that
// does not exist delivers Found=false.
func TestTypedReplicatorAdapter_AskGet_Missing(t *testing.T) {
	repl := ddata.NewReplicator("node1", nil)

	type Cmd struct {
		Resp ddata.ReplicatorResponse[*ddata.GCounter]
	}

	ctx, ref := newFakeCtx[Cmd](1)
	adapter := ddata.NewTypedReplicatorAdapter(ctx, repl,
		func(r ddata.ReplicatorResponse[*ddata.GCounter]) Cmd { return Cmd{Resp: r} })

	adapter.AskGet("missing", ddata.GCounterGetter)

	select {
	case cmd := <-ref.ch:
		assert.False(t, cmd.Resp.Found)
		assert.Equal(t, "missing", cmd.Resp.Key)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for AskGet response")
	}
}

// TestTypedReplicatorAdapter_AskUpdate verifies that AskUpdate modifies the
// CRDT and delivers the updated value to the actor.
func TestTypedReplicatorAdapter_AskUpdate(t *testing.T) {
	repl := ddata.NewReplicator("node1", nil)

	type Cmd struct {
		Resp ddata.ReplicatorResponse[*ddata.GCounter]
	}

	ctx, ref := newFakeCtx[Cmd](1)
	adapter := ddata.NewTypedReplicatorAdapter(ctx, repl,
		func(r ddata.ReplicatorResponse[*ddata.GCounter]) Cmd { return Cmd{Resp: r} })

	adapter.AskUpdate(
		"requests",
		func(r *ddata.Replicator, key string) { r.IncrementCounter(key, 10, ddata.WriteLocal) },
		ddata.WriteLocal,
		ddata.GCounterGetter,
	)

	select {
	case cmd := <-ref.ch:
		assert.True(t, cmd.Resp.Found)
		assert.Equal(t, uint64(10), cmd.Resp.Data.Value())
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for AskUpdate response")
	}
}

// TestTypedReplicatorAdapter_Subscribe verifies that Subscribe delivers a
// ReplicatorResponse to the actor when HandleIncoming merges an update.
func TestTypedReplicatorAdapter_Subscribe(t *testing.T) {
	repl := ddata.NewReplicator("node1", nil)

	type Cmd struct {
		Resp ddata.ReplicatorResponse[*ddata.GCounter]
	}

	ctx, ref := newFakeCtx[Cmd](4)
	adapter := ddata.NewTypedReplicatorAdapter(ctx, repl,
		func(r ddata.ReplicatorResponse[*ddata.GCounter]) Cmd { return Cmd{Resp: r} })

	adapter.Subscribe("counter", func(raw any) (*ddata.GCounter, bool) {
		c, ok := raw.(*ddata.GCounter)
		return c, ok
	})

	// Simulate an incoming gossip message from a peer.
	gossip := ddata.ReplicatorMsg{
		Type: "gcounter-gossip",
		Key:  "counter",
	}
	payload, err := json.Marshal(ddata.GCounterPayload{State: map[string]uint64{"node2": 7}})
	require.NoError(t, err)
	gossip.Payload = payload

	raw, err := json.Marshal(gossip)
	require.NoError(t, err)
	require.NoError(t, repl.HandleIncoming(raw))

	select {
	case cmd := <-ref.ch:
		assert.True(t, cmd.Resp.Found)
		assert.Equal(t, "counter", cmd.Resp.Key)
		assert.Equal(t, uint64(7), cmd.Resp.Data.Value())
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for Subscribe notification")
	}
}

// TestTypedReplicatorAdapter_Unsubscribe verifies that Unsubscribe stops
// notifications for the given key.
func TestTypedReplicatorAdapter_Unsubscribe(t *testing.T) {
	repl := ddata.NewReplicator("node1", nil)

	type Cmd struct {
		Resp ddata.ReplicatorResponse[*ddata.GCounter]
	}

	ctx, ref := newFakeCtx[Cmd](4)
	adapter := ddata.NewTypedReplicatorAdapter(ctx, repl,
		func(r ddata.ReplicatorResponse[*ddata.GCounter]) Cmd { return Cmd{Resp: r} })

	adapter.Subscribe("c", func(raw any) (*ddata.GCounter, bool) {
		c, ok := raw.(*ddata.GCounter)
		return c, ok
	})
	adapter.Unsubscribe("c")

	gossip := buildGossip(t, "c", map[string]uint64{"n1": 99})
	require.NoError(t, repl.HandleIncoming(gossip))

	select {
	case <-ref.ch:
		t.Fatal("should not receive notification after Unsubscribe")
	case <-time.After(100 * time.Millisecond):
		// correct: no notification
	}
}

// TestTypedReplicatorAdapter_ORSet verifies AskGet works with ORSet.
func TestTypedReplicatorAdapter_ORSet(t *testing.T) {
	repl := ddata.NewReplicator("node1", nil)
	s := repl.ORSet("members")
	s.Add("node1", "alice")
	s.Add("node1", "bob")

	type Cmd struct {
		Resp ddata.ReplicatorResponse[*ddata.ORSet]
	}

	ctx, ref := newFakeCtx[Cmd](1)
	adapter := ddata.NewTypedReplicatorAdapter(ctx, repl,
		func(r ddata.ReplicatorResponse[*ddata.ORSet]) Cmd { return Cmd{Resp: r} })

	adapter.AskGet("members", ddata.ORSetGetter)

	select {
	case cmd := <-ref.ch:
		assert.True(t, cmd.Resp.Found)
		elems := cmd.Resp.Data.Elements()
		assert.ElementsMatch(t, []string{"alice", "bob"}, elems)
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

// TestTypedReplicatorAdapter_SubscribeConcurrent verifies concurrent gossip
// merges all fire subscriptions correctly.
func TestTypedReplicatorAdapter_SubscribeConcurrent(t *testing.T) {
	repl := ddata.NewReplicator("node1", nil)

	type Cmd struct {
		Resp ddata.ReplicatorResponse[*ddata.GCounter]
	}

	ctx, ref := newFakeCtx[Cmd](20)
	adapter := ddata.NewTypedReplicatorAdapter(ctx, repl,
		func(r ddata.ReplicatorResponse[*ddata.GCounter]) Cmd { return Cmd{Resp: r} })

	adapter.Subscribe("ctr", func(raw any) (*ddata.GCounter, bool) {
		c, ok := raw.(*ddata.GCounter)
		return c, ok
	})

	const n = 10
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		nodeID := "node" + string(rune('A'+i))
		go func(id string) {
			defer wg.Done()
			payload, _ := json.Marshal(ddata.GCounterPayload{State: map[string]uint64{id: 1}})
			gossip, _ := json.Marshal(ddata.ReplicatorMsg{
				Type:    "gcounter-gossip",
				Key:     "ctr",
				Payload: payload,
			})
			_ = repl.HandleIncoming(gossip)
		}(nodeID)
	}
	wg.Wait()

	// Drain notifications — we expect exactly n.
	received := 0
	deadline := time.After(2 * time.Second)
	for received < n {
		select {
		case <-ref.ch:
			received++
		case <-deadline:
			t.Fatalf("timeout: received %d/%d notifications", received, n)
		}
	}
	assert.Equal(t, n, received)
}

// ── helpers ───────────────────────────────────────────────────────────────

func buildGossip(t *testing.T, key string, state map[string]uint64) []byte {
	t.Helper()
	payload, err := json.Marshal(ddata.GCounterPayload{State: state})
	require.NoError(t, err)
	raw, err := json.Marshal(ddata.ReplicatorMsg{
		Type:    "gcounter-gossip",
		Key:     key,
		Payload: payload,
	})
	require.NoError(t, err)
	return raw
}
