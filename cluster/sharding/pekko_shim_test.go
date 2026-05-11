/*
 * sharding/pekko_shim_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"sync"
	"testing"

	"github.com/sopranoworks/gekka/actor"
)

// recordingRef captures every Tell call so tests can inspect the messages a
// shim forwards to its downstream coordinator ref.
type recordingRef struct {
	path string
	mu   sync.Mutex
	msgs []capturedTell
}

type capturedTell struct {
	msg    any
	sender actor.Ref
}

func (r *recordingRef) Path() string { return r.path }
func (r *recordingRef) Tell(msg any, sender ...actor.Ref) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var s actor.Ref
	if len(sender) > 0 {
		s = sender[0]
	}
	r.msgs = append(r.msgs, capturedTell{msg: msg, sender: s})
}

func (r *recordingRef) captured() []capturedTell {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]capturedTell, len(r.msgs))
	copy(out, r.msgs)
	return out
}

// sentMessage is one entry recorded by the SendFunc test hook.
type sentMessage struct {
	recipient string
	msg       any
}

// newRecorderSendFn returns a SendFunc that appends every call to out and a
// pointer to inspect the slice. Goroutine-safe.
func newRecorderSendFn() (SendFunc, func() []sentMessage) {
	var mu sync.Mutex
	var out []sentMessage
	fn := func(recipient string, msg any) error {
		mu.Lock()
		defer mu.Unlock()
		out = append(out, sentMessage{recipient: recipient, msg: msg})
		return nil
	}
	read := func() []sentMessage {
		mu.Lock()
		defer mu.Unlock()
		cp := make([]sentMessage, len(out))
		copy(cp, out)
		return cp
	}
	return fn, read
}

// TestPekkoCoordinatorShim_RegisterTranslation asserts that an inbound Pekko
// Register message is translated to a gekka RegisterRegion and forwarded to
// the local coordinator ref, with the region's Pekko actor path preserved.
func TestPekkoCoordinatorShim_RegisterTranslation(t *testing.T) {
	coord := &recordingRef{path: "/user/shardCoordinatorProxy-echo"}
	sendFn, _ := newRecorderSendFn()
	shim := NewPekkoCoordinatorShim(coord, sendFn)
	shim.SetSelf(&mockRef{path: "/system/sharding/echoCoordinator/singleton/coordinator"})

	const pekkoRegion = "pekko://Sys@h:1/system/sharding/echo#42"
	// Simulate the Pekko region as the inbound sender so the shim records it
	// for the GetShardHome case; for Register the sender is unused but it
	// keeps the harness honest.
	actor.InjectSender(shim, &mockRef{path: pekkoRegion})
	shim.Receive(&PekkoSharding_Register{Ref: pekkoRegion})

	got := coord.captured()
	if len(got) != 1 {
		t.Fatalf("coord captured %d messages, want 1: %+v", len(got), got)
	}
	rr, ok := got[0].msg.(RegisterRegion)
	if !ok {
		t.Fatalf("coord got %T, want RegisterRegion", got[0].msg)
	}
	if rr.RegionPath != pekkoRegion {
		t.Errorf("RegisterRegion.RegionPath = %q, want %q", rr.RegionPath, pekkoRegion)
	}
}

// TestPekkoCoordinatorShim_GetShardHomeTranslation asserts the inbound
// GetShardHome is translated to a gekka GetShardHome, that the Pekko sender
// path is recorded keyed by shardId, and that the eventual ShardHome reply
// from the coordinator is encoded back into a PekkoSharding_ShardHome and
// dispatched to the original Pekko sender via the injected SendFunc.
func TestPekkoCoordinatorShim_GetShardHomeTranslation(t *testing.T) {
	coord := &recordingRef{path: "/user/shardCoordinatorProxy-echo"}
	sendFn, readSent := newRecorderSendFn()
	shim := NewPekkoCoordinatorShim(coord, sendFn)
	shim.SetSelf(&mockRef{path: "/system/sharding/echoCoordinator/singleton/coordinator"})

	const pekkoRegion = "pekko://Sys@h:1/system/sharding/echo#42"
	actor.InjectSender(shim, &mockRef{path: pekkoRegion})
	shim.Receive(&PekkoSharding_GetShardHome{Shard: "shard-7"})

	got := coord.captured()
	if len(got) != 1 {
		t.Fatalf("coord captured %d messages, want 1: %+v", len(got), got)
	}
	gsh, ok := got[0].msg.(GetShardHome)
	if !ok {
		t.Fatalf("coord got %T, want GetShardHome", got[0].msg)
	}
	if gsh.ShardId != "shard-7" {
		t.Errorf("GetShardHome.ShardId = %q, want shard-7", gsh.ShardId)
	}

	// Now simulate the coordinator's reply landing in the shim's mailbox.
	// Sender is the coord; the shim should look up the recorded Pekko sender
	// by shardId, encode a PekkoSharding_ShardHome, and route via sendFn.
	actor.InjectSender(shim, coord)
	shim.Receive(ShardHome{ShardId: "shard-7", RegionPath: pekkoRegion})

	sent := readSent()
	if len(sent) != 1 {
		t.Fatalf("sendFn called %d times, want 1: %+v", len(sent), sent)
	}
	if sent[0].recipient != pekkoRegion {
		t.Errorf("sendFn recipient = %q, want %q", sent[0].recipient, pekkoRegion)
	}
	reply, ok := sent[0].msg.(*PekkoSharding_ShardHome)
	if !ok {
		t.Fatalf("sendFn msg = %T, want *PekkoSharding_ShardHome", sent[0].msg)
	}
	if reply.Shard != "shard-7" {
		t.Errorf("reply.Shard = %q, want shard-7", reply.Shard)
	}
	if reply.Region != pekkoRegion {
		t.Errorf("reply.Region = %q, want %q", reply.Region, pekkoRegion)
	}
}

// TestPekkoCoordinatorShim_PipelinedGetShardHome asserts the shim's per-shard
// sender map survives multiple in-flight GetShardHome requests for different
// shards. This is the concrete mitigation for Risk #6 in the parent plan.
func TestPekkoCoordinatorShim_PipelinedGetShardHome(t *testing.T) {
	coord := &recordingRef{path: "/user/shardCoordinatorProxy-echo"}
	sendFn, readSent := newRecorderSendFn()
	shim := NewPekkoCoordinatorShim(coord, sendFn)
	shim.SetSelf(&mockRef{path: "/system/sharding/echoCoordinator/singleton/coordinator"})

	const senderA = "pekko://Sys@h:1/system/sharding/echo#A"
	const senderB = "pekko://Sys@h:1/system/sharding/echo#B"

	// Two regions ask for two different shards before either reply lands.
	actor.InjectSender(shim, &mockRef{path: senderA})
	shim.Receive(&PekkoSharding_GetShardHome{Shard: "shard-1"})
	actor.InjectSender(shim, &mockRef{path: senderB})
	shim.Receive(&PekkoSharding_GetShardHome{Shard: "shard-2"})

	// Replies come back in reverse order.
	actor.InjectSender(shim, coord)
	shim.Receive(ShardHome{ShardId: "shard-2", RegionPath: senderB})
	shim.Receive(ShardHome{ShardId: "shard-1", RegionPath: senderA})

	sent := readSent()
	if len(sent) != 2 {
		t.Fatalf("sendFn called %d times, want 2: %+v", len(sent), sent)
	}

	byShard := map[string]sentMessage{}
	for _, s := range sent {
		reply, ok := s.msg.(*PekkoSharding_ShardHome)
		if !ok {
			t.Fatalf("sendFn msg = %T, want *PekkoSharding_ShardHome", s.msg)
		}
		byShard[reply.Shard] = s
	}

	for shard, wantRecipient := range map[string]string{
		"shard-1": senderA,
		"shard-2": senderB,
	} {
		s, ok := byShard[shard]
		if !ok {
			t.Fatalf("no reply sent for %s", shard)
		}
		if s.recipient != wantRecipient {
			t.Errorf("reply for %s routed to %q, want %q", shard, s.recipient, wantRecipient)
		}
	}
}
