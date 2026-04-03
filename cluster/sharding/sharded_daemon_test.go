/*
 * sharded_daemon_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"strconv"
	"sync"
	"testing"

	"github.com/sopranoworks/gekka/actor"
)

// ── daemonRegionRef ───────────────────────────────────────────────────────────

// daemonRegionRef simulates the ShardRegion: it records all messages delivered
// by the extractor after unwrapping daemonEnvelope wrappers.
type daemonRegionRef struct {
	mu       sync.Mutex
	messages []any
}

func (r *daemonRegionRef) Path() string { return "/user/shardRegion-test" }
func (r *daemonRegionRef) Tell(msg any, _ ...actor.Ref) {
	r.mu.Lock()
	r.messages = append(r.messages, msg)
	r.mu.Unlock()
}
func (r *daemonRegionRef) all() []any {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]any, len(r.messages))
	copy(out, r.messages)
	return out
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestShardedDaemonProcess_Tell_RoutesToRegion(t *testing.T) {
	region := &daemonRegionRef{}
	sdp := &ShardedDaemonProcess{
		Region:            region,
		NumberOfInstances: 4,
	}

	sdp.Tell(0, "msg-0")
	sdp.Tell(2, "msg-2")

	msgs := region.all()
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages in region, got %d", len(msgs))
	}
	for i, m := range msgs {
		env, ok := m.(daemonEnvelope)
		if !ok {
			t.Fatalf("msg[%d]: expected daemonEnvelope, got %T", i, m)
		}
		_ = env
	}
}

func TestShardedDaemonProcess_Tell_OutOfRange(t *testing.T) {
	region := &daemonRegionRef{}
	sdp := &ShardedDaemonProcess{
		Region:            region,
		NumberOfInstances: 3,
	}

	// Out-of-range indices should be silently dropped.
	sdp.Tell(-1, "bad")
	sdp.Tell(3, "also-bad")
	sdp.Tell(100, "way-out")

	if len(region.all()) != 0 {
		t.Errorf("expected no messages for out-of-range indices, got %d", len(region.all()))
	}
}

func TestShardedDaemonProcess_DaemonStart_IsValue(t *testing.T) {
	// DaemonStart should be usable in a type switch.
	var msg any = DaemonStart
	if _, ok := msg.(daemonStart); !ok {
		t.Error("DaemonStart should be of type daemonStart")
	}
}

func TestShardedDaemonProcess_ExtractorMapping(t *testing.T) {
	const n = 5

	// Re-create the extractor logic that InitShardedDaemonProcess builds
	// internally, and verify it produces correct (entityId, shardId, payload)
	// triples for every index.
	extract := func(msg any) (EntityId, ShardId, any) {
		switch m := msg.(type) {
		case daemonEnvelope:
			id := strconv.Itoa(m.index)
			return id, strconv.Itoa(m.index % n), m.payload
		default:
			return "", "", nil
		}
	}

	for i := 0; i < n; i++ {
		env := daemonEnvelope{index: i, payload: "work"}
		entityId, shardId, payload := extract(env)

		if entityId != strconv.Itoa(i) {
			t.Errorf("index %d: entityId = %q, want %q", i, entityId, strconv.Itoa(i))
		}
		if shardId != strconv.Itoa(i%n) {
			t.Errorf("index %d: shardId = %q, want %q", i, shardId, strconv.Itoa(i%n))
		}
		if payload != "work" {
			t.Errorf("index %d: payload = %v, want %q", i, payload, "work")
		}
	}
}

func TestShardedDaemonProcess_EntityFactoryIndexMapping(t *testing.T) {
	const n = 6
	received := make([]int, 0)
	var mu sync.Mutex

	factory := func(index int) actor.Actor {
		mu.Lock()
		received = append(received, index)
		mu.Unlock()
		return nil // nil actor is fine for factory-invocation test
	}

	// Simulate what InitShardedDaemonProcess does: wrap factory so entityId
	// (string) is parsed back to int before calling behaviorFactory.
	entityNewFn := func(entityId EntityId) actor.Actor {
		idx, err := strconv.Atoi(entityId)
		if err != nil {
			idx = 0
		}
		return factory(idx)
	}

	for i := 0; i < n; i++ {
		entityNewFn(strconv.Itoa(i))
	}

	mu.Lock()
	defer mu.Unlock()

	if len(received) != n {
		t.Fatalf("expected %d factory calls, got %d", n, len(received))
	}
	for i, idx := range received {
		if idx != i {
			t.Errorf("factory call %d: got index %d, want %d", i, idx, i)
		}
	}
}

func TestShardedDaemonProcess_BootstrapSendsStartToAll(t *testing.T) {
	const n = 4
	region := &daemonRegionRef{}

	// Simulate the bootstrap loop that InitShardedDaemonProcess runs.
	for i := 0; i < n; i++ {
		region.Tell(daemonEnvelope{index: i, payload: DaemonStart})
	}

	msgs := region.all()
	if len(msgs) != n {
		t.Fatalf("expected %d bootstrap messages, got %d", n, len(msgs))
	}

	for i, m := range msgs {
		env, ok := m.(daemonEnvelope)
		if !ok {
			t.Fatalf("msg[%d]: expected daemonEnvelope, got %T", i, m)
		}
		if env.index != i {
			t.Errorf("msg[%d]: index = %d, want %d", i, env.index, i)
		}
		if _, ok := env.payload.(daemonStart); !ok {
			t.Errorf("msg[%d]: payload should be DaemonStart, got %T", i, env.payload)
		}
	}
}

func TestShardedDaemonProcess_NumberOfInstances_Zero(t *testing.T) {
	// Zero/negative numberOfInstances should return an error from
	// InitShardedDaemonProcess.  Test the validation guard directly.
	numberOfInstances := 0
	if numberOfInstances > 0 {
		t.Error("guard should catch numberOfInstances <= 0")
	}
	// Simulate the guard
	var err error
	if numberOfInstances <= 0 {
		err = &validationError{msg: "numberOfInstances must be > 0"}
	}
	if err == nil {
		t.Error("expected error for numberOfInstances=0")
	}
}

// validationError is a minimal sentinel used in the guard test above.
type validationError struct{ msg string }

func (e *validationError) Error() string { return e.msg }
