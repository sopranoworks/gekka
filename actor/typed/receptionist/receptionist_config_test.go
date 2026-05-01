/*
 * receptionist_config_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package receptionist

import (
	"testing"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/cluster/ddata"
	"github.com/stretchr/testify/assert"
)

// capturingWriter records every (key, consistency) tuple written through
// the receptionist's ddata path. ORSet reads delegate to a real replicator
// so subscribers and Find calls behave normally.
type capturingWriter struct {
	inner *ddata.Replicator
	addCalls []writeCall
	rmCalls  []writeCall
}

type writeCall struct {
	key         string
	element     string
	consistency ddata.WriteConsistency
}

func (c *capturingWriter) AddToSet(k, e string, w ddata.WriteConsistency) {
	c.addCalls = append(c.addCalls, writeCall{k, e, w})
	c.inner.AddToSet(k, e, w)
}

func (c *capturingWriter) RemoveFromSet(k, e string, w ddata.WriteConsistency) {
	c.rmCalls = append(c.rmCalls, writeCall{k, e, w})
	c.inner.RemoveFromSet(k, e, w)
}

func (c *capturingWriter) ORSet(k string) *ddata.ORSet { return c.inner.ORSet(k) }

func newReceptionistActor(t *testing.T, replicator ReplicatorWriter, cfg Config) *typed.TypedActor[any] {
	t.Helper()
	a := typed.NewTypedActor(Behavior(replicator, cfg)).(*typed.TypedActor[any])
	return a
}

// TestReceptionist_WriteConsistency_Runtime asserts that the
// pekko.cluster.typed.receptionist.write-consistency value flows from
// the Config through to the ddata.AddToSet call. If a future refactor
// reverts to the hardcoded WriteAll constant, the per-case sub-test for
// WriteLocal fails because the captured consistency disagrees.
func TestReceptionist_WriteConsistency_Runtime(t *testing.T) {
	cases := []struct {
		name string
		want ddata.WriteConsistency
	}{
		{"local", ddata.WriteLocal},
		{"all", ddata.WriteAll},
		{"majority", ddata.WriteMajority},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cap := &capturingWriter{inner: ddata.NewReplicator("nodeX", nil)}
			cfg := Config{
				WriteConsistency:    tc.want,
				PruningInterval:     0, // direct-Receive: skip timer scheduling
				DistributedKeyCount: 5,
			}
			a := newReceptionistActor(t, cap, cfg)
			a.Receive(Register[string]{
				Key:     NewServiceKey[string]("svc"),
				Service: typed.NewTypedActorRef[string](&recRef{path: "/user/s1"}),
			})
			if assert.Len(t, cap.addCalls, 1) {
				assert.Equal(t, tc.want, cap.addCalls[0].consistency,
					"AddToSet must use Config.WriteConsistency, not a hardcoded value")
			}
		})
	}
}

// TestReceptionist_DistributedKeyCount_Runtime asserts that the
// pekko.cluster.typed.receptionist.distributed-key-count value drives the
// ddata bucket name the receptionist writes to. Two different ids share
// the same bucket prefix iff their FNV-1a hashes mod count agree, so a
// count of 1 forces all ids into the "receptionist-0-*" prefix and a
// count of 100 spreads them.
func TestReceptionist_DistributedKeyCount_Runtime(t *testing.T) {
	t.Run("count=1 collapses prefix to 0", func(t *testing.T) {
		cap := &capturingWriter{inner: ddata.NewReplicator("nodeX", nil)}
		cfg := Config{
			WriteConsistency:    ddata.WriteLocal,
			DistributedKeyCount: 1,
		}
		a := newReceptionistActor(t, cap, cfg)
		a.Receive(Register[string]{Key: NewServiceKey[string]("a"), Service: typed.NewTypedActorRef[string](&recRef{path: "/a"})})
		a.Receive(Register[string]{Key: NewServiceKey[string]("b"), Service: typed.NewTypedActorRef[string](&recRef{path: "/b"})})
		assert.Equal(t, "receptionist-0-a", cap.addCalls[0].key)
		assert.Equal(t, "receptionist-0-b", cap.addCalls[1].key)
	})

	t.Run("count drives shard index", func(t *testing.T) {
		// Verify the helper itself: with a wider modulus, distinct ids
		// produce distinct shard prefixes (sanity-check on the
		// hash-mod-N split).
		seen := map[string]bool{}
		for _, id := range []string{"a", "b", "c", "d", "e", "f"} {
			seen[ShardKey(id, 100)] = true
		}
		assert.GreaterOrEqual(t, len(seen), 2,
			"with count=100 and several distinct ids, more than one shard prefix must be produced")
	})

	t.Run("zero or negative count coerces to 1", func(t *testing.T) {
		cap := &capturingWriter{inner: ddata.NewReplicator("nodeX", nil)}
		cfg := Config{
			WriteConsistency:    ddata.WriteLocal,
			DistributedKeyCount: 0,
		}
		a := newReceptionistActor(t, cap, cfg)
		a.Receive(Register[string]{Key: NewServiceKey[string]("svc"), Service: typed.NewTypedActorRef[string](&recRef{path: "/x"})})
		assert.Equal(t, "receptionist-0-svc", cap.addCalls[0].key)
	})
}

// resolverBridge is a minimal actor.ActorContext used to drive the
// pruning runtime test. It returns a configurable error for paths in the
// "dead" set; live paths resolve to a benign mock ref.
type resolverBridge struct {
	actor.ActorContext
	dead map[string]bool
}

func (b *resolverBridge) Resolve(path string) (actor.Ref, error) {
	if b.dead[path] {
		return nil, errResolveDead
	}
	return &recRef{path: path}, nil
}

var errResolveDead = &resolveErr{msg: "actor not found"}

type resolveErr struct{ msg string }

func (e *resolveErr) Error() string { return e.msg }

// TestReceptionist_Pruning_Runtime asserts that a pruneTick removes paths
// whose Resolve errors and leaves live paths intact. The test drives the
// tick directly (PruningInterval=0 keeps the timer disabled) so it does
// not depend on wall-clock scheduling.
func TestReceptionist_Pruning_Runtime(t *testing.T) {
	cap := &capturingWriter{inner: ddata.NewReplicator("nodeX", nil)}
	cfg := Config{
		WriteConsistency:    ddata.WriteLocal,
		PruningInterval:     0, // tests inject pruneTick directly
		DistributedKeyCount: 3,
	}
	bridge := &resolverBridge{dead: map[string]bool{"/dead": true}}
	a := typed.NewTypedActor(Behavior(cap, cfg)).(*typed.TypedActor[any])
	a.SetSystem(bridge)
	a.SetSelf(&recRef{path: "/user/receptionist"})

	a.Receive(Register[string]{Key: NewServiceKey[string]("svc"), Service: typed.NewTypedActorRef[string](&recRef{path: "/alive"})})
	a.Receive(Register[string]{Key: NewServiceKey[string]("svc"), Service: typed.NewTypedActorRef[string](&recRef{path: "/dead"})})

	bucket := ShardKey("svc", cfg.DistributedKeyCount)
	before := cap.inner.ORSet(bucket).Elements()
	assert.ElementsMatch(t, []string{"/alive", "/dead"}, before)

	a.Receive(pruneTick{})

	after := cap.inner.ORSet(bucket).Elements()
	assert.ElementsMatch(t, []string{"/alive"}, after,
		"pruneTick must remove paths whose Resolve errors and keep live ones")

	if assert.Len(t, cap.rmCalls, 1, "exactly one path was dead and must be removed") {
		assert.Equal(t, "/dead", cap.rmCalls[0].element)
		assert.Equal(t, ddata.WriteLocal, cap.rmCalls[0].consistency,
			"RemoveFromSet must use the configured WriteConsistency")
		assert.Equal(t, bucket, cap.rmCalls[0].key,
			"RemoveFromSet must target the same shard bucket as the original register")
	}
}

type recRef struct {
	actor.Ref
	path string
}

func (r *recRef) Path() string                      { return r.path }
func (r *recRef) Tell(msg any, sender ...actor.Ref) {}
