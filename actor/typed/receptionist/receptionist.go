/*
 * receptionist.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package receptionist

import (
	"fmt"
	"hash/fnv"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/cluster/ddata"
)

// ServiceKey is a type-safe identifier for a service.
type ServiceKey[T any] struct {
	ID string
}

func (k ServiceKey[T]) String() string {
	var zero T
	return fmt.Sprintf("ServiceKey[%s, %T]", k.ID, zero)
}

// NewServiceKey creates a new type-safe ServiceKey.
func NewServiceKey[T any](id string) ServiceKey[T] {
	return ServiceKey[T]{ID: id}
}

// Config controls the runtime behaviour of the typed receptionist actor. It
// mirrors the three Pekko config keys under
// pekko.cluster.typed.receptionist.* and is normally translated from
// cluster.TypedReceptionistConfig at receptionist construction time.
type Config struct {
	// WriteConsistency is the ddata write consistency level used when
	// registering or removing a service ref.
	WriteConsistency ddata.WriteConsistency

	// PruningInterval is the cadence at which the receptionist scans its
	// registered services and removes any whose paths no longer resolve.
	// Zero or negative disables the pruning ticker (used in tests that
	// drive pruneTick directly).
	PruningInterval time.Duration

	// DistributedKeyCount controls how many ORSet shards the receptionist
	// spreads its keyspace across. Values <= 0 are coerced to 1.
	DistributedKeyCount int
}

// DefaultConfig returns Pekko-default-equivalent values:
// WriteLocal / 3s / 5 shards.
func DefaultConfig() Config {
	return Config{
		WriteConsistency:    ddata.WriteLocal,
		PruningInterval:     3 * time.Second,
		DistributedKeyCount: 5,
	}
}

// ShardKey computes the ddata bucket name a given service id lives in. The
// hash-mod-N prefix lets the receptionist spread its keyspace across
// DistributedKeyCount ORSets without losing per-id determinism. Exposed for
// tests so they can read the same bucket the receptionist writes to.
func ShardKey(id string, count int) string {
	if count <= 0 {
		count = 1
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return fmt.Sprintf("receptionist-%d-%s", h.Sum32()%uint32(count), id)
}

// ReplicatorWriter is the subset of *ddata.Replicator the receptionist
// touches. Extracting the surface area as an interface lets tests inject a
// recording fake to observe write-consistency and bucket-name choices
// without spinning up a full replicator. *ddata.Replicator implements this
// interface directly.
type ReplicatorWriter interface {
	AddToSet(key, element string, c ddata.WriteConsistency)
	RemoveFromSet(key, element string, c ddata.WriteConsistency)
	ORSet(key string) *ddata.ORSet
}

// ─── Receptionist Protocol ───────────────────────────────────────────────

type Command interface {
	handle(ctx typed.TypedContext[any], rep ReplicatorWriter, subscribers *[]subInfo, cfg Config, knownIDs map[string]struct{})
}

type Register[T any] struct {
	Key     ServiceKey[T]
	Service typed.TypedActorRef[T]
}

func (r Register[T]) handle(ctx typed.TypedContext[any], rep ReplicatorWriter, subscribers *[]subInfo, cfg Config, knownIDs map[string]struct{}) {
	id := r.Key.ID
	path := r.Service.Path()
	bucket := ShardKey(id, cfg.DistributedKeyCount)
	rep.AddToSet(bucket, path, cfg.WriteConsistency)
	knownIDs[id] = struct{}{}
	ctx.Log().Info("Receptionist: registered service", "key", id, "path", path, "bucket", bucket)

	paths := rep.ORSet(bucket).Elements()
	for _, sub := range *subscribers {
		if sub.keyID == id {
			sub.sendUpdate(paths)
		}
	}
}

type Find[T any] struct {
	Key     ServiceKey[T]
	ReplyTo typed.TypedActorRef[Listing[T]]
}

func (f Find[T]) handle(ctx typed.TypedContext[any], rep ReplicatorWriter, _ *[]subInfo, cfg Config, knownIDs map[string]struct{}) {
	id := f.Key.ID
	bucket := ShardKey(id, cfg.DistributedKeyCount)
	knownIDs[id] = struct{}{}
	paths := rep.ORSet(bucket).Elements()
	f.ReplyTo.Tell(constructListing(ctx, f.Key, paths))
}

type Subscribe[T any] struct {
	Key        ServiceKey[T]
	Subscriber typed.TypedActorRef[Listing[T]]
}

func (s Subscribe[T]) handle(ctx typed.TypedContext[any], rep ReplicatorWriter, subscribers *[]subInfo, cfg Config, knownIDs map[string]struct{}) {
	id := s.Key.ID
	bucket := ShardKey(id, cfg.DistributedKeyCount)
	knownIDs[id] = struct{}{}
	sub := subInfo{
		keyID:      id,
		subscriber: s.Subscriber.Untyped(),
		sendUpdate: func(paths []string) {
			s.Subscriber.Tell(constructListing(ctx, s.Key, paths))
		},
	}
	*subscribers = append(*subscribers, sub)
	paths := rep.ORSet(bucket).Elements()
	s.Subscriber.Tell(constructListing(ctx, s.Key, paths))
}

type Listing[T any] struct {
	Key      ServiceKey[T]
	Services []typed.TypedActorRef[T]
}

func constructListing[T any](ctx typed.TypedContext[any], key ServiceKey[T], paths []string) Listing[T] {
	services := make([]typed.TypedActorRef[T], 0, len(paths))
	for _, p := range paths {
		if ref, err := ctx.System().Resolve(p); err == nil {
			services = append(services, typed.NewTypedActorRef[T](ref))
		}
	}
	return Listing[T]{
		Key:      key,
		Services: services,
	}
}

// ─── Public API ──────────────────────────────────────────────────────────

var globalReceptionist typed.TypedActorRef[any]

// Global returns the reference to the local Receptionist actor.
func Global() typed.TypedActorRef[any] {
	return globalReceptionist
}

// SetGlobal sets the global receptionist reference.
func SetGlobal(ref typed.TypedActorRef[any]) {
	globalReceptionist = ref
}

// ─── Internal/Non-Generic Protocol ───────────────────────────────────────

type subInfo struct {
	keyID      string
	subscriber actor.Ref
	sendUpdate func([]string)
}

type subscribeInternal struct {
	keyID      string
	subscriber typed.TypedActorRef[any]
	sendUpdate func([]string) // closure to send typed Listing back to subscriber
}

type replicatorChanged struct {
	key string
}

// pruneTick is delivered to the receptionist on each PruningInterval (and
// directly by tests via Receive) to scan tracked services for paths that no
// longer resolve and remove them.
type pruneTick struct{}

// pruneTimerKey is the timer-scheduler key used to identify the periodic
// prune ticker so it can be cancelled on actor stop. Distinct from any user
// timer key.
const pruneTimerKey = "__receptionist_prune__"

// ─── Receptionist Actor ───────────────────────────────────────────────────

// Behavior returns the typed receptionist behavior. The behavior captures
// cfg in its closure so all future Register/Find/Subscribe/pruneTick
// dispatches share the same config values.
func Behavior(replicator ReplicatorWriter, cfg Config) typed.Behavior[any] {
	return typed.Setup[any](func(ctx typed.TypedContext[any]) typed.Behavior[any] {
		// Schedule the periodic prune tick when the cadence is positive
		// and the actor system has provided a timer scheduler. The
		// nil-check keeps tests that drive Receive directly (without
		// PreStart wiring) safe — those tests pass PruningInterval=0 or
		// rely on direct pruneTick injection.
		if cfg.PruningInterval > 0 {
			if t := ctx.Timers(); t != nil {
				t.StartPeriodicTimer(pruneTimerKey, pruneTick{}, cfg.PruningInterval)
			}
		}

		var subscribers []subInfo
		knownIDs := map[string]struct{}{}

		return func(ctx typed.TypedContext[any], msg any) typed.Behavior[any] {
			switch m := msg.(type) {
			case pruneTick:
				prune(ctx, replicator, cfg, knownIDs, &subscribers)

			case Command:
				m.handle(ctx, replicator, &subscribers, cfg, knownIDs)

			case subscribeInternal:
				bucket := ShardKey(m.keyID, cfg.DistributedKeyCount)
				knownIDs[m.keyID] = struct{}{}
				sub := subInfo{
					keyID:      m.keyID,
					subscriber: m.subscriber.Untyped(),
					sendUpdate: m.sendUpdate,
				}
				subscribers = append(subscribers, sub)
				paths := replicator.ORSet(bucket).Elements()
				m.sendUpdate(paths)

			case replicatorChanged:
				bucket := ShardKey(m.key, cfg.DistributedKeyCount)
				for _, sub := range subscribers {
					if sub.keyID == m.key {
						paths := replicator.ORSet(bucket).Elements()
						sub.sendUpdate(paths)
					}
				}
			}
			return typed.Same[any]()
		}
	})
}

// prune walks every known service id and removes paths that no longer
// resolve to a live actor. Removals use the configured WriteConsistency so
// the prune is observable in the same way registers are.
func prune(ctx typed.TypedContext[any], rep ReplicatorWriter, cfg Config, knownIDs map[string]struct{}, subscribers *[]subInfo) {
	sys := ctx.System()
	for id := range knownIDs {
		bucket := ShardKey(id, cfg.DistributedKeyCount)
		set := rep.ORSet(bucket)
		paths := set.Elements()
		removed := false
		for _, p := range paths {
			if _, err := sys.Resolve(p); err != nil {
				rep.RemoveFromSet(bucket, p, cfg.WriteConsistency)
				removed = true
			}
		}
		if removed {
			updated := rep.ORSet(bucket).Elements()
			for _, sub := range *subscribers {
				if sub.keyID == id {
					sub.sendUpdate(updated)
				}
			}
		}
	}
}

// BridgeInternal allows root package to call internal subscribe.
func BridgeInternal(keyID string, subscriber typed.TypedActorRef[any], callback func([]string)) any {
	return subscribeInternal{
		keyID:      keyID,
		subscriber: subscriber,
		sendUpdate: callback,
	}
}
