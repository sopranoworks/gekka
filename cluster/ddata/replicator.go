/*
 * replicator.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sopranoworks/gekka/actor"
)

// SubscriptionID uniquely identifies a key-change subscription.
type SubscriptionID uint64

// KeyChangeCallback is invoked when a CRDT value is updated via HandleIncoming.
// key is the CRDT key; value is the current CRDT object (e.g. *GCounter, *ORSet).
type KeyChangeCallback func(key string, value any)

type subEntry struct {
	id SubscriptionID
	fn KeyChangeCallback
}

// ReplicatorMsg is the JSON envelope sent over Artery for gossip.
//
// Full-state message types: "gcounter-gossip", "orset-gossip", "lwwmap-gossip",
// "pncounter-gossip", "orflag-gossip", "lwwregister-gossip".
//
// Delta message types (Phase 18): "gcounter-delta", "orset-delta", "lwwmap-delta".
// Delta messages carry only changes since the last gossip cycle; the replicator
// falls back to a full-state message every fullStateEvery rounds.
type ReplicatorMsg struct {
	Type    string          `json:"type"`
	Key     string          `json:"key"`
	Payload json.RawMessage `json:"payload"`
}

// GCounterPayload carries a GCounter snapshot.
type GCounterPayload struct {
	State map[string]uint64 `json:"state"`
}

// ORSetPayload carries an ORSet snapshot.
type ORSetPayload struct {
	ORSetSnapshot
}

// LWWMapPayload carries an LWWMap snapshot.
type LWWMapPayload struct {
	State map[string]LWWEntry `json:"state"`
}

// PNCounterPayload carries a PNCounter snapshot.
type PNCounterPayload struct {
	PNCounterSnapshot
}

// LWWRegisterPayload carries a LWWRegister snapshot.
type LWWRegisterPayload struct {
	LWWRegisterSnapshot
}

// Consistency levels for Write operations.
type WriteConsistency int

const (
	WriteLocal WriteConsistency = iota
	WriteAll
)

// defaultFullStateEvery is the baseline cadence of full-state gossip when
// DeltaCRDT is enabled and no delta-size cap has fired. Full-state rounds
// repair deltas that may have been dropped in transit.
const defaultFullStateEvery = 10

// Replicator manages a set of named CRDTs and gossips state to peers.
//
// Delta-aware CRDTs (GCounter, ORSet, LWWMap) are gossiped using compact delta
// messages on most rounds, with a full-state fallback either on a fixed
// cadence (every defaultFullStateEvery rounds) or when the accumulated delta
// size exceeds MaxDeltaElements.
type Replicator struct {
	mu         sync.RWMutex
	nodeID     string
	counters   map[string]*GCounter
	sets       map[string]*ORSet
	maps       map[string]*LWWMap
	pnCounters map[string]*PNCounter
	orFlags    map[string]*ORFlag
	registers  map[string]*LWWRegister

	peers  []peerInfo // remote actor paths to gossip to
	router *actor.Router

	GossipInterval time.Duration
	gossipRound    int // incremented each gossipAll call
	stopCh         chan struct{}
	wg             sync.WaitGroup

	// ── Config knobs (wired from DistributedDataConfig) ──
	// NotifySubscribersInterval — when > 0, subscriber callbacks are batched
	// and flushed on this interval instead of firing synchronously inside
	// HandleIncoming. Zero disables batching (fire immediately).
	NotifySubscribersInterval time.Duration

	// MaxDeltaElements caps the total number of delta entries accumulated
	// across all CRDTs in a single gossip round. When exceeded, the
	// replicator sends full state for remaining CRDTs instead.
	MaxDeltaElements int

	// DeltaCRDTEnabled toggles delta-based gossip for delta-aware CRDTs.
	// When false, every round sends full state.
	DeltaCRDTEnabled bool

	// DeltaCRDTMaxDeltaSize caps the number of operations per single delta
	// message before the replicator falls back to full state for that CRDT.
	DeltaCRDTMaxDeltaSize int

	// PreferOldest, when true, instructs the peer-selection layer to order
	// peers by ascending upNumber. The Replicator itself gossips to all
	// peers each round; this flag is exposed for the membership-integration
	// layer to sort AddPeer calls.
	PreferOldest bool

	// Role restricts gossip participation to peers with this role. Empty =
	// all peers. Enforced by the membership layer before AddPeer is called.
	Role string

	// Name is the service key / actor name used for discovery. Exposed for
	// the registering code (cluster.go) — the Replicator itself does not
	// consume it at runtime.
	Name string

	// Callback invoked when a gossip message arrives for a key we don't know about.
	OnUnknownKey func(key string, msg ReplicatorMsg)

	// Subscription support.
	subsMu sync.RWMutex
	subs   map[string][]subEntry
	subSeq atomic.Uint64

	// Batched subscriber notification state.
	dirtyMu   sync.Mutex
	dirtyKeys map[string]any // key -> latest CRDT value since last flush

	// Pruning: optional — when non-nil, pruningManager.Tick fires on its own
	// goroutine started by Start(ctx).
	pruningManager  *PruningManager
	PruningInterval time.Duration
}

type peerInfo struct {
	path string // pekko://System@host:port/user/goReplicator
}

// NewReplicator creates a Replicator for the given nodeID.
func NewReplicator(nodeID string, router *actor.Router) *Replicator {
	return &Replicator{
		nodeID:                    nodeID,
		counters:                  make(map[string]*GCounter),
		sets:                      make(map[string]*ORSet),
		maps:                      make(map[string]*LWWMap),
		pnCounters:                make(map[string]*PNCounter),
		orFlags:                   make(map[string]*ORFlag),
		registers:                 make(map[string]*LWWRegister),
		router:                    router,
		GossipInterval:            2 * time.Second,
		NotifySubscribersInterval: 0, // default: fire immediately
		MaxDeltaElements:          500,
		DeltaCRDTEnabled:          true,
		DeltaCRDTMaxDeltaSize:     50,
		PruningInterval:           0, // default: disabled unless wired
		Name:                      "ddataReplicator",
		stopCh:                    make(chan struct{}),
		dirtyKeys:                 make(map[string]any),
	}
}

// AddPeer registers a remote replicator actor path.
func (r *Replicator) AddPeer(actorPath string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peers = append(r.peers, peerInfo{path: actorPath})
}

// NodeID returns the identifier this Replicator uses when recording
// per-node dots in CRDTs. Typically "host:port".
func (r *Replicator) NodeID() string {
	return r.nodeID
}

// GCounter returns (or creates) the named GCounter.
func (r *Replicator) GCounter(key string) *GCounter {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.counters[key]; ok {
		return c
	}
	c := NewGCounter()
	r.counters[key] = c
	return c
}

// ORSet returns (or creates) the named ORSet.
func (r *Replicator) ORSet(key string) *ORSet {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.sets[key]; ok {
		return s
	}
	s := NewORSet()
	r.sets[key] = s
	return s
}

// LWWMap returns (or creates) the named LWWMap.
func (r *Replicator) LWWMap(key string) *LWWMap {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m, ok := r.maps[key]; ok {
		return m
	}
	m := NewLWWMap()
	r.maps[key] = m
	return m
}

// PNCounter returns (or creates) the named PNCounter.
func (r *Replicator) PNCounter(key string) *PNCounter {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.pnCounters[key]; ok {
		return c
	}
	c := NewPNCounter()
	r.pnCounters[key] = c
	return c
}

// ORFlag returns (or creates) the named ORFlag.
func (r *Replicator) ORFlag(key string) *ORFlag {
	r.mu.Lock()
	defer r.mu.Unlock()
	if f, ok := r.orFlags[key]; ok {
		return f
	}
	f := NewORFlag()
	r.orFlags[key] = f
	return f
}

// LWWRegister returns (or creates) the named LWWRegister.
func (r *Replicator) LWWRegister(key string) *LWWRegister {
	r.mu.Lock()
	defer r.mu.Unlock()
	if reg, ok := r.registers[key]; ok {
		return reg
	}
	reg := NewLWWRegister()
	r.registers[key] = reg
	return reg
}

// Entry describes a CRDT known to the Replicator.  Used by debug/introspection
// callers to enumerate all live CRDTs without creating new ones.
type Entry struct {
	Key  string
	Type string // "gcounter" | "orset" | "lwwmap" | "pncounter" | "orflag" | "lwwregister"
}

// Entries returns a snapshot of every CRDT currently known to the Replicator.
// The slice is built under a short read-lock window and is safe to use after
// return.  Order is not defined — callers should sort if they need it.
func (r *Replicator) Entries() []Entry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Entry, 0,
		len(r.counters)+len(r.sets)+len(r.maps)+
			len(r.pnCounters)+len(r.orFlags)+len(r.registers))
	for k := range r.counters {
		out = append(out, Entry{Key: k, Type: "gcounter"})
	}
	for k := range r.sets {
		out = append(out, Entry{Key: k, Type: "orset"})
	}
	for k := range r.maps {
		out = append(out, Entry{Key: k, Type: "lwwmap"})
	}
	for k := range r.pnCounters {
		out = append(out, Entry{Key: k, Type: "pncounter"})
	}
	for k := range r.orFlags {
		out = append(out, Entry{Key: k, Type: "orflag"})
	}
	for k := range r.registers {
		out = append(out, Entry{Key: k, Type: "lwwregister"})
	}
	return out
}

// LookupGCounter returns the named GCounter without creating one if it is
// missing.  Second return is false on miss.
func (r *Replicator) LookupGCounter(key string) (*GCounter, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.counters[key]
	return c, ok
}

// LookupORSet is the read-only counterpart of ORSet(key).
func (r *Replicator) LookupORSet(key string) (*ORSet, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.sets[key]
	return s, ok
}

// LookupLWWMap is the read-only counterpart of LWWMap(key).
func (r *Replicator) LookupLWWMap(key string) (*LWWMap, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.maps[key]
	return m, ok
}

// LookupPNCounter is the read-only counterpart of PNCounter(key).
func (r *Replicator) LookupPNCounter(key string) (*PNCounter, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.pnCounters[key]
	return p, ok
}

// LookupORFlag is the read-only counterpart of ORFlag(key).
func (r *Replicator) LookupORFlag(key string) (*ORFlag, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, ok := r.orFlags[key]
	return f, ok
}

// LookupLWWRegister is the read-only counterpart of LWWRegister(key).
func (r *Replicator) LookupLWWRegister(key string) (*LWWRegister, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	reg, ok := r.registers[key]
	return reg, ok
}

// IncrementCounter increments the named counter for this node.
func (r *Replicator) IncrementCounter(key string, delta uint64, consistency WriteConsistency) {
	c := r.GCounter(key)
	c.Increment(r.nodeID, delta)
	if consistency == WriteAll {
		r.gossipCounter(context.Background(), key, c)
	}
}

// AddToSet adds element to the named set.
func (r *Replicator) AddToSet(key, element string, consistency WriteConsistency) {
	s := r.ORSet(key)
	s.Add(r.nodeID, element)
	if consistency == WriteAll {
		r.gossipSet(context.Background(), key, s)
	}
}

// RemoveFromSet removes element from the named set.
func (r *Replicator) RemoveFromSet(key, element string, consistency WriteConsistency) {
	s := r.ORSet(key)
	s.Remove(element)
	if consistency == WriteAll {
		r.gossipSet(context.Background(), key, s)
	}
}

// PutInMap adds/updates a value in the named LWWMap.
func (r *Replicator) PutInMap(key, itemKey string, value any, consistency WriteConsistency) {
	m := r.LWWMap(key)
	m.Put(itemKey, value)
	if consistency == WriteAll {
		r.gossipMap(context.Background(), key, m)
	}
}

// Start begins the periodic gossip loop, along with the optional
// batched-notification loop and pruning-tick loop when those features are
// enabled via configuration.
func (r *Replicator) Start(ctx context.Context) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(r.GossipInterval)
		defer ticker.Stop()
		for {
			select {
			case <-r.stopCh:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.gossipAll(ctx)
			}
		}
	}()

	if r.NotifySubscribersInterval > 0 {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			t := time.NewTicker(r.NotifySubscribersInterval)
			defer t.Stop()
			for {
				select {
				case <-r.stopCh:
					return
				case <-ctx.Done():
					return
				case <-t.C:
					r.flushNotifications()
				}
			}
		}()
	}

	if r.pruningManager != nil && r.PruningInterval > 0 {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			t := time.NewTicker(r.PruningInterval)
			defer t.Stop()
			for {
				select {
				case <-r.stopCh:
					return
				case <-ctx.Done():
					return
				case <-t.C:
					r.pruneTick()
				}
			}
		}()
	}
}

// flushNotifications delivers any accumulated subscriber notifications.
func (r *Replicator) flushNotifications() {
	r.dirtyMu.Lock()
	if len(r.dirtyKeys) == 0 {
		r.dirtyMu.Unlock()
		return
	}
	pending := r.dirtyKeys
	r.dirtyKeys = make(map[string]any)
	r.dirtyMu.Unlock()

	for key, value := range pending {
		r.fireSubscribers(key, value)
	}
}

// fireSubscribers calls every registered callback for key with the latest value.
func (r *Replicator) fireSubscribers(key string, value any) {
	r.subsMu.RLock()
	entries := append([]subEntry(nil), r.subs[key]...)
	r.subsMu.RUnlock()
	for _, e := range entries {
		e.fn(key, value)
	}
}

// SetPruningManager attaches a PruningManager and starts the pruning loop on
// the next call to Start. Must be called before Start to take effect. Passing
// nil clears the manager. When a manager is attached, PruningInterval must be
// > 0 for the pruning loop to run.
func (r *Replicator) SetPruningManager(pm *PruningManager) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruningManager = pm
}

// NotifyNodeRemoved forwards a cluster node-removal event to the pruning
// manager (if attached). Safe to call when no manager is configured.
func (r *Replicator) NotifyNodeRemoved(nodeID string) {
	r.mu.RLock()
	pm := r.pruningManager
	r.mu.RUnlock()
	if pm != nil {
		pm.NodeRemoved(nodeID)
	}
}

// snapshotPrunables returns every CRDT that implements Prunable, keyed by
// "<type>/<name>" for debugging. Safe under the replicator's read lock.
func (r *Replicator) snapshotPrunables() map[string]Prunable {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]Prunable, len(r.counters)+len(r.sets)+len(r.pnCounters))
	for k, c := range r.counters {
		out["gcounter/"+k] = c
	}
	for k, s := range r.sets {
		out["orset/"+k] = s
	}
	for k, p := range r.pnCounters {
		out["pncounter/"+k] = p
	}
	return out
}

// pruneTick drives one round of the attached pruning manager, if any.
func (r *Replicator) pruneTick() {
	r.mu.RLock()
	pm := r.pruningManager
	r.mu.RUnlock()
	if pm == nil {
		return
	}
	pm.Tick(r.snapshotPrunables())
}

// AllSetsSnapshot returns a point-in-time copy of all ORSets currently known
// to this Replicator.  Keys are set names; values are element slices.
// Safe to call from any goroutine.
func (r *Replicator) AllSetsSnapshot() map[string][]string {
	r.mu.RLock()
	sets := make(map[string]*ORSet, len(r.sets))
	for k, v := range r.sets {
		sets[k] = v
	}
	r.mu.RUnlock()

	result := make(map[string][]string, len(sets))
	for key, s := range sets {
		result[key] = s.Elements()
	}
	return result
}

// Stop halts the gossip loop.
func (r *Replicator) Stop() {
	close(r.stopCh)
	r.wg.Wait()
}

// subscribeKey registers fn to be called whenever the CRDT for key is updated
// via HandleIncoming. Returns the SubscriptionID needed to unsubscribe.
func (r *Replicator) subscribeKey(key string, fn KeyChangeCallback) SubscriptionID {
	id := SubscriptionID(r.subSeq.Add(1))
	r.subsMu.Lock()
	if r.subs == nil {
		r.subs = make(map[string][]subEntry)
	}
	r.subs[key] = append(r.subs[key], subEntry{id: id, fn: fn})
	r.subsMu.Unlock()
	return id
}

// unsubscribeKey removes the callback identified by id for the given key.
func (r *Replicator) unsubscribeKey(key string, id SubscriptionID) {
	r.subsMu.Lock()
	entries := r.subs[key]
	for i, e := range entries {
		if e.id == id {
			r.subs[key] = append(entries[:i], entries[i+1:]...)
			break
		}
	}
	r.subsMu.Unlock()
}

// notifySubscribers fans out to all registered callbacks for key. When
// NotifySubscribersInterval > 0, the change is accumulated and flushed on
// the next notify tick; otherwise it fires synchronously.
func (r *Replicator) notifySubscribers(key string, value any) {
	if r.NotifySubscribersInterval > 0 {
		r.dirtyMu.Lock()
		if r.dirtyKeys == nil {
			r.dirtyKeys = make(map[string]any)
		}
		r.dirtyKeys[key] = value // coalesce: latest value wins
		r.dirtyMu.Unlock()
		return
	}
	r.fireSubscribers(key, value)
}

// HandleIncoming processes a raw JSON message from a peer.
// Call this from your NodeManager's UserMessageCallback.
func (r *Replicator) HandleIncoming(data []byte) error {
	var msg ReplicatorMsg
	if err := json.Unmarshal(data, &msg); err != nil {
		return fmt.Errorf("replicator: unmarshal: %w", err)
	}
	switch msg.Type {
	case "gcounter-gossip":
		var p GCounterPayload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return err
		}
		c := r.GCounter(msg.Key)
		c.MergeState(p.State)
		log.Printf("Replicator: merged GCounter[%s], value=%d", msg.Key, c.Value())
		r.notifySubscribers(msg.Key, c)

	case "orset-gossip":
		var snap ORSetSnapshot
		if err := json.Unmarshal(msg.Payload, &snap); err != nil {
			return err
		}
		s := r.ORSet(msg.Key)
		s.MergeSnapshot(snap)
		log.Printf("Replicator: merged ORSet[%s], elements=%v", msg.Key, s.Elements())
		r.notifySubscribers(msg.Key, s)

	case "lwwmap-gossip":
		var p LWWMapPayload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return err
		}
		m := r.LWWMap(msg.Key)
		m.Merge(p.State)
		log.Printf("Replicator: merged LWWMap[%s], keys=%d", msg.Key, len(m.Entries()))
		r.notifySubscribers(msg.Key, m)

	case "pncounter-gossip":
		var p PNCounterPayload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return err
		}
		c := r.PNCounter(msg.Key)
		c.MergeSnapshot(p.PNCounterSnapshot)
		log.Printf("Replicator: merged PNCounter[%s], value=%d", msg.Key, c.Value())
		r.notifySubscribers(msg.Key, c)

	case "orflag-gossip":
		var snap ORSetSnapshot
		if err := json.Unmarshal(msg.Payload, &snap); err != nil {
			return err
		}
		f := r.ORFlag(msg.Key)
		f.MergeSnapshot(snap)
		log.Printf("Replicator: merged ORFlag[%s], value=%v", msg.Key, f.Value())
		r.notifySubscribers(msg.Key, f)

	case "lwwregister-gossip":
		var p LWWRegisterPayload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return err
		}
		reg := r.LWWRegister(msg.Key)
		reg.MergeSnapshot(p.LWWRegisterSnapshot)
		log.Printf("Replicator: merged LWWRegister[%s], value=%v", msg.Key, func() any { v, _ := reg.Get(); return v }())
		r.notifySubscribers(msg.Key, reg)

	// ── Delta message types ────────────────────────────────────────────────

	case "gcounter-delta":
		var d GCounterDelta
		if err := json.Unmarshal(msg.Payload, &d); err != nil {
			return err
		}
		c := r.GCounter(msg.Key)
		c.MergeCounterDelta(d)
		log.Printf("Replicator: merged GCounter delta[%s], value=%d", msg.Key, c.Value())
		r.notifySubscribers(msg.Key, c)

	case "orset-delta":
		var d ORSetDelta
		if err := json.Unmarshal(msg.Payload, &d); err != nil {
			return err
		}
		s := r.ORSet(msg.Key)
		s.MergeORSetDelta(d)
		log.Printf("Replicator: merged ORSet delta[%s], elements=%v", msg.Key, s.Elements())
		r.notifySubscribers(msg.Key, s)

	case "lwwmap-delta":
		var d LWWMapDelta
		if err := json.Unmarshal(msg.Payload, &d); err != nil {
			return err
		}
		m := r.LWWMap(msg.Key)
		m.MergeLWWMapDelta(d)
		log.Printf("Replicator: merged LWWMap delta[%s], keys=%d", msg.Key, len(m.Entries()))
		r.notifySubscribers(msg.Key, m)

	default:
		if r.OnUnknownKey != nil {
			r.OnUnknownKey(msg.Key, msg)
		}
	}
	return nil
}

func (r *Replicator) gossipAll(ctx context.Context) {
	r.mu.Lock()
	r.gossipRound++
	round := r.gossipRound
	// deltaEnabled == false → every round is a full-state round.
	// Otherwise, full state on a fixed cadence to repair dropped deltas.
	deltaEnabled := r.DeltaCRDTEnabled
	fullStateRound := !deltaEnabled || round%defaultFullStateEvery == 0
	maxDelta := r.MaxDeltaElements
	if maxDelta <= 0 {
		maxDelta = 500
	}
	maxDeltaSize := r.DeltaCRDTMaxDeltaSize
	if maxDeltaSize <= 0 {
		maxDeltaSize = 50
	}

	counters := make(map[string]*GCounter, len(r.counters))
	for k, v := range r.counters {
		counters[k] = v
	}
	sets := make(map[string]*ORSet, len(r.sets))
	for k, v := range r.sets {
		sets[k] = v
	}
	lwwMaps := make(map[string]*LWWMap, len(r.maps))
	for k, v := range r.maps {
		lwwMaps[k] = v
	}
	pnCounters := make(map[string]*PNCounter, len(r.pnCounters))
	for k, v := range r.pnCounters {
		pnCounters[k] = v
	}
	orFlags := make(map[string]*ORFlag, len(r.orFlags))
	for k, v := range r.orFlags {
		orFlags[k] = v
	}
	registers := make(map[string]*LWWRegister, len(r.registers))
	for k, v := range r.registers {
		registers[k] = v
	}
	r.mu.Unlock()

	// Track cumulative delta elements emitted this round. When we exceed
	// MaxDeltaElements we fall back to full-state for the remaining CRDTs.
	deltaBudget := maxDelta

	gossipCounterDelta := func(key string, c *GCounter) {
		if fullStateRound || deltaBudget <= 0 {
			c.ResetDelta()
			r.gossipCounter(ctx, key, c)
			return
		}
		payload, ok := c.DeltaPayload()
		if !ok {
			return
		}
		d, _ := payload.(GCounterDelta)
		if len(d.Delta) > maxDeltaSize {
			c.ResetDelta()
			r.gossipCounter(ctx, key, c)
			return
		}
		r.gossipDelta(ctx, "gcounter-delta", key, payload)
		c.ResetDelta()
		deltaBudget -= len(d.Delta)
	}

	gossipSetDelta := func(key string, s *ORSet) {
		if fullStateRound || deltaBudget <= 0 {
			s.ResetDelta()
			r.gossipSet(ctx, key, s)
			return
		}
		payload, ok := s.DeltaPayload()
		if !ok {
			return
		}
		d, _ := payload.(ORSetDelta)
		size := len(d.AddedDots) + len(d.RemovedElements)
		if size > maxDeltaSize {
			s.ResetDelta()
			r.gossipSet(ctx, key, s)
			return
		}
		r.gossipDelta(ctx, "orset-delta", key, payload)
		s.ResetDelta()
		deltaBudget -= size
	}

	gossipMapDelta := func(key string, m *LWWMap) {
		if fullStateRound || deltaBudget <= 0 {
			m.ResetDelta()
			r.gossipMap(ctx, key, m)
			return
		}
		payload, ok := m.DeltaPayload()
		if !ok {
			return
		}
		d, _ := payload.(LWWMapDelta)
		size := len(d.Changed)
		if size > maxDeltaSize {
			m.ResetDelta()
			r.gossipMap(ctx, key, m)
			return
		}
		r.gossipDelta(ctx, "lwwmap-delta", key, payload)
		m.ResetDelta()
		deltaBudget -= size
	}

	for key, c := range counters {
		gossipCounterDelta(key, c)
	}
	for key, s := range sets {
		gossipSetDelta(key, s)
	}
	for key, m := range lwwMaps {
		gossipMapDelta(key, m)
	}

	// Non-delta CRDTs: always send full state.
	for key, c := range pnCounters {
		r.gossipPNCounter(ctx, key, c)
	}
	for key, f := range orFlags {
		r.gossipORFlag(ctx, key, f)
	}
	for key, reg := range registers {
		r.gossipLWWRegister(ctx, key, reg)
	}
}

func (r *Replicator) gossipDelta(ctx context.Context, msgType, key string, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	r.sendToPeers(ctx, ReplicatorMsg{Type: msgType, Key: key, Payload: data})
}

func (r *Replicator) gossipCounter(ctx context.Context, key string, c *GCounter) {
	payload, err := json.Marshal(GCounterPayload{State: c.Snapshot()})
	if err != nil {
		return
	}
	msg := ReplicatorMsg{Type: "gcounter-gossip", Key: key, Payload: payload}
	r.sendToPeers(ctx, msg)
}

func (r *Replicator) gossipSet(ctx context.Context, key string, s *ORSet) {
	snap := s.Snapshot()
	payload, err := json.Marshal(snap)
	if err != nil {
		return
	}
	msg := ReplicatorMsg{Type: "orset-gossip", Key: key, Payload: payload}
	r.sendToPeers(ctx, msg)
}

func (r *Replicator) gossipMap(ctx context.Context, key string, m *LWWMap) {
	payload, err := json.Marshal(LWWMapPayload{State: m.Snapshot()})
	if err != nil {
		return
	}
	msg := ReplicatorMsg{Type: "lwwmap-gossip", Key: key, Payload: payload}
	r.sendToPeers(ctx, msg)
}

func (r *Replicator) gossipPNCounter(ctx context.Context, key string, c *PNCounter) {
	payload, err := json.Marshal(PNCounterPayload{c.Snapshot()})
	if err != nil {
		return
	}
	r.sendToPeers(ctx, ReplicatorMsg{Type: "pncounter-gossip", Key: key, Payload: payload})
}

func (r *Replicator) gossipORFlag(ctx context.Context, key string, f *ORFlag) {
	payload, err := json.Marshal(f.Snapshot())
	if err != nil {
		return
	}
	r.sendToPeers(ctx, ReplicatorMsg{Type: "orflag-gossip", Key: key, Payload: payload})
}

func (r *Replicator) gossipLWWRegister(ctx context.Context, key string, reg *LWWRegister) {
	payload, err := json.Marshal(LWWRegisterPayload{reg.Snapshot()})
	if err != nil {
		return
	}
	r.sendToPeers(ctx, ReplicatorMsg{Type: "lwwregister-gossip", Key: key, Payload: payload})
}

func (r *Replicator) sendToPeers(ctx context.Context, msg ReplicatorMsg) {
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Replicator: marshal error: %v", err)
		return
	}
	r.mu.RLock()
	peers := append([]peerInfo(nil), r.peers...)
	r.mu.RUnlock()

	for _, p := range peers {
		if err := r.router.Send(ctx, p.path, data); err != nil {
			log.Printf("Replicator: send to %s failed: %v", p.path, err)
		}
	}
}
