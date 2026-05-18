// cmd/showcase-gekka/ddata_actors.go — Gekka-side DDDT actors for FT3
// of the gekka_showcase_test plan.
//
// startDdata launches one goroutine per supported CRDT type. Each goroutine
// performs a random write or read against gekka's local Replicator every
// [1s, 5s). State propagates to peers via the Replicator's periodic gossip
// loop (already started by gekka.NewCluster), so no explicit fan-out is
// needed here.
//
// NOTE: gekka's built-in *ddata.Replicator currently exposes gossip wiring
// for SIX of Pekko's eight standard CRDTs:
//
//	GCounter, PNCounter, ORSet, ORFlag, LWWRegister, LWWMap.
//
// GSet and ORMap exist as CRDT types under cluster/ddata/ but the Replicator
// has no accessor or gossip path for them. Scala's Task 8 still exercises
// all 8 CRDTs Scala<->Scala; the cross-language FT3 convergence contract is
// therefore limited to the 6 CRDTs above on the gekka<->Scala leg.
//
// SPDX-License-Identifier: MIT
package main

import (
	"math/rand"
	"strconv"
	"time"

	gekka "github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/cluster/ddata"
	"github.com/sopranoworks/gekka/logger"
)

// Showcase CRDT keys. These must match the Scala-side key names in
// test/showcase/scala/src/main/scala/com/gekka/showcase/DdataActors.scala
// for the cross-language convergence smoke test (FT3).
const (
	keyGCounter    = "showcase-gcounter"
	keyPNCounter   = "showcase-pncounter"
	keyORSet       = "showcase-orset"
	keyORFlag      = "showcase-orflag"
	keyLWWRegister = "showcase-lwwregister"
	keyLWWMap      = "showcase-lwwmap"
)

// ddataTickInterval returns a pseudo-random tick duration in [1s, 5s).
// Mirrors the Scala-side CrdtActor scheduling jitter so cross-language load
// patterns are comparable.
func ddataTickInterval(rng *rand.Rand) time.Duration {
	return time.Duration(1000+rng.Intn(4000)) * time.Millisecond
}

// startDdata launches one goroutine per supported CRDT. The goroutines are
// fire-and-forget (no graceful shutdown wiring) — they exit when the process
// exits. Each goroutine owns its own *rand.Rand so the RNGs do not contend.
//
// startDdata runs on every node regardless of peer configuration. The local
// Replicator gossip loop discovers peers as the cluster membership reaches
// Up, and unicast updates the moment a peer becomes reachable.
func startDdata(cluster *gekka.Cluster, selfLabel string) {
	repl := cluster.Replicator()
	if repl == nil {
		// Defensive: NewCluster always wires a Replicator, but if a future
		// refactor makes it optional we don't want a nil-deref to crash the
		// whole showcase binary.
		logger.Default().Error("DdataActor: cluster.Replicator() returned nil; FT3 disabled")
		return
	}

	logger.Default().Info("DdataActor: starting CRDT loops",
		"selfLabel", selfLabel,
		"supported", []string{"gcounter", "pncounter", "orset", "orflag", "lwwregister", "lwwmap"},
		"unsupported", []string{"gset", "ormap"},
	)

	seed := time.Now().UnixNano()
	go gcounterLoop(repl, selfLabel, rand.New(rand.NewSource(seed+1)))
	go pncounterLoop(repl, selfLabel, rand.New(rand.NewSource(seed+2)))
	go orsetLoop(repl, selfLabel, rand.New(rand.NewSource(seed+3)))
	go orflagLoop(repl, selfLabel, rand.New(rand.NewSource(seed+4)))
	go lwwregisterLoop(repl, selfLabel, rand.New(rand.NewSource(seed+5)))
	go lwwmapLoop(repl, selfLabel, rand.New(rand.NewSource(seed+6)))
}

// gcounterLoop ticks every [1s, 5s) and alternates Increment / Get on the
// shared GCounter key. WriteLocal lets the periodic gossip loop carry the
// state to peers — this matches Scala's WriteLocal / ReadLocal usage.
func gcounterLoop(repl *ddata.Replicator, selfLabel string, rng *rand.Rand) {
	for {
		time.Sleep(ddataTickInterval(rng))
		if rng.Intn(2) == 0 {
			repl.IncrementCounter(keyGCounter, 1, ddata.WriteLocal)
		} else {
			c := repl.GCounter(keyGCounter)
			_ = c.Value()
		}
	}
}

// pncounterLoop exercises Increment / Decrement / Get with equal probability,
// mirroring the Scala-side PNCounterActor. Decrement-on-empty is a no-op in
// gekka's PNCounter implementation, so no guard is needed on cold start.
func pncounterLoop(repl *ddata.Replicator, selfLabel string, rng *rand.Rand) {
	for {
		time.Sleep(ddataTickInterval(rng))
		p := repl.PNCounter(keyPNCounter)
		switch rng.Intn(3) {
		case 0:
			p.Increment(repl.NodeID(), 1)
		case 1:
			p.Decrement(repl.NodeID(), 1)
		default:
			_ = p.Value()
		}
	}
}

// orsetLoop exercises Add / Remove / Get with equal probability over a
// 50-element key-space — matches Scala's ORSetActor.
func orsetLoop(repl *ddata.Replicator, selfLabel string, rng *rand.Rand) {
	for {
		time.Sleep(ddataTickInterval(rng))
		elem := "item-" + strconv.Itoa(rng.Intn(50))
		switch rng.Intn(3) {
		case 0:
			repl.AddToSet(keyORSet, elem, ddata.WriteLocal)
		case 1:
			repl.RemoveFromSet(keyORSet, elem, ddata.WriteLocal)
		default:
			s := repl.ORSet(keyORSet)
			_ = s.Elements()
		}
	}
}

// orflagLoop alternates SwitchOn / Get. ORFlag has no Replicator convenience
// wrapper, so we mutate the CRDT directly; gekka's gossip loop snapshots all
// known CRDTs each round and disseminates them, so the change still
// propagates on the periodic schedule. Scala's actor only ever SwitchOns
// (the Flag is monotonic), so we omit SwitchOff here too.
func orflagLoop(repl *ddata.Replicator, selfLabel string, rng *rand.Rand) {
	for {
		time.Sleep(ddataTickInterval(rng))
		f := repl.ORFlag(keyORFlag)
		if rng.Intn(2) == 0 {
			f.SwitchOn(repl.NodeID())
		} else {
			_ = f.Value()
		}
	}
}

// lwwregisterLoop alternates Set / Get. Mirrors Scala's LWWRegisterActor,
// which writes "v<0..999>" strings.
func lwwregisterLoop(repl *ddata.Replicator, selfLabel string, rng *rand.Rand) {
	for {
		time.Sleep(ddataTickInterval(rng))
		reg := repl.LWWRegister(keyLWWRegister)
		if rng.Intn(2) == 0 {
			reg.Set(repl.NodeID(), "v"+strconv.Itoa(rng.Intn(1000)))
		} else {
			_, _ = reg.Get()
		}
	}
}

// lwwmapLoop alternates Put / Get over a 10-key, 1000-value space — matches
// Scala's LWWMapActor.
func lwwmapLoop(repl *ddata.Replicator, selfLabel string, rng *rand.Rand) {
	for {
		time.Sleep(ddataTickInterval(rng))
		if rng.Intn(2) == 0 {
			k := "k" + strconv.Itoa(rng.Intn(10))
			v := "v" + strconv.Itoa(rng.Intn(1000))
			repl.PutInMap(keyLWWMap, k, v, ddata.WriteLocal)
		} else {
			m := repl.LWWMap(keyLWWMap)
			_ = m.Entries()
		}
	}
}
