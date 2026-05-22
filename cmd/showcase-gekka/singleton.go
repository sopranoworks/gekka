// cmd/showcase-gekka/singleton.go — Gekka-side SingletonActor + ClientActor
// for FT4 of the gekka_showcase_test plan.
//
// The Scala side hosts five singletons under role names singleton-s1 …
// singleton-s5; gekka hosts three under singleton-g1 … singleton-g3. Every
// node runs a ClientActor that pings all eight singletons on a 4-second
// tick with a 5-second per-call budget.
//
// Warm-up grace (spec §5.4.2):
//   - For the first 30s after startClient is invoked, a timeout against a
//     role that has never produced a Pong is SILENT (the manager handover
//     and gossip convergence is still in flight on the showcase clock).
//   - After the 30s window elapses, every role that has never produced a
//     Pong gets exactly one ERROR log line ("never established within
//     warmup-grace=30s") so the test runner can count warm-up misses.
//   - Established-role timeouts (a role that previously produced a Pong)
//     always emit ERROR, including during the warm-up window — they signal
//     a regression in an already-working singleton.
//
// API discovery:
//   - cluster.SingletonManager(props, role) creates a manager actor; spawn
//     it via cluster.System.ActorOf(Props{...}, "singleton-manager-<role>")
//     so the singleton ends up at /user/singleton-manager-<role>/singleton,
//     matching the path the Scala-side ClientActor proxy expects.
//   - cluster.SingletonProxy(managerPath, role) returns a started
//     ClusterSingletonProxy. The proxy only exposes Send(ctx, msg); it does
//     not have an Ask method. To get request-reply for Ping/Pong we resolve
//     the singleton path via proxy.CurrentOldestPath() and dispatch through
//     cluster.Ask, which threads a temp sender path under /temp/ask-<id>.
//
// SPDX-License-Identifier: MIT
package main

import (
	"context"
	"sync"
	"time"

	gekka "github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
	gcluster "github.com/sopranoworks/gekka/cluster"
	"github.com/sopranoworks/gekka/logger"
)

// Ping and Pong are defined in envelope.go alongside the other showcase
// wire types. They embed jacksoncbor.JVMClassManifest and are registered
// with the JacksonCborSerializer in main.go so cross-language FT4 traffic
// flows over Pekko's jackson-cbor wire (the same path FT1/FT2 use).

// ShowcaseSingletonActor is the singleton actor hosted by each
// ClusterSingletonManager. It accepts a Ping and replies with a matching
// Pong to whoever the Sender() resolved to.
type ShowcaseSingletonActor struct {
	actor.BaseActor
}

// Receive handles Ping (typed delivery via JacksonCborSerializer for both
// local and remote, after Phase 3 wiring). Replies with a Pong carrying
// matching seq/origin.
func (a *ShowcaseSingletonActor) Receive(msg any) {
	switch m := msg.(type) {
	case *Ping:
		if s := a.Sender(); s != nil && s.Path() != "" {
			s.Tell(NewPong(m.SeqNo, m.Origin), a.Self())
			return
		}
		logger.Default().Error("ShowcaseSingletonActor: Ping with no sender",
			"seq", m.SeqNo, "origin", m.Origin)

	default:
		logger.Default().Error("ShowcaseSingletonActor: unexpected message",
			"type", fmtType(msg))
	}
}

const (
	// singletonWarmupGrace is the suppression window during which a
	// never-established role's timeout is silent.
	singletonWarmupGrace = 30 * time.Second
	// singletonInterval is the cadence at which the ClientActor pings every
	// configured singleton role.
	singletonInterval = 4 * time.Second
	// singletonPingTimeout is the per-call Ask budget.
	singletonPingTimeout = 5 * time.Second
)

// allRoles lists every singleton role the showcase exercises. The s1-s5
// roles are hosted by Scala nodes; g1-g3 by gekka nodes.
var allRoles = []string{
	"singleton-s1", "singleton-s2", "singleton-s3", "singleton-s4", "singleton-s5",
	"singleton-g1", "singleton-g2", "singleton-g3",
}

// showcaseClient owns the per-role established/pending state and the
// warm-up state machine. All access to the maps is guarded by mu.
type showcaseClient struct {
	cluster   *gekka.Cluster
	selfLabel string

	mu           sync.Mutex
	established  map[string]bool
	warmupActive bool
}

// startClient registers the Ping/Pong manifests, builds one
// ClusterSingletonProxy per role, and launches the ticker + warm-up-end
// goroutines.
//
// State machine for a per-tick Ask outcome:
//
//	established=true  → always log ERROR on timeout/error
//	established=false, warmup=true  → silent
//	established=false, warmup=false → log ERROR ("post-warmup, unresolved")
//
// success → set established[role]=true (and never reset)
func startClient(cluster *gekka.Cluster, selfLabel string) {
	// Ping/Pong registration with the gekka SerializationRegistry happens
	// in main.go via the JacksonCborSerializer; no JSON-manifest fallback
	// is needed because Pekko's jackson-cbor wire path handles both
	// in-language and cross-language deliveries.

	c := &showcaseClient{
		cluster:      cluster,
		selfLabel:    selfLabel,
		established:  make(map[string]bool),
		warmupActive: true,
	}

	// Build one proxy per role. The manager path mirrors how the Scala
	// side names its singleton-manager actor: /user/singleton-manager-<role>.
	// The proxy returned here is already Start()'ed and will route Send /
	// Ask calls through cluster.OldestNode(role) re-resolution on each call.
	proxies := make(map[string]gcluster.ClusterSingletonProxyInterface, len(allRoles))
	for _, role := range allRoles {
		p := cluster.SingletonProxy("/user/singleton-manager-"+role, role)
		if p == nil {
			logger.Default().Error("ClientActor: SingletonProxy returned nil", "role", role)
			continue
		}
		proxies[role] = p
	}

	logger.Default().Info("SingletonClient: started",
		"selfLabel", selfLabel,
		"roles", allRoles,
		"interval", singletonInterval.String(),
		"timeout", singletonPingTimeout.String(),
		"warmupGrace", singletonWarmupGrace.String())

	go c.warmupEnder()
	go c.tickLoop(proxies)
}

// warmupEnder waits the configured grace window, flips warmupActive=false,
// and emits one ERROR per role that never produced a Pong during warmup.
// The unestablished roles are snapshot under the lock to avoid racing with
// concurrent doAsk goroutines updating established[].
func (c *showcaseClient) warmupEnder() {
	time.Sleep(singletonWarmupGrace)

	c.mu.Lock()
	c.warmupActive = false
	unestablished := make([]string, 0, len(allRoles))
	for _, r := range allRoles {
		if !c.established[r] {
			unestablished = append(unestablished, r)
		}
	}
	c.mu.Unlock()

	for _, r := range unestablished {
		logger.Default().Error("SingletonClient: role never established within warmup-grace=30s",
			"role", r,
			"selfLabel", c.selfLabel)
	}
}

// tickLoop fires one round of per-role pings every singletonInterval.
// Each ping runs on its own goroutine so a hung Ask never blocks the
// ticker or other peers' rounds.
func (c *showcaseClient) tickLoop(proxies map[string]gcluster.ClusterSingletonProxyInterface) {
	t := time.NewTicker(singletonInterval)
	defer t.Stop()

	var seqCounter int64
	for range t.C {
		for role, p := range proxies {
			seqCounter++
			go c.doPing(role, p, seqCounter)
		}
	}
}

// doPing resolves the singleton path via the proxy, issues a cluster.Ask
// with a 5s budget, and applies the warm-up state machine to the outcome.
// The proxy itself only exposes Send; for request-reply we use
// CurrentOldestPath + cluster.Ask so the temp sender path is wired up and
// the singleton can reply via Sender().Tell(...).
func (c *showcaseClient) doPing(role string, p gcluster.ClusterSingletonProxyInterface, seq int64) {
	ctx, cancel := context.WithTimeout(context.Background(), singletonPingTimeout)
	defer cancel()

	path, err := p.CurrentOldestPath()
	if err != nil {
		// Singleton not yet elected for this role on the current oldest
		// node. Treat as an unestablished-role outcome and let the
		// post-warmup classifier decide whether to log it.
		c.classifyUnresolved(role, seq, err)
		return
	}

	ping := NewPing(seq, c.selfLabel)
	reply, err := c.cluster.Ask(ctx, path, ping)
	if err != nil {
		c.classifyUnresolved(role, seq, err)
		return
	}

	// Success path: mark the role established and (optionally) record the
	// payload-kind for debugging. Cross-node Go-Go replies arrive as a
	// decoded *Pong because we registered the manifest above. Scala
	// replies arrive as raw bytes via the CBOR gap and surface as a
	// raw *IncomingMessage with an unregistered manifest.
	c.mu.Lock()
	c.established[role] = true
	c.mu.Unlock()

	if reply == nil {
		logger.Default().Error("SingletonClient: nil reply",
			"role", role, "seq", seq, "selfLabel", c.selfLabel)
		return
	}
	if pong, ok := reply.DeserializedMessage.(*Pong); ok {
		logger.Default().Info("SingletonClient: pong",
			"role", role,
			"seq", pong.SeqNo,
			"origin", pong.Origin,
			"selfLabel", c.selfLabel)
		return
	}
	// Raw-bytes reply (cross-language CBOR path): the architectural
	// concern is tracked in the plan as a pre-existing issue. Log INFO
	// so it doesn't trip the warmup-miss accounting, and surface the
	// codec gap as a structured event for the runner.
	logger.Default().Info("SingletonClient: established (raw reply)",
		"role", role,
		"seq", seq,
		"serializerId", reply.SerializerId,
		"manifest", reply.Manifest,
		"payloadLen", len(reply.Payload),
		"selfLabel", c.selfLabel)
}

// classifyUnresolved applies the warm-up state machine to a failed Ask.
// The decision is made under the same lock that warmupEnder uses so the
// transition from warmupActive=true → false is consistent with the
// established[] map snapshot it took.
func (c *showcaseClient) classifyUnresolved(role string, seq int64, err error) {
	c.mu.Lock()
	established := c.established[role]
	warmup := c.warmupActive
	c.mu.Unlock()

	switch {
	case established:
		logger.Default().Error("SingletonClient: ping timed out",
			"role", role,
			"seq", seq,
			"err", err.Error(),
			"selfLabel", c.selfLabel)
	case warmup:
		// Silent during warmup grace per spec §5.4.2. The runner will
		// learn about the miss via the post-warmup "never established"
		// ERROR if the role never comes online.
	default:
		logger.Default().Error("SingletonClient: ping timed out (post-warmup, unresolved)",
			"role", role,
			"seq", seq,
			"err", err.Error(),
			"selfLabel", c.selfLabel)
	}
}
