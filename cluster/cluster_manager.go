/*
 * cluster_manager.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"

	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sopranoworks/gekka/actor"
	icluster "github.com/sopranoworks/gekka/internal/cluster"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	"github.com/sopranoworks/gekka/logger"

	"google.golang.org/protobuf/proto"
)

// Cluster-internal messages (heartbeats, gossip, etc.) are handled automatically

// gzipDecompress decompresses GZIP-compressed bytes (used for Welcome and GossipEnvelope.serializedGossip).
func gzipDecompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// gzipCompress compresses bytes with GZIP (used when building GossipEnvelope.serializedGossip).
func gzipCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ClusterManager handles node membership and gossip.
type ClusterManager struct {
	Mu              sync.RWMutex
	LocalAddress    *gproto_cluster.UniqueAddress
	LocalHash       int32  // for VectorClock version (initial UID-based hash)
	LocalHashStr    string // Pekko-assigned hash for this node (murmur format); overrides LocalHash
	State           *gproto_cluster.Gossip
	Metrics         Metrics
	Fd              *PhiAccrualFailureDetector
	WatchFd         *WatchFailureDetector // remote-watch FD; pekko.remote.watch-failure-detector.*
	Sys             actor.ActorContext    // bridge back to the node's actor system
	WelcomeReceived atomic.Bool
	// joinAttempted is set true the first time JoinCluster is called.
	// Used by the leader-action loop to gate the "len(members)==1 ->
	// self-promote" bootstrap path: when joinAttempted is true the
	// node has explicitly tried to join a remote (or itself) and must
	// wait for Welcome before treating its local single-member view as
	// authoritative.  Without this gate, gekka self-promotes as
	// upNumber=1 in the brief window before Welcome arrives, then SBR
	// detects the view conflict against the seed's gossip and self-downs.
	joinAttempted atomic.Bool
	CancelHeartbeat context.CancelFunc

	// Protocol is the configured actor-path scheme ("pekko" or "akka").
	Protocol string

	// LocalDataCenter is this node's data-center label.
	// Pekko encodes it as a role with prefix "dc-" (e.g. "dc-us-east").
	// Defaults to "default" when empty.
	LocalDataCenter string

	// localRoles holds application-defined cluster roles (e.g. "metrics-exporter").
	// Set via SetLocalRoles before JoinCluster.  The dc-<dc> role is always appended.
	localRoles []string

	// LocalAppVersion is this node's application version, advertised during join.
	LocalAppVersion AppVersion

	// Router is a function for sending messages, avoiding a cycle with the root package.
	Router func(ctx context.Context, path string, msg any) error

	// CrossDataCenterGossipProbability is the probability [0.0,1.0] with which a
	// gossip round targets a node in a foreign data center.  Intra-DC gossip
	// always proceeds; cross-DC rounds are skipped when rand >= probability.
	// Defaults to 0.1 when zero.
	CrossDataCenterGossipProbability float64

	// CrossDataCenterConnections limits the number of distinct foreign-DC nodes
	// used as gossip targets per round. Defaults to 5 when zero.
	CrossDataCenterConnections int

	// SBRStrategy is an optional static-quorum strategy from the internal SBR
	// primitives.  When set, CheckReachability invokes it after the partition
	// has been unreachable for SBRStableAfter and calls LeaveCluster on Down.
	SBRStrategy icluster.Strategy

	// SBRStableAfter is how long the unreachable set must remain non-empty
	// before the SBRStrategy is consulted.  Defaults to 20s when zero.
	SBRStableAfter time.Duration

	// sbrUnreachableSince records when the current unreachable set first
	// appeared.  Zero means the cluster is currently fully reachable.
	// Protected by Mu.
	sbrUnreachableSince time.Time

	// heartbeatMuted suppresses heartbeat responses when true, allowing
	// the remote failure detector to trigger.  Set by StopHeartbeat, cleared
	// by StartHeartbeat.
	heartbeatMuted atomic.Bool

	// exitingConfirmedSent guards against duplicate ExitingConfirmed sends.
	// Pekko's leader will only transition this node from Exiting → Removed
	// after it receives an ExitingConfirmed(selfUniqueAddress) message back
	// from the leaving node. We send it once when our local gossip view
	// first marks us as Exiting.
	exitingConfirmedSent atomic.Bool

	// localLeaveInitiated is set when the user (or CoordinatedShutdown's
	// cluster-leave phase) has called LeaveCluster() on this node.  Until
	// then, any incoming gossip that carries our exact-UID slot in a
	// terminal status (Down / Removed / Leaving / Exiting) is treated as
	// stale state inherited from a prior gekka incarnation at the same
	// host:port — the seed has not yet pruned its records — and the
	// reincarnation repair resets it back to Joining so the leader's next
	// promotion cycle can move us to Up.  Without this gate, the
	// CoordinatedShutdown self-downed subscriber fires within ~1 s of
	// receiving Welcome and the freshly-joined node terminates before
	// reaching Up.  See live diag against the production cluster on
	// 127.0.0.1:37777 for the verbatim reproducer.
	localLeaveInitiated atomic.Bool

	// Cluster event subscribers — managed by cluster_events.go methods.
	SubMu sync.RWMutex
	Subs  []eventSubscriber

	// MissEmitter is notified when the failure detector marks a node unreachable.
	// Implemented by *core.FlightRecorder via a thin interface to avoid an import cycle.
	MissEmitter FlightMissEmitter

	// HeartbeatInterval is how often heartbeat messages are sent.
	// Corresponds to pekko.cluster.failure-detector.heartbeat-interval.
	// Default: 1s.
	HeartbeatInterval time.Duration

	// GossipInterval is the duration between gossip rounds.
	// Corresponds to pekko.cluster.gossip-interval.
	// Default: 1s.
	GossipInterval time.Duration

	// RetryUnsuccessfulJoinAfter is how often to retry InitJoin when no Welcome is received.
	// Corresponds to pekko.cluster.retry-unsuccessful-join-after.
	// Default: 10s (Pekko default).
	RetryUnsuccessfulJoinAfter time.Duration

	// MinNrOfMembers is the minimum number of members that must join before the
	// leader promotes Joining members to Up status.
	// Corresponds to pekko.cluster.min-nr-of-members.
	// Default: 1 (no gate).
	MinNrOfMembers int

	// RoleMinNrOfMembers maps role names to the minimum number of members
	// with that role required before the leader promotes Joining members.
	// Corresponds to pekko.cluster.role.{name}.min-nr-of-members.
	RoleMinNrOfMembers map[string]int

	// LeaderActionsInterval is how often leader actions are evaluated.
	// When zero, leader actions run on every gossip tick (legacy behavior).
	// Corresponds to pekko.cluster.leader-actions-interval.
	// Default: 1s.
	LeaderActionsInterval time.Duration

	// PeriodicTasksInitialDelay is the delay before starting gossip and
	// heartbeat loops after cluster initialization.
	// Corresponds to pekko.cluster.periodic-tasks-initial-delay.
	// Default: 1s.
	PeriodicTasksInitialDelay time.Duration

	// ShutdownAfterUnsuccessfulJoinSeedNodes is the maximum time to retry
	// joining seed nodes before aborting and shutting down the cluster.
	// A zero or negative value means no timeout (retry forever).
	// Corresponds to pekko.cluster.shutdown-after-unsuccessful-join-seed-nodes.
	// Default: 0 (off).
	ShutdownAfterUnsuccessfulJoinSeedNodes time.Duration

	// LogInfo controls whether informational cluster messages are logged.
	// When false, cluster transitions and general messages are suppressed.
	// Corresponds to pekko.cluster.log-info.
	// Default: true.
	LogInfo bool

	// LogInfoVerbose controls whether verbose cluster messages are logged
	// (gossip details, heartbeat events, etc.).
	// Corresponds to pekko.cluster.log-info-verbose.
	// Default: false.
	LogInfoVerbose bool

	// AllowWeaklyUpMembers is the duration after which Joining members
	// are promoted to WeaklyUp even without convergence.
	// A zero value disables WeaklyUp promotion (requires full convergence).
	// Corresponds to pekko.cluster.allow-weakly-up-members.
	// Default: 7s.
	AllowWeaklyUpMembers time.Duration

	// GossipDifferentViewProbability is the probability [0.0, 1.0] that a
	// gossip target with a different state view is preferred over one with the
	// same view, speeding convergence.
	// Corresponds to pekko.cluster.gossip-different-view-probability.
	// Default: 0.8.
	GossipDifferentViewProbability float64

	// ReduceGossipDifferentViewProbability is the cluster size above which
	// the different-view probability is halved (to 0.4).
	// Corresponds to pekko.cluster.reduce-gossip-different-view-probability.
	// Default: 400.
	ReduceGossipDifferentViewProbability int

	// GossipTimeToLive is the maximum acceptable age of incoming gossip.
	// Gossip older than this is discarded. Zero disables TTL checking.
	// Corresponds to pekko.cluster.gossip-time-to-live.
	// Default: 2s.
	GossipTimeToLive time.Duration

	// PruneGossipTombstonesAfter is how long removed-member tombstones are
	// kept before pruning. Zero disables pruning.
	// Corresponds to pekko.cluster.prune-gossip-tombstones-after.
	// Default: 24h.
	PruneGossipTombstonesAfter time.Duration

	// lastGossipUpdate records when the local gossip state was last updated
	// (for gossip TTL enforcement). Protected by Mu.
	lastGossipUpdate time.Time

	// tombstones tracks removed members and when they were removed,
	// for pruning after PruneGossipTombstonesAfter. Protected by Mu.
	tombstones map[string]time.Time

	// joiningFirstSeen tracks when Joining members were first observed
	// without convergence, for WeaklyUp promotion timing.
	// Protected by Mu.
	joiningFirstSeen map[int32]time.Time

	// MonitoredByNrOfMembers limits the number of nodes this node sends
	// heartbeats to. When zero or negative, heartbeats are sent to all known
	// members (legacy behavior).
	// Corresponds to pekko.cluster.failure-detector.monitored-by-nr-of-members.
	// Default: 9.
	MonitoredByNrOfMembers int

	// UnreachableNodesReaperInterval is how often the periodic reaper goroutine
	// re-evaluates phi for all nodes and publishes unreachable events.
	// A zero value disables the reaper (relies on gossipTick-driven checks only).
	// Corresponds to pekko.cluster.unreachable-nodes-reaper-interval.
	// Default: 1s.
	UnreachableNodesReaperInterval time.Duration

	// DownRemovalMargin is the duration to delay the Down → Removed transition.
	// When a member is marked Down, it is not immediately removed; instead it
	// remains in Down status for this duration to allow coordinated shutdown.
	// Zero means immediate removal (legacy behavior).
	// Corresponds to pekko.cluster.down-removal-margin.
	// Default: 0 (off).
	DownRemovalMargin time.Duration

	// downedAt tracks when each member was first seen in Down status,
	// for enforcing DownRemovalMargin. Protected by Mu.
	downedAt map[int32]time.Time

	// SeedNodeTimeout is the maximum time to wait for the first seed node to
	// respond before the node considers the seed unreachable.
	// Corresponds to pekko.cluster.seed-node-timeout.
	// Default: 5s.
	SeedNodeTimeout time.Duration

	// EnforceConfigCompatOnJoin controls whether incoming InitJoin config is
	// validated against the local node's config. When true, mismatches in
	// downing-provider-class cause the join to be rejected.
	// Corresponds to pekko.cluster.configuration-compatibility-check.enforce-on-join.
	// Default: true.
	EnforceConfigCompatOnJoin bool

	// PublishStatsInterval is how often a CurrentClusterStats event is
	// published to subscribers. Zero ("off") disables stats publication.
	// Corresponds to pekko.cluster.publish-stats-interval.
	// Default: 0 (off).
	PublishStatsInterval time.Duration

	// CrossDCHeartbeatInterval, CrossDCAcceptableHeartbeatPause and
	// CrossDCExpectedResponseAfter tune the multi-data-center failure
	// detector and only apply when sending heartbeats to (or judging
	// reachability of) a node in a different data center.
	// When zero, the intra-DC values from FailureDetector are used.
	// Corresponds to pekko.cluster.multi-data-center.failure-detector.{
	//   heartbeat-interval, acceptable-heartbeat-pause, expected-response-after}.
	// Pekko defaults: 3s / 10s / 1s.
	CrossDCHeartbeatInterval        time.Duration
	CrossDCAcceptableHeartbeatPause time.Duration
	CrossDCExpectedResponseAfter    time.Duration

	// ExpectedResponseAfter is the intra-DC failure-detector calibration
	// value, mirroring FailureDetectorConfig.ExpectedResponseAfter so that
	// EffectiveExpectedResponseAfter can pick between cross-DC and
	// intra-DC values per target. Set by ApplyDetectorConfig from
	// FailureDetectorConfig.ExpectedResponseAfter.
	// Corresponds to pekko.cluster.failure-detector.expected-response-after.
	// Pekko default: 1s.
	ExpectedResponseAfter time.Duration

	// ShutdownCallback is invoked when the join timeout fires
	// (shutdown-after-unsuccessful-join-seed-nodes). The Cluster layer wires
	// this to CoordinatedShutdown.Run, matching Pekko's behavior of
	// CoordinatedShutdown(system).run(clusterDowningReason).
	ShutdownCallback func()
}

// FlightMissEmitter can record a heartbeat-miss event.
// Implemented by *core.FlightRecorder via a thin adapter.
type FlightMissEmitter interface {
	EmitHeartbeatMiss(remoteAddr string, phi float64)
}

// Metrics is an interface for recording cluster-related counters.
type Metrics interface {
	IncrementGossipSent()
	IncrementGossipReceived()
	RecordConvergence(duration time.Duration)
	IncrementMemberUp()
	IncrementMemberRemoved()
}

// Proto returns the configured actor-path scheme, defaulting to "pekko".
func (cm *ClusterManager) Proto() string {
	if cm.Protocol != "" {
		return cm.Protocol
	}
	if cm.LocalAddress != nil && cm.LocalAddress.GetAddress() != nil {
		return cm.LocalAddress.GetAddress().GetProtocol()
	}
	return "pekko"
}

// ClusterCorePath builds the actor path to the cluster core daemon on a remote node.
func (cm *ClusterManager) ClusterCorePath(system, host string, port uint32) string {
	return fmt.Sprintf("%s://%s@%s:%d/system/cluster/core/daemon", cm.Proto(), system, host, port)
}

// HeartbeatPath builds the actor path to the heartbeat receiver on a remote node.
func (cm *ClusterManager) HeartbeatPath(system, host string, port uint32) string {
	return fmt.Sprintf("%s://%s@%s:%d/system/cluster/heartbeatReceiver", cm.Proto(), system, host, port)
}

// HeartbeatSenderPath builds the actor path to the heartbeat sender on a remote node.
// In Akka 2.6.x, ClusterHeartbeatSender is a child of ClusterCoreDaemon (at /system/cluster/core/daemon).
func (cm *ClusterManager) HeartbeatSenderPath(system, host string, port uint32) string {
	return fmt.Sprintf("%s://%s@%s:%d/system/cluster/core/daemon/heartbeatSender", cm.Proto(), system, host, port)
}

// CheckConfigCompat validates the joining node's HOCON config string against
// the local node's downing-provider-class. Returns true if compatible.
// This implements the core of pekko.cluster.configuration-compatibility-check.enforce-on-join.
func (cm *ClusterManager) CheckConfigCompat(remoteConfig string) bool {
	// Pekko checks that downing-provider-class matches between nodes.
	// Extract the value from the remote config string.
	remoteDP := extractHOCONValue(remoteConfig, "downing-provider-class")
	// Our own config always advertises SplitBrainResolverProvider (or empty).
	// We consider them compatible if both are either empty or both reference SBR.
	localProto := cm.Proto()
	localDP := fmt.Sprintf("%s.cluster.sbr.SplitBrainResolverProvider", localProto)

	// Normalize: treat empty and the SBR class as the two valid values.
	// If both are empty or both are set to SBR, they are compatible.
	remoteIsSBR := strings.Contains(remoteDP, "SplitBrainResolverProvider")
	remoteIsEmpty := remoteDP == "" || remoteDP == `""`

	localIsSBR := true // we always advertise SBR
	_ = localDP

	if remoteIsEmpty && !localIsSBR {
		return false
	}
	if !remoteIsEmpty && !remoteIsSBR {
		// Remote has a non-SBR downing provider — incompatible
		logger.Default().Warn("Cluster: config compat check failed", slog.String("remoteDowningProviderClass", remoteDP))
		return false
	}
	return true
}

// extractHOCONValue extracts a simple value for a key from an inline HOCON string.
// This handles the format: key = value or key = "value"
func extractHOCONValue(hoconStr, key string) string {
	idx := strings.Index(hoconStr, key)
	if idx < 0 {
		return ""
	}
	rest := hoconStr[idx+len(key):]
	rest = strings.TrimLeft(rest, " \t")
	if len(rest) > 0 && rest[0] == '=' {
		rest = rest[1:]
	}
	rest = strings.TrimLeft(rest, " \t")
	// Read until end of line or next key
	end := strings.IndexAny(rest, "\n\r")
	if end >= 0 {
		rest = rest[:end]
	}
	return strings.TrimSpace(rest)
}

// localHashString returns the hash string used for this node's VectorClock entry.
// If a Pekko-assigned hash has been adopted it takes precedence; otherwise the
// UID-based fallback is used.
func (cm *ClusterManager) localHashString() string {
	if cm.LocalHashStr != "" {
		return cm.LocalHashStr
	}
	return fmt.Sprintf("%d", cm.LocalHash)
}

// syncLocalHashFromStateLocked scans cm.State.AllAddresses for the local
// node and, if a non-empty hash is found in AllHashes at the corresponding
// index, adopts it as LocalHashStr and updates the AllHashes entry in
// cm.State.  Must be called with cm.Mu held (write).
// repairSelfReincarnationLocked is invoked after we adopt an incoming
// gossip wholesale (ClockBefore) or merge it (ClockConcurrent).  Its
// job: if the incoming gossip carried an entry for our host:port with
// a DIFFERENT UID — typically a previous gekka instance the seed
// recorded as Down/Removed before we restarted on the same port —
// rewrite that entry to point at our CURRENT UniqueAddress with status
// Joining (the legitimate state of a freshly-joined member).
//
// Without this, the seed's stale "you were Down" gossip overwrites our
// liveness on the very first GossipEnvelope we receive, the
// CoordinatedShutdown subscriber sees self transition to Down, and the
// freshly-joined node terminates within ~1s of being welcomed — exactly
// the dashboard's "I see no nodes" symptom.
//
// Caller must hold cm.Mu (write lock).
func (cm *ClusterManager) repairSelfReincarnationLocked() {
	localHost := cm.LocalAddress.GetAddress().GetHostname()
	localPort := cm.LocalAddress.GetAddress().GetPort()
	myUid := cm.LocalAddress.GetUid()
	myUid2 := cm.LocalAddress.GetUid2()

	// Find our host:port slot in AllAddresses.  If the UID at that slot
	// matches ours we're fine.  If it differs, replace the slot's
	// UniqueAddress with our current one and reset the corresponding
	// member entry to Joining.  If no slot exists for our address yet,
	// append one — the leader hasn't seen us yet (we just sent Join).
	addrIdx := -1
	for i, ua := range cm.State.AllAddresses {
		a := ua.GetAddress()
		if a.GetHostname() == localHost && a.GetPort() == localPort {
			addrIdx = i
			break
		}
	}
	if addrIdx < 0 {
		// No slot for our address — append one with our identity.
		newUA := &gproto_cluster.UniqueAddress{
			Address: cm.LocalAddress.GetAddress(),
			Uid:     proto.Uint32(myUid),
			Uid2:    proto.Uint32(myUid2),
		}
		cm.State.AllAddresses = append(cm.State.AllAddresses, newUA)
		// Pad AllHashes to keep the indexes aligned.
		for len(cm.State.AllHashes) < len(cm.State.AllAddresses) {
			cm.State.AllHashes = append(cm.State.AllHashes, cm.localHashString())
		}
		newIdx := int32(len(cm.State.AllAddresses) - 1)
		cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
			AddressIndex: proto.Int32(newIdx),
			UpNumber:     proto.Int32(0),
			Status:       gproto_cluster.MemberStatus_Joining.Enum(),
		})
		return
	}

	stale := cm.State.AllAddresses[addrIdx]
	if stale.GetUid() == myUid && stale.GetUid2() == myUid2 {
		// Same UID — not a UID-mismatch reincarnation.  But the seed can
		// still send us back a Welcome carrying our exact-UID slot in a
		// terminal status (Down / Removed / Leaving / Exiting): this
		// happens when the seed's gossip carries down-removal-margin
		// linger from a prior gekka process that crashed at the same
		// host:port, then the seed maps our fresh Join onto the
		// already-existing slot without resetting its status.  When that
		// happens BEFORE we have initiated our own Leave, treat the
		// terminal status as stale and reset the slot to Joining so the
		// leader's next promotion cycle can move us to Up.
		if cm.localLeaveInitiated.Load() {
			return
		}
		for _, m := range cm.State.Members {
			if m.GetAddressIndex() != int32(addrIdx) {
				continue
			}
			s := m.GetStatus()
			if s == gproto_cluster.MemberStatus_Down ||
				s == gproto_cluster.MemberStatus_Removed ||
				s == gproto_cluster.MemberStatus_Leaving ||
				s == gproto_cluster.MemberStatus_Exiting {
				logger.Default().Info("cluster: resetting stale self-UID slot to Joining",
					slog.String("host", localHost),
					slog.Int("port", int(localPort)),
					slog.String("staleStatus", s.String()))
				m.Status = gproto_cluster.MemberStatus_Joining.Enum()
			}
			break
		}
		return
	}
	logger.Default().Info("cluster: repairing self-reincarnation in incoming gossip",
		slog.String("host", localHost),
		slog.Int("port", int(localPort)),
		slog.Uint64("staleUid", uint64(stale.GetUid())|uint64(stale.GetUid2())<<32),
		slog.Uint64("ourUid", uint64(myUid)|uint64(myUid2)<<32))

	cm.State.AllAddresses[addrIdx] = &gproto_cluster.UniqueAddress{
		Address: cm.LocalAddress.GetAddress(),
		Uid:     proto.Uint32(myUid),
		Uid2:    proto.Uint32(myUid2),
	}
	for _, m := range cm.State.Members {
		if m.GetAddressIndex() == int32(addrIdx) {
			m.Status = gproto_cluster.MemberStatus_Joining.Enum()
			break
		}
	}
}

func (cm *ClusterManager) syncLocalHashFromStateLocked() {
	localHost := cm.LocalAddress.GetAddress().GetHostname()
	localPort := cm.LocalAddress.GetAddress().GetPort()
	for i, ua := range cm.State.AllAddresses {
		if ua.GetAddress().GetHostname() != localHost || ua.GetAddress().GetPort() != localPort {
			continue
		}
		if i >= len(cm.State.AllHashes) || cm.State.AllHashes[i] == "" {
			break
		}
		newHash := cm.State.AllHashes[i]
		oldHash := cm.localHashString()
		if newHash == oldHash {
			return
		}
		logger.Default().Debug("cluster: adopting Pekko hash for local node", "new", newHash, "old", oldHash)
		cm.LocalHashStr = newHash
		// Replace the old hash key in the VectorClock Version entries.
		for _, ver := range cm.State.Version.GetVersions() {
			if int(ver.GetHashIndex()) < len(cm.State.AllHashes) &&
				cm.State.AllHashes[ver.GetHashIndex()] == oldHash {
				// AllHashes[i] is already newHash; nothing else to change.
				break
			}
		}
		break
	}
}

// adoptHashFromGossipLocked looks for the local node's address in an
// incoming gossip message.  If found, and the corresponding AllHashes entry
// differs from the current LocalHashStr, it updates LocalHashStr and patches
// the same index in cm.State.AllHashes so future VectorClock increments use
// the Pekko-compatible murmur hash.  Must be called with cm.Mu held (write).
func (cm *ClusterManager) adoptHashFromGossipLocked(incoming *gproto_cluster.Gossip) {
	localHost := cm.LocalAddress.GetAddress().GetHostname()
	localPort := cm.LocalAddress.GetAddress().GetPort()

	// Find our address in the incoming gossip.
	var pekkoHash string
	for i, ua := range incoming.AllAddresses {
		if ua.GetAddress().GetHostname() == localHost && ua.GetAddress().GetPort() == localPort {
			if i < len(incoming.AllHashes) && incoming.AllHashes[i] != "" {
				pekkoHash = incoming.AllHashes[i]
			}
			break
		}
	}
	if pekkoHash == "" || pekkoHash == cm.localHashString() {
		return
	}
	logger.Default().Debug("cluster: adopting Pekko hash for local node", "new", pekkoHash, "old", cm.localHashString())
	oldHash := cm.localHashString()
	cm.LocalHashStr = pekkoHash

	// Update the AllHashes entry for our address in cm.State so that
	// vectorClockToMap and incrementVersionWithLockHeld use the new hash.
	for i, ua := range cm.State.AllAddresses {
		if ua.GetAddress().GetHostname() == localHost && ua.GetAddress().GetPort() == localPort {
			if i < len(cm.State.AllHashes) && cm.State.AllHashes[i] == oldHash {
				cm.State.AllHashes[i] = pekkoHash
			}
			break
		}
	}
}

func NewClusterManager(local *gproto_cluster.UniqueAddress, router func(context.Context, string, any) error) *ClusterManager {
	clLocal := local                   // Already a gproto_cluster.UniqueAddress
	localHash := int32(local.GetUid()) // UID-based hash fallback (used only when leader)
	return &ClusterManager{
		LocalAddress:         local,
		LocalHash:            localHash,
		Router:               router,
		Fd:                   NewPhiAccrualFailureDetector(8.0, 1000),
		WatchFd:              NewWatchFailureDetector(WatchFailureDetectorConfig{}),
		LogInfo:              true,
		AllowWeaklyUpMembers: 7 * time.Second,
		State: &gproto_cluster.Gossip{
			Members: []*gproto_cluster.Member{
				{
					AddressIndex: proto.Int32(0),
					Status:       gproto_cluster.MemberStatus_Joining.Enum(),
					UpNumber:     proto.Int32(0),
				},
			},
			AllAddresses: []*gproto_cluster.UniqueAddress{clLocal},
			// Start with no AllHashes/Version so that the first incoming gossip
			// from the Pekko seed is always ClockBefore → we replace our state
			// with Pekko's canonical state including Pekko's murmur hashes.
			// incrementVersionWithLockHeld becomes a no-op until we adopt a hash
			// from an incoming gossip state via syncLocalHashFromStateLocked.
			AllHashes: []string{},
			Overview:  &gproto_cluster.GossipOverview{},
			Version:   &gproto_cluster.VectorClock{},
		},
	}
}

func (cm *ClusterManager) IsUp() bool {
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()
	for _, m := range cm.State.GetMembers() {
		if m.GetStatus() == gproto_cluster.MemberStatus_Up {
			return true
		}
	}
	return false
}

// IsLocalNodeUp returns true if the local node itself has reached Up or
// WeaklyUp status in the gossip state. WeaklyUp is included because
// multi-DC members may remain WeaklyUp from the majority DC's perspective
// while still being fully functional.
func (cm *ClusterManager) IsLocalNodeUp() bool {
	localHost := cm.LocalAddress.GetAddress().GetHostname()
	localPort := cm.LocalAddress.GetAddress().GetPort()
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()
	for _, m := range cm.State.GetMembers() {
		st := m.GetStatus()
		if st != gproto_cluster.MemberStatus_Up && st != gproto_cluster.MemberStatus_WeaklyUp {
			continue
		}
		ua := cm.State.AllAddresses[m.GetAddressIndex()]
		a := ua.GetAddress()
		if a.GetHostname() == localHost && a.GetPort() == localPort {
			return true
		}
	}
	return false
}

func (cm *ClusterManager) IsWelcomeReceived() bool {
	return cm.WelcomeReceived.Load()
}

func toClusterAddress(a *gproto_cluster.Address) *gproto_cluster.Address {
	if a == nil {
		return nil
	}
	return &gproto_cluster.Address{
		System:   a.System,
		Hostname: a.Hostname,
		Port:     a.Port,
		Protocol: a.Protocol,
	}
}

// JoinCluster initiates the joining protocol to a seed node.
func (cm *ClusterManager) JoinCluster(ctx context.Context, seedHost string, seedPort uint32) error {
	cm.joinAttempted.Store(true)
	system := cm.LocalAddress.GetAddress().GetSystem()
	path := cm.ClusterCorePath(system, seedHost, seedPort)
	if cm.LogInfo {
		logger.Default().Info("Cluster: initiating join to seed node", slog.String("path", path))
	}

	// Send a minimal config so the remote's JoinConfigCompatCheckCluster.check
	// can call getString("<proto>.cluster.downing-provider-class") without
	// throwing AND so the value matches what the remote actually carries.
	// Pekko's JoinConfigCompatCheckCluster checks two things:
	//   - downing-provider-class must EQUAL the remote's value (not just be
	//     non-empty).  Pekko's canonical SBR class is fully qualified as
	//     "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"; the
	//     short form "pekko.cluster.sbr.SplitBrainResolverProvider" is what
	//     this code used to emit, and Pekko reported it as "incompatible".
	//   - sharding.state-store-mode must be PRESENT.  Without this key,
	//     Pekko logs "state-store-mode is missing" on every join.  We
	//     advertise the Pekko default ("ddata"), which is also the cluster
	//     sharding default that scala-server tests use.
	// For Akka 2.6.x, the SBR class is exposed under the akka.* package as
	// "akka.cluster.sbr.SplitBrainResolverProvider"; the short form here is
	// the actual FQCN, so the existing string is correct.
	proto := cm.Proto()
	var sbrFQCN string
	switch proto {
	case "pekko":
		sbrFQCN = "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"
	case "akka":
		sbrFQCN = "akka.cluster.sbr.SplitBrainResolverProvider"
	default:
		// Unknown protocol — fall back to the short form.  Better to send
		// something than to crash the remote's JoinConfigCompatCheckCluster.
		sbrFQCN = proto + ".cluster.sbr.SplitBrainResolverProvider"
	}
	minConfig := fmt.Sprintf(
		"%s.cluster.downing-provider-class = \"%s\"\n"+
			"%s.cluster.sharding.state-store-mode = \"ddata\"",
		proto, sbrFQCN, proto)
	initJoin := &gproto_cluster.InitJoin{CurrentConfig: &minConfig}

	// Send InitJoin immediately; also start a retry loop that re-sends until
	// the Welcome message is received.  The first attempt may be lost if the
	// Artery handshake is still in progress.
	_ = cm.Router(ctx, path, initJoin)

	go func() {
		retryInterval := cm.RetryUnsuccessfulJoinAfter
		if retryInterval <= 0 {
			retryInterval = 10 * time.Second
		}
		ticker := time.NewTicker(retryInterval)
		defer ticker.Stop()

		// Shutdown timeout: abort joining after configured duration.
		var deadline <-chan time.Time
		if cm.ShutdownAfterUnsuccessfulJoinSeedNodes > 0 {
			timer := time.NewTimer(cm.ShutdownAfterUnsuccessfulJoinSeedNodes)
			defer timer.Stop()
			deadline = timer.C
		}

		// Seed-node timeout: if no response from the seed within this
		// duration, log a warning. This corresponds to
		// pekko.cluster.seed-node-timeout (default 5s).
		seedTimeout := cm.SeedNodeTimeout
		if seedTimeout <= 0 {
			seedTimeout = 5 * time.Second
		}
		var seedDeadline <-chan time.Time
		seedTimer := time.NewTimer(seedTimeout)
		defer seedTimer.Stop()
		seedDeadline = seedTimer.C

		for {
			select {
			case <-ctx.Done():
				return
			case <-deadline:
				if !cm.WelcomeReceived.Load() {
					logger.Default().Error("Cluster: join seed nodes timed out, shutting down", slog.Duration("after", cm.ShutdownAfterUnsuccessfulJoinSeedNodes))
					if cm.ShutdownCallback != nil {
						cm.ShutdownCallback()
					}
					return
				}
				return
			case <-seedDeadline:
				seedDeadline = nil // fire only once
				if !cm.WelcomeReceived.Load() {
					// Informational only: the retry loop above continues
					// to resend InitJoin every retryInterval until either
					// Welcome arrives, the shutdown deadline elapses
					// (logged at ERROR), or context is canceled.  This
					// fires routinely in tests that join a not-yet-running
					// seed, so it must not be at WARN level.
					logger.Default().Info("Cluster: seed node did not respond within seed-node-timeout (will keep retrying)", slog.String("host", seedHost), slog.Int("port", int(seedPort)), slog.Duration("timeout", seedTimeout))
				}
			case <-ticker.C:
				if cm.WelcomeReceived.Load() {
					return
				}
				if cm.LogInfo {
					logger.Default().Info("Cluster: retrying InitJoin", slog.String("path", path))
				}
				_ = cm.Router(ctx, path, initJoin)
			}
		}
	}()

	return nil
}

// SetLocalDataCenter configures the data-center label for this node.
// Must be called before JoinCluster. It also upserts the "dc-<dc>" role into
// the local member's initial gossip state so that OldestNodeInDC works even
// before a Welcome is received.
func (cm *ClusterManager) SetLocalDataCenter(dc string) {
	if dc == "" {
		dc = "default"
	}
	cm.LocalDataCenter = dc

	// Update the initial member entry (index 0 = local) with the DC role.
	dcRole := "dc-" + dc
	cm.Mu.Lock()
	roleIdxs := cm.upsertRolesLocked([]string{dcRole})
	if len(cm.State.Members) > 0 {
		cm.State.Members[0].RolesIndexes = roleIdxs
	}
	cm.Mu.Unlock()
}

// SetLocalRoles records application-defined roles (e.g. "metrics-exporter") that
// are advertised to other cluster members alongside the automatic "dc-<dc>" role.
// Must be called before JoinCluster.
func (cm *ClusterManager) SetLocalRoles(roles []string) {
	cm.localRoles = roles
}

// localDCRole returns the "dc-<dc>" role string for this node.
func (cm *ClusterManager) localDCRole() string {
	dc := cm.LocalDataCenter
	if dc == "" {
		dc = "default"
	}
	return "dc-" + dc
}

// upsertRolesLocked ensures all given role strings are present in AllRoles and
// returns their indexes. Must be called with cm.Mu held (write).
func (cm *ClusterManager) upsertRolesLocked(roles []string) []int32 {
	var idxs []int32
	for _, role := range roles {
		found := false
		for i, r := range cm.State.AllRoles {
			if r == role {
				idxs = append(idxs, int32(i))
				found = true
				break
			}
		}
		if !found {
			idx := int32(len(cm.State.AllRoles))
			cm.State.AllRoles = append(cm.State.AllRoles, role)
			idxs = append(idxs, idx)
		}
	}
	return idxs
}

// SetLocalAppVersion configures the application version for this node.
// Must be called before JoinCluster.  The version is advertised in the Join
// message and stored in the gossip state for other members to inspect.
func (cm *ClusterManager) SetLocalAppVersion(v AppVersion) {
	cm.LocalAppVersion = v

	// Update the initial member entry (index 0 = local) with the version.
	if !v.IsZero() {
		cm.Mu.Lock()
		verIdx := cm.upsertAppVersionsLocked([]string{v.String()})
		if len(cm.State.Members) > 0 {
			cm.State.Members[0].AppVersionIndex = proto.Int32(verIdx)
		}
		cm.Mu.Unlock()
	}
}

// upsertAppVersionsLocked ensures the given version string is present in
// AllAppVersions and returns its index.  Must be called with cm.Mu held (write).
func (cm *ClusterManager) upsertAppVersionsLocked(versions []string) int32 {
	ver := versions[0]
	for i, v := range cm.State.AllAppVersions {
		if v == ver {
			return int32(i)
		}
	}
	idx := int32(len(cm.State.AllAppVersions))
	cm.State.AllAppVersions = append(cm.State.AllAppVersions, ver)
	return idx
}

// AppVersionForMember returns the AppVersion for a member at the given address
// index, or zero AppVersion if not set.
func (cm *ClusterManager) AppVersionForMember(m *gproto_cluster.Member) AppVersion {
	idx := m.GetAppVersionIndex()
	if idx >= 0 && int(idx) < len(cm.State.AllAppVersions) {
		return ParseAppVersion(cm.State.AllAppVersions[idx])
	}
	return AppVersion{}
}

// ProceedJoin sends the actual Join message after receiving InitJoinAck
func (cm *ClusterManager) ProceedJoin(ctx context.Context, actorPath string) error {
	roles := append([]string{cm.localDCRole()}, cm.localRoles...)
	join := &gproto_cluster.Join{
		Node:  cm.LocalAddress,
		Roles: roles,
	}
	if !cm.LocalAppVersion.IsZero() {
		v := cm.LocalAppVersion.String()
		join.AppVersion = &v
	}
	if cm.LogInfo {
		logger.Default().Info("Cluster: sending Join", slog.String("path", actorPath))
	}
	return cm.Router(ctx, actorPath, join)
}

// LeaveCluster sends a Leave message and immediately updates the local gossip
// state to Leaving so that cluster event subscribers observe MemberLeft even
// when the remote leader is unreachable (e.g. during an SBR DownSelf decision).
func (cm *ClusterManager) LeaveCluster() error {
	// Flag that the user has initiated graceful leave.  This disables the
	// reincarnation repair guard that resets stale self-UID Down/Removed
	// slots inherited from prior incarnations — once we are actually
	// leaving, those statuses are legitimate and must propagate to
	// WaitForSelfRemoved.
	cm.localLeaveInitiated.Store(true)
	// Update own gossip entry to Leaving before broadcasting so that local
	// subscribers see MemberLeft regardless of whether remote nodes are alive.
	cm.Mu.Lock()
	localHost := cm.LocalAddress.GetAddress().GetHostname()
	localPort := cm.LocalAddress.GetAddress().GetPort()
	for _, m := range cm.State.Members {
		a := cm.State.AllAddresses[m.GetAddressIndex()].GetAddress()
		if a.GetHostname() == localHost && a.GetPort() == localPort {
			if m.GetStatus() != gproto_cluster.MemberStatus_Leaving &&
				m.GetStatus() != gproto_cluster.MemberStatus_Exiting &&
				m.GetStatus() != gproto_cluster.MemberStatus_Removed {
				m.Status = gproto_cluster.MemberStatus_Leaving.Enum()
				cm.incrementVersionWithLockHeld()
			}
			break
		}
	}
	state := cm.State
	cm.Mu.Unlock()

	// Publish MemberLeft event for self (the gossip entry is now Leaving).
	cm.publishEvent(MemberLeft{Member: memberAddressFromUA(cm.LocalAddress)})

	leave := toClusterAddress(cm.LocalAddress.Address)

	// Broadcast Leave to all currently Up/WeaklyUp members.
	var lastErr error
	for _, m := range state.GetMembers() {
		if m.GetStatus() == gproto_cluster.MemberStatus_Up || m.GetStatus() == gproto_cluster.MemberStatus_WeaklyUp {
			addr := state.GetAllAddresses()[m.GetAddressIndex()]
			a := addr.GetAddress()
			// Skip self — no point sending Leave to ourselves and it adds noise.
			if a.GetHostname() == localHost && a.GetPort() == localPort {
				continue
			}
			path := fmt.Sprintf("%s://%s@%s:%d/system/cluster/core/daemon",
				cm.Proto(),
				a.GetSystem(),
				a.GetHostname(),
				a.GetPort())
			if err := cm.Router(context.Background(), path, leave); err != nil {
				lastErr = err
			}
		}
	}
	return lastErr
}

// DownMember marks the member with the given address as Down in the local
// gossip state. The leader's performLeaderActions loop will subsequently
// transition Down members to Removed and gossip the change to the cluster.
func (cm *ClusterManager) DownMember(addr MemberAddress) {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()
	for _, m := range cm.State.Members {
		ua := cm.State.AllAddresses[m.GetAddressIndex()]
		a := ua.GetAddress()
		if a.GetHostname() == addr.Host && a.GetPort() == addr.Port {
			if m.GetStatus() == gproto_cluster.MemberStatus_Down ||
				m.GetStatus() == gproto_cluster.MemberStatus_Removed {
				return // already down/removed
			}
			m.Status = gproto_cluster.MemberStatus_Down.Enum()
			cm.incrementVersionWithLockHeld()
			if cm.LogInfo {
				logger.Default().Info("SBR: marked member as Down", slog.String("host", addr.Host), slog.Int("port", int(addr.Port)))
			}
			return
		}
	}
	logger.Default().Warn("SBR: DownMember: address not found in gossip", slog.String("host", addr.Host), slog.Int("port", int(addr.Port)))
}

// IsRemotePeerDeparting reports whether a remote peer at (host, port) is
// in any non-membership-active state in the current gossip view — i.e.
// Leaving, Exiting, Down, Removed, or not present in members at all.
// Transport-level callers use this to distinguish "peer is in a normal
// teardown / departure window" (expected close) from "peer is Up and
// should be reachable" (genuine network failure) when classifying write
// errors and Quarantined notifications.
//
// Returns true when:
//   - the peer is not present in cm.State.Members (already pruned), OR
//   - the peer's status is Leaving, Exiting, Down, or Removed.
//
// Returns false when the peer is Up, WeaklyUp, or Joining (i.e., expected
// to be reachable).
func (cm *ClusterManager) IsRemotePeerDeparting(host string, port uint32) bool {
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()
	for _, m := range cm.State.GetMembers() {
		idx := int(m.GetAddressIndex())
		if idx < 0 || idx >= len(cm.State.AllAddresses) {
			continue
		}
		a := cm.State.AllAddresses[idx].GetAddress()
		if a.GetHostname() == host && a.GetPort() == port {
			switch m.GetStatus() {
			case gproto_cluster.MemberStatus_Up,
				gproto_cluster.MemberStatus_WeaklyUp,
				gproto_cluster.MemberStatus_Joining:
				return false
			default:
				return true
			}
		}
	}
	// Not in members at all — either never joined, or already dropped
	// post-Removal.  Treat as departing.
	return true
}

// WaitForSelfRemoved polls the gossip state every 200 ms until this node's
// own membership entry is absent or in Removed status, or the context expires.
// It is called by the coordinated-shutdown cluster-leave phase to block until
// the cluster leader has driven the transition all the way to Removed.
//
// Fast paths:
//   - If this node never received a Welcome (no peer ever knew about it),
//     there is no leader anywhere driving the transition, so waiting cannot
//     succeed; return immediately so unit-test teardown isn't penalised with
//     the full phase timeout.
//   - If self is already absent or Removed, return immediately.
func (cm *ClusterManager) WaitForSelfRemoved(ctx context.Context) error {
	localHost := cm.LocalAddress.GetAddress().GetHostname()
	localPort := cm.LocalAddress.GetAddress().GetPort()

	if cm.isSelfRemovedOrGone(localHost, localPort) {
		return nil
	}
	if !cm.WelcomeReceived.Load() {
		// Never joined a cluster — there is no remote leader to drive
		// the Leaving → Exiting → Removed transitions and no peer to
		// gossip a Removed state back to us.  Cluster-leave still ran
		// (LeaveCluster updates self to Leaving for local subscribers);
		// we just don't have a counterparty to wait on.
		return nil
	}

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if cm.isSelfRemovedOrGone(localHost, localPort) {
				return nil
			}
		}
	}
}

// isSelfRemovedOrGone returns true when this node no longer appears in the
// gossip members list, or when its status is Removed.
func (cm *ClusterManager) isSelfRemovedOrGone(host string, port uint32) bool {
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()
	for _, m := range cm.State.GetMembers() {
		a := cm.State.GetAllAddresses()[m.GetAddressIndex()].GetAddress()
		if a.GetHostname() == host && a.GetPort() == port {
			return m.GetStatus() == gproto_cluster.MemberStatus_Removed
		}
	}
	// Member not present in gossip state — treat as fully removed.
	return true
}

// HandleIncomingClusterMessage dispatches cluster-level messages.
// Pekko's ClusterMessageSerializer uses short manifests: "IJ", "IJA", "J", "W", "GE", "GS", "HB", "HBR", "L".
// remoteAddr is the UniqueAddress of the node that sent this message (from the association handshake).
func (cm *ClusterManager) HandleIncomingClusterMessage(ctx context.Context, payload []byte, manifest string, remoteAddr *gproto_cluster.UniqueAddress, senderPath string) error {
	logger.Default().Debug("cluster: HandleIncomingClusterMessage", "manifest", manifest)
	switch manifest {
	case "IJ": // InitJoin — we are the seed; reply with InitJoinAck
		if remoteAddr == nil {
			logger.Default().Debug("cluster: InitJoin: no remote address (handshake pending), ignoring")
			return nil
		}
		logger.Default().Info("cluster: received InitJoin", "from", remoteAddr.GetAddress(), "sender", senderPath)

		// Configuration compatibility check (pekko.cluster.configuration-compatibility-check.enforce-on-join).
		// When enforced, validate the joining node's config against our own.
		if cm.EnforceConfigCompatOnJoin {
			ij := &gproto_cluster.InitJoin{}
			if err := proto.Unmarshal(payload, ij); err == nil && ij.GetCurrentConfig() != "" {
				if !cm.CheckConfigCompat(ij.GetCurrentConfig()) {
					raddr := remoteAddr.GetAddress()
					logger.Default().Warn("Cluster: rejecting InitJoin — configuration incompatible", slog.String("host", raddr.GetHostname()), slog.Int("port", int(raddr.GetPort())))
					replyPath := senderPath
					if replyPath == "" {
						replyPath = cm.ClusterCorePath(raddr.GetSystem(), raddr.GetHostname(), raddr.GetPort())
					}
					incompatConfig := proto.String(`pekko.cluster.downing-provider-class = ""`)
					nack := &gproto_cluster.InitJoinAck{
						Address: toClusterAddress(cm.LocalAddress.Address),
						ConfigCheck: &gproto_cluster.ConfigCheck{
							Type:          gproto_cluster.ConfigCheck_IncompatibleConfig.Enum(),
							ClusterConfig: incompatConfig,
						},
					}
					return cm.Router(ctx, replyPath, nack)
				}
			}
		}

		// Include a minimal config so Pekko's JoinConfigCompatCheckCluster
		// can parse it without crashing AND so the values match what the
		// joining node carries.  This is the reverse direction of the
		// InitJoin builder fix above: when gekka is the seed and a Pekko
		// node is joining, the joining node compares OUR config against
		// its own.  If we send an empty downing-provider-class here, the
		// joining Pekko side logs "Cluster validated this node config, but
		// sent back incompatible settings" — see JoinSeedNodeProcess'
		// validateConfig.  Mirror the InitJoin builder: emit the
		// fully-qualified SBR class (Pekko uses org.apache.pekko.*, Akka
		// uses akka.*) and advertise the state-store-mode key.
		ackProto := cm.Proto()
		var ackSbrFQCN string
		switch ackProto {
		case "pekko":
			ackSbrFQCN = "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"
		case "akka":
			ackSbrFQCN = "akka.cluster.sbr.SplitBrainResolverProvider"
		default:
			ackSbrFQCN = ackProto + ".cluster.sbr.SplitBrainResolverProvider"
		}
		minConfig := proto.String(fmt.Sprintf(
			"%s.cluster.downing-provider-class = \"%s\"\n"+
				"%s.cluster.sharding.state-store-mode = \"ddata\"",
			ackProto, ackSbrFQCN, ackProto))
		ack := &gproto_cluster.InitJoinAck{
			Address: toClusterAddress(cm.LocalAddress.Address),
			ConfigCheck: &gproto_cluster.ConfigCheck{
				Type:          gproto_cluster.ConfigCheck_CompatibleConfig.Enum(),
				ClusterConfig: minConfig,
			},
		}
		// Reply to the SENDER of the InitJoin, not a hardcoded path.
		// Pekko's InitJoin is sent by JoinSeedNodeProcess (a child actor
		// of ClusterDaemon), not by ClusterCoreDaemon. Sending the reply
		// to /system/cluster/core/daemon causes Pekko to log "unhandled"
		// and the InitJoinAck goes to dead letters, preventing the join.
		raddr := remoteAddr.GetAddress()
		replyPath := senderPath
		if replyPath == "" {
			replyPath = cm.ClusterCorePath(raddr.GetSystem(), raddr.GetHostname(), raddr.GetPort())
		}
		return cm.Router(ctx, replyPath, ack)
	case "IJA": // InitJoinAck — received Ack, now send Join
		ack := &gproto_cluster.InitJoinAck{}
		if err := proto.Unmarshal(payload, ack); err != nil {
			return err
		}
		addr := ack.GetAddress()
		path := cm.ClusterCorePath(addr.GetSystem(), addr.GetHostname(), addr.GetPort())
		return cm.ProceedJoin(ctx, path)
	case "J": // Join
		return cm.handleJoin(payload, manifest)
	case "W": // Welcome
		return cm.handleWelcome(payload, manifest)
	case "GE": // GossipEnvelope
		return cm.handleGossipEnvelope(payload, manifest)
	case "HB": // Heartbeat
		return cm.handleHeartbeat(payload, manifest, remoteAddr)
	case "HBR": // HeartbeatRsp
		return cm.handleHeartbeatRsp(payload, manifest)
	case "GS": // GossipStatus
		return cm.handleGossipStatus(payload, manifest)
	case "L": // Leave — a member is requesting graceful departure
		leave := &gproto_cluster.Address{}
		if err := proto.Unmarshal(payload, leave); err != nil {
			return err
		}
		logger.Default().Info("cluster: received Leave", "host", leave.GetHostname(), "port", leave.GetPort())
		cm.Mu.Lock()
		cm.markMemberLeavingLocked(leave)
		cm.Mu.Unlock()
		return nil
	default:
		logger.Default().Debug("cluster: unknown manifest", "manifest", manifest)
		return nil
	}
}

func (cm *ClusterManager) handleHeartbeat(payload []byte, manifest string, remoteAddr *gproto_cluster.UniqueAddress) error {
	hb := &gproto_cluster.Heartbeat{}
	if err := proto.Unmarshal(payload, hb); err != nil {
		return err
	}

	// Update Failure Detector
	if remoteAddr != nil {
		addr := remoteAddr.GetAddress()
		uid64 := uint64(remoteAddr.GetUid()) | (uint64(remoteAddr.GetUid2()) << 32)
		key := fmt.Sprintf("%s:%d-%d", addr.GetHostname(), addr.GetPort(), uid64)
		// Pass the per-target expected-response-after into the FD so the
		// per-node detector seeds its first-heartbeat estimate from the
		// configured value (cross-DC vs intra-DC).
		cm.Fd.HeartbeatWithEstimate(key, cm.EffectiveExpectedResponseAfter(addr))
		if cm.WatchFd != nil {
			cm.WatchFd.Heartbeat(key)
		}
	}

	// When heartbeats are muted (StopHeartbeat), suppress the response so the
	// remote failure detector can detect this node as unreachable.
	if cm.heartbeatMuted.Load() {
		return nil
	}

	// Reply with HeartBeatResponse
	rsp := &gproto_cluster.HeartBeatResponse{
		From:         cm.LocalAddress,
		SequenceNr:   hb.SequenceNr,
		CreationTime: hb.CreationTime,
	}

	addr := hb.GetFrom()
	path := cm.HeartbeatSenderPath(addr.GetSystem(), addr.GetHostname(), addr.GetPort())
	return cm.Router(context.Background(), path, rsp)
}

func (cm *ClusterManager) handleHeartbeatRsp(payload []byte, manifest string) error {
	rsp := &gproto_cluster.HeartBeatResponse{}
	if err := proto.Unmarshal(payload, rsp); err != nil {
		return err
	}

	// Update Failure Detector
	from := rsp.GetFrom()
	addr := from.GetAddress()
	uid64 := uint64(from.GetUid()) | (uint64(from.GetUid2()) << 32)
	key := fmt.Sprintf("%s:%d-%d", addr.GetHostname(), addr.GetPort(), uid64)
	cm.Fd.HeartbeatWithEstimate(key, cm.EffectiveExpectedResponseAfter(addr))
	if cm.WatchFd != nil {
		cm.WatchFd.Heartbeat(key)
	}
	return nil
}

func (cm *ClusterManager) handleJoin(payload []byte, manifest string) error {
	join := &gproto_cluster.Join{}
	if err := proto.Unmarshal(payload, join); err != nil {
		return err
	}
	joiningNode := join.GetNode()
	if cm.LogInfo {
		logger.Default().Info("Cluster: received Join", slog.String("from", fmt.Sprintf("%v", joiningNode)))
	}

	// Add the joining node to our gossip state as Joining, then send Welcome with
	// the updated state.  The gossip loop's performLeaderActions will transition
	// Joining → Up on the next tick.
	cm.Mu.Lock()
	cm.addMemberToGossipLocked(joiningNode, join.GetRoles(), join.GetAppVersion())
	welcomeGossip := proto.Clone(cm.State).(*gproto_cluster.Gossip)
	cm.connectToNewMembers(cm.State)
	cm.Mu.Unlock()

	welcome := &gproto_cluster.Welcome{
		From:   cm.LocalAddress,
		Gossip: welcomeGossip,
	}

	addr := joiningNode.GetAddress()
	system := cm.LocalAddress.GetAddress().GetSystem()
	path := cm.ClusterCorePath(system, addr.GetHostname(), addr.GetPort())

	if cm.LogInfo {
		logger.Default().Info("Cluster: sending Welcome", slog.String("path", path))
	}
	return cm.Router(context.Background(), path, welcome)
}

// addMemberToGossipLocked adds a joining node to the gossip Members list (as Joining).
// roles are stored into AllRoles / RolesIndexes so that OldestNodeInDC works.
// appVersion is the joining node's version string (e.g. "1.2.3"); empty means unset.
// Must be called with cm.Mu held.
func (cm *ClusterManager) addMemberToGossipLocked(joiningAddr *gproto_cluster.UniqueAddress, roles []string, appVersion string) {
	roleIdxs := cm.upsertRolesLocked(roles)

	var appVerIdx *int32
	if appVersion != "" {
		idx := cm.upsertAppVersionsLocked([]string{appVersion})
		appVerIdx = proto.Int32(idx)
	}

	// Look for an existing AllAddresses entry by host:port.
	for i, addr := range cm.State.AllAddresses {
		if addr.GetAddress().GetHostname() == joiningAddr.GetAddress().GetHostname() &&
			addr.GetAddress().GetPort() == joiningAddr.GetAddress().GetPort() {
			// Already known — add Member if not present.
			for _, m := range cm.State.Members {
				if m.GetAddressIndex() == int32(i) {
					// Already a member; still ensure the local hash is present
					// and bump the version so that the Welcome we're about to
					// send is guaranteed to be ClockAfter the joiner's empty
					// initial state (handles races where the joiner's address
					// was inserted into our state before the Join arrived).
					cm.ensureLocalHashInAllHashesLocked()
					cm.incrementVersionWithLockHeld()
					return
				}
			}
			newMem := &gproto_cluster.Member{
				AddressIndex: proto.Int32(int32(i)),
				Status:       gproto_cluster.MemberStatus_Joining.Enum(),
				// UpNumber stays 0 until promotion to Up — Pekko's
				// MembershipState.leaderOrdering treats non-Up members as having
				// UpNumber=MaxValue (sentinel). Assigning a real value here while
				// the member is still Joining causes DetermineLeader to pick the
				// joiner over the existing local node whose UpNumber is still 0
				// (= sentinel, sorts last), breaking self-promotion in the
				// Go-Seed-with-self-join case. Live diag and
				// TestGoSeed_FailureRecovery both reproduce this storage flake
				// when Scala's Join arrives BEFORE gekka's first leader-action
				// tick: gekka adds Scala with UpNumber=2 → Scala wins the
				// leader sort → gekka can never promote itself → Joining-stuck.
				UpNumber:     proto.Int32(0),
				RolesIndexes: roleIdxs,
			}
			if appVerIdx != nil {
				newMem.AppVersionIndex = appVerIdx
			}
			cm.State.Members = append(cm.State.Members, newMem)
			cm.ensureLocalHashInAllHashesLocked()
			cm.incrementVersionWithLockHeld()
			return
		}
	}

	// New address — append to AllAddresses and create a Member.
	addrIdx := int32(len(cm.State.AllAddresses))
	cm.State.AllAddresses = append(cm.State.AllAddresses, joiningAddr)
	cm.State.AllHashes = append(cm.State.AllHashes, fmt.Sprintf("%d", joiningAddr.GetUid()))
	newMem := &gproto_cluster.Member{
		AddressIndex: proto.Int32(addrIdx),
		Status:       gproto_cluster.MemberStatus_Joining.Enum(),
		// UpNumber stays 0 until promotion to Up — see commentary on the
		// sibling code path above.
		UpNumber:     proto.Int32(0),
		RolesIndexes: roleIdxs,
	}
	if appVerIdx != nil {
		newMem.AppVersionIndex = appVerIdx
	}
	cm.State.Members = append(cm.State.Members, newMem)
	cm.ensureLocalHashInAllHashesLocked()
	cm.incrementVersionWithLockHeld()
}

// mergeSeenLocked unions the incoming gossip's Overview.Seen into the local state,
// remapping indices from the incoming AllAddresses space to the local AllAddresses
// space. Without remapping, the Seen set becomes inconsistent after any member
// join/leave because the same AllAddresses index refers to different nodes in the
// sender's vs receiver's gossip. This inconsistency prevents leader convergence
// and blocks Joining → Up promotion in multi-iteration churn scenarios.
// Must be called with cm.Mu held (write).
func (cm *ClusterManager) mergeSeenLocked(incoming *gproto_cluster.Gossip) {
	if incoming.GetOverview() == nil {
		return
	}
	if cm.State.Overview == nil {
		cm.State.Overview = &gproto_cluster.GossipOverview{}
	}

	// Build a lookup from UniqueAddress → local index for fast remapping.
	type addrKey struct {
		host string
		port uint32
		uid  uint64
	}
	localIndex := make(map[addrKey]int32, len(cm.State.AllAddresses))
	for i, ua := range cm.State.AllAddresses {
		a := ua.GetAddress()
		uid := uint64(ua.GetUid()) | (uint64(ua.GetUid2()) << 32)
		localIndex[addrKey{a.GetHostname(), a.GetPort(), uid}] = int32(i)
	}

	existing := make(map[int32]bool, len(cm.State.Overview.Seen))
	for _, s := range cm.State.Overview.Seen {
		existing[s] = true
	}

	for _, incomingIdx := range incoming.GetOverview().GetSeen() {
		if int(incomingIdx) >= len(incoming.AllAddresses) {
			continue // stale index
		}
		// Resolve the UniqueAddress the incoming index refers to.
		iua := incoming.AllAddresses[incomingIdx]
		ia := iua.GetAddress()
		iuid := uint64(iua.GetUid()) | (uint64(iua.GetUid2()) << 32)

		// Remap to the local index space.
		if localIdx, ok := localIndex[addrKey{ia.GetHostname(), ia.GetPort(), iuid}]; ok {
			if !existing[localIdx] {
				cm.State.Overview.Seen = append(cm.State.Overview.Seen, localIdx)
				existing[localIdx] = true
			}
		}
	}
}

// markLocalSeenLocked adds the local node's AllAddresses index to Overview.Seen,
// signalling to Pekko's leader that this node has "seen" the current gossip.
// Pekko's convergence check (members.forall(m => Exiting || seen(m.address)))
// requires ALL members (not just Up ones) to be in Seen before leader actions
// (Joining→Up promotion) can run. Must be called with cm.Mu held (write).
func (cm *ClusterManager) markLocalSeenLocked() {
	localHost := cm.LocalAddress.GetAddress().GetHostname()
	localPort := cm.LocalAddress.GetAddress().GetPort()
	localIdx := int32(-1)
	for i, ua := range cm.State.AllAddresses {
		if ua.GetAddress().GetHostname() == localHost && ua.GetAddress().GetPort() == localPort {
			localIdx = int32(i)
			break
		}
	}
	if localIdx < 0 {
		return
	}
	if cm.State.Overview == nil {
		cm.State.Overview = &gproto_cluster.GossipOverview{}
	}
	for _, s := range cm.State.Overview.Seen {
		if s == localIdx {
			return // already marked
		}
	}
	cm.State.Overview.Seen = append(cm.State.Overview.Seen, localIdx)
}

// buildSeenDigest creates a compact bitfield encoding of Overview.Seen.
// Bit i is set if AllAddresses[i] is in the Seen set. This matches
// Pekko's SeenDigest wire format used in GossipStatus messages.
// Must be called with cm.Mu held (read).
func (cm *ClusterManager) buildSeenDigest() []byte {
	n := len(cm.State.AllAddresses)
	if n == 0 {
		return nil
	}
	digest := make([]byte, (n+7)/8)
	if cm.State.Overview != nil {
		for _, idx := range cm.State.Overview.Seen {
			if int(idx) < n {
				digest[idx/8] |= 1 << (idx % 8)
			}
		}
	}
	return digest
}

// seenDigestHasNew returns true if localDigest has any bits set that
// remoteDigest does not. This means the local node has Seen entries
// that the remote node is missing.
func seenDigestHasNew(local, remote []byte) bool {
	for i := 0; i < len(local); i++ {
		r := byte(0)
		if i < len(remote) {
			r = remote[i]
		}
		if local[i] & ^r != 0 {
			return true
		}
	}
	return false
}

// ensureLocalHashInAllHashesLocked adds the local node's hash to AllHashes if
// it is not already present. This is needed when AllHashes starts empty (e.g.
// after adopting a Pekko seed's canonical gossip state) and this node is acting
// as a seed for another Go node — incrementVersionWithLockHeld silently drops
// increments for hashes that are not listed in AllHashes.
// Must be called with cm.Mu held (write).
func (cm *ClusterManager) ensureLocalHashInAllHashesLocked() {
	localHashStr := cm.localHashString()
	for _, h := range cm.State.AllHashes {
		if h == localHashStr {
			return
		}
	}
	cm.State.AllHashes = append(cm.State.AllHashes, localHashStr)
}

// markMemberLeavingLocked transitions an Up/WeaklyUp member to Leaving status.
// Must be called with cm.Mu held.
func (cm *ClusterManager) markMemberLeavingLocked(leaveAddr *gproto_cluster.Address) {
	for _, m := range cm.State.Members {
		addr := cm.State.AllAddresses[m.GetAddressIndex()]
		if addr.GetAddress().GetHostname() == leaveAddr.GetHostname() &&
			addr.GetAddress().GetPort() == leaveAddr.GetPort() {
			if m.GetStatus() == gproto_cluster.MemberStatus_Up || m.GetStatus() == gproto_cluster.MemberStatus_WeaklyUp {
				m.Status = gproto_cluster.MemberStatus_Leaving.Enum()
				cm.incrementVersionWithLockHeld()
				if cm.LogInfo {
					logger.Default().Info("Cluster: marked member as Leaving", slog.String("host", leaveAddr.GetHostname()), slog.Int("port", int(leaveAddr.GetPort())))
				}
				// publishEvent is safe while holding cm.Mu — it only acquires cm.SubMu.
				cm.publishEvent(MemberLeft{Member: MemberAddress{
					Protocol: addr.GetAddress().GetProtocol(),
					System:   addr.GetAddress().GetSystem(),
					Host:     leaveAddr.GetHostname(),
					Port:     leaveAddr.GetPort(),
				}})
			}
			return
		}
	}
}

func (cm *ClusterManager) handleWelcome(payload []byte, manifest string) error {
	// Pekko GZIP-compresses the Welcome payload (compress(welcomeToProto(...)))
	decompressed, err := gzipDecompress(payload)
	if err != nil {
		return fmt.Errorf("failed to decompress Welcome payload: %w", err)
	}
	welcome := &gproto_cluster.Welcome{}
	if err := proto.Unmarshal(decompressed, welcome); err != nil {
		return err
	}
	if cm.LogInfo {
		logger.Default().Info("Cluster: welcomed by peer", slog.String("from", fmt.Sprintf("%v", welcome.GetFrom())))
	}

	err = cm.processIncomingGossip(welcome.Gossip, welcome.From)
	if err != nil {
		return err
	}

	// Signal that this node has successfully joined a cluster
	cm.WelcomeReceived.Store(true)
	return nil
}

func (cm *ClusterManager) handleGossipEnvelope(payload []byte, manifest string) error {
	if cm.Metrics != nil {
		cm.Metrics.IncrementGossipReceived()
	}
	envelope := &gproto_cluster.GossipEnvelope{}
	if err := proto.Unmarshal(payload, envelope); err != nil {
		return err
	}
	if cm.LogInfoVerbose {
		logger.Default().Debug("Cluster: received GossipEnvelope", slog.String("from", fmt.Sprintf("%v", envelope.GetFrom())))
	}

	// Pekko GZIP-compresses GossipEnvelope.serializedGossip
	decompressed, err := gzipDecompress(envelope.SerializedGossip)
	if err != nil {
		return fmt.Errorf("failed to decompress GossipEnvelope.serializedGossip: %w", err)
	}
	gossip := &gproto_cluster.Gossip{}
	if err := proto.Unmarshal(decompressed, gossip); err != nil {
		return fmt.Errorf("failed to unmarshal gossip inside envelope: %w", err)
	}

	return cm.processIncomingGossip(gossip, envelope.From)
}

func (cm *ClusterManager) handleGossipStatus(payload []byte, manifest string) error {
	status := &gproto_cluster.GossipStatus{}
	if err := proto.Unmarshal(payload, status); err != nil {
		return err
	}

	system := cm.LocalAddress.GetAddress().GetSystem()
	addr := status.GetFrom().GetAddress()
	path := cm.ClusterCorePath(system, addr.GetHostname(), addr.GetPort())

	cm.Mu.RLock()
	m1 := cm.vectorClockToMap(cm.State.Version, cm.State.AllHashes)
	m2 := cm.vectorClockToMap(status.Version, status.AllHashes)
	ordering := cm.compareResolvedClocks(m1, m2)

	// Update Failure Detector for the sender
	from := status.GetFrom()
	faddr := from.GetAddress()
	uid64 := uint64(from.GetUid()) | (uint64(from.GetUid2()) << 32)
	key := fmt.Sprintf("%s:%d-%d", faddr.GetHostname(), faddr.GetPort(), uid64)
	cm.Fd.Heartbeat(key)

	logger.Default().Debug("cluster: handleGossipStatus", "from", status.GetFrom(), "ordering", ordering)

	var statePayload []byte
	var localVersion *gproto_cluster.VectorClock
	var localHashes []string
	var localDigest []byte
	if ordering == ClockAfter || ordering == ClockConcurrent {
		statePayload, _ = proto.Marshal(cm.State)
	}
	if ordering == ClockSame {
		localDigest = cm.buildSeenDigest()
	}
	if ordering == ClockBefore {
		localVersion = cm.State.Version
		localHashes = cm.State.AllHashes
		localDigest = cm.buildSeenDigest()
	}
	cm.Mu.RUnlock()

	if ordering == ClockAfter || ordering == ClockConcurrent {
		// Our state is newer or concurrent: send our full GossipEnvelope so the
		// partner can merge any state it is missing.
		// Pekko expects GossipEnvelope.serializedGossip to be GZIP-compressed.
		if statePayload != nil {
			compressedGossip, err := gzipCompress(statePayload)
			if err != nil {
				return fmt.Errorf("failed to compress gossip: %w", err)
			}
			env := &gproto_cluster.GossipEnvelope{
				From:             cm.LocalAddress,
				To:               status.From,
				SerializedGossip: compressedGossip,
			}
			if err := cm.Router(context.Background(), path, env); err != nil {
				return err
			}
		}
	}

	if ordering == ClockSame {
		// Same VectorClock — compare SeenDigests. If we have Seen entries
		// the remote doesn't, send a full GossipEnvelope so it can merge
		// our Seen set and achieve convergence.
		remoteDigest := status.GetSeenDigest()
		if seenDigestHasNew(localDigest, remoteDigest) {
			cm.Mu.RLock()
			samePayload, _ := proto.Marshal(cm.State)
			cm.Mu.RUnlock()
			if samePayload != nil {
				if compressed, err := gzipCompress(samePayload); err == nil {
					env := &gproto_cluster.GossipEnvelope{
						From:             cm.LocalAddress,
						To:               status.From,
						SerializedGossip: compressed,
					}
					_ = cm.Router(context.Background(), path, env)
				}
			}
		}
		return nil
	}

	if ordering == ClockBefore {
		// Their state is strictly newer: send our GossipStatus so the partner
		// knows to reply with its full GossipEnvelope.
		myStatus := &gproto_cluster.GossipStatus{
			From:       cm.LocalAddress,
			AllHashes:  localHashes,
			Version:    localVersion,
			SeenDigest: localDigest,
		}
		return cm.Router(context.Background(), path, myStatus)
	}

	return nil
}

func (cm *ClusterManager) processIncomingGossip(gossip *gproto_cluster.Gossip, remoteAddr *gproto_cluster.UniqueAddress) error {
	cm.Mu.Lock()

	// Gossip TTL check: discard incoming gossip that is stale relative to
	// our last state update (pekko.cluster.gossip-time-to-live).
	ttl := cm.GossipTimeToLive
	if ttl <= 0 {
		ttl = 2 * time.Second
	}
	if !cm.lastGossipUpdate.IsZero() && time.Since(cm.lastGossipUpdate) > ttl {
		// Only discard if the incoming gossip is strictly older than ours.
		m1Chk := cm.vectorClockToMap(cm.State.Version, cm.State.AllHashes)
		m2Chk := cm.vectorClockToMap(gossip.Version, gossip.AllHashes)
		if cm.compareResolvedClocks(m1Chk, m2Chk) == ClockAfter {
			cm.Mu.Unlock()
			if cm.LogInfoVerbose {
				logger.Default().Debug("Cluster: discarding stale gossip (TTL exceeded)", slog.Duration("ttl", ttl))
			}
			return nil
		}
	}

	// Strip stale/zombie Removed entries from incoming gossip BEFORE any
	// diff or merge runs.  See intakeRemovedFromIncomingLocked for the
	// rationale (dashboard self-down Issue 3).  Synthesised events are
	// merged into the regular event slice below so they reach subscribers.
	intakeEvents := cm.intakeRemovedFromIncomingLocked(gossip)

	m1 := cm.vectorClockToMap(cm.State.Version, cm.State.AllHashes)
	m2 := cm.vectorClockToMap(gossip.Version, gossip.AllHashes)
	ordering := cm.compareResolvedClocks(m1, m2)

	var events []ClusterDomainEvent
	if ordering == ClockBefore {
		// Incoming is newer — diff before replacing so we can emit events.
		if cm.LogInfoVerbose {
			logger.Default().Debug("Cluster: received newer Gossip, replacing local state")
		}
		events = diffGossipMembers(cm.State, gossip)
		cm.State = gossip
		// Reincarnation guard: if the incoming gossip carries an entry for
		// our address with a DIFFERENT UID (a previous gekka instance that
		// crashed and was marked Down/Removed by the seed), do NOT let it
		// overwrite our current liveness.  Replace the stale entry with
		// our own current Joining/Up identity.  Without this, the
		// CoordinatedShutdown subscriber sees self transition to Down and
		// kills the freshly-joined node within ~1s of receiving the seed's
		// gossip — exactly the dashboard symptom.
		cm.repairSelfReincarnationLocked()
		// Adopt Pekko's hash for our own address so future VectorClock comparisons
		// use a format compatible with Pekko's murmur-hash scheme.
		cm.syncLocalHashFromStateLocked()
		// Mark this node as having "seen" the gossip so Pekko's leader can achieve
		// convergence (Pekko requires all members to be in Overview.Seen before
		// promoting Joining→Up). Without this, Pekko's convergence check fails
		// and joining nodes are never promoted.
		cm.markLocalSeenLocked()
		cm.connectToNewMembers(gossip)
	} else if ordering == ClockConcurrent {
		// Merge concurrent states: union of members, pairwise-max vector clock.
		if cm.LogInfoVerbose {
			logger.Default().Debug("Cluster: received concurrent Gossip, merging")
		}
		merged := cm.mergeGossipStates(cm.State, gossip)
		events = diffGossipMembers(cm.State, merged)
		cm.State = merged
		cm.repairSelfReincarnationLocked()
		// Adopt Pekko's hash for our own address if the incoming gossip carries it.
		// This must happen before incrementVersionWithLockHeld so the new hash is used.
		cm.adoptHashFromGossipLocked(gossip)
		cm.incrementVersionWithLockHeld()
		cm.mergeSeenLocked(gossip)
		cm.markLocalSeenLocked()
		cm.connectToNewMembers(merged)
	} else if ordering == ClockSame {
		// Same VectorClock version: member statuses are identical, but Overview.Seen
		// and AllAddresses may differ (e.g. a new member was added under the same
		// version number by a concurrent Join on another node). Merge member/address
		// sets and union the Seen sets so convergence can progress.
		merged := cm.mergeGossipStates(cm.State, gossip)
		if len(merged.AllAddresses) > len(cm.State.AllAddresses) || len(merged.Members) > len(cm.State.Members) {
			events = diffGossipMembers(cm.State, merged)
			cm.State = merged
		}
		// Always merge Seen sets into our local state so Pekko's convergence
		// check can see that we've acknowledged all known members.
		cm.mergeSeenLocked(gossip)
		cm.markLocalSeenLocked()
	}

	// Defence-in-depth: belt-and-suspenders prune of any residual Removed
	// entries that may have slipped through mergeGossipStates (e.g.
	// pre-existing Removed entries already in cm.State from before this
	// filter shipped, or entries introduced via a future code path).
	// Mirrors what performLeaderActions does at leader-action time so the
	// invariant "cm.State.Members never carries status=Removed" holds for
	// EVERY node, not just the leader.
	if cm.dropRemovedMembersLocked() {
		cm.incrementVersionWithLockHeld()
	}

	// Prepend events synthesised at gossip ingress (MemberRemoved for
	// stripped-then-locally-known addresses) so subscribers observe the
	// transition before the regular per-merge diff events.
	if len(intakeEvents) > 0 {
		events = append(intakeEvents, events...)
	}

	// Update lastGossipUpdate timestamp when state was modified.
	if ordering == ClockBefore || ordering == ClockConcurrent {
		cm.lastGossipUpdate = time.Now()
	}

	if remoteAddr != nil {
		addr := remoteAddr.GetAddress()
		uid64 := uint64(remoteAddr.GetUid()) | (uint64(remoteAddr.GetUid2()) << 32)
		key := fmt.Sprintf("%s:%d-%d", addr.GetHostname(), addr.GetPort(), uid64)
		cm.Fd.Heartbeat(key)
	}

	cm.Mu.Unlock()

	// Record convergence timestamp when all Up members have seen this state.
	if cm.Metrics != nil && cm.CheckConvergence() {
		cm.Metrics.RecordConvergence(0)
	}

	// If our own gossip status is now Exiting, send ExitingConfirmed back to
	// the leader. Pekko's leader will not transition this node from Exiting
	// to Removed until it receives this confirmation. The send is idempotent
	// (Pekko keeps a Set), but we use a once-flag to avoid spam.
	cm.maybeSendExitingConfirmed()

	// Publish events outside the lock so slow subscribers can't stall gossip.
	for _, evt := range events {
		cm.publishEvent(evt)
	}
	return nil
}

// maybeSendExitingConfirmed checks whether the local node's current gossip
// status is Exiting and, if so, sends a one-shot ExitingConfirmed message
// to every Up/WeaklyUp member that could be the cluster leader. Pekko's
// ClusterCoreDaemon adds the node to its `exitingConfirmed` set, which is
// the precondition for the leader to remove the node from gossip.
//
// Without this confirmation Pekko will leave a Leaving node stuck in the
// Exiting state forever and never emit MemberRemoved.
func (cm *ClusterManager) maybeSendExitingConfirmed() {
	if cm.exitingConfirmedSent.Load() {
		return
	}

	cm.Mu.RLock()
	localHost := cm.LocalAddress.GetAddress().GetHostname()
	localPort := cm.LocalAddress.GetAddress().GetPort()

	// Find our own member entry.
	isExiting := false
	for _, m := range cm.State.GetMembers() {
		ua := cm.State.AllAddresses[m.GetAddressIndex()]
		a := ua.GetAddress()
		if a.GetHostname() == localHost && a.GetPort() == localPort {
			if m.GetStatus() == gproto_cluster.MemberStatus_Exiting {
				isExiting = true
			}
			break
		}
	}

	if !isExiting {
		cm.Mu.RUnlock()
		return
	}

	// Snapshot recipient paths under the lock so we can release it before sending.
	type target struct {
		path string
	}
	var targets []target
	for _, m := range cm.State.GetMembers() {
		st := m.GetStatus()
		if st != gproto_cluster.MemberStatus_Up && st != gproto_cluster.MemberStatus_WeaklyUp {
			continue
		}
		ua := cm.State.AllAddresses[m.GetAddressIndex()]
		a := ua.GetAddress()
		if a.GetHostname() == localHost && a.GetPort() == localPort {
			continue // skip self
		}
		path := fmt.Sprintf("%s://%s@%s:%d/system/cluster/core/daemon",
			cm.Proto(), a.GetSystem(), a.GetHostname(), a.GetPort())
		targets = append(targets, target{path: path})
	}

	// Build the ExitingConfirmed payload — our own UniqueAddress.
	confirmation := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: cm.LocalAddress.GetAddress().Protocol,
			System:   cm.LocalAddress.GetAddress().System,
			Hostname: cm.LocalAddress.GetAddress().Hostname,
			Port:     cm.LocalAddress.GetAddress().Port,
		},
		Uid:  cm.LocalAddress.Uid,
		Uid2: cm.LocalAddress.Uid2,
	}
	cm.Mu.RUnlock()

	// CAS the once-flag — only the first goroutine to reach here sends.
	if !cm.exitingConfirmedSent.CompareAndSwap(false, true) {
		return
	}

	if cm.LogInfo {
		logger.Default().Info("Cluster: local node is Exiting — sending ExitingConfirmed", slog.Int("upMembers", len(targets)))
	}
	for _, t := range targets {
		if err := cm.Router(context.Background(), t.path, confirmation); err != nil {
			logger.Default().Error("Cluster: ExitingConfirmed failed", slog.String("path", t.path), slog.Any("err", err))
		}
	}
}

// connectToNewMembers must be called with cm.Mu held (read or write).
// Heartbeat targets are limited to MonitoredByNrOfMembers nodes, selected
// deterministically by sorting remote members by address and picking the N
// nearest successors in a ring from this node's position.
func (cm *ClusterManager) connectToNewMembers(gossip *gproto_cluster.Gossip) {
	localHost := cm.LocalAddress.Address.GetHostname()
	localPort := cm.LocalAddress.Address.GetPort()

	// Collect non-self, non-removed/downed member addresses.
	type addrKey struct {
		host string
		port uint32
	}
	var remotes []addrKey
	for _, m := range gossip.Members {
		if m.GetStatus() == gproto_cluster.MemberStatus_Removed || m.GetStatus() == gproto_cluster.MemberStatus_Down {
			continue
		}
		addr := gossip.AllAddresses[m.GetAddressIndex()]
		h := addr.GetAddress().GetHostname()
		p := addr.GetAddress().GetPort()
		if h == localHost && p == localPort {
			continue
		}
		remotes = append(remotes, addrKey{h, p})
	}

	// Select heartbeat targets: if MonitoredByNrOfMembers is set and smaller
	// than the remote count, pick the N deterministic successors from a sorted
	// ring.  Otherwise heartbeat all remotes.
	targets := remotes
	limit := cm.MonitoredByNrOfMembers
	if limit > 0 && len(remotes) > limit {
		// Sort all remotes deterministically.
		sort.Slice(remotes, func(i, j int) bool {
			if remotes[i].host != remotes[j].host {
				return remotes[i].host < remotes[j].host
			}
			return remotes[i].port < remotes[j].port
		})
		// Find this node's position in the sorted ring and take the next N.
		selfKey := fmt.Sprintf("%s:%d", localHost, localPort)
		startIdx := 0
		for i, r := range remotes {
			rk := fmt.Sprintf("%s:%d", r.host, r.port)
			if rk > selfKey {
				startIdx = i
				break
			}
		}
		targets = make([]addrKey, limit)
		for i := 0; i < limit; i++ {
			targets[i] = remotes[(startIdx+i)%len(remotes)]
		}
	}

	// Start heartbeats to selected targets only.
	selectedSet := make(map[string]struct{}, len(targets))
	for _, t := range targets {
		key := fmt.Sprintf("%s:%d", t.host, t.port)
		selectedSet[key] = struct{}{}
	}

	// Stop heartbeats to nodes no longer in the target set.
	heartbeatTasksMu.Lock()
	for key := range heartbeatTasks {
		if _, selected := selectedSet[key]; !selected {
			// Only stop if this is a remote peer no longer selected (don't
			// stop tasks for the seed which may have been started externally).
			if task, ok := heartbeatTasks[key]; ok {
				task.cancel()
				delete(heartbeatTasks, key)
			}
		}
	}
	heartbeatTasksMu.Unlock()

	for _, t := range targets {
		addr := &gproto_cluster.Address{
			Hostname: proto.String(t.host),
			Port:     proto.Uint32(t.port),
		}
		// Find the system name from the gossip state.
		for _, m := range gossip.Members {
			a := gossip.AllAddresses[m.GetAddressIndex()]
			if a.GetAddress().GetHostname() == t.host && a.GetAddress().GetPort() == t.port {
				addr.System = a.GetAddress().System
				addr.Protocol = a.GetAddress().Protocol
				break
			}
		}
		cm.StartHeartbeat(addr)
	}
}

func (cm *ClusterManager) CheckConvergence() bool {
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()
	return cm.CheckConvergenceLocked()
}

func (cm *ClusterManager) CheckConvergenceLocked() bool {
	if cm.State.Overview == nil || len(cm.State.Overview.Seen) == 0 {
		return true // single node
	}

	// All UP/LEAVING members must have seen the current state.
	// Key by AddressIndex (the member's slot in AllAddresses), NOT by the
	// member's position in the Members slice — Overview.Seen stores address
	// indices, so the maps must use the same index space.
	upMembers := make(map[int32]bool)
	for _, m := range cm.State.Members {
		if m.GetStatus() == gproto_cluster.MemberStatus_Up || m.GetStatus() == gproto_cluster.MemberStatus_Leaving {
			upMembers[m.GetAddressIndex()] = true
		}
	}

	for _, seenIdx := range cm.State.Overview.Seen {
		delete(upMembers, seenIdx)
	}

	if cm.LogInfoVerbose {
		if len(upMembers) == 0 {
			logger.Default().Debug("Cluster: convergence check passed (all Up/Leaving members in Seen set)", slog.Int("members", len(cm.State.Members)))
		} else {
			logger.Default().Debug("Cluster: convergence check failed (members not yet in Seen set)", slog.Int("missingFromSeen", len(upMembers)))
		}
	}

	return len(upMembers) == 0
}

type ClockOrdering int

const (
	ClockSame ClockOrdering = iota
	ClockBefore
	ClockAfter
	ClockConcurrent
)

func CompareVectorClock(v1, v2 *gproto_cluster.VectorClock) ClockOrdering {
	m1 := make(map[int32]int64)
	if v1 != nil {
		for _, v := range v1.Versions {
			m1[v.GetHashIndex()] = v.GetTimestamp()
		}
	}
	m2 := make(map[int32]int64)
	if v2 != nil {
		for _, v := range v2.Versions {
			m2[v.GetHashIndex()] = v.GetTimestamp()
		}
	}

	allKeys := make(map[int32]bool)
	for k := range m1 {
		allKeys[k] = true
	}
	for k := range m2 {
		allKeys[k] = true
	}

	greater := false
	less := false

	for k := range allKeys {
		v1Val := m1[k]
		v2Val := m2[k]
		if v1Val > v2Val {
			greater = true
		} else if v1Val < v2Val {
			less = true
		}
	}

	if greater && less {
		return ClockConcurrent
	}
	if greater {
		return ClockAfter
	}
	if less {
		return ClockBefore
	}
	return ClockSame
}

func (cm *ClusterManager) vectorClockToMap(v *gproto_cluster.VectorClock, hashes []string) map[string]int64 {
	m := make(map[string]int64)
	if v == nil {
		return m
	}
	for _, entry := range v.Versions {
		idx := entry.GetHashIndex()
		if int(idx) < len(hashes) {
			m[hashes[idx]] = entry.GetTimestamp()
		}
	}
	return m
}

func (cm *ClusterManager) compareResolvedClocks(m1, m2 map[string]int64) ClockOrdering {
	allKeys := make(map[string]bool)
	for k := range m1 {
		allKeys[k] = true
	}
	for k := range m2 {
		allKeys[k] = true
	}

	greater := false
	less := false

	for k := range allKeys {
		v1Val := m1[k]
		v2Val := m2[k]
		if v1Val > v2Val {
			greater = true
		} else if v1Val < v2Val {
			less = true
		}
	}

	if greater && less {
		return ClockConcurrent
	}
	if greater {
		return ClockAfter
	}
	if less {
		return ClockBefore
	}
	return ClockSame
}

func (cm *ClusterManager) mergeGossipStates(s1, s2 *gproto_cluster.Gossip) *gproto_cluster.Gossip {
	merged := proto.Clone(s1).(*gproto_cluster.Gossip)

	// Combine AllAddresses and AllHashes, keeping indices stable.
	addrMap := make(map[string]int32)
	for i, ua := range merged.AllAddresses {
		key := fmt.Sprintf("%s:%d-%d", ua.GetAddress().GetHostname(), ua.GetAddress().GetPort(), ua.GetUid())
		addrMap[key] = int32(i)
	}

	for i, ua := range s2.AllAddresses {
		key := fmt.Sprintf("%s:%d-%d", ua.GetAddress().GetHostname(), ua.GetAddress().GetPort(), ua.GetUid())
		if _, ok := addrMap[key]; !ok {
			addrMap[key] = int32(len(merged.AllAddresses))
			merged.AllAddresses = append(merged.AllAddresses, ua)
			// AllHashes may be shorter than AllAddresses in Pekko 1.1.x gossip
			// when tombstone entries are present without a corresponding hash.
			var hash string
			if i < len(s2.AllHashes) {
				hash = s2.AllHashes[i]
			}
			merged.AllHashes = append(merged.AllHashes, hash)
		}
	}

	// Pairwise-max VectorClock
	m1 := cm.vectorClockToMap(s1.Version, s1.AllHashes)
	m2 := cm.vectorClockToMap(s2.Version, s2.AllHashes)
	allKeys := make(map[string]bool)
	for k := range m1 {
		allKeys[k] = true
	}
	for k := range m2 {
		allKeys[k] = true
	}

	merged.Version = &gproto_cluster.VectorClock{}
	for k := range allKeys {
		v1 := m1[k]
		v2 := m2[k]
		maxV := v1
		if v2 > maxV {
			maxV = v2
		}
		// find index in merged.AllHashes
		for i, h := range merged.AllHashes {
			if h == k {
				merged.Version.Versions = append(merged.Version.Versions, &gproto_cluster.VectorClock_Version{
					HashIndex: proto.Int32(int32(i)),
					Timestamp: proto.Int64(maxV),
				})
				break
			}
		}
	}

	// Merge AllRoles from s2 into merged, building a remap table so that
	// RolesIndexes carried by s2 members can be translated to merged indices.
	// This is necessary because s1 and s2 may have different AllRoles orderings
	// (Pekko 1.1.x no longer guarantees a stable ordering across gossip messages).
	roleRemap := make(map[int32]int32) // s2 role idx → merged role idx
	for s2Idx, role := range s2.AllRoles {
		found := false
		for mergedIdx, r := range merged.AllRoles {
			if r == role {
				roleRemap[int32(s2Idx)] = int32(mergedIdx)
				found = true
				break
			}
		}
		if !found {
			newIdx := int32(len(merged.AllRoles))
			merged.AllRoles = append(merged.AllRoles, role)
			roleRemap[int32(s2Idx)] = newIdx
		}
	}

	// Merge AllAppVersions from s2 into merged, building a remap table.
	appVerRemap := make(map[int32]int32) // s2 appVersion idx → merged appVersion idx
	for s2Idx, ver := range s2.AllAppVersions {
		found := false
		for mergedIdx, v := range merged.AllAppVersions {
			if v == ver {
				appVerRemap[int32(s2Idx)] = int32(mergedIdx)
				found = true
				break
			}
		}
		if !found {
			newIdx := int32(len(merged.AllAppVersions))
			merged.AllAppVersions = append(merged.AllAppVersions, ver)
			appVerRemap[int32(s2Idx)] = newIdx
		}
	}

	// Union of members with highest status/upNumber
	memberMap := make(map[int32]*gproto_cluster.Member)
	for _, m := range merged.Members {
		memberMap[m.GetAddressIndex()] = m
	}

	for _, m2 := range s2.Members {
		ua2 := s2.AllAddresses[m2.GetAddressIndex()]
		key2 := fmt.Sprintf("%s:%d-%d", ua2.GetAddress().GetHostname(), ua2.GetAddress().GetPort(), ua2.GetUid())
		idxMerged := addrMap[key2]

		if m1, ok := memberMap[idxMerged]; ok {
			if m2.GetStatus() > m1.GetStatus() {
				m1.Status = m2.Status
			}
			if m2.GetUpNumber() > m1.GetUpNumber() {
				m1.UpNumber = m2.UpNumber
			}
			// Adopt roles from incoming state if the local entry has none.
			// Roles are immutable for the lifetime of a node, so the first
			// non-empty RolesIndexes we see from any gossip source is canonical.
			if len(m1.GetRolesIndexes()) == 0 && len(m2.GetRolesIndexes()) > 0 {
				remapped := make([]int32, 0, len(m2.GetRolesIndexes()))
				for _, ri := range m2.GetRolesIndexes() {
					if idx, ok2 := roleRemap[ri]; ok2 {
						remapped = append(remapped, idx)
					}
				}
				m1.RolesIndexes = remapped
			}
			// Adopt AppVersionIndex from incoming state if local entry has none.
			if m1.AppVersionIndex == nil && m2.AppVersionIndex != nil {
				if idx, ok2 := appVerRemap[m2.GetAppVersionIndex()]; ok2 {
					m1.AppVersionIndex = proto.Int32(idx)
				}
			}
		} else {
			newM := proto.Clone(m2).(*gproto_cluster.Member)
			newM.AddressIndex = proto.Int32(idxMerged)
			// Remap RolesIndexes from s2's AllRoles space to merged AllRoles space.
			remapped := make([]int32, 0, len(newM.GetRolesIndexes()))
			for _, ri := range newM.GetRolesIndexes() {
				if idx, ok := roleRemap[ri]; ok {
					remapped = append(remapped, idx)
				}
			}
			newM.RolesIndexes = remapped
			// Remap AppVersionIndex from s2's space to merged space.
			if newM.AppVersionIndex != nil {
				if idx, ok := appVerRemap[newM.GetAppVersionIndex()]; ok {
					newM.AppVersionIndex = proto.Int32(idx)
				}
			}
			merged.Members = append(merged.Members, newM)
			memberMap[idxMerged] = newM
		}
	}

	return merged
}

func (cm *ClusterManager) incrementVersionWithLockHeld() {
	m := cm.vectorClockToMap(cm.State.Version, cm.State.AllHashes)
	localHashStr := cm.localHashString()
	m[localHashStr]++

	cm.State.Version = &gproto_cluster.VectorClock{}
	for i, h := range cm.State.AllHashes {
		if v, ok := m[h]; ok {
			cm.State.Version.Versions = append(cm.State.Version.Versions, &gproto_cluster.VectorClock_Version{
				HashIndex: proto.Int32(int32(i)),
				Timestamp: proto.Int64(v),
			})
		}
	}

}

// DetermineLeader selects the cluster leader using Pekko semantics: the
// Up/WeaklyUp member with the smallest UpNumber wins, with address
// (hostname, port, uid) as the deterministic tiebreaker.  When NO member
// has been promoted yet (UpNumber=0 across the board, e.g. a one-node
// bootstrap window before the first leader-action), the address tiebreak
// alone picks a deterministic leader.
//
// Why the upNumber-first ordering matters (dashboard self-down Issue 3,
// 2026-05-16): the previous implementation sorted purely by
// (hostname, port, uid), so a fresh joiner that happened to bind to a
// numerically smaller port than the existing seed-of-record was elected
// "leader" within seconds of receiving Welcome, then drove Down → Removed
// on the cluster's real members.  Pekko's MembershipState.leaderOf
// instead picks the first Up member in leaderOrdering — i.e. lowest
// UpNumber — so a joiner with UpNumber assigned mid-cluster (always >=
// existing-member upNumber) can never displace an existing leader on
// address alone.  Live diag against the production cluster on
// 127.0.0.1:37777 with gekka pinned to 2560 reproduces the misroll
// deterministically until this fix lands.
func (cm *ClusterManager) DetermineLeader() *gproto_cluster.UniqueAddress {
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()

	type cand struct {
		ua       *gproto_cluster.UniqueAddress
		upNumber int32
	}
	var candidates []cand
	for _, m := range cm.State.Members {
		if m.GetStatus() == gproto_cluster.MemberStatus_Up || m.GetStatus() == gproto_cluster.MemberStatus_WeaklyUp {
			candidates = append(candidates, cand{
				ua:       cm.State.AllAddresses[m.GetAddressIndex()],
				upNumber: m.GetUpNumber(),
			})
		}
	}

	if len(candidates) == 0 {
		// No Up/WeaklyUp members yet (bootstrap window).  Fall back to the
		// full member list so the very first node has a deterministic
		// self-leader for self-promotion via the canBootstrap path.
		for _, m := range cm.State.Members {
			candidates = append(candidates, cand{
				ua:       cm.State.AllAddresses[m.GetAddressIndex()],
				upNumber: m.GetUpNumber(),
			})
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		// Smallest UpNumber wins (Pekko's leaderOrdering).  UpNumber 0
		// (unassigned) sorts last among assigned values because all
		// real promotions yield UpNumber >= 1; treat 0 as "infinitely
		// large" so an unpromoted joiner never beats a promoted peer.
		ai, bi := a.upNumber, b.upNumber
		const sentinel = int32(0x7fffffff)
		if ai == 0 {
			ai = sentinel
		}
		if bi == 0 {
			bi = sentinel
		}
		if ai != bi {
			return ai < bi
		}
		// Deterministic tiebreak on address: hostname → port → uid.
		aa, ba := a.ua.GetAddress(), b.ua.GetAddress()
		if aa.GetHostname() != ba.GetHostname() {
			return aa.GetHostname() < ba.GetHostname()
		}
		if aa.GetPort() != ba.GetPort() {
			return aa.GetPort() < ba.GetPort()
		}
		return a.ua.GetUid() < b.ua.GetUid()
	})

	return candidates[0].ua
}

// OldestNode returns the oldest Up/WeaklyUp member, optionally filtered by role.
// "Oldest" = member with the lowest upNumber (the one that became Up first).
// This matches Pekko's ClusterSingletonManager oldest-member selection.
func (cm *ClusterManager) OldestNode(role string) *gproto_cluster.UniqueAddress {
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()

	type candidate struct {
		ua       *gproto_cluster.UniqueAddress
		upNumber int32
		addr     *gproto_cluster.Address
	}
	var best *candidate

	for _, m := range cm.State.Members {
		st := m.GetStatus()
		if st != gproto_cluster.MemberStatus_Up && st != gproto_cluster.MemberStatus_WeaklyUp {
			continue
		}
		if role != "" {
			found := false
			for _, idx := range m.GetRolesIndexes() {
				if int(idx) < len(cm.State.AllRoles) && cm.State.AllRoles[idx] == role {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		ua := cm.State.AllAddresses[m.GetAddressIndex()]
		c := &candidate{ua: ua, upNumber: m.GetUpNumber(), addr: ua.GetAddress()}
		if best == nil {
			best = c
			continue
		}
		// Lowest upNumber wins; break ties by address (hostname then port)
		if c.upNumber < best.upNumber {
			best = c
		} else if c.upNumber == best.upNumber {
			ca, ba := c.addr, best.addr
			if ca.GetHostname() < ba.GetHostname() ||
				(ca.GetHostname() == ba.GetHostname() && ca.GetPort() < ba.GetPort()) {
				best = c
			}
		}
	}

	if best == nil {
		return nil
	}
	return best.ua
}

// OldestNodeInDC returns the oldest Up/WeaklyUp member in the given data center,
// optionally filtered by role. Pass dc="" for no DC filter.
func (cm *ClusterManager) OldestNodeInDC(dc, role string) *gproto_cluster.UniqueAddress {
	if dc == "" {
		return cm.OldestNode(role)
	}
	dcRole := "dc-" + dc

	cm.Mu.RLock()
	defer cm.Mu.RUnlock()

	type candidate struct {
		ua       *gproto_cluster.UniqueAddress
		upNumber int32
		addr     *gproto_cluster.Address
	}
	var best *candidate

	for _, m := range cm.State.Members {
		st := m.GetStatus()
		if st != gproto_cluster.MemberStatus_Up && st != gproto_cluster.MemberStatus_WeaklyUp {
			continue
		}
		// DC filter
		hasDC := false
		for _, idx := range m.GetRolesIndexes() {
			if int(idx) < len(cm.State.AllRoles) && cm.State.AllRoles[idx] == dcRole {
				hasDC = true
				break
			}
		}
		if !hasDC {
			continue
		}
		// Optional role filter (non-DC)
		if role != "" {
			found := false
			for _, idx := range m.GetRolesIndexes() {
				if int(idx) < len(cm.State.AllRoles) && cm.State.AllRoles[idx] == role {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		ua := cm.State.AllAddresses[m.GetAddressIndex()]
		c := &candidate{ua: ua, upNumber: m.GetUpNumber(), addr: ua.GetAddress()}
		if best == nil {
			best = c
			continue
		}
		if c.upNumber < best.upNumber {
			best = c
		} else if c.upNumber == best.upNumber {
			ca, ba := c.addr, best.addr
			if ca.GetHostname() < ba.GetHostname() ||
				(ca.GetHostname() == ba.GetHostname() && ca.GetPort() < ba.GetPort()) {
				best = c
			}
		}
	}
	if best == nil {
		return nil
	}
	return best.ua
}

// MembersInDataCenter returns the MemberAddress of all Up/WeaklyUp members
// in the given data center.
func (cm *ClusterManager) MembersInDataCenter(dc string) []MemberAddress {
	dcRole := "dc-" + dc
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()

	var result []MemberAddress
	for _, m := range cm.State.Members {
		st := m.GetStatus()
		if st != gproto_cluster.MemberStatus_Up && st != gproto_cluster.MemberStatus_WeaklyUp {
			continue
		}
		hasDC := false
		for _, idx := range m.GetRolesIndexes() {
			if int(idx) < len(cm.State.AllRoles) && cm.State.AllRoles[idx] == dcRole {
				hasDC = true
				break
			}
		}
		if !hasDC {
			continue
		}
		ua := cm.State.AllAddresses[m.GetAddressIndex()]
		ma := memberAddressFromUA(ua)
		ma.DataCenter = dc
		result = append(result, ma)
	}
	return result
}

// IsInDataCenter reports whether the member with the given host:port is in
// the named data center according to the current gossip state.
func (cm *ClusterManager) IsInDataCenter(host string, port uint32, dc string) bool {
	dcRole := "dc-" + dc
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()

	for _, m := range cm.State.Members {
		ua := cm.State.AllAddresses[m.GetAddressIndex()]
		a := ua.GetAddress()
		if a.GetHostname() != host || a.GetPort() != port {
			continue
		}
		for _, idx := range m.GetRolesIndexes() {
			if int(idx) < len(cm.State.AllRoles) && cm.State.AllRoles[idx] == dcRole {
				return true
			}
		}
		return false
	}
	return false
}

// localDC returns the data-center name for the local node extracted from its
// roles (format "dc-<name>"). Returns "" if the local node has no DC role.
func (cm *ClusterManager) localDC() string {
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()

	localHost := cm.LocalAddress.GetAddress().GetHostname()
	localPort := cm.LocalAddress.GetAddress().GetPort()

	for _, m := range cm.State.Members {
		ua := cm.State.AllAddresses[m.GetAddressIndex()]
		if ua.GetAddress().GetHostname() != localHost || ua.GetAddress().GetPort() != localPort {
			continue
		}
		for _, idx := range m.GetRolesIndexes() {
			if int(idx) < len(cm.State.AllRoles) {
				role := cm.State.AllRoles[idx]
				if strings.HasPrefix(role, "dc-") {
					return strings.TrimPrefix(role, "dc-")
				}
			}
		}
		return ""
	}
	return ""
}

// determineDCLeader returns the DC leader for the given data center: the
// address-order-smallest Up/WeaklyUp member, falling back to Joining if none
// are Up/WeaklyUp yet.
func (cm *ClusterManager) determineDCLeader(dc string) *gproto_cluster.UniqueAddress {
	dcRole := "dc-" + dc
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()

	var candidates []*gproto_cluster.UniqueAddress
	var joiningFallback []*gproto_cluster.UniqueAddress

	for _, m := range cm.State.Members {
		hasDC := false
		for _, idx := range m.GetRolesIndexes() {
			if int(idx) < len(cm.State.AllRoles) && cm.State.AllRoles[idx] == dcRole {
				hasDC = true
				break
			}
		}
		if !hasDC {
			continue
		}
		ua := cm.State.AllAddresses[m.GetAddressIndex()]
		st := m.GetStatus()
		if st == gproto_cluster.MemberStatus_Up || st == gproto_cluster.MemberStatus_WeaklyUp {
			candidates = append(candidates, ua)
		} else if st == gproto_cluster.MemberStatus_Joining {
			joiningFallback = append(joiningFallback, ua)
		}
	}

	pool := candidates
	if len(pool) == 0 {
		pool = joiningFallback
	}
	if len(pool) == 0 {
		return nil
	}

	sort.Slice(pool, func(i, j int) bool {
		a, b := pool[i], pool[j]
		if a.GetAddress().GetHostname() != b.GetAddress().GetHostname() {
			return a.GetAddress().GetHostname() < b.GetAddress().GetHostname()
		}
		return a.GetAddress().GetPort() < b.GetAddress().GetPort()
	})
	return pool[0]
}

// promoteDCMembers promotes Joining members within the given DC to Up.
// Called when the local node is the DC leader.
func (cm *ClusterManager) promoteDCMembers(dc string) {
	dcRole := "dc-" + dc
	cm.Mu.Lock()
	var events []ClusterDomainEvent
	changed := false
	for _, m := range cm.State.Members {
		if m.GetStatus() != gproto_cluster.MemberStatus_Joining {
			continue
		}
		hasDC := false
		for _, idx := range m.GetRolesIndexes() {
			if int(idx) < len(cm.State.AllRoles) && cm.State.AllRoles[idx] == dcRole {
				hasDC = true
				break
			}
		}
		if !hasDC {
			continue
		}
		ua := cm.State.AllAddresses[m.GetAddressIndex()]
		ma := memberAddressFromUA(ua)
		m.Status = gproto_cluster.MemberStatus_Up.Enum()
		m.UpNumber = proto.Int32(int32(len(cm.State.Members)))
		events = append(events, MemberUp{Member: ma})
		if cm.Metrics != nil {
			cm.Metrics.IncrementMemberUp()
		}
		changed = true
		if cm.LogInfo {
			logger.Default().Info("DC-Leader: transitioned member Joining → Up", slog.String("dc", dc), slog.String("host", ma.Host), slog.Int("port", int(ma.Port)))
		}
	}
	if changed {
		cm.incrementVersionWithLockHeld()
	}
	cm.Mu.Unlock()

	for _, evt := range events {
		cm.publishEvent(evt)
	}
}

func (cm *ClusterManager) performLeaderActions() {
	leader := cm.DetermineLeader()
	if leader == nil {
		return
	}

	localHost := cm.LocalAddress.GetAddress().GetHostname()
	localPort := cm.LocalAddress.GetAddress().GetPort()

	isLeader := leader.GetAddress().GetHostname() == localHost &&
		leader.GetAddress().GetPort() == localPort &&
		leader.GetUid() == cm.LocalAddress.GetUid()

	if isLeader {
		if cm.LogInfoVerbose {
			logger.Default().Debug("Leader: performing leader actions", slog.Int("members", len(cm.State.Members)))
		}
		cm.Mu.Lock()
		var events []ClusterDomainEvent
		changed := false
		for _, m := range cm.State.Members {
			ua := cm.State.AllAddresses[m.GetAddressIndex()]
			ma := memberAddressFromUA(ua)
			switch m.GetStatus() {
			case gproto_cluster.MemberStatus_Joining:
				// Only promote Joining to Up if we have convergence (everyone has
				// seen the state) or if it's the very first node (bootstrap).
				// Pekko convergence check: all members in Overview.Seen.
				// Also gate on min-nr-of-members (pekko.cluster.min-nr-of-members).
				minMembers := cm.MinNrOfMembers
				if minMembers <= 0 {
					minMembers = 1
				}
				meetsMinMembers := len(cm.State.Members) >= minMembers && cm.meetsRoleMinNrOfMembersLocked()
				// Bootstrap shortcut: if we are a 1-member cluster, self-promote
				// without waiting for convergence.  GATE: only when no remote
				// JOIN has been attempted, OR Welcome from the seed has already
				// arrived.  Without this gate, gekka self-promotes as upNumber=1
				// in the window between JoinCluster and Welcome — then when
				// Welcome arrives and reveals the seed's larger cluster view,
				// SBR detects the upNumber-1 conflict and self-downs.
				canBootstrap := len(cm.State.Members) == 1 &&
					(!cm.joinAttempted.Load() || cm.WelcomeReceived.Load())
				if meetsMinMembers && (canBootstrap || cm.CheckConvergenceLocked()) {
					m.Status = gproto_cluster.MemberStatus_Up.Enum()
					m.UpNumber = proto.Int32(int32(len(cm.State.Members))) // Simplified upNumber
					events = append(events, MemberUp{Member: ma})
					if cm.Metrics != nil {
						cm.Metrics.IncrementMemberUp()
					}
					changed = true
					// Clear WeaklyUp tracking for this member.
					delete(cm.joiningFirstSeen, m.GetAddressIndex())
					if cm.LogInfo {
						logger.Default().Info("Leader: transitioned member Joining → Up", slog.String("host", ma.Host), slog.Int("port", int(ma.Port)), slog.Int("upNumber", int(m.GetUpNumber())))
					}
				} else if cm.AllowWeaklyUpMembers > 0 && meetsMinMembers {
					// WeaklyUp promotion: if convergence is not achieved within
					// AllowWeaklyUpMembers duration, promote Joining → WeaklyUp.
					addrIdx := m.GetAddressIndex()
					if cm.joiningFirstSeen == nil {
						cm.joiningFirstSeen = make(map[int32]time.Time)
					}
					firstSeen, ok := cm.joiningFirstSeen[addrIdx]
					if !ok {
						cm.joiningFirstSeen[addrIdx] = time.Now()
					} else if time.Since(firstSeen) >= cm.AllowWeaklyUpMembers {
						m.Status = gproto_cluster.MemberStatus_WeaklyUp.Enum()
						events = append(events, MemberWeaklyUp{Member: ma})
						changed = true
						delete(cm.joiningFirstSeen, addrIdx)
						if cm.LogInfo {
							logger.Default().Info("Leader: transitioned member Joining → WeaklyUp (convergence timeout)", slog.String("host", ma.Host), slog.Int("port", int(ma.Port)), slog.Duration("timeout", cm.AllowWeaklyUpMembers))
						}
					}
				}
			case gproto_cluster.MemberStatus_WeaklyUp:
				// Promote WeaklyUp → Up once convergence is achieved.
				if cm.CheckConvergenceLocked() {
					m.Status = gproto_cluster.MemberStatus_Up.Enum()
					m.UpNumber = proto.Int32(int32(len(cm.State.Members)))
					events = append(events, MemberUp{Member: ma})
					if cm.Metrics != nil {
						cm.Metrics.IncrementMemberUp()
					}
					changed = true
					if cm.LogInfo {
						logger.Default().Info("Leader: transitioned member WeaklyUp → Up (convergence achieved)", slog.String("host", ma.Host), slog.Int("port", int(ma.Port)))
					}
				}
			case gproto_cluster.MemberStatus_Leaving:
				m.Status = gproto_cluster.MemberStatus_Exiting.Enum()
				events = append(events, MemberExited{Member: ma})
				changed = true
				if cm.LogInfo {
					logger.Default().Info("Leader: transitioned member Leaving → Exiting", slog.String("host", ma.Host), slog.Int("port", int(ma.Port)))
				}
			case gproto_cluster.MemberStatus_Exiting:
				m.Status = gproto_cluster.MemberStatus_Removed.Enum()
				events = append(events, MemberRemoved{Member: ma})
				if cm.Metrics != nil {
					cm.Metrics.IncrementMemberRemoved()
				}
				changed = true
				// Record tombstone for pruning.
				cm.recordTombstoneLocked(ma.Host, ma.Port)
				if cm.LogInfo {
					logger.Default().Info("Leader: transitioned member Exiting → Removed", slog.String("host", ma.Host), slog.Int("port", int(ma.Port)))
				}
			case gproto_cluster.MemberStatus_Down:
				// Enforce down-removal-margin: delay the Down → Removed
				// transition to give the downed node time to detect its own
				// downed status and perform coordinated shutdown.
				if cm.DownRemovalMargin > 0 {
					addrIdx := m.GetAddressIndex()
					if cm.downedAt == nil {
						cm.downedAt = make(map[int32]time.Time)
					}
					firstDown, ok := cm.downedAt[addrIdx]
					if !ok {
						cm.downedAt[addrIdx] = time.Now()
						if cm.LogInfoVerbose {
							logger.Default().Debug("Leader: member Down, removal delayed", slog.String("host", ma.Host), slog.Int("port", int(ma.Port)), slog.Duration("downRemovalMargin", cm.DownRemovalMargin))
						}
						continue
					}
					if time.Since(firstDown) < cm.DownRemovalMargin {
						continue // margin not yet elapsed
					}
					delete(cm.downedAt, addrIdx)
				}
				m.Status = gproto_cluster.MemberStatus_Removed.Enum()
				events = append(events, MemberRemoved{Member: ma})
				if cm.Metrics != nil {
					cm.Metrics.IncrementMemberRemoved()
				}
				changed = true
				// Record tombstone for pruning.
				cm.recordTombstoneLocked(ma.Host, ma.Port)
				if cm.LogInfo {
					logger.Default().Info("Leader: transitioned member Down → Removed", slog.String("host", ma.Host), slog.Int("port", int(ma.Port)))
				}
			}
		}
		if changed {
			cm.incrementVersionWithLockHeld()
		}
		// Prune stale tombstones (removed members older than configured TTL).
		cm.pruneGossipTombstonesLocked()
		// Drop every Member with status Removed from gossip state immediately,
		// regardless of tombstone age, and record their UniqueAddress in
		// tombstones.  Mirrors Akka's MembershipState.copyWithMembers
		// semantics where Removed members are dropped from `members` at the
		// same gossip version where the transition happens.  Without this,
		// stale Removed entries inherited via incoming gossip (or carried over
		// from prior incarnations) get rebroadcast every time gekka becomes
		// leader, causing application-level subscribers to re-fire
		// `Cluster.down(addr)` against the live address — see
		// docs/superpowers/specs/2026-05-16-dashboard-self-down-followups.md
		// Issue 3.
		if cm.dropRemovedMembersLocked() {
			cm.incrementVersionWithLockHeld()
		}
		cm.Mu.Unlock()

		for _, evt := range events {
			cm.publishEvent(evt)
		}
	} else {
		// Not the global leader. Check if we are the DC leader and can promote
		// Joining members within our own DC without waiting for the global leader.
		// This is necessary in multi-DC deployments where the global leader only
		// promotes members in its own DC.
		localDC := cm.localDC()
		if localDC != "" && localDC != "default" {
			dcLeader := cm.determineDCLeader(localDC)
			if dcLeader != nil &&
				dcLeader.GetAddress().GetHostname() == localHost &&
				dcLeader.GetAddress().GetPort() == localPort {
				cm.promoteDCMembers(localDC)
			}
		}
	}
}

// recordTombstoneLocked records a removed member's timestamp for later pruning.
// Must be called with cm.Mu held.
func (cm *ClusterManager) recordTombstoneLocked(host string, port uint32) {
	key := fmt.Sprintf("%s:%d", host, port)
	if cm.tombstones == nil {
		cm.tombstones = make(map[string]time.Time)
	}
	cm.tombstones[key] = time.Now()
}

// intakeRemovedFromIncomingLocked filters Removed entries out of an
// incoming gossip (Welcome or GossipEnvelope payload) BEFORE we merge it
// into cm.State.  It mirrors Akka's MembershipState invariant that
// `Gossip.members` never carries status=Removed: those entries are
// dropped on receipt and the address tombstoned.
//
// Why this is necessary (dashboard self-down Issue 3, 2026-05-16): when
// gekka re-joins a cluster, the seed's Welcome carries stale Removed slots
// for prior gekka incarnations (same host:port, different UID) and for
// previously-evicted peers.  Without this filter, gekka adopts those
// entries verbatim and the very next gossip-tick rebroadcasts them, at
// which point peers' user-level subscribers observe MemberRemoved for the
// addresses and react (e.g. by calling Cluster.down(addr) on what is now
// a live address) — taking out members of the production cluster.
// The leader-action `dropRemovedMembersLocked` runs too late: the leak
// happens on the very first gossip tick after we adopt the Welcome,
// before performLeaderActions ever fires.
//
// Side effects:
//   - In-place strips every status=Removed Member from incoming.Members
//     (except the local node — handled by repairSelfReincarnationLocked).
//   - Records each stripped entry's host:port in cm.tombstones.
//   - For every stripped entry whose host:port also exists in
//     cm.State.Members with a non-Removed status: drops the matching
//     local entry and synthesises a MemberRemoved event so local
//     subscribers still observe the transition.  Without the local drop,
//     mergeGossipStates (ClockConcurrent / ClockSame) would re-introduce
//     the entry from cm.State even though the incoming view considers
//     it gone.
//
// Returns the synthesised MemberRemoved events; the caller must publish
// them after releasing cm.Mu.  Must be called with cm.Mu held (write).
func (cm *ClusterManager) intakeRemovedFromIncomingLocked(incoming *gproto_cluster.Gossip) []ClusterDomainEvent {
	if incoming == nil || len(incoming.Members) == 0 {
		return nil
	}

	localHost := cm.LocalAddress.GetAddress().GetHostname()
	localPort := cm.LocalAddress.GetAddress().GetPort()

	type addrKey struct {
		host string
		port uint32
	}
	droppedAddrs := make(map[addrKey]struct{})

	kept := incoming.Members[:0]
	for _, m := range incoming.Members {
		if m.GetStatus() != gproto_cluster.MemberStatus_Removed {
			kept = append(kept, m)
			continue
		}
		idx := int(m.GetAddressIndex())
		if idx < 0 || idx >= len(incoming.AllAddresses) {
			continue
		}
		a := incoming.AllAddresses[idx].GetAddress()
		host, port := a.GetHostname(), a.GetPort()
		if host == localHost && port == localPort {
			// Never tombstone or drop self via this path.  The reincarnation
			// guard (repairSelfReincarnationLocked) handles UID-mismatched
			// self entries and resets us back to Joining.  Keeping the entry
			// in the incoming members lets the existing guard see it.
			kept = append(kept, m)
			continue
		}
		cm.recordTombstoneLocked(host, port)
		droppedAddrs[addrKey{host: host, port: port}] = struct{}{}
	}
	if len(droppedAddrs) == 0 {
		// No stripping happened — leave incoming.Members exactly as it was
		// (kept aliases the same underlying array but len matches).
		incoming.Members = kept
		return nil
	}
	incoming.Members = kept

	// Drop local cm.State entries at the stripped addresses; synthesise
	// MemberRemoved events for any that were not already Removed locally.
	var events []ClusterDomainEvent
	localKept := cm.State.Members[:0]
	for _, lm := range cm.State.Members {
		lidx := int(lm.GetAddressIndex())
		var lhost string
		var lport uint32
		if lidx >= 0 && lidx < len(cm.State.AllAddresses) {
			la := cm.State.AllAddresses[lidx].GetAddress()
			lhost = la.GetHostname()
			lport = la.GetPort()
		}
		if _, drop := droppedAddrs[addrKey{host: lhost, port: lport}]; drop {
			if lm.GetStatus() != gproto_cluster.MemberStatus_Removed && lidx >= 0 && lidx < len(cm.State.AllAddresses) {
				appVer := ""
				if vi := lm.GetAppVersionIndex(); int(vi) >= 0 && int(vi) < len(cm.State.AllAppVersions) {
					appVer = cm.State.AllAppVersions[vi]
				}
				events = append(events, MemberRemoved{Member: memberAddressFromGossip(cm.State.AllAddresses[lidx], appVer)})
				if cm.Metrics != nil {
					cm.Metrics.IncrementMemberRemoved()
				}
			}
			continue
		}
		localKept = append(localKept, lm)
	}
	cm.State.Members = localKept

	return events
}

// dropRemovedMembersLocked removes every Member with status Removed from
// cm.State.Members and records each one's host:port in cm.tombstones so
// subsequent incoming gossip cannot resurrect the slot.  Returns true when
// at least one entry was dropped so the caller can bump the vector clock.
// Must be called with cm.Mu held.
//
// SELF IS PRESERVED.  The self entry must remain in cm.State.Members even
// when its status is Removed so that:
//
//   - WaitForSelfRemoved (the coordinated-shutdown cluster-leave gate) can
//     observe the terminal state and unblock cleanly.
//   - External callers polling cm.GetState() can see the lifecycle complete
//     through Leaving → Exiting → Removed before the actor system tears down.
//
// This mirrors intakeRemovedFromIncomingLocked's self-preservation guard
// at the gossip-ingress filter; both filters must agree that self is the
// one address allowed to carry status=Removed transiently.  Without this
// guard the live-diag against the production cluster on 127.0.0.1:37777
// observed only "Leaving" — the leader's Exiting → Removed transitions
// completed in cm.State.Members briefly but were stripped before the diag
// poller's next 50 ms tick.
func (cm *ClusterManager) dropRemovedMembersLocked() bool {
	if len(cm.State.Members) == 0 {
		return false
	}
	localHost := cm.LocalAddress.GetAddress().GetHostname()
	localPort := cm.LocalAddress.GetAddress().GetPort()
	myUid := cm.LocalAddress.GetUid()
	myUid2 := cm.LocalAddress.GetUid2()

	kept := cm.State.Members[:0]
	dropped := false
	for _, m := range cm.State.Members {
		if m.GetStatus() != gproto_cluster.MemberStatus_Removed {
			kept = append(kept, m)
			continue
		}
		idx := int(m.GetAddressIndex())
		if idx >= 0 && idx < len(cm.State.AllAddresses) {
			ua := cm.State.AllAddresses[idx]
			a := ua.GetAddress()
			// Self-Removed (our exact UID) is the legitimate terminal
			// state of CoordinatedShutdown's cluster-leave phase.  Keep
			// the entry so WaitForSelfRemoved and external pollers can
			// observe Removed before the actor system tears down.
			//
			// Stale-UID entries at our host:port (prior gekka
			// incarnations the seed has not yet pruned from its gossip
			// view) MUST be dropped here — they are not us, they are
			// zombies that processIncomingGossip's repair guard already
			// rewrote at most ONE slot of.  Surviving zombie slots
			// otherwise loop forever: incoming gossip re-adds them as
			// non-Removed, leader transitions them back to Removed, drop
			// keeps them.  See live-diag run 2 against the production
			// cluster on 127.0.0.1:37777 — that loop is exactly what
			// surfaced gekka's self-down within 1s of receiving Welcome
			// (the stale slots' MemberDowned diff fired the
			// CoordinatedShutdown self-down subscriber before our actual
			// promotion gossip arrived).
			isSelf := a.GetHostname() == localHost &&
				a.GetPort() == localPort &&
				ua.GetUid() == myUid &&
				ua.GetUid2() == myUid2
			cm.recordTombstoneLocked(a.GetHostname(), a.GetPort())
			if isSelf {
				kept = append(kept, m)
				continue
			}
		}
		dropped = true
	}
	if !dropped {
		return false
	}
	// Re-slice so callers iterating cm.State.Members afterwards see the
	// pruned set; the underlying array beyond len(kept) may still alias
	// the dropped pointers, which is fine because gossip serialisation
	// uses the slice header length.
	cm.State.Members = kept

	// Strip orphan reachability references.  Pekko's ClusterMessageSerializer
	// builds its outgoing-gossip address-to-hash map from members only and
	// will throw IllegalArgumentException ("Unknown address ... in cluster
	// message") if any ObserverReachability entry — Observer or any
	// SubjectReachability subject — references an address whose Member has
	// been dropped.  Akka's Reachability invariant is the same: reachability
	// records may only reference current members.  Without this scrub, Pekko
	// nodes downstream of gekka emit one ERROR per Removed peer per gossip
	// tick.
	cm.pruneOrphanReachabilityLocked()
	return true
}

// pruneOrphanReachabilityLocked drops every ObserverReachability entry whose
// Observer AddressIndex no longer maps to a present member, drops each
// SubjectReachability whose Subject AddressIndex no longer maps to a present
// member, and strips Overview.Seen of any address index that no longer maps
// to a present member.  Must be called with cm.Mu held and only after
// cm.State.Members has been pruned to its final shape.
//
// Pekko's ClusterMessageSerializer.gossipToProto rejects any GossipEnvelope
// whose reachability OR seen-set references a UniqueAddress not in the
// derived-from-members address index, throwing IllegalArgumentException at
// ClusterMessageSerializer.scala:403 (mapWithErrorMessage).  Two distinct
// closures inside gossipToProto trip this: $anonfun$gossipToProto$6 (line
// 486, reachability) and $anonfun$gossipToProto$9 (line 502, seen set).
// This helper restores Akka's Gossip invariant that both data structures
// only reference current members.
func (cm *ClusterManager) pruneOrphanReachabilityLocked() {
	if cm.State.Overview == nil {
		return
	}

	// Build the set of address indices that still correspond to a present
	// member.  An index is "present" iff some Member.AddressIndex equals it.
	presentIdx := make(map[int32]struct{}, len(cm.State.Members))
	for _, m := range cm.State.Members {
		presentIdx[m.GetAddressIndex()] = struct{}{}
	}

	// 1) ObserverReachability + nested SubjectReachability.
	if len(cm.State.Overview.ObserverReachability) > 0 {
		keptObservers := cm.State.Overview.ObserverReachability[:0]
		for _, obs := range cm.State.Overview.ObserverReachability {
			if _, ok := presentIdx[obs.GetAddressIndex()]; !ok {
				// Observer itself was Removed — drop the whole record.
				continue
			}
			keptSubjects := obs.SubjectReachability[:0]
			for _, sub := range obs.SubjectReachability {
				if _, ok := presentIdx[sub.GetAddressIndex()]; !ok {
					continue
				}
				keptSubjects = append(keptSubjects, sub)
			}
			obs.SubjectReachability = keptSubjects
			// Keep observer even if its subject list is now empty: Pekko
			// tolerates that, and the empty record may legitimately come
			// back via a later updateReachability call.
			keptObservers = append(keptObservers, obs)
		}
		cm.State.Overview.ObserverReachability = keptObservers
	}

	// 2) Seen set.  Pekko's gossipToProto closure 9 maps each Seen entry
	//    through allHashesByAddress; an orphan trips
	//    "Unknown address ... in cluster message".
	if len(cm.State.Overview.Seen) > 0 {
		keptSeen := cm.State.Overview.Seen[:0]
		for _, idx := range cm.State.Overview.Seen {
			if _, ok := presentIdx[idx]; !ok {
				continue
			}
			keptSeen = append(keptSeen, idx)
		}
		cm.State.Overview.Seen = keptSeen
	}
}

// pruneGossipTombstonesLocked removes tombstone entries older than
// PruneGossipTombstonesAfter and strips the corresponding Removed members
// from the gossip state. Must be called with cm.Mu held.
func (cm *ClusterManager) pruneGossipTombstonesLocked() {
	ttl := cm.PruneGossipTombstonesAfter
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	if len(cm.tombstones) == 0 {
		return
	}

	now := time.Now()
	var pruneKeys []string
	for key, removedAt := range cm.tombstones {
		if now.Sub(removedAt) >= ttl {
			pruneKeys = append(pruneKeys, key)
		}
	}
	if len(pruneKeys) == 0 {
		return
	}

	// Build a set of address keys to prune from gossip state.
	pruneSet := make(map[string]struct{}, len(pruneKeys))
	for _, k := range pruneKeys {
		pruneSet[k] = struct{}{}
		delete(cm.tombstones, k)
	}

	// Remove pruned members from State.Members.
	var kept []*gproto_cluster.Member
	for _, m := range cm.State.Members {
		if int(m.GetAddressIndex()) < len(cm.State.AllAddresses) {
			ua := cm.State.AllAddresses[m.GetAddressIndex()]
			addr := ua.GetAddress()
			key := fmt.Sprintf("%s:%d", addr.GetHostname(), addr.GetPort())
			if _, shouldPrune := pruneSet[key]; shouldPrune && m.GetStatus() == gproto_cluster.MemberStatus_Removed {
				if cm.LogInfoVerbose {
					logger.Default().Debug("Cluster: pruning tombstone (age exceeded)", slog.String("key", key), slog.Duration("ttl", ttl))
				}
				continue
			}
		}
		kept = append(kept, m)
	}
	if len(kept) != len(cm.State.Members) {
		cm.State.Members = kept
		// See dropRemovedMembersLocked for rationale: orphaned reachability
		// refs trip Pekko's ClusterMessageSerializer.
		cm.pruneOrphanReachabilityLocked()
	}
}

func (cm *ClusterManager) CheckReachability() {
	cm.Mu.Lock()
	addresses := make([]*gproto_cluster.UniqueAddress, len(cm.State.AllAddresses))
	copy(addresses, cm.State.AllAddresses)
	localHost := cm.LocalAddress.GetAddress().GetHostname()
	localPort := cm.LocalAddress.GetAddress().GetPort()
	localUid64 := uint64(cm.LocalAddress.GetUid()) | (uint64(cm.LocalAddress.GetUid2()) << 32)
	// Build a set of address indices currently referenced by some member
	// in a membership-active status.  Reachability writes will be gated on
	// this set: an Address that is no longer referenced by a Up/WeaklyUp/
	// Joining member must not receive a fresh ObserverReachability entry,
	// otherwise every CheckReachability tick re-introduces an orphan
	// reference to a since-Removed member and Pekko's
	// ClusterMessageSerializer.gossipToProto throws
	// "Unknown address ... in cluster message" when it tries to forward
	// our gossip.  Akka's Gossip invariant requires reachability records
	// to reference only current members.
	activeIdx := make(map[int32]struct{}, len(cm.State.Members))
	for _, m := range cm.State.Members {
		switch m.GetStatus() {
		case gproto_cluster.MemberStatus_Up,
			gproto_cluster.MemberStatus_WeaklyUp,
			gproto_cluster.MemberStatus_Joining:
			activeIdx[m.GetAddressIndex()] = struct{}{}
		}
	}
	cm.Mu.Unlock()

	for i, addr := range addresses {
		a := addr.GetAddress()
		uid64 := uint64(addr.GetUid()) | (uint64(addr.GetUid2()) << 32)

		// Never run the failure detector against ourselves — the local node is
		// always reachable by definition and has no heartbeat history in the FD.
		if a.GetHostname() == localHost && a.GetPort() == localPort && uid64 == localUid64 {
			continue
		}

		// Skip addresses whose member is no longer membership-active.
		// AllAddresses retains entries for Removed/Leaving/Exiting/Down
		// members so historical reachability records remain decodable,
		// but writing FRESH reachability state about a departed member
		// just creates the very orphan refs that gossipToProto rejects.
		if _, active := activeIdx[int32(i)]; !active {
			continue
		}

		key := fmt.Sprintf("%s:%d-%d", a.GetHostname(), a.GetPort(), uid64)
		phi := cm.Fd.Phi(key)

		if cm.LogInfoVerbose {
			logger.Default().Debug("Cluster: failure-detector phi", slog.String("host", a.GetHostname()), slog.Int("port", int(a.GetPort())), slog.Float64("phi", phi), slog.Float64("threshold", cm.Fd.threshold))
		}

		// Pekko Cluster logic: update ObserverReachability.
		// Cross-DC targets get an additive `acceptable-heartbeat-pause` grace
		// window via IsTargetAvailable; intra-DC targets use the standard
		// phi-only IsAvailable check.
		if !cm.IsTargetAvailable(key, a) {
			// Mark as UNREACHABLE
			logger.Default().Debug("cluster: failure detector marked node UNREACHABLE", "key", key, "phi", phi)
			if cm.MissEmitter != nil {
				remoteKey := fmt.Sprintf("%s:%d", a.GetHostname(), a.GetPort())
				cm.MissEmitter.EmitHeartbeatMiss(remoteKey, phi)
			}
			cm.Mu.Lock()
			cm.updateReachability(int32(i), gproto_cluster.ReachabilityStatus_Unreachable)
			cm.Mu.Unlock()
		} else if phi < cm.Fd.threshold {
			// Mark as REACHABLE if it was previously unreachable
			cm.Mu.Lock()
			cm.updateReachability(int32(i), gproto_cluster.ReachabilityStatus_Reachable)
			cm.Mu.Unlock()
		}
	}

	// Internal SBR strategy check (static-quorum primitive).
	// Consult the strategy only after the unreachable set has been stable for
	// SBRStableAfter, preventing spurious downing during transient partitions.
	if cm.SBRStrategy != nil {
		cm.checkInternalSBR()
	}
}

// checkInternalSBR evaluates the internal SBR strategy after the stable-after
// period and calls LeaveCluster when the strategy returns Down.
func (cm *ClusterManager) checkInternalSBR() {
	stableAfter := cm.SBRStableAfter
	if stableAfter <= 0 {
		stableAfter = 20 * time.Second
	}

	allMembers, unreachableMembers := cm.collectSBRMembersLocked()

	cm.Mu.Lock()
	if len(unreachableMembers) > 0 {
		if cm.sbrUnreachableSince.IsZero() {
			cm.sbrUnreachableSince = time.Now()
		}
	} else {
		cm.sbrUnreachableSince = time.Time{} // reset: all members reachable again
		cm.Mu.Unlock()
		return
	}
	elapsed := time.Since(cm.sbrUnreachableSince)
	cm.Mu.Unlock()

	if elapsed < stableAfter {
		return // partition too recent — wait for stability
	}

	action := cm.SBRStrategy.Decide(allMembers, unreachableMembers)
	if cm.LogInfo {
		logger.Default().Info("SBR(internal): decision",
			slog.Any("action", action),
			slog.Int("reachable", len(allMembers)-len(unreachableMembers)),
			slog.Int("total", len(allMembers)))
	}

	if action == icluster.Down {
		logger.Default().Warn("SBR(internal): downing self — partition below static quorum")
		if err := cm.LeaveCluster(); err != nil {
			logger.Default().Error("SBR(internal): LeaveCluster error", slog.Any("err", err))
		}
	}
}

// collectSBRMembersLocked reads the gossip state and returns two slices:
// all Up/WeaklyUp members and the unreachable subset.
// Safe to call without holding Mu (acquires read lock internally).
func (cm *ClusterManager) collectSBRMembersLocked() (all []icluster.Member, unreachable []icluster.Member) {
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()

	state := cm.State

	// Find the local node's address index. The local observer's
	// ObserverReachability records are keyed by THIS index — assuming 0
	// only happens to work when the local node is the first joiner; for
	// later joiners (e.g. Go joining a Scala seed) the local index is
	// non-zero and a hardcoded 0 silently misses every reachability
	// observation, leaving SBR blind to unreachable peers.
	localHost := cm.LocalAddress.Address.GetHostname()
	localPort := cm.LocalAddress.Address.GetPort()
	localIdx := int32(-1)
	for i, ua := range state.AllAddresses {
		if ua.GetAddress().GetHostname() == localHost && ua.GetAddress().GetPort() == localPort {
			localIdx = int32(i)
			break
		}
	}

	unreachableIdx := make(map[int32]struct{})
	if state.Overview != nil && localIdx >= 0 {
		for _, obs := range state.Overview.ObserverReachability {
			if obs.GetAddressIndex() != localIdx {
				continue
			}
			for _, sub := range obs.SubjectReachability {
				if sub.GetStatus() == gproto_cluster.ReachabilityStatus_Unreachable {
					unreachableIdx[sub.GetAddressIndex()] = struct{}{}
				}
			}
		}
	}

	for _, mem := range state.Members {
		st := mem.GetStatus()
		if st != gproto_cluster.MemberStatus_Up && st != gproto_cluster.MemberStatus_WeaklyUp {
			continue
		}
		addrIdx := mem.GetAddressIndex()
		ua := state.AllAddresses[addrIdx]
		a := ua.GetAddress()
		roles := make([]string, 0, len(mem.GetRolesIndexes()))
		for _, idx := range mem.GetRolesIndexes() {
			if int(idx) < len(state.AllRoles) {
				roles = append(roles, state.AllRoles[idx])
			}
		}
		appVer := ""
		if idx := mem.GetAppVersionIndex(); idx >= 0 && int(idx) < len(state.AllAppVersions) {
			appVer = state.AllAppVersions[idx]
		}
		m := icluster.Member{
			Host:       a.GetHostname(),
			Port:       a.GetPort(),
			Roles:      roles,
			UpNumber:   mem.GetUpNumber(),
			AppVersion: appVer,
		}
		all = append(all, m)
		if _, isUnreachable := unreachableIdx[addrIdx]; isUnreachable {
			unreachable = append(unreachable, m)
		}
	}
	return
}

func (cm *ClusterManager) updateReachability(addrIdx int32, status gproto_cluster.ReachabilityStatus) {
	if cm.State.Overview == nil {
		cm.State.Overview = &gproto_cluster.GossipOverview{}
	}

	localHost := cm.LocalAddress.Address.GetHostname()
	localPort := cm.LocalAddress.Address.GetPort()
	localIdx := int32(-1)
	for i, ua := range cm.State.AllAddresses {
		if ua.GetAddress().GetHostname() == localHost && ua.GetAddress().GetPort() == localPort {
			localIdx = int32(i)
			break
		}
	}
	if localIdx == -1 {
		logger.Default().Warn("Cluster: could not find local address in AllAddresses, using index 0 as fallback")
		localIdx = 0
	}

	// Build the MemberAddress for event publishing (safe: addrIdx is always valid here).
	var ma MemberAddress
	if int(addrIdx) < len(cm.State.AllAddresses) {
		ma = memberAddressFromUA(cm.State.AllAddresses[addrIdx])
	}

	found := false
	for _, r := range cm.State.Overview.ObserverReachability {
		if r.GetAddressIndex() == localIdx {
			for _, s := range r.SubjectReachability {
				if s.GetAddressIndex() == addrIdx {
					if s.GetStatus() != status {
						oldStatus := s.GetStatus()
						s.Status = status.Enum()
						s.Version = proto.Int64(s.GetVersion() + 1)
						// Fix: use lock-held variant — caller holds cm.Mu (write).
						cm.incrementVersionWithLockHeld()
						// publishEvent is safe here: only acquires cm.SubMu, not cm.Mu.
						if status == gproto_cluster.ReachabilityStatus_Unreachable {
							cm.publishEvent(UnreachableMember{Member: ma})
						} else if oldStatus == gproto_cluster.ReachabilityStatus_Unreachable {
							cm.publishEvent(ReachableMember{Member: ma})
						}
					}
					found = true
					break
				}
			}
			if !found {
				r.SubjectReachability = append(r.SubjectReachability, &gproto_cluster.SubjectReachability{
					AddressIndex: proto.Int32(addrIdx),
					Status:       status.Enum(),
					Version:      proto.Int64(1),
				})
				cm.incrementVersionWithLockHeld()
				if status == gproto_cluster.ReachabilityStatus_Unreachable {
					cm.publishEvent(UnreachableMember{Member: ma})
				}
			}
			found = true
			break
		}
	}

	if !found {
		cm.State.Overview.ObserverReachability = append(cm.State.Overview.ObserverReachability, &gproto_cluster.ObserverReachability{
			AddressIndex: proto.Int32(localIdx),
			Version:      proto.Int64(1),
			SubjectReachability: []*gproto_cluster.SubjectReachability{
				{
					AddressIndex: proto.Int32(addrIdx),
					Status:       status.Enum(),
					Version:      proto.Int64(1),
				},
			},
		})
		cm.incrementVersionWithLockHeld()
		if status == gproto_cluster.ReachabilityStatus_Unreachable {
			cm.publishEvent(UnreachableMember{Member: ma})
		}
	}
}

func (cm *ClusterManager) gossipTick(runLeaderActions bool) {
	cm.CheckReachability()
	if runLeaderActions {
		cm.performLeaderActions()
	}

	cm.Mu.RLock()
	members := cm.State.Members
	addresses := cm.State.AllAddresses
	allRoles := cm.State.AllRoles
	localIdx := int32(-1)
	localHost := cm.LocalAddress.Address.GetHostname()
	localPort := cm.LocalAddress.Address.GetPort()
	for i, ua := range cm.State.AllAddresses {
		if ua.GetAddress().GetHostname() == localHost && ua.GetAddress().GetPort() == localPort {
			localIdx = int32(i)
			break
		}
	}
	localInSeen := false
	if cm.State.Overview != nil {
		for _, s := range cm.State.Overview.Seen {
			if s == localIdx {
				localInSeen = true
				break
			}
		}
	}
	cm.Mu.RUnlock()

	if len(members) <= 1 {
		return
	}

	// Build a list of candidate indices (excluding self).
	var candidates []int
	for i, m := range members {
		ai := m.GetAddressIndex()
		a := addresses[ai]
		if a.GetAddress().GetHostname() == localHost && a.GetAddress().GetPort() == localPort {
			continue
		}
		candidates = append(candidates, i)
	}
	if len(candidates) == 0 {
		return
	}

	// Gossip different-view probability: prefer nodes whose SeenDigest
	// differs from ours (i.e. they have NOT marked themselves as Seen in
	// our current state version). This speeds convergence.
	targetIdx := candidates[rand.Intn(len(candidates))]
	{
		diffProb := cm.GossipDifferentViewProbability
		if diffProb <= 0 {
			diffProb = 0.8
		}
		reduceAt := cm.ReduceGossipDifferentViewProbability
		if reduceAt <= 0 {
			reduceAt = 400
		}
		if len(members) > reduceAt {
			diffProb *= 0.5
		}

		// Check if the randomly selected target has the same view (is in Seen set).
		cm.Mu.RLock()
		selectedAddrIdx := members[targetIdx].GetAddressIndex()
		targetInSeen := false
		if cm.State.Overview != nil {
			for _, s := range cm.State.Overview.Seen {
				if s == selectedAddrIdx {
					targetInSeen = true
					break
				}
			}
		}
		cm.Mu.RUnlock()

		// If the target has the same view (is in Seen), re-roll with diffProb
		// probability to pick a different-view node instead.
		if targetInSeen && rand.Float64() < diffProb {
			// Try to find a candidate not in Seen.
			cm.Mu.RLock()
			var diffViewCandidates []int
			for _, ci := range candidates {
				ai := members[ci].GetAddressIndex()
				inSeen := false
				if cm.State.Overview != nil {
					for _, s := range cm.State.Overview.Seen {
						if s == ai {
							inSeen = true
							break
						}
					}
				}
				if !inSeen {
					diffViewCandidates = append(diffViewCandidates, ci)
				}
			}
			cm.Mu.RUnlock()
			if len(diffViewCandidates) > 0 {
				targetIdx = diffViewCandidates[rand.Intn(len(diffViewCandidates))]
			}
		}
	}

	addrIdx := members[targetIdx].GetAddressIndex()
	targetAddr := addresses[addrIdx]

	if cm.LogInfoVerbose {
		logger.Default().Debug("Cluster: gossip target selected",
			slog.String("host", targetAddr.GetAddress().GetHostname()),
			slog.Int("port", int(targetAddr.GetAddress().GetPort())),
			slog.Int("candidates", len(candidates)))
	}

	// Cross-DC gossip throttling: skip foreign-DC targets according to
	// CrossDataCenterGossipProbability (default 0.1) and cap the number of
	// foreign-DC nodes used as gossip targets (CrossDataCenterConnections, default 5).
	{
		localDC := cm.LocalDataCenter
		if localDC == "" {
			localDC = "default"
		}
		tmpGossip := &gproto_cluster.Gossip{AllRoles: allRoles}
		targetDC := DataCenterForMember(tmpGossip, members[targetIdx])
		if targetDC != localDC {
			prob := cm.CrossDataCenterGossipProbability
			if prob <= 0 {
				prob = 0.1
			}
			if rand.Float64() >= prob {
				return
			}

			// Cap: only target the first N foreign-DC members (deterministic)
			maxCrossDC := cm.CrossDataCenterConnections
			if maxCrossDC <= 0 {
				maxCrossDC = 5 // Pekko default
			}
			foreignIdx := 0
			for i, m := range members {
				mDC := DataCenterForMember(tmpGossip, m)
				if mDC != localDC {
					if int32(i) == int32(targetIdx) {
						break
					}
					foreignIdx++
				}
			}
			if foreignIdx >= maxCrossDC {
				return
			}
		}
	}

	path := cm.ClusterCorePath(
		cm.LocalAddress.Address.GetSystem(),
		targetAddr.GetAddress().GetHostname(),
		targetAddr.GetAddress().GetPort())

	// If this node has marked itself as Seen (i.e., it has processed and
	// acknowledged the current gossip), send a full GossipEnvelope so the
	// target (especially the Pekko seed) can learn about our Seen state.
	// Pekko's convergence check requires ALL members to be in Overview.Seen
	// before promoting Joining→Up; if we only send GossipStatus, Pekko never
	// receives our Seen info and convergence never completes.
	if localInSeen {
		cm.Mu.RLock()
		statePayload, err := proto.Marshal(cm.State)
		cm.Mu.RUnlock()
		if err == nil {
			compressedGossip, err := gzipCompress(statePayload)
			if err == nil {
				// Use the AllAddresses snapshot captured at the top of
				// gossipTick (line ~3235).  Re-reading cm.State.AllAddresses
				// here races against a concurrent processIncomingGossip that
				// can replace cm.State with a merged Gossip whose
				// AllAddresses is shorter than the snapshot we computed
				// addrIdx against.  The snapshot is consistent with the
				// targetAddr chosen above and is safe to reuse without the
				// lock — slice headers in Go are copy-on-assignment so
				// `addresses` still points at the original backing array.
				env := &gproto_cluster.GossipEnvelope{
					From:             cm.LocalAddress,
					To:               targetAddr,
					SerializedGossip: compressedGossip,
				}
				_ = cm.Router(context.Background(), path, env)
				return
			}
		}
	}

	cm.Mu.RLock()
	version := cm.State.Version
	allHashes := cm.State.AllHashes
	cm.Mu.RUnlock()

	cm.Mu.RLock()
	seenDigest := cm.buildSeenDigest()
	cm.Mu.RUnlock()

	status := &gproto_cluster.GossipStatus{
		From:       cm.LocalAddress,
		AllHashes:  allHashes,
		Version:    version,
		SeenDigest: seenDigest,
	}
	_ = cm.Router(context.Background(), path, status)
}

// StartGossipLoop begins the background gossip process.
func (cm *ClusterManager) StartGossipLoop(ctx context.Context) {
	// Periodic-tasks initial delay (pekko.cluster.periodic-tasks-initial-delay).
	if delay := cm.PeriodicTasksInitialDelay; delay > 0 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}

	interval := cm.GossipInterval
	if interval <= 0 {
		interval = 1 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Leader actions on a separate interval when configured.
	leaderInterval := cm.LeaderActionsInterval
	runLeaderOnGossip := leaderInterval <= 0
	var leaderTicker *time.Ticker
	if !runLeaderOnGossip {
		leaderTicker = time.NewTicker(leaderInterval)
		defer leaderTicker.Stop()
	}

	for {
		if runLeaderOnGossip {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cm.gossipTick(true)
			}
		} else {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cm.gossipTick(false)
			case <-leaderTicker.C:
				cm.performLeaderActions()
			}
		}
	}
}

// StartReaper begins a periodic goroutine that re-evaluates phi for all known
// nodes and publishes unreachable events. This supplements the gossipTick-driven
// CheckReachability by running at a higher frequency.
// Corresponds to pekko.cluster.unreachable-nodes-reaper-interval.
func (cm *ClusterManager) StartReaper(ctx context.Context) {
	interval := cm.UnreachableNodesReaperInterval
	if interval <= 0 {
		return // reaper disabled
	}

	go func() {
		// Honor periodic-tasks initial delay.
		if delay := cm.PeriodicTasksInitialDelay; delay > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cm.CheckReachability()
			}
		}
	}()
}

// StartPublishStatsLoop begins a periodic goroutine that emits a
// CurrentClusterStats event to subscribers at PublishStatsInterval cadence.
// A zero (or negative) interval disables stats publication.
// Corresponds to pekko.cluster.publish-stats-interval.
func (cm *ClusterManager) StartPublishStatsLoop(ctx context.Context) {
	interval := cm.PublishStatsInterval
	if interval <= 0 {
		return // disabled
	}

	go func() {
		// Honor periodic-tasks initial delay.
		if delay := cm.PeriodicTasksInitialDelay; delay > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cm.publishEvent(cm.computeClusterStats())
			}
		}
	}()
}

// computeClusterStats walks the current gossip state and returns the
// per-status member counts plus unreachable count.
func (cm *ClusterManager) computeClusterStats() CurrentClusterStats {
	cm.Mu.RLock()
	state := cm.State
	cm.Mu.RUnlock()

	stats := CurrentClusterStats{}
	if state == nil {
		return stats
	}
	for _, m := range state.Members {
		switch m.GetStatus() {
		case gproto_cluster.MemberStatus_Up:
			stats.Up++
		case gproto_cluster.MemberStatus_Joining, gproto_cluster.MemberStatus_WeaklyUp:
			stats.Joining++
		case gproto_cluster.MemberStatus_Leaving:
			stats.Leaving++
		case gproto_cluster.MemberStatus_Exiting:
			stats.Exiting++
		case gproto_cluster.MemberStatus_Down:
			stats.Down++
		}
	}
	stats.Members = len(state.Members)

	// Unreachable count from the local failure detector (not gossip), so
	// stats reflect this node's current view rather than convergence-delayed
	// reachability.
	if cm.Fd != nil {
		for _, ua := range state.AllAddresses {
			if ua == nil || ua.GetAddress() == nil {
				continue
			}
			key := fmt.Sprintf("%s:%d-%d", ua.GetAddress().GetHostname(), ua.GetAddress().GetPort(), ua.GetUid())
			if !cm.IsTargetAvailable(key, ua.GetAddress()) {
				stats.Unreachable++
			}
		}
	}
	return stats
}

// GetState returns the current gossip state.
func (cm *ClusterManager) GetState() *gproto_cluster.Gossip {
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()
	return cm.State
}

// StartHeartbeat begins sending Heartbeat messages to a specific node (usually the seed).
// Target heartbeating is now handled by a per-target map to support multiple peers.
type heartbeatTask struct {
	cancel context.CancelFunc
}

var (
	heartbeatTasksMu sync.Mutex
	heartbeatTasks   = make(map[string]heartbeatTask)
)

func (cm *ClusterManager) StartHeartbeat(target *gproto_cluster.Address) {
	key := fmt.Sprintf("%s:%d", target.GetHostname(), target.GetPort())

	heartbeatTasksMu.Lock()
	if _, ok := heartbeatTasks[key]; ok {
		heartbeatTasksMu.Unlock()
		// Task already running — do NOT clear heartbeatMuted here.
		// Clearing it unconditionally caused a bug: connectToNewMembers calls
		// StartHeartbeat for already-running peers and was inadvertently unblocking
		// a StopHeartbeat-induced mute, preventing the failure detector from firing.
		return
	}

	// When heartbeats are globally muted (StopHeartbeat was called), do not
	// create new tasks.  This prevents connectToNewMembers from re-creating a
	// deleted task and undoing the mute via the Store(false) below.
	// The mute is cleared explicitly by ResumeHeartbeat (called from
	// Cluster.StartHeartbeat) which sets heartbeatMuted=false first.
	if cm.heartbeatMuted.Load() {
		heartbeatTasksMu.Unlock()
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	heartbeatTasks[key] = heartbeatTask{cancel: cancel}
	heartbeatTasksMu.Unlock()

	// Only clear the muted flag when actually starting a new task.  This is the
	// initial start path (heartbeatMuted is false).
	cm.heartbeatMuted.Store(false)

	go func() {
		// Periodic-tasks initial delay (pekko.cluster.periodic-tasks-initial-delay).
		if delay := cm.PeriodicTasksInitialDelay; delay > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
		}

		interval := cm.EffectiveHeartbeatInterval(target)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		path := cm.HeartbeatPath(target.GetSystem(), target.GetHostname(), target.GetPort())
		var seq int64 = 0

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if cm.heartbeatMuted.Load() {
					continue
				}
				seq++
				hb := &gproto_cluster.Heartbeat{
					From:         toClusterAddress(cm.LocalAddress.Address),
					SequenceNr:   proto.Int64(seq),
					CreationTime: proto.Int64(time.Now().UnixNano() / int64(time.Millisecond)),
				}
				if cm.Router != nil {
					if err := cm.Router(context.Background(), path, hb); err != nil {
						if cm.LogInfoVerbose {
							logger.Default().Debug("Cluster: failed to send heartbeat", slog.String("host", target.GetHostname()), slog.Int("port", int(target.GetPort())), slog.Any("err", err))
						}
					} else if cm.LogInfoVerbose {
						logger.Default().Debug("Cluster: heartbeat sent", slog.String("host", target.GetHostname()), slog.Int("port", int(target.GetPort())), slog.Int64("seq", seq))
					}
				}
			}
		}
	}()
}

// ClearHeartbeatMute explicitly un-mutes heartbeat responses.  Called by
// Cluster.StartHeartbeat before re-creating the heartbeat task so that the
// guard in StartHeartbeat (which skips task creation while muted) is cleared.
func (cm *ClusterManager) ClearHeartbeatMute() {
	cm.heartbeatMuted.Store(false)
}

// StopHeartbeat stops sending heartbeats to simulate failure.
func (cm *ClusterManager) StopHeartbeat(target *gproto_cluster.Address) {
	cm.heartbeatMuted.Store(true)
	key := fmt.Sprintf("%s:%d", target.GetHostname(), target.GetPort())
	heartbeatTasksMu.Lock()
	if t, ok := heartbeatTasks[key]; ok {
		t.cancel()
		delete(heartbeatTasks, key)
	}
	heartbeatTasksMu.Unlock()
}

// GetLocalAddress returns the local unique address.
func (cm *ClusterManager) GetLocalAddress() *gproto_cluster.UniqueAddress {
	return cm.LocalAddress
}

// IsCrossDC reports whether target is a member of a data center that differs
// from this node's LocalDataCenter. Returns false when the target's DC cannot
// be determined from gossip state (treated as intra-DC for safety).
//
// Used by the multi-data-center failure detector to choose between intra-DC
// and cross-DC heartbeat / pause / response-after parameters.
func (cm *ClusterManager) IsCrossDC(target *gproto_cluster.Address) bool {
	if target == nil {
		return false
	}
	localDC := cm.LocalDataCenter
	if localDC == "" {
		localDC = "default"
	}
	cm.Mu.RLock()
	state := cm.State
	cm.Mu.RUnlock()
	if state == nil {
		return false
	}
	host := target.GetHostname()
	port := target.GetPort()
	for _, m := range state.Members {
		idx := m.GetAddressIndex()
		if int(idx) >= len(state.AllAddresses) {
			continue
		}
		ua := state.AllAddresses[idx]
		if ua == nil || ua.GetAddress() == nil {
			continue
		}
		if ua.GetAddress().GetHostname() != host || ua.GetAddress().GetPort() != port {
			continue
		}
		dc := DataCenterForMember(state, m)
		if dc == "" {
			dc = "default"
		}
		return dc != localDC
	}
	return false
}

// EffectiveHeartbeatInterval returns the heartbeat-interval to use for target,
// preferring CrossDCHeartbeatInterval when target is in a foreign DC and the
// cross-DC value is non-zero. Falls back to HeartbeatInterval (then to 1s when
// that is also zero).
func (cm *ClusterManager) EffectiveHeartbeatInterval(target *gproto_cluster.Address) time.Duration {
	if cm.CrossDCHeartbeatInterval > 0 && cm.IsCrossDC(target) {
		return cm.CrossDCHeartbeatInterval
	}
	if cm.HeartbeatInterval > 0 {
		return cm.HeartbeatInterval
	}
	return 1 * time.Second
}

// EffectiveExpectedResponseAfter returns the failure-detector
// firstHeartbeatEstimate (Pekko: `expected-response-after`) to use for target,
// preferring CrossDCExpectedResponseAfter when target is in a foreign DC and
// the cross-DC value is non-zero. Falls back to ExpectedResponseAfter (the
// intra-DC value, plumbed via FailureDetectorConfig) and finally to 1s.
//
// Consumed by handleHeartbeat / handleHeartbeatRsp via
// PhiAccrualFailureDetector.HeartbeatWithEstimate, which seeds the per-node
// detector's history window with the configured estimate on first contact.
// This makes `pekko.cluster.multi-data-center.failure-detector.expected-response-after`
// a live runtime knob instead of a dead assignment.
func (cm *ClusterManager) EffectiveExpectedResponseAfter(target *gproto_cluster.Address) time.Duration {
	if cm.CrossDCExpectedResponseAfter > 0 && cm.IsCrossDC(target) {
		return cm.CrossDCExpectedResponseAfter
	}
	if cm.ExpectedResponseAfter > 0 {
		return cm.ExpectedResponseAfter
	}
	return 1 * time.Second
}

// EffectiveAcceptableHeartbeatPause returns the failure-detector
// `acceptable-heartbeat-pause` margin to apply when evaluating reachability
// of target. Cross-DC targets receive `CrossDCAcceptableHeartbeatPause` (when
// non-zero); intra-DC targets receive 0 (no margin — the standard phi-only
// reachability check stands). Consumed by IsTargetAvailable to grant
// cross-DC nodes a longer tolerance before flipping unreachable, matching
// `pekko.cluster.multi-data-center.failure-detector.acceptable-heartbeat-pause`.
func (cm *ClusterManager) EffectiveAcceptableHeartbeatPause(target *gproto_cluster.Address) time.Duration {
	if cm.CrossDCAcceptableHeartbeatPause > 0 && cm.IsCrossDC(target) {
		return cm.CrossDCAcceptableHeartbeatPause
	}
	return 0
}

// IsTargetAvailable reports reachability for nodeKey, routing cross-DC
// targets through the margin-aware failure-detector check so they tolerate
// up to `CrossDCAcceptableHeartbeatPause` of accrued φ above threshold
// before being marked unreachable. Intra-DC targets keep the unchanged
// phi-only IsAvailable semantics.
func (cm *ClusterManager) IsTargetAvailable(nodeKey string, target *gproto_cluster.Address) bool {
	if cm.Fd == nil {
		return false
	}
	margin := cm.EffectiveAcceptableHeartbeatPause(target)
	if margin > 0 {
		return cm.Fd.IsAvailableWithMargin(nodeKey, margin)
	}
	return cm.Fd.IsAvailable(nodeKey)
}

// GetFailureDetector returns the internal failure detector.
func (cm *ClusterManager) GetFailureDetector() *PhiAccrualFailureDetector {
	return cm.Fd
}

// GetRolesForMember maps the rolesIndexes in a Member to the string roles in the Gossip message.
func GetRolesForMember(gossip *gproto_cluster.Gossip, member *gproto_cluster.Member) []string {
	var roles []string
	if gossip == nil || member == nil {
		return roles
	}
	allRoles := gossip.GetAllRoles()
	for _, idx := range member.GetRolesIndexes() {
		if int(idx) < len(allRoles) {
			roles = append(roles, allRoles[idx])
		}
	}
	return roles
}

// meetsRoleMinNrOfMembersLocked returns true if, for every role listed in
// RoleMinNrOfMembers, the cluster currently has at least that many members
// with that role. Must be called with cm.Mu held.
func (cm *ClusterManager) meetsRoleMinNrOfMembersLocked() bool {
	if len(cm.RoleMinNrOfMembers) == 0 {
		return true
	}

	// Count members per role.
	roleCounts := make(map[string]int)
	allRoles := cm.State.GetAllRoles()
	for _, m := range cm.State.Members {
		for _, idx := range m.GetRolesIndexes() {
			if int(idx) < len(allRoles) {
				roleCounts[allRoles[idx]]++
			}
		}
	}

	for role, minNr := range cm.RoleMinNrOfMembers {
		if roleCounts[role] < minNr {
			return false
		}
	}
	return true
}
