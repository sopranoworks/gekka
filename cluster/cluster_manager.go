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
	"log"
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
	Sys             actor.ActorContext // bridge back to the node's actor system
	WelcomeReceived atomic.Bool
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

	localIsSBR := true  // we always advertise SBR
	_ = localDP

	if remoteIsEmpty && !localIsSBR {
		return false
	}
	if !remoteIsEmpty && !remoteIsSBR {
		// Remote has a non-SBR downing provider — incompatible
		log.Printf("Cluster: config compat check failed: remote downing-provider-class=%q", remoteDP)
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
		slog.Debug("cluster: adopting Pekko hash for local node", "new", newHash, "old", oldHash)
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
	slog.Debug("cluster: adopting Pekko hash for local node", "new", pekkoHash, "old", cm.localHashString())
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
	system := cm.LocalAddress.GetAddress().GetSystem()
	path := cm.ClusterCorePath(system, seedHost, seedPort)
	if cm.LogInfo {
		log.Printf("Cluster: initiating join to seed node %s", path)
	}

	// Send a minimal config so the remote's JoinConfigCompatCheckCluster.check
	// can call getString("<proto>.cluster.downing-provider-class") without throwing.
	// The config key prefix must match the remote's actor-system protocol ("pekko" or "akka").
	proto := cm.Proto()
	minConfig := fmt.Sprintf(`%s.cluster.downing-provider-class = "%s.cluster.sbr.SplitBrainResolverProvider"`, proto, proto)
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
					log.Printf("Cluster: join seed nodes timed out after %v, shutting down", cm.ShutdownAfterUnsuccessfulJoinSeedNodes)
					if cm.ShutdownCallback != nil {
						cm.ShutdownCallback()
					}
					return
				}
				return
			case <-seedDeadline:
				seedDeadline = nil // fire only once
				if !cm.WelcomeReceived.Load() {
					log.Printf("Cluster: seed node %s:%d did not respond within %v (seed-node-timeout)", seedHost, seedPort, seedTimeout)
				}
			case <-ticker.C:
				if cm.WelcomeReceived.Load() {
					return
				}
				if cm.LogInfo {
					log.Printf("Cluster: retrying InitJoin to %s", path)
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
		log.Printf("Cluster: sending Join to %s", actorPath)
	}
	return cm.Router(ctx, actorPath, join)
}

// LeaveCluster sends a Leave message and immediately updates the local gossip
// state to Leaving so that cluster event subscribers observe MemberLeft even
// when the remote leader is unreachable (e.g. during an SBR DownSelf decision).
func (cm *ClusterManager) LeaveCluster() error {
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
				log.Printf("SBR: marked %s:%d as Down", addr.Host, addr.Port)
			}
			return
		}
	}
	log.Printf("SBR: DownMember: address %s:%d not found in gossip", addr.Host, addr.Port)
}

// WaitForSelfRemoved polls the gossip state every 200 ms until this node's
// own membership entry is absent or in Removed status, or the context expires.
// It is called by the coordinated-shutdown cluster-leave phase to block until
// the cluster leader has driven the transition all the way to Removed.
func (cm *ClusterManager) WaitForSelfRemoved(ctx context.Context) error {
	localHost := cm.LocalAddress.GetAddress().GetHostname()
	localPort := cm.LocalAddress.GetAddress().GetPort()

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
	slog.Debug("cluster: HandleIncomingClusterMessage", "manifest", manifest)
	switch manifest {
	case "IJ": // InitJoin — we are the seed; reply with InitJoinAck
		if remoteAddr == nil {
			slog.Debug("cluster: InitJoin: no remote address (handshake pending), ignoring")
			return nil
		}
		slog.Info("cluster: received InitJoin", "from", remoteAddr.GetAddress(), "sender", senderPath)

		// Configuration compatibility check (pekko.cluster.configuration-compatibility-check.enforce-on-join).
		// When enforced, validate the joining node's config against our own.
		if cm.EnforceConfigCompatOnJoin {
			ij := &gproto_cluster.InitJoin{}
			if err := proto.Unmarshal(payload, ij); err == nil && ij.GetCurrentConfig() != "" {
				if !cm.CheckConfigCompat(ij.GetCurrentConfig()) {
					raddr := remoteAddr.GetAddress()
					log.Printf("Cluster: rejecting InitJoin from %s:%d — configuration incompatible", raddr.GetHostname(), raddr.GetPort())
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
		// can parse it without crashing. The key checked unconditionally is
		// pekko.cluster.downing-provider-class (same as the InitJoin fix).
		minConfig := proto.String(`pekko.cluster.downing-provider-class = ""`)
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
		slog.Info("cluster: received Leave", "host", leave.GetHostname(), "port", leave.GetPort())
		cm.Mu.Lock()
		cm.markMemberLeavingLocked(leave)
		cm.Mu.Unlock()
		return nil
	default:
		slog.Debug("cluster: unknown manifest", "manifest", manifest)
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
		cm.Fd.Heartbeat(key)
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
	cm.Fd.Heartbeat(key)
	return nil
}

func (cm *ClusterManager) handleJoin(payload []byte, manifest string) error {
	join := &gproto_cluster.Join{}
	if err := proto.Unmarshal(payload, join); err != nil {
		return err
	}
	joiningNode := join.GetNode()
	if cm.LogInfo {
		log.Printf("Cluster: received Join from %v", joiningNode)
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
		log.Printf("Cluster: sending Welcome to %s", path)
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
				UpNumber:     proto.Int32(int32(len(cm.State.Members) + 1)),
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
		UpNumber:     proto.Int32(int32(len(cm.State.Members) + 1)),
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
					log.Printf("Cluster: marked %s:%d as Leaving", leaveAddr.GetHostname(), leaveAddr.GetPort())
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
		log.Printf("Cluster: welcomed by %v", welcome.GetFrom())
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
		log.Printf("Cluster: received GossipEnvelope from %v", envelope.GetFrom())
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

	slog.Debug("cluster: handleGossipStatus", "from", status.GetFrom(), "ordering", ordering)

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
				log.Printf("Cluster: discarding stale gossip (TTL %v exceeded)", ttl)
			}
			return nil
		}
	}

	m1 := cm.vectorClockToMap(cm.State.Version, cm.State.AllHashes)
	m2 := cm.vectorClockToMap(gossip.Version, gossip.AllHashes)
	ordering := cm.compareResolvedClocks(m1, m2)

	var events []ClusterDomainEvent
	if ordering == ClockBefore {
		// Incoming is newer — diff before replacing so we can emit events.
		if cm.LogInfoVerbose {
			log.Printf("Cluster: received newer Gossip, replacing local state")
		}
		events = diffGossipMembers(cm.State, gossip)
		cm.State = gossip
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
			log.Printf("Cluster: received concurrent Gossip, merging")
		}
		merged := cm.mergeGossipStates(cm.State, gossip)
		events = diffGossipMembers(cm.State, merged)
		cm.State = merged
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
		log.Printf("Cluster: local node is Exiting — sending ExitingConfirmed to %d Up members", len(targets))
	}
	for _, t := range targets {
		if err := cm.Router(context.Background(), t.path, confirmation); err != nil {
			log.Printf("Cluster: ExitingConfirmed to %s failed: %v", t.path, err)
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
			log.Printf("Cluster: convergence check passed (all %d Up/Leaving members in Seen set)", len(cm.State.Members))
		} else {
			log.Printf("Cluster: convergence check failed (%d members not yet in Seen set)", len(upMembers))
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

// DetermineLeader selects the leader based on sorted UniqueAddress.
func (cm *ClusterManager) DetermineLeader() *gproto_cluster.UniqueAddress {
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()

	var upMembers []*gproto_cluster.UniqueAddress
	for _, m := range cm.State.Members {
		if m.GetStatus() == gproto_cluster.MemberStatus_Up || m.GetStatus() == gproto_cluster.MemberStatus_WeaklyUp {
			upMembers = append(upMembers, cm.State.AllAddresses[m.GetAddressIndex()])
		}
	}

	if len(upMembers) == 0 {
		// If no nodes are UP yet, use all members including JOINING
		for _, m := range cm.State.Members {
			upMembers = append(upMembers, cm.State.AllAddresses[m.GetAddressIndex()])
		}
	}

	if len(upMembers) == 0 {
		return nil
	}

	sort.Slice(upMembers, func(i, j int) bool {
		a, b := upMembers[i], upMembers[j]
		if a.GetAddress().GetHostname() != b.GetAddress().GetHostname() {
			return a.GetAddress().GetHostname() < b.GetAddress().GetHostname()
		}
		if a.GetAddress().GetPort() != b.GetAddress().GetPort() {
			return a.GetAddress().GetPort() < b.GetAddress().GetPort()
		}
		return a.GetUid() < b.GetUid()
	})

	return upMembers[0]
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
			log.Printf("DC-Leader[%s]: transitioned member %s:%d Joining → Up", dc, ma.Host, ma.Port)
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
			log.Printf("Leader: performing leader actions (members=%d)", len(cm.State.Members))
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
				if meetsMinMembers && (len(cm.State.Members) == 1 || cm.CheckConvergenceLocked()) {
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
						log.Printf("Leader: transitioned member %s:%d Joining → Up (upNumber=%d)", ma.Host, ma.Port, m.GetUpNumber())
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
							log.Printf("Leader: transitioned member %s:%d Joining → WeaklyUp (convergence timeout %v)", ma.Host, ma.Port, cm.AllowWeaklyUpMembers)
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
						log.Printf("Leader: transitioned member %s:%d WeaklyUp → Up (convergence achieved)", ma.Host, ma.Port)
					}
				}
			case gproto_cluster.MemberStatus_Leaving:
				m.Status = gproto_cluster.MemberStatus_Exiting.Enum()
				events = append(events, MemberExited{Member: ma})
				changed = true
				if cm.LogInfo {
					log.Printf("Leader: transitioned member %s:%d Leaving → Exiting", ma.Host, ma.Port)
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
					log.Printf("Leader: transitioned member %s:%d Exiting → Removed", ma.Host, ma.Port)
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
							log.Printf("Leader: member %s:%d Down, removal delayed by %v", ma.Host, ma.Port, cm.DownRemovalMargin)
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
					log.Printf("Leader: transitioned member %s:%d Down → Removed", ma.Host, ma.Port)
				}
			}
		}
		if changed {
			cm.incrementVersionWithLockHeld()
		}
		// Prune stale tombstones (removed members older than configured TTL).
		cm.pruneGossipTombstonesLocked()
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
					log.Printf("Cluster: pruning tombstone for %s (age > %v)", key, ttl)
				}
				continue
			}
		}
		kept = append(kept, m)
	}
	if len(kept) != len(cm.State.Members) {
		cm.State.Members = kept
	}
}

func (cm *ClusterManager) CheckReachability() {
	cm.Mu.Lock()
	addresses := make([]*gproto_cluster.UniqueAddress, len(cm.State.AllAddresses))
	copy(addresses, cm.State.AllAddresses)
	localHost := cm.LocalAddress.GetAddress().GetHostname()
	localPort := cm.LocalAddress.GetAddress().GetPort()
	localUid64 := uint64(cm.LocalAddress.GetUid()) | (uint64(cm.LocalAddress.GetUid2()) << 32)
	cm.Mu.Unlock()

	for i, addr := range addresses {
		a := addr.GetAddress()
		uid64 := uint64(addr.GetUid()) | (uint64(addr.GetUid2()) << 32)

		// Never run the failure detector against ourselves — the local node is
		// always reachable by definition and has no heartbeat history in the FD.
		if a.GetHostname() == localHost && a.GetPort() == localPort && uid64 == localUid64 {
			continue
		}

		key := fmt.Sprintf("%s:%d-%d", a.GetHostname(), a.GetPort(), uid64)
		phi := cm.Fd.Phi(key)

		if cm.LogInfoVerbose {
			log.Printf("Cluster: failure-detector phi for %s:%d = %.4f (threshold=%.1f)", a.GetHostname(), a.GetPort(), phi, cm.Fd.threshold)
		}

		// Pekko Cluster logic: update ObserverReachability
		if !cm.Fd.IsAvailable(key) {
			// Mark as UNREACHABLE
			slog.Debug("cluster: failure detector marked node UNREACHABLE", "key", key, "phi", phi)
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
		log.Printf("SBR(internal): action=%v reachable=%d total=%d",
			action, len(allMembers)-len(unreachableMembers), len(allMembers))
	}

	if action == icluster.Down {
		log.Printf("SBR(internal): downing self — partition below static quorum")
		if err := cm.LeaveCluster(); err != nil {
			log.Printf("SBR(internal): LeaveCluster error: %v", err)
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
		log.Printf("Cluster: could not find local address in AllAddresses, using index 0 as fallback")
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
		log.Printf("Cluster: gossip target selected %s:%d (candidates=%d)",
			targetAddr.GetAddress().GetHostname(), targetAddr.GetAddress().GetPort(),
			len(candidates))
	}

	// Cross-DC gossip throttling: skip foreign-DC targets according to
	// CrossDataCenterGossipProbability (default 0.1).
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
				cm.Mu.RLock()
				to := cm.State.AllAddresses[addrIdx]
				cm.Mu.RUnlock()
				env := &gproto_cluster.GossipEnvelope{
					From:             cm.LocalAddress,
					To:               to,
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

		interval := cm.HeartbeatInterval
		if interval <= 0 {
			interval = 1 * time.Second
		}
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
							log.Printf("Cluster: failed to send heartbeat to %s:%d: %v", target.GetHostname(), target.GetPort(), err)
						}
					} else if cm.LogInfoVerbose {
						log.Printf("Cluster: heartbeat sent to %s:%d (seq=%d)", target.GetHostname(), target.GetPort(), seq)
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
