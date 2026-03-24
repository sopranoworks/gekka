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

	// Router is a function for sending messages, avoiding a cycle with the root package.
	Router func(ctx context.Context, path string, msg any) error

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

	// Cluster event subscribers — managed by cluster_events.go methods.
	SubMu sync.RWMutex
	Subs  []eventSubscriber
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
			break
		}
		log.Printf("Cluster: adopting Pekko hash %q for local node (was %q)", newHash, oldHash)
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

	log.Printf("Cluster: adopting Pekko hash %q for local node (was %q)", pekkoHash, cm.localHashString())
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
		LocalAddress: local,
		LocalHash:    localHash,
		Router:       router,
		Fd:           NewPhiAccrualFailureDetector(12.0, 1000),
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
	log.Printf("Cluster: initiating join to seed node %s", path)

	// Send a minimal config so the remote's JoinConfigCompatCheckCluster.check
	// can call getString("<proto>.cluster.downing-provider-class") without throwing.
	// The config key prefix must match the remote's actor-system protocol ("pekko" or "akka").
	proto := cm.Proto()
	minConfig := fmt.Sprintf(`%s.cluster.downing-provider-class = "%s.cluster.sbr.SplitBrainResolverProvider"`, proto, proto)
	initJoin := &gproto_cluster.InitJoin{CurrentConfig: &minConfig}
	return cm.Router(ctx, path, initJoin)
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

// ProceedJoin sends the actual Join message after receiving InitJoinAck
func (cm *ClusterManager) ProceedJoin(ctx context.Context, actorPath string) error {
	roles := append([]string{cm.localDCRole()}, cm.localRoles...)
	join := &gproto_cluster.Join{
		Node:  cm.LocalAddress,
		Roles: roles,
	}
	log.Printf("Cluster: sending Join to %s", actorPath)
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
			path := fmt.Sprintf("%s://%s@%s:%d/system/cluster/core/daemon",
				cm.Proto(),
				addr.GetAddress().GetSystem(),
				addr.GetAddress().GetHostname(),
				addr.GetAddress().GetPort())
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
			log.Printf("SBR: marked %s:%d as Down", addr.Host, addr.Port)
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
func (cm *ClusterManager) HandleIncomingClusterMessage(ctx context.Context, payload []byte, manifest string, remoteAddr *gproto_cluster.UniqueAddress) error {
	log.Printf("Cluster: HandleIncomingClusterMessage manifest=%q", manifest)
	switch manifest {
	case "IJ": // InitJoin — we are the seed; reply with InitJoinAck
		if remoteAddr == nil {
			log.Printf("Cluster: InitJoin: no remote address (handshake pending), ignoring")
			return nil
		}
		log.Printf("Cluster: received InitJoin from %v — sending InitJoinAck", remoteAddr.GetAddress())
		ack := &gproto_cluster.InitJoinAck{
			Address:     toClusterAddress(cm.LocalAddress.Address),
			ConfigCheck: &gproto_cluster.ConfigCheck{Type: gproto_cluster.ConfigCheck_CompatibleConfig.Enum()},
		}
		raddr := remoteAddr.GetAddress()
		system := cm.LocalAddress.GetAddress().GetSystem()
		path := cm.ClusterCorePath(system, raddr.GetHostname(), raddr.GetPort())
		return cm.Router(ctx, path, ack)
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
		log.Printf("Cluster: received Leave from %s:%d", leave.GetHostname(), leave.GetPort())
		cm.Mu.Lock()
		cm.markMemberLeavingLocked(leave)
		cm.Mu.Unlock()
		return nil
	default:
		log.Printf("Cluster: unknown manifest %q", manifest)
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
	log.Printf("Cluster: received Join from %v", joiningNode)

	// Add the joining node to our gossip state as Joining, then send Welcome with
	// the updated state.  The gossip loop's performLeaderActions will transition
	// Joining → Up on the next tick.
	cm.Mu.Lock()
	cm.addMemberToGossipLocked(joiningNode, join.GetRoles())
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

	log.Printf("Cluster: sending Welcome to %s", path)
	return cm.Router(context.Background(), path, welcome)
}

// addMemberToGossipLocked adds a joining node to the gossip Members list (as Joining).
// roles are stored into AllRoles / RolesIndexes so that OldestNodeInDC works.
// Must be called with cm.Mu held.
func (cm *ClusterManager) addMemberToGossipLocked(joiningAddr *gproto_cluster.UniqueAddress, roles []string) {
	roleIdxs := cm.upsertRolesLocked(roles)

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
			cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
				AddressIndex: proto.Int32(int32(i)),
				Status:       gproto_cluster.MemberStatus_Joining.Enum(),
				UpNumber:     proto.Int32(int32(len(cm.State.Members) + 1)),
				RolesIndexes: roleIdxs,
			})
			cm.ensureLocalHashInAllHashesLocked()
			cm.incrementVersionWithLockHeld()
			return
		}
	}

	// New address — append to AllAddresses and create a Member.
	addrIdx := int32(len(cm.State.AllAddresses))
	cm.State.AllAddresses = append(cm.State.AllAddresses, joiningAddr)
	cm.State.AllHashes = append(cm.State.AllHashes, fmt.Sprintf("%d", joiningAddr.GetUid()))
	cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
		AddressIndex: proto.Int32(addrIdx),
		Status:       gproto_cluster.MemberStatus_Joining.Enum(),
		UpNumber:     proto.Int32(int32(len(cm.State.Members) + 1)),
		RolesIndexes: roleIdxs,
	})
	cm.ensureLocalHashInAllHashesLocked()
	cm.incrementVersionWithLockHeld()
}

// mergeSeenLocked unions the incoming gossip's Overview.Seen into the local state.
// Must be called with cm.Mu held (write).
func (cm *ClusterManager) mergeSeenLocked(incoming *gproto_cluster.Gossip) {
	if incoming.GetOverview() == nil {
		return
	}
	if cm.State.Overview == nil {
		cm.State.Overview = &gproto_cluster.GossipOverview{}
	}
	existing := make(map[int32]bool, len(cm.State.Overview.Seen))
	for _, s := range cm.State.Overview.Seen {
		existing[s] = true
	}
	for _, s := range incoming.GetOverview().GetSeen() {
		if !existing[s] {
			cm.State.Overview.Seen = append(cm.State.Overview.Seen, s)
			existing[s] = true
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
				log.Printf("Cluster: marked %s:%d as Leaving", leaveAddr.GetHostname(), leaveAddr.GetPort())
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
	log.Printf("Cluster: welcomed by %v", welcome.GetFrom())

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
	log.Printf("Cluster: received GossipEnvelope from %v", envelope.GetFrom())

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

	log.Printf("Cluster: handleGossipStatus from %v, ordering=%v localHashes=%v statusHashes=%v", status.GetFrom(), ordering, cm.State.AllHashes, status.AllHashes)

	var statePayload []byte
	var localVersion *gproto_cluster.VectorClock
	var localHashes []string
	if ordering == ClockAfter || ordering == ClockConcurrent {
		statePayload, _ = proto.Marshal(cm.State)
	}
	if ordering == ClockBefore {
		localVersion = cm.State.Version
		localHashes = cm.State.AllHashes
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

	if ordering == ClockBefore {
		// Their state is strictly newer: send our GossipStatus so the partner
		// knows to reply with its full GossipEnvelope.
		myStatus := &gproto_cluster.GossipStatus{
			From:      cm.LocalAddress,
			AllHashes: localHashes,
			Version:   localVersion,
		}
		return cm.Router(context.Background(), path, myStatus)
	}

	return nil
}

func (cm *ClusterManager) processIncomingGossip(gossip *gproto_cluster.Gossip, remoteAddr *gproto_cluster.UniqueAddress) error {
	cm.Mu.Lock()

	m1 := cm.vectorClockToMap(cm.State.Version, cm.State.AllHashes)
	m2 := cm.vectorClockToMap(gossip.Version, gossip.AllHashes)
	ordering := cm.compareResolvedClocks(m1, m2)

	var events []ClusterDomainEvent
	if ordering == ClockBefore {
		// Incoming is newer — diff before replacing so we can emit events.
		log.Printf("Cluster: received newer Gossip, replacing local state")
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
		log.Printf("Cluster: received concurrent Gossip, merging")
		merged := cm.mergeGossipStates(cm.State, gossip)
		events = diffGossipMembers(cm.State, merged)
		cm.State = merged
		// Adopt Pekko's hash for our own address if the incoming gossip carries it.
		// This must happen before incrementVersionWithLockHeld so the new hash is used.
		cm.adoptHashFromGossipLocked(gossip)
		cm.incrementVersionWithLockHeld()
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

	// Publish events outside the lock so slow subscribers can't stall gossip.
	for _, evt := range events {
		cm.publishEvent(evt)
	}
	return nil
}

// connectToNewMembers must be called with cm.Mu held (read or write).
func (cm *ClusterManager) connectToNewMembers(gossip *gproto_cluster.Gossip) {
	for _, m := range gossip.Members {
		if m.GetStatus() == gproto_cluster.MemberStatus_Removed || m.GetStatus() == gproto_cluster.MemberStatus_Down {
			continue
		}

		addr := gossip.AllAddresses[m.GetAddressIndex()]
		if addr.GetAddress().GetHostname() == cm.LocalAddress.Address.GetHostname() &&
			addr.GetAddress().GetPort() == cm.LocalAddress.Address.GetPort() {
			continue // Skip ourselves
		}

		// Only initiate heartbeats if we are not already doing so.
		// StartHeartbeat now supports multiple targets.
		cm.StartHeartbeat(addr.GetAddress())
	}
}

func (cm *ClusterManager) CheckConvergence() bool {
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()

	if cm.State.Overview == nil || len(cm.State.Overview.Seen) == 0 {
		return true // single node
	}

	// All UP/LEAVING members must have seen the current state
	upMembers := make(map[int32]bool)
	for i, m := range cm.State.Members {
		if m.GetStatus() == gproto_cluster.MemberStatus_Up || m.GetStatus() == gproto_cluster.MemberStatus_Leaving {
			upMembers[int32(i)] = true
		}
	}

	for _, seenIdx := range cm.State.Overview.Seen {
		delete(upMembers, seenIdx)
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
		log.Printf("DC-Leader[%s]: transitioned member %s:%d Joining → Up", dc, ma.Host, ma.Port)
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
		cm.Mu.Lock()
		var events []ClusterDomainEvent
		changed := false
		for _, m := range cm.State.Members {
			ua := cm.State.AllAddresses[m.GetAddressIndex()]
			ma := memberAddressFromUA(ua)
			switch m.GetStatus() {
			case gproto_cluster.MemberStatus_Joining:
				m.Status = gproto_cluster.MemberStatus_Up.Enum()
				m.UpNumber = proto.Int32(int32(len(cm.State.Members))) // Simplified upNumber
				events = append(events, MemberUp{Member: ma})
				if cm.Metrics != nil {
					cm.Metrics.IncrementMemberUp()
				}
				changed = true
				log.Printf("Leader: transitioned member %s:%d Joining → Up (upNumber=%d)", ma.Host, ma.Port, m.GetUpNumber())
			case gproto_cluster.MemberStatus_Leaving:
				m.Status = gproto_cluster.MemberStatus_Exiting.Enum()
				events = append(events, MemberExited{Member: ma})
				changed = true
				log.Printf("Leader: transitioned member %s:%d Leaving → Exiting", ma.Host, ma.Port)
			case gproto_cluster.MemberStatus_Exiting:
				m.Status = gproto_cluster.MemberStatus_Removed.Enum()
				events = append(events, MemberRemoved{Member: ma})
				if cm.Metrics != nil {
					cm.Metrics.IncrementMemberRemoved()
				}
				changed = true
				log.Printf("Leader: transitioned member %s:%d Exiting → Removed", ma.Host, ma.Port)
			case gproto_cluster.MemberStatus_Down:
				m.Status = gproto_cluster.MemberStatus_Removed.Enum()
				events = append(events, MemberRemoved{Member: ma})
				if cm.Metrics != nil {
					cm.Metrics.IncrementMemberRemoved()
				}
				changed = true
				log.Printf("Leader: transitioned member %s:%d Down → Removed", ma.Host, ma.Port)
			}
		}
		if changed {
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

		// Pekko Cluster logic: update ObserverReachability
		if !cm.Fd.IsAvailable(key) {
			// Mark as UNREACHABLE
			log.Printf("FailureDetector: node %s is UNREACHABLE (phi=%.2f)", key, phi)
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
	log.Printf("SBR(internal): action=%v reachable=%d total=%d",
		action, len(allMembers)-len(unreachableMembers), len(allMembers))

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

	unreachableIdx := make(map[int32]struct{})
	if state.Overview != nil {
		for _, obs := range state.Overview.ObserverReachability {
			if obs.GetAddressIndex() != 0 {
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
		m := icluster.Member{
			Host:     a.GetHostname(),
			Port:     a.GetPort(),
			Roles:    roles,
			UpNumber: mem.GetUpNumber(),
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

func (cm *ClusterManager) gossipTick() {
	cm.CheckReachability()
	cm.performLeaderActions()

	cm.Mu.RLock()
	members := cm.State.Members
	addresses := cm.State.AllAddresses
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

	// Randomly select a node to gossip with
	targetIdx := rand.Intn(len(members))
	addrIdx := members[targetIdx].GetAddressIndex()
	targetAddr := addresses[addrIdx]

	// If it's us, skip
	if targetAddr.GetAddress().GetHostname() == localHost &&
		targetAddr.GetAddress().GetPort() == localPort {
		return
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

	status := &gproto_cluster.GossipStatus{
		From:      cm.LocalAddress,
		AllHashes: allHashes,
		Version:   version,
	}
	_ = cm.Router(context.Background(), path, status)
}

// StartGossipLoop begins the background gossip process.
func (cm *ClusterManager) StartGossipLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cm.gossipTick()
		}
	}
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
		return // already heartbeating
	}

	ctx, cancel := context.WithCancel(context.Background())
	heartbeatTasks[key] = heartbeatTask{cancel: cancel}
	heartbeatTasksMu.Unlock()

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		path := cm.HeartbeatPath(target.GetSystem(), target.GetHostname(), target.GetPort())
		var seq int64 = 0

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				seq++
				hb := &gproto_cluster.Heartbeat{
					From:         toClusterAddress(cm.LocalAddress.Address),
					SequenceNr:   proto.Int64(seq),
					CreationTime: proto.Int64(time.Now().UnixNano() / int64(time.Millisecond)),
				}
				if cm.Router != nil {
					if err := cm.Router(context.Background(), path, hb); err != nil {
						log.Printf("Cluster: failed to send heartbeat to %v: %v", target, err)
					}
				}
			}
		}
	}()
}

// StopHeartbeat stops sending heartbeats to simulate failure.
func (cm *ClusterManager) StopHeartbeat(target *gproto_cluster.Address) {
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
