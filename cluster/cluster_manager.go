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
	"sync"
	"sync/atomic"
	"time"

	"github.com/sopranoworks/gekka/actor"
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
	LocalHash       int32 // for VectorClock version
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
	if cm.Protocol == "" {
		return "pekko"
	}
	return cm.Protocol
}

// ClusterCorePath builds the actor path to the cluster core daemon on a remote node.
func (cm *ClusterManager) ClusterCorePath(system, host string, port uint32) string {
	return fmt.Sprintf("%s://%s@%s:%d/system/cluster/core/daemon", cm.Proto(), system, host, port)
}

// HeartbeatPath builds the actor path to the heartbeat receiver on a remote node.
func (cm *ClusterManager) HeartbeatPath(system, host string, port uint32) string {
	return fmt.Sprintf("%s://%s@%s:%d/system/cluster/heartbeatReceiver", cm.Proto(), system, host, port)
}

func NewClusterManager(local *gproto_cluster.UniqueAddress, router func(context.Context, string, any) error) *ClusterManager {
	clLocal := local                   // Already a gproto_cluster.UniqueAddress
	localHash := int32(local.GetUid()) // simplified hash as UID
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
			AllHashes:    []string{fmt.Sprintf("%d", localHash)},
			Overview:     &gproto_cluster.GossipOverview{},
			Version: &gproto_cluster.VectorClock{
				Versions: []*gproto_cluster.VectorClock_Version{
					{
						HashIndex: proto.Int32(0),
						Timestamp: proto.Int64(1),
					},
				},
			},
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
	minConfig := proto.String(fmt.Sprintf(`%s.cluster.downing-provider-class = ""`, cm.Proto()))
	initJoin := &gproto_cluster.InitJoin{CurrentConfig: minConfig}
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
	path := cm.HeartbeatPath(addr.GetSystem(), addr.GetHostname(), addr.GetPort())
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
					return // already a member
				}
			}
			cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
				AddressIndex: proto.Int32(int32(i)),
				Status:       gproto_cluster.MemberStatus_Joining.Enum(),
				UpNumber:     proto.Int32(int32(len(cm.State.Members) + 1)),
				RolesIndexes: roleIdxs,
			})
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
	cm.incrementVersionWithLockHeld()
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
	} else if ordering == ClockBefore {
		localVersion = cm.State.Version
		localHashes = cm.State.AllHashes
	}
	cm.Mu.RUnlock()

	if ordering == ClockAfter || ordering == ClockConcurrent {
		// Our state is newer or concurrent, send full GossipEnvelope.
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
			return cm.Router(context.Background(), path, env)
		}
	} else if ordering == ClockBefore {
		// Their state is newer, reply with our GossipStatus so they will send us their GossipEnvelope
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
	log.Printf("Cluster: Comparison - localHashes=%v incomingHashes=%v localMap=%v incomingMap=%v", cm.State.AllHashes, gossip.AllHashes, m1, m2)
	ordering := cm.compareResolvedClocks(m1, m2)

	var events []ClusterDomainEvent
	if ordering == ClockBefore {
		// Incoming is newer — diff before replacing so we can emit events.
		log.Printf("Cluster: received newer Gossip, replacing local state")
		events = diffGossipMembers(cm.State, gossip)
		cm.State = gossip
		cm.connectToNewMembers(gossip)
	} else if ordering == ClockConcurrent {
		// Merge concurrent states: union of members, pairwise-max vector clock.
		log.Printf("Cluster: received concurrent Gossip, merging")
		merged := cm.mergeGossipStates(cm.State, gossip)
		events = diffGossipMembers(cm.State, merged)
		cm.State = merged
		cm.incrementVersionWithLockHeld()
		cm.connectToNewMembers(merged)
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
			merged.AllHashes = append(merged.AllHashes, s2.AllHashes[i])
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
		} else {
			newM := proto.Clone(m2).(*gproto_cluster.Member)
			newM.AddressIndex = proto.Int32(idxMerged)
			merged.Members = append(merged.Members, newM)
			memberMap[idxMerged] = newM
		}
	}

	return merged
}

func (cm *ClusterManager) incrementVersionWithLockHeld() {
	m := cm.vectorClockToMap(cm.State.Version, cm.State.AllHashes)
	localHashStr := fmt.Sprintf("%d", cm.LocalHash)
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

func (cm *ClusterManager) performLeaderActions() {
	leader := cm.DetermineLeader()
	if leader == nil {
		return
	}

	isLeader := leader.GetAddress().GetHostname() == cm.LocalAddress.Address.GetHostname() &&
		leader.GetAddress().GetPort() == cm.LocalAddress.Address.GetPort() &&
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
		log.Printf("Leader is %s:%d-%d, I am %s:%d-%d",
			leader.GetAddress().GetHostname(), leader.GetAddress().GetPort(), leader.GetUid(),
			cm.LocalAddress.GetAddress().GetHostname(), cm.LocalAddress.GetAddress().GetPort(), cm.LocalAddress.GetUid())
	}
}

func (cm *ClusterManager) CheckReachability() {
	cm.Mu.Lock()
	addresses := make([]*gproto_cluster.UniqueAddress, len(cm.State.AllAddresses))
	copy(addresses, cm.State.AllAddresses)
	cm.Mu.Unlock()

	for i, addr := range addresses {
		key := fmt.Sprintf("%s:%d-%d", addr.GetAddress().GetHostname(), addr.GetAddress().GetPort(), addr.GetUid())
		phi := cm.Fd.Phi(key)

		// Pekko Cluster logic: update ObserverReachability
		if phi > cm.Fd.threshold {
			// Mark as UNREACHABLE
			log.Printf("FailureDetector: node %s is UNREACHABLE (phi=%.2f)", key, phi)
			cm.Mu.Lock()
			cm.updateReachability(int32(i), gproto_cluster.ReachabilityStatus_Unreachable)
			cm.Mu.Unlock()
		} else if phi < 1.0 {
			// Mark as REACHABLE if it was previously unreachable
			cm.Mu.Lock()
			cm.updateReachability(int32(i), gproto_cluster.ReachabilityStatus_Reachable)
			cm.Mu.Unlock()
		}
	}
}

func (cm *ClusterManager) updateReachability(addrIdx int32, status gproto_cluster.ReachabilityStatus) {
	if cm.State.Overview == nil {
		cm.State.Overview = &gproto_cluster.GossipOverview{}
	}

	// Build the MemberAddress for event publishing (safe: addrIdx is always valid here).
	var ma MemberAddress
	if int(addrIdx) < len(cm.State.AllAddresses) {
		ma = memberAddressFromUA(cm.State.AllAddresses[addrIdx])
	}

	found := false
	for _, r := range cm.State.Overview.ObserverReachability {
		if r.GetAddressIndex() == 0 { // 0 is us (local address index)
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
			AddressIndex: proto.Int32(0),
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
	version := cm.State.Version
	cm.Mu.RUnlock()

	if len(members) <= 1 {
		return
	}

	// Randomly select a node to gossip with
	targetIdx := rand.Intn(len(members))
	addrIdx := members[targetIdx].GetAddressIndex()
	targetAddr := addresses[addrIdx]

	// If it's us, skip
	if targetAddr.GetAddress().GetHostname() == cm.LocalAddress.Address.GetHostname() &&
		targetAddr.GetAddress().GetPort() == cm.LocalAddress.Address.GetPort() {
		return
	}

	status := &gproto_cluster.GossipStatus{
		From:      cm.LocalAddress,
		AllHashes: cm.State.AllHashes,
		Version:   version,
	}

	path := cm.ClusterCorePath(
		cm.LocalAddress.Address.GetSystem(),
		targetAddr.GetAddress().GetHostname(),
		targetAddr.GetAddress().GetPort())

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
					From:       toClusterAddress(cm.LocalAddress.Address),
					SequenceNr: proto.Int64(seq),
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
