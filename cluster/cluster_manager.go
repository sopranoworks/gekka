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
		Fd:           NewPhiAccrualFailureDetector(8.0, 1000),
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

	// In Pekko Cluster, we usually start with InitJoin.
	// Send a minimal config so Pekko's JoinConfigCompatCheckCluster.check
	// can call getString("pekko.downing-provider-class") without throwing.
	minConfig := proto.String(`pekko.downing-provider-class = "org.apache.pekko.sbr.SplitBrainResolverProvider"`)
	initJoin := &gproto_cluster.InitJoin{CurrentConfig: minConfig}
	return cm.Router(ctx, path, initJoin)
}

// ProceedJoin sends the actual Join message after receiving InitJoinAck
func (cm *ClusterManager) ProceedJoin(ctx context.Context, actorPath string) error {
	join := &gproto_cluster.Join{
		Node:  cm.LocalAddress,
		Roles: []string{"default"},
	}
	log.Printf("Cluster: sending Join to %s", actorPath)
	return cm.Router(ctx, actorPath, join)
}

// LeaveCluster sends a Leave message.
func (cm *ClusterManager) LeaveCluster() error {
	cm.Mu.RLock()
	state := cm.State
	cm.Mu.RUnlock()

	leave := toClusterAddress(cm.LocalAddress.Address)

	// Send to all known members or just the leader/seed. For simplicity, broadcast to all UP members.
	var lastErr error
	for _, m := range state.GetMembers() {
		if m.GetStatus() == gproto_cluster.MemberStatus_Up || m.GetStatus() == gproto_cluster.MemberStatus_WeaklyUp {
			addr := state.GetAllAddresses()[m.GetAddressIndex()]
			path := fmt.Sprintf("pekko://%s@%s:%d/system/cluster/core/daemon",
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
		return cm.handleHeartbeat(payload, manifest)
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

func (cm *ClusterManager) handleHeartbeat(payload []byte, manifest string) error {
	hb := &gproto_cluster.Heartbeat{}
	if err := proto.Unmarshal(payload, hb); err != nil {
		return err
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
	addr := rsp.GetFrom().GetAddress()
	key := fmt.Sprintf("%s:%d-%d", addr.GetHostname(), addr.GetPort(), rsp.GetFrom().GetUid())
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
	cm.addMemberToGossipLocked(joiningNode)
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
// Must be called with cm.Mu held.
func (cm *ClusterManager) addMemberToGossipLocked(joiningAddr *gproto_cluster.UniqueAddress) {
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
			})
			cm.incrementVersionWithLockHeld()
			return
		}
	}

	// New address — append to AllAddresses and create a Member.
	addrIdx := int32(len(cm.State.AllAddresses))
	cm.State.AllAddresses = append(cm.State.AllAddresses, joiningAddr)
	cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
		AddressIndex: proto.Int32(addrIdx),
		Status:       gproto_cluster.MemberStatus_Joining.Enum(),
		UpNumber:     proto.Int32(int32(len(cm.State.Members) + 1)),
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
	cm.Mu.Lock()
	cm.State = welcome.Gossip
	cm.connectToNewMembers(welcome.Gossip)
	cm.Mu.Unlock()
	// Signal that this node has successfully joined a
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

	return cm.processIncomingGossip(gossip)
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

	hasBefore := false
	hasAfter := false

	for k := range allKeys {
		val1 := m1[k]
		val2 := m2[k]
		if val1 < val2 {
			hasBefore = true
		} else if val1 > val2 {
			hasAfter = true
		}
	}

	if hasBefore && hasAfter {
		return ClockConcurrent
	}
	if hasBefore {
		return ClockBefore
	}
	if hasAfter {
		return ClockAfter
	}
	return ClockSame
}

func (cm *ClusterManager) vectorClockToMap(vc *gproto_cluster.VectorClock, hashes []string) map[string]int64 {
	m := make(map[string]int64)
	if vc == nil {
		return m
	}
	for _, v := range vc.Versions {
		idx := v.GetHashIndex()
		if int(idx) >= 0 && int(idx) < len(hashes) {
			m[hashes[idx]] = v.GetTimestamp()
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

	hasBefore := false
	hasAfter := false

	for k := range allKeys {
		val1 := m1[k]
		val2 := m2[k]
		if val1 < val2 {
			hasBefore = true
		} else if val1 > val2 {
			hasAfter = true
		}
	}

	if hasBefore && hasAfter {
		return ClockConcurrent
	}
	if hasBefore {
		return ClockBefore
	}
	if hasAfter {
		return ClockAfter
	}
	return ClockSame
}

// mergeGossipStates merges two concurrent gossip states into one.
// It takes the union of AllAddresses and Members (keeping higher member status
// for duplicates), and the pairwise-max of the VectorClocks.
// Must be called with cm.Mu held (read or write).
func (cm *ClusterManager) mergeGossipStates(local, incoming *gproto_cluster.Gossip) *gproto_cluster.Gossip {
	type addrKey struct {
		host string
		port uint32
	}

	// ── AllAddresses (union, dedup by host:port) ─────────────────────────────
	mergedAddresses := make([]*gproto_cluster.UniqueAddress, 0, len(local.AllAddresses)+len(incoming.AllAddresses))
	addrIndexMap := make(map[addrKey]int32)

	for _, addr := range local.AllAddresses {
		k := addrKey{addr.GetAddress().GetHostname(), addr.GetAddress().GetPort()}
		if _, ok := addrIndexMap[k]; !ok {
			addrIndexMap[k] = int32(len(mergedAddresses))
			mergedAddresses = append(mergedAddresses, addr)
		}
	}
	incomingIdxRemap := make(map[int32]int32, len(incoming.AllAddresses))
	for i, addr := range incoming.AllAddresses {
		k := addrKey{addr.GetAddress().GetHostname(), addr.GetAddress().GetPort()}
		if existIdx, ok := addrIndexMap[k]; ok {
			incomingIdxRemap[int32(i)] = existIdx
		} else {
			newIdx := int32(len(mergedAddresses))
			incomingIdxRemap[int32(i)] = newIdx
			addrIndexMap[k] = newIdx
			mergedAddresses = append(mergedAddresses, addr)
		}
	}

	// ── Members (union, higher status wins for duplicates) ───────────────────
	// Status lifecycle order: Joining < WeaklyUp < Up < Leaving < Exiting < Removed / Down
	statusOrd := map[gproto_cluster.MemberStatus]int{
		gproto_cluster.MemberStatus_Joining:  0,
		gproto_cluster.MemberStatus_WeaklyUp: 1,
		gproto_cluster.MemberStatus_Up:       2,
		gproto_cluster.MemberStatus_Leaving:  3,
		gproto_cluster.MemberStatus_Exiting:  4,
		gproto_cluster.MemberStatus_Removed:  5,
		gproto_cluster.MemberStatus_Down:     6,
	}

	mergedMembers := make([]*gproto_cluster.Member, 0, len(local.Members)+len(incoming.Members))
	memberByMergedIdx := make(map[int32]*gproto_cluster.Member)

	for _, m := range local.Members {
		addr := local.AllAddresses[m.GetAddressIndex()]
		k := addrKey{addr.GetAddress().GetHostname(), addr.GetAddress().GetPort()}
		mergedIdx := addrIndexMap[k]
		newM := proto.Clone(m).(*gproto_cluster.Member)
		newM.AddressIndex = proto.Int32(mergedIdx)
		memberByMergedIdx[mergedIdx] = newM
		mergedMembers = append(mergedMembers, newM)
	}
	for _, m := range incoming.Members {
		mergedIdx := incomingIdxRemap[m.GetAddressIndex()]
		if existing, ok := memberByMergedIdx[mergedIdx]; ok {
			if statusOrd[m.GetStatus()] > statusOrd[existing.GetStatus()] {
				existing.Status = m.Status
			}
		} else {
			newM := proto.Clone(m).(*gproto_cluster.Member)
			newM.AddressIndex = proto.Int32(mergedIdx)
			memberByMergedIdx[mergedIdx] = newM
			mergedMembers = append(mergedMembers, newM)
		}
	}

	// ── AllHashes (union) + VectorClock (pairwise max) ───────────────────────
	mergedHashes := make([]string, 0, len(local.AllHashes)+len(incoming.AllHashes))
	hashIdxMap := make(map[string]int32)
	for _, h := range local.AllHashes {
		if _, ok := hashIdxMap[h]; !ok {
			hashIdxMap[h] = int32(len(mergedHashes))
			mergedHashes = append(mergedHashes, h)
		}
	}
	for _, h := range incoming.AllHashes {
		if _, ok := hashIdxMap[h]; !ok {
			hashIdxMap[h] = int32(len(mergedHashes))
			mergedHashes = append(mergedHashes, h)
		}
	}

	localVCMap := cm.vectorClockToMap(local.Version, local.AllHashes)
	incomingVCMap := cm.vectorClockToMap(incoming.Version, incoming.AllHashes)

	mergedVC := &gproto_cluster.VectorClock{}
	for h, idx := range hashIdxMap {
		t1, t2 := localVCMap[h], incomingVCMap[h]
		maxT := t1
		if t2 > maxT {
			maxT = t2
		}
		if maxT > 0 {
			mergedVC.Versions = append(mergedVC.Versions, &gproto_cluster.VectorClock_Version{
				HashIndex: proto.Int32(idx),
				Timestamp: proto.Int64(maxT),
			})
		}
	}

	return &gproto_cluster.Gossip{
		AllAddresses: mergedAddresses,
		Members:      mergedMembers,
		AllHashes:    mergedHashes,
		Version:      mergedVC,
		Overview:     &gproto_cluster.GossipOverview{},
	}
}

func (cm *ClusterManager) handleGossipStatus(payload []byte, manifest string) error {
	status := &gproto_cluster.GossipStatus{}
	if err := proto.Unmarshal(payload, status); err != nil {
		return err
	}
	log.Printf("Cluster: received GossipStatus from %v", status.GetFrom())

	addr := status.GetFrom().GetAddress()
	system := cm.LocalAddress.GetAddress().GetSystem()
	path := cm.ClusterCorePath(system, addr.GetHostname(), addr.GetPort())

	cm.Mu.RLock()
	m1 := cm.vectorClockToMap(cm.State.Version, cm.State.AllHashes)
	m2 := cm.vectorClockToMap(status.Version, status.AllHashes)
	ordering := cm.compareResolvedClocks(m1, m2)
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

func (cm *ClusterManager) processIncomingGossip(gossip *gproto_cluster.Gossip) error {
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

	for _, addr := range gossip.GetAllAddresses() {
		key := fmt.Sprintf("%s:%d-%d", addr.GetAddress().GetHostname(), addr.GetAddress().GetPort(), addr.GetUid())
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
	localState := cm.State

	for _, m := range gossip.Members {
		if m.GetStatus() == gproto_cluster.MemberStatus_Removed || m.GetStatus() == gproto_cluster.MemberStatus_Down {
			continue
		}

		addr := gossip.AllAddresses[m.GetAddressIndex()]
		if addr.GetAddress().GetHostname() == cm.LocalAddress.Address.GetHostname() &&
			addr.GetAddress().GetPort() == cm.LocalAddress.Address.GetPort() {
			continue // Skip ourselves
		}

		path := cm.ClusterCorePath(
			addr.GetAddress().GetSystem(),
			addr.GetAddress().GetHostname(),
			addr.GetAddress().GetPort())

		// Sending a GossipStatus to standard cluster path triggers a connection if one doesn't exist
		status := &gproto_cluster.GossipStatus{
			From:      cm.LocalAddress,
			AllHashes: localState.AllHashes,
			Version:   localState.Version,
		}

		_ = cm.Router(context.Background(), path, status)
	}
}

func (cm *ClusterManager) incrementVersionWithLockHeld() {
	if cm.State.Version == nil {
		cm.State.Version = &gproto_cluster.VectorClock{}
	}

	myHashStr := fmt.Sprintf("%d", cm.LocalHash)
	myIndex := -1
	for i, h := range cm.State.AllHashes {
		if h == myHashStr {
			myIndex = i
			break
		}
	}

	if myIndex == -1 {
		myIndex = len(cm.State.AllHashes)
		cm.State.AllHashes = append(cm.State.AllHashes, myHashStr)
	}

	found := false
	for _, v := range cm.State.Version.Versions {
		if v.GetHashIndex() == int32(myIndex) {
			v.Timestamp = proto.Int64(v.GetTimestamp() + 1)
			found = true
			break
		}
	}
	if !found {
		cm.State.Version.Versions = append(cm.State.Version.Versions, &gproto_cluster.VectorClock_Version{
			HashIndex: proto.Int32(int32(myIndex)),
			Timestamp: proto.Int64(1),
		})
	}
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

// DetermineLeader selects the leader based on sorted UniqueAddress.
func (cm *ClusterManager) DetermineLeader() *gproto_cluster.UniqueAddress {
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()

	type memberInfo struct {
		ua     *gproto_cluster.UniqueAddress
		status gproto_cluster.MemberStatus
	}
	var available []memberInfo
	for _, m := range cm.State.Members {
		ua := cm.State.AllAddresses[m.GetAddressIndex()]
		if m.GetStatus() != gproto_cluster.MemberStatus_Removed && m.GetStatus() != gproto_cluster.MemberStatus_Down {
			available = append(available, memberInfo{ua: ua, status: m.GetStatus()})
		}
	}

	if len(available) == 0 {
		return nil
	}

	// Sort by Host:Port and UID
	sort.Slice(available, func(i, j int) bool {
		ai, aj := available[i].ua, available[j].ua
		ri, rj := ai.GetAddress(), aj.GetAddress()
		if ri.GetHostname() != rj.GetHostname() {
			return ri.GetHostname() < rj.GetHostname()
		}
		if ri.GetPort() != rj.GetPort() {
			return ri.GetPort() < rj.GetPort()
		}
		return ai.GetUid() < aj.GetUid()
	})

	return available[0].ua
}

func (cm *ClusterManager) performLeaderActions() {
	leader := cm.DetermineLeader()
	if leader == nil {
		return
	}

	// If we are the leader, transition Joining -> Up
	if leader.GetAddress().GetHostname() == cm.LocalAddress.Address.GetHostname() &&
		leader.GetAddress().GetPort() == cm.LocalAddress.Address.GetPort() &&
		leader.GetUid() == uint32(cm.LocalAddress.GetUid()&0xFFFFFFFF) {

		cm.Mu.Lock()
		var events []ClusterDomainEvent
		changed := false
		for _, m := range cm.State.Members {
			ua := cm.State.AllAddresses[m.GetAddressIndex()]
			ma := memberAddressFromUA(ua)
			switch m.GetStatus() {
			case gproto_cluster.MemberStatus_Joining:
				m.Status = gproto_cluster.MemberStatus_Up.Enum()
				m.UpNumber = proto.Int32(m.GetUpNumber() + 1)
				events = append(events, MemberUp{Member: ma})
				if cm.Metrics != nil {
					cm.Metrics.IncrementMemberUp()
				}
				changed = true
				log.Printf("Leader: transitioned member %d Joining → Up", m.GetAddressIndex())
			case gproto_cluster.MemberStatus_Leaving:
				m.Status = gproto_cluster.MemberStatus_Exiting.Enum()
				events = append(events, MemberExited{Member: ma})
				changed = true
				log.Printf("Leader: transitioned member %d Leaving → Exiting", m.GetAddressIndex())
			case gproto_cluster.MemberStatus_Exiting:
				m.Status = gproto_cluster.MemberStatus_Removed.Enum()
				events = append(events, MemberRemoved{Member: ma})
				if cm.Metrics != nil {
					cm.Metrics.IncrementMemberRemoved()
				}
				changed = true
				log.Printf("Leader: transitioned member %d Exiting → Removed", m.GetAddressIndex())
			}
		}
		if changed {
			// Use the lock-held variant — we already hold cm.Mu (write).
			// The former cm.incrementVersion() call was a deadlock bug.
			cm.incrementVersionWithLockHeld()
		}
		cm.Mu.Unlock()

		// Publish events outside the lock so a slow subscriber can't stall
		// the gossip loop.
		for _, evt := range events {
			cm.publishEvent(evt)
		}
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
func (cm *ClusterManager) StartHeartbeat(target *gproto_cluster.Address) {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()

	if cm.CancelHeartbeat != nil {
		cm.CancelHeartbeat()
	}

	ctx, cancel := context.WithCancel(context.Background())
	cm.CancelHeartbeat = cancel

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
	cm.Mu.Lock()
	defer cm.Mu.Unlock()
	if cm.CancelHeartbeat != nil {
		cm.CancelHeartbeat()
		cm.CancelHeartbeat = nil
	}
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

// CheckConvergence returns true if all UP/LEAVING members have seen the current gossip state.
func (cm *ClusterManager) CheckConvergence() bool {
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()

	state := cm.State
	if state == nil || state.Overview == nil {
		return false
	}

	seenSet := make(map[int32]bool)
	for _, idx := range state.Overview.Seen {
		seenSet[idx] = true
	}

	// We also consider ourselves as having seen our own state.
	// Wait, we need to find our own address index.
	ourIdx := int32(-1)
	for i, addr := range state.AllAddresses {
		if addr.GetAddress().GetHostname() == cm.LocalAddress.Address.GetHostname() &&
			addr.GetAddress().GetPort() == cm.LocalAddress.Address.GetPort() {
			ourIdx = int32(i)
			break
		}
	}
	if ourIdx != -1 {
		seenSet[ourIdx] = true
	}

	for _, m := range state.Members {
		st := m.GetStatus()
		if st == gproto_cluster.MemberStatus_Up || st == gproto_cluster.MemberStatus_Leaving {
			if !seenSet[m.GetAddressIndex()] {
				return false
			}
		}
	}

	return true
}

// IsWelcomeReceived returns true once this node has processed a Welcome
// message from the cluster seed — i.e. it is considered "joined".
func (cm *ClusterManager) IsWelcomeReceived() bool {
	return cm.WelcomeReceived.Load()
}

// IsUp returns true when the cluster state contains at least one Up member.
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
