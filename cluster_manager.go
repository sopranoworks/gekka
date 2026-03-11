/*
 * cluster_manager.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

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

	"gekka/actor"
	"gekka/cluster"

	"google.golang.org/protobuf/proto"
)

const (
	ClusterSerializerID = 5 // Pekko's ClusterMessageSerializer ID
)

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
	mu              sync.RWMutex
	localAddress    *UniqueAddress
	localHash       int32 // for VectorClock version
	state           *cluster.Gossip
	router          *Router
	fd              *PhiAccrualFailureDetector
	cancelHeartbeat context.CancelFunc
	protocol        string // "pekko" or "akka"; set by GekkaNode.Spawn

	// welcomeReceived is set to true once a Welcome message has been
	// processed — i.e. this node has successfully joined a cluster.
	welcomeReceived atomic.Bool

	// metrics is the shared NodeMetrics instance (set by GekkaNode.Spawn).
	// Nil-safe: all callers check before touching.
	metrics *NodeMetrics

	// Sys is the actor system context used for resolving actor paths.
	Sys actor.ActorContext

	// Cluster event subscribers — managed by cluster_events.go methods.
	subMu sync.RWMutex
	subs  []eventSubscriber
}

// proto returns the configured actor-path scheme, defaulting to "pekko".
func (cm *ClusterManager) proto() string {
	if cm.protocol == "" {
		return "pekko"
	}
	return cm.protocol
}

// clusterCorePath builds the actor path to the cluster core daemon on a remote node.
func (cm *ClusterManager) clusterCorePath(system, host string, port uint32) string {
	return fmt.Sprintf("%s://%s@%s:%d/system/cluster/core/daemon", cm.proto(), system, host, port)
}

// heartbeatPath builds the actor path to the heartbeat receiver on a remote node.
func (cm *ClusterManager) heartbeatPath(system, host string, port uint32) string {
	return fmt.Sprintf("%s://%s@%s:%d/system/cluster/heartbeatReceiver", cm.proto(), system, host, port)
}

func NewClusterManager(local *UniqueAddress, router *Router) *ClusterManager {
	clLocal := toClusterUniqueAddress(local)
	localHash := int32(local.GetUid()) // simplified hash as UID
	return &ClusterManager{
		localAddress: local,
		localHash:    localHash,
		router:       router,
		fd:           NewPhiAccrualFailureDetector(8.0, 1000),
		state: &cluster.Gossip{
			Members: []*cluster.Member{
				{
					AddressIndex: proto.Int32(0),
					Status:       cluster.MemberStatus_Up.Enum(),
					UpNumber:     proto.Int32(1),
				},
			},
			AllAddresses: []*cluster.UniqueAddress{clLocal},
			AllHashes:    []string{fmt.Sprintf("%d", localHash)},
			Overview:     &cluster.GossipOverview{},
			Version: &cluster.VectorClock{
				Versions: []*cluster.VectorClock_Version{
					{
						HashIndex: proto.Int32(0),
						Timestamp: proto.Int64(1),
					},
				},
			},
		},
	}
}

func toClusterAddress(a *Address) *cluster.Address {
	if a == nil {
		return nil
	}
	return &cluster.Address{
		System:   a.System,
		Hostname: a.Hostname,
		Port:     a.Port,
		Protocol: a.Protocol,
	}
}

func toClusterUniqueAddress(ua *UniqueAddress) *cluster.UniqueAddress {
	if ua == nil {
		return nil
	}
	uid := ua.GetUid()
	return &cluster.UniqueAddress{
		Address: toClusterAddress(ua.Address),
		Uid:     proto.Uint32(uint32(uid & 0xFFFFFFFF)),
		Uid2:    proto.Uint32(uint32(uid >> 32)),
	}
}

// JoinCluster initiates the joining protocol to a seed node.
func (cm *ClusterManager) JoinCluster(ctx context.Context, seedHost string, seedPort uint32) error {
	system := cm.localAddress.GetAddress().GetSystem()
	path := cm.clusterCorePath(system, seedHost, seedPort)
	log.Printf("Cluster: initiating join to seed node %s", path)

	// In Pekko Cluster, we usually start with InitJoin.
	// Send a minimal config so Pekko's JoinConfigCompatCheckCluster.check
	// can call getString("pekko.cluster.downing-provider-class") without throwing.
	minConfig := proto.String(`pekko.cluster.downing-provider-class = "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"`)
	initJoin := &cluster.InitJoin{CurrentConfig: minConfig}
	return cm.router.Send(ctx, path, initJoin)
}

// ProceedJoin sends the actual Join message after receiving InitJoinAck
func (cm *ClusterManager) ProceedJoin(ctx context.Context, actorPath string) error {
	join := &cluster.Join{
		Node:  toClusterUniqueAddress(cm.localAddress),
		Roles: []string{"default"},
	}
	log.Printf("Cluster: sending Join to %s", actorPath)
	return cm.router.Send(ctx, actorPath, join)
}

// LeaveCluster sends a Leave message.
func (cm *ClusterManager) LeaveCluster() error {
	cm.mu.RLock()
	state := cm.state
	cm.mu.RUnlock()

	leave := toClusterAddress(cm.localAddress.Address)

	// Send to all known members or just the leader/seed. For simplicity, broadcast to all UP members.
	var lastErr error
	for _, m := range state.GetMembers() {
		if m.GetStatus() == cluster.MemberStatus_Up || m.GetStatus() == cluster.MemberStatus_WeaklyUp {
			addr := state.GetAllAddresses()[m.GetAddressIndex()]
			path := fmt.Sprintf("pekko://%s@%s:%d/system/cluster/core/daemon",
				addr.GetAddress().GetSystem(),
				addr.GetAddress().GetHostname(),
				addr.GetAddress().GetPort())
			if err := cm.router.Send(context.Background(), path, leave); err != nil {
				lastErr = err
			}
		}
	}
	return lastErr
}

// HandleIncomingClusterMessage dispatches cluster-level messages.
// Pekko's ClusterMessageSerializer uses short manifests: "IJ", "IJA", "J", "W", "GE", "GS", "HB", "HBR", "L".
// remoteAddr is the UniqueAddress of the node that sent this message (from the association handshake).
func (cm *ClusterManager) HandleIncomingClusterMessage(ctx context.Context, meta *ArteryMetadata, remoteAddr *UniqueAddress) error {
	manifest := string(meta.MessageManifest)
	log.Printf("Cluster: HandleIncomingClusterMessage manifest=%q", manifest)
	switch manifest {
	case "IJ": // InitJoin — we are the seed; reply with InitJoinAck
		if remoteAddr == nil {
			log.Printf("Cluster: InitJoin: no remote address (handshake pending), ignoring")
			return nil
		}
		log.Printf("Cluster: received InitJoin from %v — sending InitJoinAck", remoteAddr.GetAddress())
		ack := &cluster.InitJoinAck{
			Address:     toClusterAddress(cm.localAddress.Address),
			ConfigCheck: &cluster.ConfigCheck{Type: cluster.ConfigCheck_CompatibleConfig.Enum()},
		}
		raddr := remoteAddr.GetAddress()
		system := cm.localAddress.GetAddress().GetSystem()
		path := cm.clusterCorePath(system, raddr.GetHostname(), raddr.GetPort())
		return cm.router.Send(ctx, path, ack)
	case "IJA": // InitJoinAck — received Ack, now send Join
		ack := &cluster.InitJoinAck{}
		if err := proto.Unmarshal(meta.Payload, ack); err != nil {
			return err
		}
		addr := ack.GetAddress()
		path := cm.clusterCorePath(addr.GetSystem(), addr.GetHostname(), addr.GetPort())
		return cm.ProceedJoin(ctx, path)
	case "J": // Join
		return cm.handleJoin(meta)
	case "W": // Welcome
		return cm.handleWelcome(meta)
	case "GE": // GossipEnvelope
		return cm.handleGossipEnvelope(meta)
	case "GS": // GossipStatus
		return cm.handleGossipStatus(meta)
	case "HB": // Heartbeat
		return cm.handleHeartbeat(meta)
	case "HBR": // HeartbeatRsp
		return cm.handleHeartbeatRsp(meta)
	case "L": // Leave — a member is requesting graceful departure
		leave := &cluster.Address{}
		if err := proto.Unmarshal(meta.Payload, leave); err != nil {
			return err
		}
		log.Printf("Cluster: received Leave from %s:%d", leave.GetHostname(), leave.GetPort())
		cm.mu.Lock()
		cm.markMemberLeavingLocked(leave)
		cm.mu.Unlock()
		return nil
	default:
		log.Printf("Cluster: unknown manifest %q", manifest)
		return nil
	}
}

func (cm *ClusterManager) handleHeartbeat(meta *ArteryMetadata) error {
	hb := &cluster.Heartbeat{}
	if err := proto.Unmarshal(meta.Payload, hb); err != nil {
		return err
	}

	// Reply with HeartBeatResponse
	rsp := &cluster.HeartBeatResponse{
		From:         toClusterUniqueAddress(cm.localAddress),
		SequenceNr:   hb.SequenceNr,
		CreationTime: hb.CreationTime,
	}

	addr := hb.GetFrom()
	path := cm.heartbeatPath(addr.GetSystem(), addr.GetHostname(), addr.GetPort())
	return cm.router.Send(context.Background(), path, rsp)
}

func (cm *ClusterManager) handleHeartbeatRsp(meta *ArteryMetadata) error {
	rsp := &cluster.HeartBeatResponse{}
	if err := proto.Unmarshal(meta.Payload, rsp); err != nil {
		return err
	}

	// Update Failure Detector
	addr := rsp.GetFrom().GetAddress()
	key := fmt.Sprintf("%s:%d-%d", addr.GetHostname(), addr.GetPort(), rsp.GetFrom().GetUid())
	cm.fd.Heartbeat(key)
	return nil
}

func (cm *ClusterManager) handleJoin(meta *ArteryMetadata) error {
	join := &cluster.Join{}
	if err := proto.Unmarshal(meta.Payload, join); err != nil {
		return err
	}
	joiningNode := join.GetNode()
	log.Printf("Cluster: received Join from %v", joiningNode)

	// Add the joining node to our gossip state as Joining, then send Welcome with
	// the updated state.  The gossip loop's performLeaderActions will transition
	// Joining → Up on the next tick.
	cm.mu.Lock()
	cm.addMemberToGossipLocked(joiningNode)
	welcomeGossip := proto.Clone(cm.state).(*cluster.Gossip)
	cm.connectToNewMembers(cm.state)
	cm.mu.Unlock()

	welcome := &cluster.Welcome{
		From:   toClusterUniqueAddress(cm.localAddress),
		Gossip: welcomeGossip,
	}

	addr := joiningNode.GetAddress()
	system := cm.localAddress.GetAddress().GetSystem()
	path := cm.clusterCorePath(system, addr.GetHostname(), addr.GetPort())

	log.Printf("Cluster: sending Welcome to %s", path)
	return cm.router.Send(context.Background(), path, welcome)
}

// addMemberToGossipLocked adds a joining node to the gossip Members list (as Joining).
// Must be called with cm.mu held.
func (cm *ClusterManager) addMemberToGossipLocked(joiningAddr *cluster.UniqueAddress) {
	// Look for an existing AllAddresses entry by host:port.
	for i, addr := range cm.state.AllAddresses {
		if addr.GetAddress().GetHostname() == joiningAddr.GetAddress().GetHostname() &&
			addr.GetAddress().GetPort() == joiningAddr.GetAddress().GetPort() {
			// Already known — add Member if not present.
			for _, m := range cm.state.Members {
				if m.GetAddressIndex() == int32(i) {
					return // already a member
				}
			}
			cm.state.Members = append(cm.state.Members, &cluster.Member{
				AddressIndex: proto.Int32(int32(i)),
				Status:       cluster.MemberStatus_Joining.Enum(),
				UpNumber:     proto.Int32(int32(len(cm.state.Members) + 1)),
			})
			cm.incrementVersionWithLockHeld()
			return
		}
	}

	// New address — append to AllAddresses and create a Member.
	addrIdx := int32(len(cm.state.AllAddresses))
	cm.state.AllAddresses = append(cm.state.AllAddresses, joiningAddr)
	cm.state.Members = append(cm.state.Members, &cluster.Member{
		AddressIndex: proto.Int32(addrIdx),
		Status:       cluster.MemberStatus_Joining.Enum(),
		UpNumber:     proto.Int32(int32(len(cm.state.Members) + 1)),
	})
	cm.incrementVersionWithLockHeld()
}

// markMemberLeavingLocked transitions an Up/WeaklyUp member to Leaving status.
// Must be called with cm.mu held.
func (cm *ClusterManager) markMemberLeavingLocked(leaveAddr *cluster.Address) {
	for _, m := range cm.state.Members {
		addr := cm.state.AllAddresses[m.GetAddressIndex()]
		if addr.GetAddress().GetHostname() == leaveAddr.GetHostname() &&
			addr.GetAddress().GetPort() == leaveAddr.GetPort() {
			if m.GetStatus() == cluster.MemberStatus_Up || m.GetStatus() == cluster.MemberStatus_WeaklyUp {
				m.Status = cluster.MemberStatus_Leaving.Enum()
				cm.incrementVersionWithLockHeld()
				log.Printf("Cluster: marked %s:%d as Leaving", leaveAddr.GetHostname(), leaveAddr.GetPort())
				// publishEvent is safe while holding cm.mu — it only acquires cm.subMu.
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

func (cm *ClusterManager) handleWelcome(meta *ArteryMetadata) error {
	// Pekko GZIP-compresses the Welcome payload (compress(welcomeToProto(...)))
	decompressed, err := gzipDecompress(meta.Payload)
	if err != nil {
		return fmt.Errorf("failed to decompress Welcome payload: %w", err)
	}
	welcome := &cluster.Welcome{}
	if err := proto.Unmarshal(decompressed, welcome); err != nil {
		return err
	}
	log.Printf("Cluster: welcomed by %v", welcome.GetFrom())
	cm.mu.Lock()
	cm.state = welcome.Gossip
	cm.connectToNewMembers(welcome.Gossip)
	cm.mu.Unlock()
	// Signal that this node has successfully joined a cluster.
	cm.welcomeReceived.Store(true)
	return nil
}

func (cm *ClusterManager) handleGossipEnvelope(meta *ArteryMetadata) error {
	if cm.metrics != nil {
		cm.metrics.GossipsReceived.Add(1)
	}
	envelope := &cluster.GossipEnvelope{}
	if err := proto.Unmarshal(meta.Payload, envelope); err != nil {
		return err
	}
	log.Printf("Cluster: received GossipEnvelope from %v", envelope.GetFrom())

	// Pekko GZIP-compresses GossipEnvelope.serializedGossip
	decompressed, err := gzipDecompress(envelope.SerializedGossip)
	if err != nil {
		return fmt.Errorf("failed to decompress GossipEnvelope.serializedGossip: %w", err)
	}
	gossip := &cluster.Gossip{}
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

func CompareVectorClock(v1, v2 *cluster.VectorClock) ClockOrdering {
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

func (cm *ClusterManager) vectorClockToMap(vc *cluster.VectorClock, hashes []string) map[string]int64 {
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
// Must be called with cm.mu held (read or write).
func (cm *ClusterManager) mergeGossipStates(local, incoming *cluster.Gossip) *cluster.Gossip {
	type addrKey struct {
		host string
		port uint32
	}

	// ── AllAddresses (union, dedup by host:port) ─────────────────────────────
	mergedAddresses := make([]*cluster.UniqueAddress, 0, len(local.AllAddresses)+len(incoming.AllAddresses))
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
	statusOrd := map[cluster.MemberStatus]int{
		cluster.MemberStatus_Joining:  0,
		cluster.MemberStatus_WeaklyUp: 1,
		cluster.MemberStatus_Up:       2,
		cluster.MemberStatus_Leaving:  3,
		cluster.MemberStatus_Exiting:  4,
		cluster.MemberStatus_Removed:  5,
		cluster.MemberStatus_Down:     6,
	}

	mergedMembers := make([]*cluster.Member, 0, len(local.Members)+len(incoming.Members))
	memberByMergedIdx := make(map[int32]*cluster.Member)

	for _, m := range local.Members {
		addr := local.AllAddresses[m.GetAddressIndex()]
		k := addrKey{addr.GetAddress().GetHostname(), addr.GetAddress().GetPort()}
		mergedIdx := addrIndexMap[k]
		newM := proto.Clone(m).(*cluster.Member)
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
			newM := proto.Clone(m).(*cluster.Member)
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

	mergedVC := &cluster.VectorClock{}
	for h, idx := range hashIdxMap {
		t1, t2 := localVCMap[h], incomingVCMap[h]
		maxT := t1
		if t2 > maxT {
			maxT = t2
		}
		if maxT > 0 {
			mergedVC.Versions = append(mergedVC.Versions, &cluster.VectorClock_Version{
				HashIndex: proto.Int32(idx),
				Timestamp: proto.Int64(maxT),
			})
		}
	}

	return &cluster.Gossip{
		AllAddresses: mergedAddresses,
		Members:      mergedMembers,
		AllHashes:    mergedHashes,
		Version:      mergedVC,
		Overview:     &cluster.GossipOverview{},
	}
}

func (cm *ClusterManager) processIncomingGossip(gossip *cluster.Gossip) error {
	cm.mu.Lock()

	m1 := cm.vectorClockToMap(cm.state.Version, cm.state.AllHashes)
	m2 := cm.vectorClockToMap(gossip.Version, gossip.AllHashes)
	log.Printf("Cluster: Comparison - localHashes=%v incomingHashes=%v localMap=%v incomingMap=%v", cm.state.AllHashes, gossip.AllHashes, m1, m2)
	ordering := cm.compareResolvedClocks(m1, m2)

	var events []ClusterDomainEvent
	if ordering == ClockBefore {
		// Incoming is newer — diff before replacing so we can emit events.
		log.Printf("Cluster: received newer Gossip, replacing local state")
		events = diffGossipMembers(cm.state, gossip)
		cm.state = gossip
		cm.connectToNewMembers(gossip)
	} else if ordering == ClockConcurrent {
		// Merge concurrent states: union of members, pairwise-max vector clock.
		log.Printf("Cluster: received concurrent Gossip, merging")
		merged := cm.mergeGossipStates(cm.state, gossip)
		events = diffGossipMembers(cm.state, merged)
		cm.state = merged
		cm.incrementVersionWithLockHeld()
		cm.connectToNewMembers(merged)
	}

	for _, addr := range gossip.GetAllAddresses() {
		key := fmt.Sprintf("%s:%d-%d", addr.GetAddress().GetHostname(), addr.GetAddress().GetPort(), addr.GetUid())
		cm.fd.Heartbeat(key)
	}

	cm.mu.Unlock()

	// Record convergence timestamp when all Up members have seen this state.
	if cm.metrics != nil && cm.CheckConvergence() {
		cm.metrics.RecordConvergence()
	}

	// Publish events outside the lock so slow subscribers can't stall gossip.
	for _, evt := range events {
		cm.publishEvent(evt)
	}
	return nil
}

// connectToNewMembers must be called with cm.mu held (read or write).
func (cm *ClusterManager) connectToNewMembers(gossip *cluster.Gossip) {
	localState := cm.state

	for _, m := range gossip.Members {
		if m.GetStatus() == cluster.MemberStatus_Removed || m.GetStatus() == cluster.MemberStatus_Down {
			continue
		}

		addr := gossip.AllAddresses[m.GetAddressIndex()]
		if addr.GetAddress().GetHostname() == cm.localAddress.Address.GetHostname() &&
			addr.GetAddress().GetPort() == cm.localAddress.Address.GetPort() {
			continue // Skip ourselves
		}

		path := cm.clusterCorePath(
			addr.GetAddress().GetSystem(),
			addr.GetAddress().GetHostname(),
			addr.GetAddress().GetPort())

		// Sending a GossipStatus to standard cluster path triggers a connection if one doesn't exist
		status := &cluster.GossipStatus{
			From:      toClusterUniqueAddress(cm.localAddress),
			AllHashes: localState.AllHashes,
			Version:   localState.Version,
		}

		_ = cm.router.Send(context.Background(), path, status)
	}
}

func (cm *ClusterManager) incrementVersionWithLockHeld() {
	if cm.state.Version == nil {
		cm.state.Version = &cluster.VectorClock{}
	}

	myHashStr := fmt.Sprintf("%d", cm.localHash)
	myIndex := -1
	for i, h := range cm.state.AllHashes {
		if h == myHashStr {
			myIndex = i
			break
		}
	}

	if myIndex == -1 {
		myIndex = len(cm.state.AllHashes)
		cm.state.AllHashes = append(cm.state.AllHashes, myHashStr)
	}

	found := false
	for _, v := range cm.state.Version.Versions {
		if v.GetHashIndex() == int32(myIndex) {
			v.Timestamp = proto.Int64(v.GetTimestamp() + 1)
			found = true
			break
		}
	}
	if !found {
		cm.state.Version.Versions = append(cm.state.Version.Versions, &cluster.VectorClock_Version{
			HashIndex: proto.Int32(int32(myIndex)),
			Timestamp: proto.Int64(1),
		})
	}
}

// OldestNode returns the oldest Up/WeaklyUp member, optionally filtered by role.
// "Oldest" = member with the lowest upNumber (the one that became Up first).
// This matches Pekko's ClusterSingletonManager oldest-member selection.
func (cm *ClusterManager) OldestNode(role string) *cluster.UniqueAddress {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	type candidate struct {
		ua       *cluster.UniqueAddress
		upNumber int32
		addr     *cluster.Address
	}
	var best *candidate

	for _, m := range cm.state.Members {
		st := m.GetStatus()
		if st != cluster.MemberStatus_Up && st != cluster.MemberStatus_WeaklyUp {
			continue
		}
		if role != "" {
			found := false
			for _, idx := range m.GetRolesIndexes() {
				if int(idx) < len(cm.state.AllRoles) && cm.state.AllRoles[idx] == role {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		ua := cm.state.AllAddresses[m.GetAddressIndex()]
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
func (cm *ClusterManager) DetermineLeader() *cluster.UniqueAddress {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	type memberInfo struct {
		ua     *cluster.UniqueAddress
		status cluster.MemberStatus
	}
	var available []memberInfo
	for _, m := range cm.state.Members {
		ua := cm.state.AllAddresses[m.GetAddressIndex()]
		if m.GetStatus() != cluster.MemberStatus_Removed && m.GetStatus() != cluster.MemberStatus_Down {
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
	if leader.GetAddress().GetHostname() == cm.localAddress.Address.GetHostname() &&
		leader.GetAddress().GetPort() == cm.localAddress.Address.GetPort() &&
		leader.GetUid() == uint32(cm.localAddress.GetUid()&0xFFFFFFFF) {

		cm.mu.Lock()
		var events []ClusterDomainEvent
		changed := false
		for _, m := range cm.state.Members {
			ua := cm.state.AllAddresses[m.GetAddressIndex()]
			ma := memberAddressFromUA(ua)
			switch m.GetStatus() {
			case cluster.MemberStatus_Joining:
				m.Status = cluster.MemberStatus_Up.Enum()
				m.UpNumber = proto.Int32(m.GetUpNumber() + 1)
				events = append(events, MemberUp{Member: ma})
				if cm.metrics != nil {
					cm.metrics.MemberUpEvents.Add(1)
				}
				changed = true
				log.Printf("Leader: transitioned member %d Joining → Up", m.GetAddressIndex())
			case cluster.MemberStatus_Leaving:
				m.Status = cluster.MemberStatus_Exiting.Enum()
				events = append(events, MemberExited{Member: ma})
				changed = true
				log.Printf("Leader: transitioned member %d Leaving → Exiting", m.GetAddressIndex())
			case cluster.MemberStatus_Exiting:
				m.Status = cluster.MemberStatus_Removed.Enum()
				events = append(events, MemberRemoved{Member: ma})
				if cm.metrics != nil {
					cm.metrics.MemberRemovedEvents.Add(1)
				}
				changed = true
				log.Printf("Leader: transitioned member %d Exiting → Removed", m.GetAddressIndex())
			}
		}
		if changed {
			// Use the lock-held variant — we already hold cm.mu (write).
			// The former cm.incrementVersion() call was a deadlock bug.
			cm.incrementVersionWithLockHeld()
		}
		cm.mu.Unlock()

		// Publish events outside the lock so a slow subscriber can't stall
		// the gossip loop.
		for _, evt := range events {
			cm.publishEvent(evt)
		}
	}
}

func (cm *ClusterManager) CheckReachability() {
	cm.mu.Lock()
	addresses := make([]*cluster.UniqueAddress, len(cm.state.AllAddresses))
	copy(addresses, cm.state.AllAddresses)
	cm.mu.Unlock()

	for i, addr := range addresses {
		key := fmt.Sprintf("%s:%d-%d", addr.GetAddress().GetHostname(), addr.GetAddress().GetPort(), addr.GetUid())
		phi := cm.fd.Phi(key)

		// Pekko Cluster logic: update ObserverReachability
		if phi > cm.fd.threshold {
			// Mark as UNREACHABLE
			log.Printf("FailureDetector: node %s is UNREACHABLE (phi=%.2f)", key, phi)
			cm.mu.Lock()
			cm.updateReachability(int32(i), cluster.ReachabilityStatus_Unreachable)
			cm.mu.Unlock()
		} else if phi < 1.0 {
			// Mark as REACHABLE if it was previously unreachable
			cm.mu.Lock()
			cm.updateReachability(int32(i), cluster.ReachabilityStatus_Reachable)
			cm.mu.Unlock()
		}
	}
}

func (cm *ClusterManager) updateReachability(addrIdx int32, status cluster.ReachabilityStatus) {
	if cm.state.Overview == nil {
		cm.state.Overview = &cluster.GossipOverview{}
	}

	// Build the MemberAddress for event publishing (safe: addrIdx is always valid here).
	var ma MemberAddress
	if int(addrIdx) < len(cm.state.AllAddresses) {
		ma = memberAddressFromUA(cm.state.AllAddresses[addrIdx])
	}

	found := false
	for _, r := range cm.state.Overview.ObserverReachability {
		if r.GetAddressIndex() == 0 { // 0 is us (local address index)
			for _, s := range r.SubjectReachability {
				if s.GetAddressIndex() == addrIdx {
					if s.GetStatus() != status {
						oldStatus := s.GetStatus()
						s.Status = status.Enum()
						s.Version = proto.Int64(s.GetVersion() + 1)
						// Fix: use lock-held variant — caller holds cm.mu (write).
						cm.incrementVersionWithLockHeld()
						// publishEvent is safe here: only acquires cm.subMu, not cm.mu.
						if status == cluster.ReachabilityStatus_Unreachable {
							cm.publishEvent(UnreachableMember{Member: ma})
						} else if oldStatus == cluster.ReachabilityStatus_Unreachable {
							cm.publishEvent(ReachableMember{Member: ma})
						}
					}
					found = true
					break
				}
			}
			if !found {
				r.SubjectReachability = append(r.SubjectReachability, &cluster.SubjectReachability{
					AddressIndex: proto.Int32(addrIdx),
					Status:       status.Enum(),
					Version:      proto.Int64(1),
				})
				cm.incrementVersionWithLockHeld()
				if status == cluster.ReachabilityStatus_Unreachable {
					cm.publishEvent(UnreachableMember{Member: ma})
				}
			}
			found = true
			break
		}
	}

	if !found {
		cm.state.Overview.ObserverReachability = append(cm.state.Overview.ObserverReachability, &cluster.ObserverReachability{
			AddressIndex: proto.Int32(0),
			Version:      proto.Int64(1),
			SubjectReachability: []*cluster.SubjectReachability{
				{
					AddressIndex: proto.Int32(addrIdx),
					Status:       status.Enum(),
					Version:      proto.Int64(1),
				},
			},
		})
		cm.incrementVersionWithLockHeld()
		if status == cluster.ReachabilityStatus_Unreachable {
			cm.publishEvent(UnreachableMember{Member: ma})
		}
	}
}

func (cm *ClusterManager) handleGossipStatus(meta *ArteryMetadata) error {
	status := &cluster.GossipStatus{}
	if err := proto.Unmarshal(meta.Payload, status); err != nil {
		return err
	}
	// log.Printf("Cluster: received GossipStatus from %v", status.GetFrom())

	addr := status.GetFrom().GetAddress()
	system := cm.localAddress.GetAddress().GetSystem()
	path := cm.clusterCorePath(system, addr.GetHostname(), addr.GetPort())

	cm.mu.RLock()
	m1 := cm.vectorClockToMap(cm.state.Version, cm.state.AllHashes)
	m2 := cm.vectorClockToMap(status.Version, status.AllHashes)
	ordering := cm.compareResolvedClocks(m1, m2)
	log.Printf("Cluster: handleGossipStatus from %v, ordering=%v localHashes=%v statusHashes=%v", status.GetFrom(), ordering, cm.state.AllHashes, status.AllHashes)

	var statePayload []byte
	var localVersion *cluster.VectorClock
	var localHashes []string
	if ordering == ClockAfter || ordering == ClockConcurrent {
		statePayload, _ = proto.Marshal(cm.state)
	} else if ordering == ClockBefore {
		localVersion = cm.state.Version
		localHashes = cm.state.AllHashes
	}
	cm.mu.RUnlock()

	if ordering == ClockAfter || ordering == ClockConcurrent {
		// Our state is newer or concurrent, send full GossipEnvelope.
		// Pekko expects GossipEnvelope.serializedGossip to be GZIP-compressed.
		if statePayload != nil {
			compressedGossip, err := gzipCompress(statePayload)
			if err != nil {
				return fmt.Errorf("failed to compress gossip: %w", err)
			}
			env := &cluster.GossipEnvelope{
				From:             toClusterUniqueAddress(cm.localAddress),
				To:               status.From,
				SerializedGossip: compressedGossip,
			}
			return cm.router.Send(context.Background(), path, env)
		}
	} else if ordering == ClockBefore {
		// Their state is newer, reply with our GossipStatus so they will send us their GossipEnvelope
		myStatus := &cluster.GossipStatus{
			From:      toClusterUniqueAddress(cm.localAddress),
			AllHashes: localHashes,
			Version:   localVersion,
		}
		return cm.router.Send(context.Background(), path, myStatus)
	}

	return nil
}

func (cm *ClusterManager) gossipTick() {
	cm.CheckReachability()
	cm.performLeaderActions()

	cm.mu.RLock()
	members := cm.state.Members
	addresses := cm.state.AllAddresses
	version := cm.state.Version
	cm.mu.RUnlock()

	if len(members) <= 1 {
		return
	}

	// Randomly select a node to gossip with
	targetIdx := rand.Intn(len(members))
	addrIdx := members[targetIdx].GetAddressIndex()
	targetAddr := addresses[addrIdx]

	// If it's us, skip
	if targetAddr.GetAddress().GetHostname() == cm.localAddress.Address.GetHostname() &&
		targetAddr.GetAddress().GetPort() == cm.localAddress.Address.GetPort() {
		return
	}

	status := &cluster.GossipStatus{
		From:      toClusterUniqueAddress(cm.localAddress),
		AllHashes: cm.state.AllHashes,
		Version:   version,
	}

	path := cm.clusterCorePath(
		cm.localAddress.Address.GetSystem(),
		targetAddr.GetAddress().GetHostname(),
		targetAddr.GetAddress().GetPort())

	_ = cm.router.Send(context.Background(), path, status)
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
func (cm *ClusterManager) GetState() *cluster.Gossip {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.state
}

// StartHeartbeat begins sending Heartbeat messages to a specific node (usually the seed).
func (cm *ClusterManager) StartHeartbeat(target *Address) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.cancelHeartbeat != nil {
		cm.cancelHeartbeat()
	}

	ctx, cancel := context.WithCancel(context.Background())
	cm.cancelHeartbeat = cancel

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		path := cm.heartbeatPath(target.GetSystem(), target.GetHostname(), target.GetPort())
		var seq int64 = 0

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				seq++
				hb := &cluster.Heartbeat{
					From:       toClusterAddress(cm.localAddress.Address),
					SequenceNr: proto.Int64(seq),
				}
				if err := cm.router.Send(context.Background(), path, hb); err != nil {
					log.Printf("Cluster: failed to send heartbeat to %v: %v", target, err)
				}
			}
		}
	}()
}

// StopHeartbeat stops sending heartbeats to simulate failure.
func (cm *ClusterManager) StopHeartbeat(target *Address) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.cancelHeartbeat != nil {
		cm.cancelHeartbeat()
		cm.cancelHeartbeat = nil
	}
}

// GetLocalAddress returns the local unique address.
func (cm *ClusterManager) GetLocalAddress() *UniqueAddress {
	return cm.localAddress
}

// GetFailureDetector returns the internal failure detector.
func (cm *ClusterManager) GetFailureDetector() *PhiAccrualFailureDetector {
	return cm.fd
}

// GetRolesForMember maps the rolesIndexes in a Member to the string roles in the Gossip message.
func GetRolesForMember(gossip *cluster.Gossip, member *cluster.Member) []string {
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
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	state := cm.state
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
		if addr.GetAddress().GetHostname() == cm.localAddress.Address.GetHostname() &&
			addr.GetAddress().GetPort() == cm.localAddress.Address.GetPort() {
			ourIdx = int32(i)
			break
		}
	}
	if ourIdx != -1 {
		seenSet[ourIdx] = true
	}

	for _, m := range state.Members {
		st := m.GetStatus()
		if st == cluster.MemberStatus_Up || st == cluster.MemberStatus_Leaving {
			if !seenSet[m.GetAddressIndex()] {
				return false
			}
		}
	}

	return true
}
