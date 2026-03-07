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
	"time"

	"gekka/gekka/cluster"

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
	return &ClusterManager{
		localAddress: local,
		localHash:    int32(local.GetUid()), // simplified hash as UID
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
			Overview:     &cluster.GossipOverview{},
			Version:      &cluster.VectorClock{},
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
	minConfig := proto.String(`pekko.cluster.downing-provider-class = ""`)
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
	for _, m := range state.GetMembers() {
		if m.GetStatus() == cluster.MemberStatus_Up || m.GetStatus() == cluster.MemberStatus_WeaklyUp {
			addr := state.GetAllAddresses()[m.GetAddressIndex()]
			path := fmt.Sprintf("pekko://%s@%s:%d/system/cluster/core/daemon",
				addr.GetAddress().GetSystem(),
				addr.GetAddress().GetHostname(),
				addr.GetAddress().GetPort())
			cm.router.Send(context.Background(), path, leave)
		}
	}
	return nil
}

// HandleIncomingClusterMessage dispatches cluster-level messages.
// Pekko's ClusterMessageSerializer uses short manifests: "IJ", "IJA", "J", "W", "GE", "GS", "HB", "HBR", "L".
func (cm *ClusterManager) HandleIncomingClusterMessage(ctx context.Context, meta *ArteryMetadata) error {
	manifest := string(meta.MessageManifest)
	switch manifest {
	case "IJ": // InitJoin — handle if we are the seed
		return nil
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
	case "L": // Leave
		log.Printf("Cluster: received Leave")
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
	log.Printf("Cluster: received Join from %v", join.GetNode())

	// Send Welcome back to the joining node
	welcome := &cluster.Welcome{
		From:   toClusterUniqueAddress(cm.localAddress),
		Gossip: cm.state,
	}

	// We need a path to the joining node.
	addr := join.GetNode().GetAddress()
	system := cm.localAddress.GetAddress().GetSystem()
	path := cm.clusterCorePath(system, addr.GetHostname(), addr.GetPort())

	log.Printf("Cluster: sending Welcome to %s", path)
	return cm.router.Send(context.Background(), path, welcome)
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
	cm.mu.Unlock()
	return nil
}

func (cm *ClusterManager) handleGossipEnvelope(meta *ArteryMetadata) error {
	envelope := &cluster.GossipEnvelope{}
	if err := proto.Unmarshal(meta.Payload, envelope); err != nil {
		return err
	}

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

func (cm *ClusterManager) processIncomingGossip(gossip *cluster.Gossip) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	ordering := CompareVectorClock(cm.state.Version, gossip.Version)

	if ordering == ClockBefore {
		// Incoming is newer, replace local state
		log.Printf("Cluster: received newer Gossip, replacing local state")
		cm.state = gossip
		cm.connectToNewMembers(gossip)
	} else if ordering == ClockConcurrent {
		// Merge concurrent states
		log.Printf("Cluster: received concurrent Gossip, merging (simplified replacement for now)")
		// In a real implementation we would merge member status, seen tables, and vector clocks.
		cm.state = gossip
		cm.incrementVersion()
		cm.connectToNewMembers(gossip)
	} else {
		// Log.Printf("Cluster: received older or same Gossip, ignoring")
	}

	// Update Failure Detector for all members
	for _, addr := range gossip.GetAllAddresses() {
		key := fmt.Sprintf("%s:%d-%d", addr.GetAddress().GetHostname(), addr.GetAddress().GetPort(), addr.GetUid())
		cm.fd.Heartbeat(key)
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
			From:    toClusterUniqueAddress(cm.localAddress),
			Version: localState.Version,
		}

		_ = cm.router.Send(context.Background(), path, status)
	}
}

func (cm *ClusterManager) incrementVersion() {
	// Vector clock logic: Increment timestamp for local node (simplified by hash)
	if cm.state.Version == nil {
		cm.state.Version = &cluster.VectorClock{}
	}
	found := false
	for _, v := range cm.state.Version.Versions {
		if v.GetHashIndex() == cm.localHash {
			v.Timestamp = proto.Int64(v.GetTimestamp() + 1)
			found = true
			break
		}
	}
	if !found {
		cm.state.Version.Versions = append(cm.state.Version.Versions, &cluster.VectorClock_Version{
			HashIndex: proto.Int32(cm.localHash),
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
		changed := false
		for _, m := range cm.state.Members {
			if m.GetStatus() == cluster.MemberStatus_Joining {
				m.Status = cluster.MemberStatus_Up.Enum()
				m.UpNumber = proto.Int32(m.GetUpNumber() + 1)
				changed = true
				log.Printf("Leader: transitioned member %d to UP", m.GetAddressIndex())
			}
		}
		if changed {
			cm.incrementVersion()
		}
		cm.mu.Unlock()
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

	found := false
	for _, r := range cm.state.Overview.ObserverReachability {
		if r.GetAddressIndex() == 0 { // 0 is us (local address index)
			for _, s := range r.SubjectReachability {
				if s.GetAddressIndex() == addrIdx {
					if s.GetStatus() != status {
						s.Status = status.Enum()
						s.Version = proto.Int64(s.GetVersion() + 1)
						cm.incrementVersion()
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
				cm.incrementVersion()
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
		cm.incrementVersion()
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
	ordering := CompareVectorClock(cm.state.Version, status.Version)
	var statePayload []byte
	var localVersion *cluster.VectorClock
	if ordering == ClockAfter || ordering == ClockConcurrent {
		statePayload, _ = proto.Marshal(cm.state)
	} else if ordering == ClockBefore {
		localVersion = cm.state.Version
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
			From:    toClusterUniqueAddress(cm.localAddress),
			Version: localVersion,
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
		From:    toClusterUniqueAddress(cm.localAddress),
		Version: version,
	}

	path := cm.clusterCorePath(
		cm.localAddress.Address.GetSystem(),
		targetAddr.GetAddress().GetHostname(),
		targetAddr.GetAddress().GetPort())

	cm.router.Send(context.Background(), path, status)
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
				cm.router.Send(context.Background(), path, hb)
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
