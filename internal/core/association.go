/*
 * association.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster"
	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// AssociationState represents the state of a connection to a remote node.
type AssociationState = actor.AssociationState

const (
	ASSOCIATED            = actor.ASSOCIATED
	QUARANTINED           = actor.QUARANTINED
	INITIATED             = actor.INITIATED
	WAITING_FOR_HANDSHAKE = actor.WAITING_FOR_HANDSHAKE
)

// AssociationRole indicates whether the connection was initiated locally or remotely.
type AssociationRole = actor.AssociationRole

const (
	INBOUND  = actor.INBOUND
	OUTBOUND = actor.OUTBOUND
)

// Association is a type alias for GekkaAssociation for backward compatibility.
type Association = GekkaAssociation

// GekkaAssociation tracks the state of a single connection.
type GekkaAssociation struct {
	mu        sync.RWMutex
	state     AssociationState
	role      AssociationRole
	conn      net.Conn
	remote    *gproto_remote.UniqueAddress
	lastSeen  time.Time
	nodeMgr   *NodeManager
	Handshake chan struct{} // signaled when associated
	nextSeq   uint64        // for user messages
	nextSeqNo uint64        // for system messages
	pending   [][]byte      // buffered frames during Handshake
	localUid  uint64
	outbox    chan []byte
	streamId  int32

	lastHeartbeatSentAt time.Time
	lastRTT             time.Duration
}

var _ actor.RemoteAssociation = (*GekkaAssociation)(nil)

type NodeManager struct {
	mu                  sync.RWMutex
	LocalAddr           *gproto_remote.Address
	associations        map[string]*GekkaAssociation // key: host:port, or UID string
	localUid            uint64
	clusterMgr          *cluster.ClusterManager
	compressionMgr      *CompressionTableManager
	SerializerRegistry  *SerializationRegistry
	UserMessageCallback func(ctx context.Context, meta *ArteryMetadata) error

	pendingRepliesMu sync.RWMutex
	pendingReplies   map[string]chan *ArteryMetadata // keyed by temp actor path

	// mutedMu guards mutedNodes.
	mutedMu sync.RWMutex
	// mutedNodes is a set of "host:port" strings for which all outbound
	// frames are silently dropped and all inbound frames are ignored.
	// Populate via MuteNode / UnmuteNode to simulate a network partition
	// in tests without terminating the process.
	mutedNodes map[string]struct{}

	// TLSConfig is the *tls.Config to use for outbound connections.
	// Nil means plain TCP (default).
	TLSConfig *tls.Config

	// NodeMetrics is the shared NodeMetrics instance (set by Cluster.Spawn).
	// Nil-safe: all callers check before touching.
	NodeMetrics *NodeMetrics
}

func NewNodeManager(local *gproto_remote.Address, uid uint64) *NodeManager {
	return &NodeManager{
		LocalAddr:          local,
		associations:       make(map[string]*GekkaAssociation),
		localUid:           uid,
		SerializerRegistry: NewSerializationRegistry(),
		pendingReplies:     make(map[string]chan *ArteryMetadata),
		mutedNodes:         make(map[string]struct{}),
	}
}

// MuteNode silently drops all outbound frames to and all inbound frames from
// the node at host:port. This simulates a one-sided or full network partition
// without terminating either process. Safe to call concurrently.
func (nm *NodeManager) MuteNode(host string, port uint32) {
	key := fmt.Sprintf("%s:%d", host, port)
	nm.mutedMu.Lock()
	nm.mutedNodes[key] = struct{}{}
	nm.mutedMu.Unlock()
	log.Printf("NodeManager: muted node %s (outbound dropped, inbound ignored)", key)
}

// UnmuteNode reverses a previous MuteNode call. Safe to call even if the node
// was never muted.
func (nm *NodeManager) UnmuteNode(host string, port uint32) {
	key := fmt.Sprintf("%s:%d", host, port)
	nm.mutedMu.Lock()
	delete(nm.mutedNodes, key)
	nm.mutedMu.Unlock()
	log.Printf("NodeManager: unmuted node %s", key)
}

// isNodeMuted returns true when host:port has been muted.
func (nm *NodeManager) isNodeMuted(host string, port uint32) bool {
	key := fmt.Sprintf("%s:%d", host, port)
	nm.mutedMu.RLock()
	_, ok := nm.mutedNodes[key]
	nm.mutedMu.RUnlock()
	return ok
}

// RegisterPendingReply records a channel to receive a single reply addressed to path.
func (nm *NodeManager) RegisterPendingReply(path string, ch chan *ArteryMetadata) {
	nm.pendingRepliesMu.Lock()
	nm.pendingReplies[path] = ch
	nm.pendingRepliesMu.Unlock()
}

// UnregisterPendingReply removes the pending reply entry for path.
func (nm *NodeManager) UnregisterPendingReply(path string) {
	nm.pendingRepliesMu.Lock()
	delete(nm.pendingReplies, path)
	nm.pendingRepliesMu.Unlock()
}

// CountAssociations returns the number of Artery connections currently in
// ASSOCIATED state.  It is called by the monitoring server to populate the
// active_associations metric and to evaluate the /healthz readiness check.
func (nm *NodeManager) CountAssociations() int {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	count := 0
	for _, assoc := range nm.associations {
		assoc.mu.RLock()
		st := assoc.state
		assoc.mu.RUnlock()
		if st == ASSOCIATED {
			count++
		}
	}
	return count
}

// HasQuarantinedAssociation reports whether any known Artery association is in
// QUARANTINED state.  A quarantined association means the remote node restarted
// with a different UID — a network-split symptom that makes the local node
// unreliable for cluster operations.  Used by the /health/ready endpoint.
func (nm *NodeManager) HasQuarantinedAssociation() bool {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	for _, assoc := range nm.associations {
		assoc.mu.RLock()
		st := assoc.state
		assoc.mu.RUnlock()
		if st == QUARANTINED {
			return true
		}
	}
	return false
}

// RoutePendingReply delivers meta to a waiting Ask call if path is registered.
// Returns true when a waiting caller was found and the message routed.
func (nm *NodeManager) RoutePendingReply(path string, meta *ArteryMetadata) bool {
	nm.pendingRepliesMu.RLock()
	ch, ok := nm.pendingReplies[path]
	nm.pendingRepliesMu.RUnlock()
	if !ok {
		return false
	}
	select {
	case ch <- meta:
	default:
		log.Printf("NodeManager: Ask reply channel full for path %s, dropping", path)
	}
	return true
}

var _ actor.RemoteMessagingProvider = (*NodeManager)(nil)

func (nm *NodeManager) LocalAddress() *gproto_remote.Address {
	return nm.LocalAddr
}

func (nm *NodeManager) GetAssociationByHost(host string, port uint32) (actor.RemoteAssociation, bool) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	// Prioritize Control Stream (1) for outbound delivery.
	for _, assoc := range nm.associations {
		assoc.mu.RLock()
		remote := assoc.remote
		role := assoc.role
		state := assoc.state
		streamId := assoc.streamId
		assoc.mu.RUnlock()

		if remote != nil && remote.Address.GetHostname() == host && remote.Address.GetPort() == port {
			if role == OUTBOUND && (state == ASSOCIATED || state == INITIATED || state == WAITING_FOR_HANDSHAKE) {
				if streamId == 1 {
					return assoc, true
				}
			}
		}
	}

	// Fallback to any outbound if control not found
	for _, assoc := range nm.associations {
		assoc.mu.RLock()
		remote := assoc.remote
		role := assoc.role
		state := assoc.state
		assoc.mu.RUnlock()

		if remote != nil && remote.Address.GetHostname() == host && remote.Address.GetPort() == port {
			if role == OUTBOUND && (state == ASSOCIATED || state == INITIATED || state == WAITING_FOR_HANDSHAKE) {
				return assoc, true
			}
		}
	}
	return nil, false
}

func (nm *NodeManager) DialRemote(ctx context.Context, target *gproto_remote.Address) (actor.RemoteAssociation, error) {
	addrStr := fmt.Sprintf("%s:%d", target.GetHostname(), target.GetPort())

	client, err := NewTcpClient(TcpClientConfig{
		Addr: addrStr,
		Handler: func(ctx context.Context, conn net.Conn) error {
			return nm.ProcessConnection(ctx, conn, OUTBOUND, target, 1) // Default to Control stream for outbound
		},
		TLSConfig: nm.TLSConfig,
	})
	if err != nil {
		return nil, err
	}

	// Start connection in a goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- client.Connect(ctx)
	}()

	// Wait for the association to appear or for an error
	timeout := time.After(5 * time.Second)
	for {
		select {
		case err := <-errChan:
			return nil, err
		case <-timeout:
			return nil, fmt.Errorf("dial timeout")
		case <-time.After(100 * time.Millisecond):
			if assoc, ok := nm.GetAssociationByHost(target.GetHostname(), target.GetPort()); ok {
				return assoc, nil
			}
		}
	}
}

func (nm *NodeManager) Serializer(id int32) (actor.RemoteSerializer, error) {
	s, err := nm.SerializerRegistry.GetSerializer(id)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (nm *NodeManager) Metrics() actor.RemoteMetrics {
	return nm.NodeMetrics
}

func (nm *NodeManager) SetClusterManager(cm *cluster.ClusterManager) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	nm.clusterMgr = cm
}

func (nm *NodeManager) SetCompressionManager(ctm *CompressionTableManager) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	nm.compressionMgr = ctm
}

// MessageContext carries metadata about an incoming Artery message.
type MessageContext struct {
	Sender     *gproto_remote.UniqueAddress
	Recipient  *gproto_remote.Address
	Serializer int32
	Manifest   string
	SeqNo      uint64
	AckReplyTo *gproto_remote.UniqueAddress
}

// GetAssociation looks up an existing association by unique address and streamId.
func (nm *NodeManager) GetAssociation(remote *gproto_remote.UniqueAddress, streamId int32) (*GekkaAssociation, bool) {
	if remote == nil || remote.Address == nil {
		return nil, false
	}
	addr := remote.GetAddress()
	key := fmt.Sprintf("%s:%d-%d-s%d", addr.GetHostname(), addr.GetPort(), remote.GetUid(), streamId)
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	assoc, ok := nm.associations[key]
	return assoc, ok
}

// RegisterAssociation stores an association in the registry.
func (nm *NodeManager) RegisterAssociation(remote *gproto_remote.UniqueAddress, assoc *GekkaAssociation) {
	if remote == nil || remote.Address == nil {
		return
	}
	addr := remote.GetAddress()
	newKey := fmt.Sprintf("%s:%d-%d-s%d", addr.GetHostname(), addr.GetPort(), remote.GetUid(), assoc.streamId)

	nm.mu.Lock()
	defer nm.mu.Unlock()

	// UID Check: Check if there's an existing association for the same Host:Port but different UID.
	// UID=0 (newKey) is an early placeholder — the confirmed remote UID is not yet known, so
	// we cannot make a node-restart determination and must skip the quarantine check entirely.
	hostPortKey := fmt.Sprintf("%s:%d-", addr.GetHostname(), addr.GetPort())
	if !strings.Contains(newKey, "-0-s") {
		for k, existing := range nm.associations {
			if strings.HasPrefix(k, hostPortKey) && !strings.Contains(k, fmt.Sprintf("-%d-s", remote.GetUid())) {
				// Skip early registrations (UID=0 placeholders).
				if strings.Contains(k, "-0-s") {
					log.Printf("NodeManager: existing early registration %s found for host-port %s", k, hostPortKey)
					continue
				}

				log.Printf("NodeManager: detected node restart for %s. Old association %s being quarantined.", hostPortKey, k)
				existing.mu.Lock()
				existing.state = QUARANTINED
				if existing.conn != nil {
					existing.conn.Close()
				}
				existing.mu.Unlock()
				delete(nm.associations, k)
			}
		}
	}

	// Never let an INBOUND association overwrite an existing OUTBOUND one for the same key.
	assoc.mu.RLock()
	incomingIsInbound := assoc.role == INBOUND
	assoc.mu.RUnlock()
	if incomingIsInbound {
		if existing, found := nm.associations[newKey]; found {
			existing.mu.RLock()
			existingIsOutbound := existing.role == OUTBOUND
			existing.mu.RUnlock()
			if existingIsOutbound {
				log.Printf("NodeManager: INBOUND %p skipping registration %s — OUTBOUND %p already exists", assoc, newKey, existing)
				return
			}
		}
	}

	nm.associations[newKey] = assoc
	log.Printf("NodeManager: registered association %p with key %s", assoc, newKey)
}

// ProcessConnection is the unified entry point for both inbound and outbound connections.
func (nm *NodeManager) ProcessConnection(ctx context.Context, conn net.Conn, role AssociationRole, remote *gproto_remote.Address, streamId int32) error {
	assocCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	protocol := "akka" // default
	if nm.LocalAddr != nil && nm.LocalAddr.GetProtocol() != "" {
		protocol = nm.LocalAddr.GetProtocol()
	}

	if role == INBOUND {
		// Read first 3 bytes to decide preamble format
		magic3 := make([]byte, 3)
		if _, err := io.ReadFull(conn, magic3); err != nil {
			return fmt.Errorf("failed to read artery magic: %w", err)
		}

		if string(magic3) == "ART" {
			// Artery 2.0 (Pekko): ART (3) + version (1) + streamId (1) = 5 bytes total
			rest := make([]byte, 2)
			if _, err := io.ReadFull(conn, rest); err != nil {
				return fmt.Errorf("failed to read Pekko preamble rest: %w", err)
			}
			streamId = int32(rest[1])
			protocol = "pekko"
			log.Printf("ProcessConnection: INBOUND Pekko preamble read, streamId=%d", streamId)
		} else {
			// Artery 1.0 (Akka): AKKA (4) + streamId (1) = 5 bytes total
			// magic3 already consumed "AKK"
			next := make([]byte, 2) // 'A' + streamId byte
			if _, err := io.ReadFull(conn, next); err != nil {
				return fmt.Errorf("failed to read Akka magic rest: %w", err)
			}
			if magic3[0] != 'A' || magic3[1] != 'K' || magic3[2] != 'K' || next[0] != 'A' {
				return fmt.Errorf("invalid artery magic: %q", string(magic3)+string(next))
			}
			streamId = int32(next[1])
			protocol = "akka"
			log.Printf("ProcessConnection: INBOUND Akka preamble read, streamId=%d", streamId)
		}
	}

	assoc := &GekkaAssociation{
		state:     INITIATED,
		role:      role,
		conn:      conn,
		nodeMgr:   nm,
		lastSeen:  time.Now(),
		Handshake: make(chan struct{}),
		localUid:  nm.localUid,
		outbox:    make(chan []byte, 100),
		remote:    &gproto_remote.UniqueAddress{Address: remote, Uid: proto.Uint64(0)},
		streamId:  streamId,
	}
	// Register early so handleHandshakeRsp can find it
	if remote != nil {
		nm.RegisterAssociation(assoc.remote, assoc)
	}

	// Start background write loop.
	// Uses the outer ctx (node lifetime), NOT assocCtx, so that the write loop
	// keeps draining the outbox even after the read loop exits.  In Artery-TCP
	// the outbound TCP stream is unidirectional: Pekko half-closes its write end
	// immediately, causing the read loop to get EOF and return, but writes in the
	// opposite direction are still valid and must continue.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-assoc.outbox:
				if !ok {
					return
				}
				// Check mute state before writing (catches buffered frames
				// queued before MuteNode was called). Use assoc.remote under
				// read lock since it's updated after the handshake.
				assoc.mu.RLock()
				remoteAddr := assoc.remote.GetAddress()
				assoc.mu.RUnlock()
				if remoteAddr != nil && nm.isNodeMuted(remoteAddr.GetHostname(), remoteAddr.GetPort()) {
					continue
				}
				log.Printf("Association %p: SENDING frame of %d bytes", assoc, len(msg))
				if err := WriteFrame(assoc.conn, msg); err != nil {
					log.Printf("Association %p: write error: %v", assoc, err)
					// Close so the read loop also exits cleanly.
					_ = assoc.conn.Close()
					return
				}
			}
		}
	}()

	if role == OUTBOUND && remote != nil {
		go func() {
			// Give handler a moment to start and send magic header
			time.Sleep(200 * time.Millisecond)
			if err := assoc.initiateHandshake(remote); err != nil {
				log.Printf("Association %p: initiateHandshake error: %v", assoc, err)
			}

			// Start heartbeat loop.
			// Guard: only send heartbeats after the handshake is fully complete
			// (state == ASSOCIATED) AND the remote UID is known (non-zero).
			// Sending "m" frames before these conditions are met is a protocol
			// violation: Akka's ControlMessageObserver associates the heartbeat
			// with a specific remote UID, and an unknown (zero) UID causes it to
			// drop or misroute the frame.
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-assocCtx.Done():
					return
				case <-ticker.C:
					assoc.mu.RLock()
					isAssociated := assoc.state == ASSOCIATED
					remoteUID := uint64(0)
					if assoc.remote != nil {
						remoteUID = assoc.remote.GetUid()
					}
					assoc.mu.RUnlock()
					if isAssociated && remoteUID != 0 {
						if err := SendArteryHeartbeat(assoc); err != nil {
							log.Printf("Association %p: failed to send heartbeat: %v", assoc, err)
						}
					}
				}
			}
		}()
	}

	return assoc.Process(assocCtx, protocol)
}

func (assoc *GekkaAssociation) initiateHandshake(to *gproto_remote.Address) error {
	assoc.mu.Lock()
	assoc.state = WAITING_FOR_HANDSHAKE
	uid := assoc.localUid
	assoc.mu.Unlock()

	// Correctly initialize HandshakeReq using pointer types from proto package
	req := &gproto_remote.HandshakeReq{
		From: &gproto_remote.UniqueAddress{
			Address: assoc.nodeMgr.LocalAddr,
			Uid:     proto.Uint64(uid),
		},
		To: to,
	}
	// Pekko ArteryMessageSerializer (17) uses "d" for HandshakeReq.
	// We use 0 (UnknownUid) because we don't know the remote UID yet.
	return SendArteryMessageWithAck(assoc.conn, int64(uid), actor.ArteryInternalSerializerID, "d", req, req.From, true)
}

func (assoc *GekkaAssociation) Process(ctx context.Context, protocol string) error {
	dispatch := func(ctx context.Context, meta *ArteryMetadata) error {
		return assoc.dispatch(ctx, meta)
	}
	remoteUid := uint64(0)
	if assoc.remote != nil {
		remoteUid = assoc.remote.GetUid()
	}
	if assoc.role == OUTBOUND {
		return TcpArteryOutboundHandler(ctx, assoc.conn, dispatch, assoc.nodeMgr.compressionMgr, remoteUid, assoc.streamId, protocol)
	}
	return TcpArteryHandlerWithCallback(ctx, assoc.conn, dispatch, assoc.nodeMgr.compressionMgr, remoteUid, assoc.streamId)
}

func (assoc *GekkaAssociation) dispatch(ctx context.Context, meta *ArteryMetadata) error {
	// Drop all inbound frames from a muted node (simulates network partition).
	if assoc.nodeMgr != nil && assoc.remote != nil {
		a := assoc.remote.GetAddress()
		if assoc.nodeMgr.isNodeMuted(a.GetHostname(), a.GetPort()) {
			return nil
		}
	}

	assoc.mu.Lock()
	assoc.lastSeen = time.Now()
	assoc.mu.Unlock()

	if meta.SeqNo != 0 && meta.AckReplyTo != nil {
		if err := assoc.sendSystemAck(meta.SeqNo, meta.AckReplyTo); err != nil {
			log.Printf("Dispatcher: failed to send ACK for seq %d: %v", meta.SeqNo, err)
		}
	}

	switch meta.SerializerId {
	case actor.ArteryInternalSerializerID, 6: // 17 or 6
		manifest := string(meta.MessageManifest)
		if manifest == "SystemMessage" {
			return assoc.handleSystemMessage(meta)
		}
		return assoc.handleControlMessage(ctx, meta)

	case actor.ClusterSerializerID:
		if assoc.nodeMgr.clusterMgr != nil {
			assoc.mu.RLock()
			remote := assoc.remote
			assoc.mu.RUnlock()
			return assoc.nodeMgr.clusterMgr.HandleIncomingClusterMessage(ctx, meta.Payload, string(meta.MessageManifest), ToClusterUniqueAddress(remote))
		}
		return nil

	default:
		return assoc.handleUserMessage(meta)
	}
}

func (assoc *GekkaAssociation) sendSystemAck(seq uint64, to *gproto_remote.UniqueAddress) error {
	ack := &gproto_remote.SystemMessageDeliveryAck{
		SeqNo: proto.Uint64(seq),
		From: &gproto_remote.UniqueAddress{
			Address: assoc.nodeMgr.LocalAddr,
			Uid:     proto.Uint64(assoc.localUid),
		},
	}
	// "h" is the ArteryMessageSerializer short manifest for SystemMessageDeliveryAck.
	// Using the long class name causes Akka's ControlMessageObserver to skip the frame
	// and pass it to MessageDispatcher.dispatch with an empty recipient → OptionVal.None.get.
	return SendArteryMessageWithAck(assoc.conn, int64(assoc.localUid), actor.ArteryInternalSerializerID, "h", ack, to, true)
}

func (assoc *GekkaAssociation) handleSystemMessage(meta *ArteryMetadata) error {
	// The Artery payload for manifest "SystemMessage" is a SystemMessageEnvelope
	// which carries the SeqNo, AckReplyTo, and the inner SystemMessage bytes.
	env := &gproto_remote.SystemMessageEnvelope{}
	if err := proto.Unmarshal(meta.Payload, env); err != nil {
		return fmt.Errorf("failed to unmarshal SystemMessageEnvelope: %w", err)
	}
	if env.GetSeqNo() != 0 && env.AckReplyTo != nil {
		if err := assoc.sendSystemAck(env.GetSeqNo(), env.AckReplyTo); err != nil {
			log.Printf("Association: failed to send ACK for seq %d: %v", env.GetSeqNo(), err)
		}
	}
	sm := &gproto_remote.SystemMessage{}
	if err := proto.Unmarshal(env.Message, sm); err != nil {
		return fmt.Errorf("failed to unmarshal inner SystemMessage: %w", err)
	}
	log.Printf("Association: received system message of type %v", sm.GetType())
	return nil
}

func (assoc *GekkaAssociation) handleUserMessage(meta *ArteryMetadata) error {
	// Count every incoming user message (cluster-internal messages never
	// reach this handler — they go to handleControlMessage/cluster.ClusterManager).
	if assoc.nodeMgr.NodeMetrics != nil {
		assoc.nodeMgr.NodeMetrics.MessagesReceived.Add(1)
		assoc.nodeMgr.NodeMetrics.BytesReceived.Add(int64(len(meta.Payload)))
	}

	if assoc.nodeMgr.SerializerRegistry != nil {
		obj, err := assoc.nodeMgr.SerializerRegistry.DeserializePayload(meta.SerializerId, string(meta.MessageManifest), meta.Payload)
		if err == nil {
			meta.DeserializedMessage = obj
		} else {
			log.Printf("Dispatcher: failed to deserialize payload (id=%d, manifest=%s): %v", meta.SerializerId, meta.MessageManifest, err)
		}
	}

	// Route to a pending Ask call if the recipient path is registered.
	if meta.Recipient != nil {
		recipientPath := meta.Recipient.GetPath()
		// If the recipient is a full URI, extract the path segment for pending-reply lookup.
		if strings.Contains(recipientPath, "://") {
			// Find the start of the path part (after the authority)
			// URI format: scheme://system@host:port/path
			if firstSlash := strings.Index(recipientPath[strings.Index(recipientPath, "://")+3:], "/"); firstSlash != -1 {
				recipientPath = recipientPath[strings.Index(recipientPath, "://")+3+firstSlash:]
			}
		}
		if recipientPath != "" && assoc.nodeMgr.RoutePendingReply(recipientPath, meta) {
			return nil
		}
	}

	if assoc.nodeMgr.UserMessageCallback != nil {
		return assoc.nodeMgr.UserMessageCallback(context.Background(), meta)
	}
	return nil
}

// SendWithSender delivers a message to recipient over this association using
// senderPath as the Artery sender actor path (required by the Ask pattern).
func (assoc *GekkaAssociation) SendWithSender(recipient, senderPath string, payload []byte, serializerId int32, manifest string) error {
	assoc.mu.RLock()
	st := assoc.state
	assoc.mu.RUnlock()

	if st == QUARANTINED {
		return fmt.Errorf("cannot send to quarantined node")
	}

	// Drop outbound frames when the target node is muted.
	if assoc.nodeMgr != nil && assoc.remote != nil {
		a := assoc.remote.GetAddress()
		if assoc.nodeMgr.isNodeMuted(a.GetHostname(), a.GetPort()) {
			return nil
		}
	}

	frame, err := BuildArteryFrame(int64(assoc.localUid), serializerId, senderPath, recipient, manifest, payload, false)
	if err != nil {
		return err
	}
	log.Printf("Association %p: SendWithSender frame of %d bytes, sender=%q, recipient=%q", assoc, len(frame), senderPath, recipient)

	assoc.mu.Lock()
	defer assoc.mu.Unlock()

	if assoc.state != ASSOCIATED {
		assoc.pending = append(assoc.pending, frame)
		return nil
	}

	select {
	case assoc.outbox <- frame:
		return nil
	default:
		return fmt.Errorf("association outbox full")
	}
}

func (assoc *GekkaAssociation) GetState() AssociationState {
	assoc.mu.RLock()
	defer assoc.mu.RUnlock()
	return assoc.state
}

func (assoc *GekkaAssociation) GetRTT() time.Duration {
	assoc.mu.RLock()
	defer assoc.mu.RUnlock()
	return assoc.lastRTT
}

func (assoc *GekkaAssociation) NextSeq() uint64 {
	assoc.mu.Lock()
	defer assoc.mu.Unlock()
	assoc.nextSeq++
	return assoc.nextSeq
}

func (assoc *GekkaAssociation) NextSeqNo() uint64 {
	assoc.mu.Lock()
	defer assoc.mu.Unlock()
	assoc.nextSeqNo++
	return assoc.nextSeqNo
}

func (assoc *GekkaAssociation) Send(recipient string, payload []byte, serializerId int32, manifest string) error {
	assoc.mu.RLock()
	st := assoc.state
	assoc.mu.RUnlock()

	if st == QUARANTINED {
		return fmt.Errorf("cannot send to quarantined node")
	}

	// Drop outbound frames when the target node is muted.
	if assoc.nodeMgr != nil && assoc.remote != nil {
		a := assoc.remote.GetAddress()
		if assoc.nodeMgr.isNodeMuted(a.GetHostname(), a.GetPort()) {
			return nil
		}
	}

	// Artery messages from Gekka's cluster manager should appear to come from
	// the cluster daemon. Use a full URL so Akka/Pekko can resolve the sender.
	sender := fmt.Sprintf("%s://%s@%s:%d/system/cluster/core/daemon",
		assoc.nodeMgr.LocalAddr.GetProtocol(),
		assoc.nodeMgr.LocalAddr.GetSystem(),
		assoc.nodeMgr.LocalAddr.GetHostname(),
		assoc.nodeMgr.LocalAddr.GetPort())

	frame, err := BuildArteryFrame(int64(assoc.localUid), serializerId, sender, recipient, manifest, payload, false)
	if err != nil {
		return err
	}
	log.Printf("Association %p: SENDING frame of %d bytes for remote UID %d, serializerId=%d manifest=%q payloadLen=%d",
		assoc, len(frame), assoc.remote.GetUid(), serializerId, manifest, len(payload))

	assoc.mu.Lock()
	defer assoc.mu.Unlock()

	if assoc.state != ASSOCIATED {
		log.Printf("Association %p: buffering message (state=%v, isAssociated=%v)", assoc, assoc.state, assoc.state == ASSOCIATED)
		assoc.pending = append(assoc.pending, frame)
		return nil
	}

	select {
	case assoc.outbox <- frame:
		return nil
	default:
		return fmt.Errorf("association outbox full")
	}
}

func (assoc *GekkaAssociation) handleControlMessage(ctx context.Context, meta *ArteryMetadata) error {
	manifest := string(meta.MessageManifest)
	log.Printf("Association %p: handling control message with manifest %q", assoc, manifest)
	switch manifest {
	case "d": // HandshakeReq
		req := &gproto_remote.HandshakeReq{}
		if err := proto.Unmarshal(meta.Payload, req); err != nil {
			return err
		}
		return assoc.handleHandshakeReq(req)

	case "e": // HandshakeRsp
		mwa := &gproto_remote.MessageWithAddress{}
		if err := proto.Unmarshal(meta.Payload, mwa); err != nil {
			return err
		}
		return assoc.handleHandshakeRsp(mwa)

	case "m": // ArteryHeartbeat — Pekko ArteryMessageSerializer manifest
		// ArteryHeartbeat is a Scala singleton with an empty payload.
		// Reply immediately with ArteryHeartbeatRsp containing our local UID so
		// Pekko's RemoteWatcher does not mark the Go node as unreachable.
		log.Printf("Association %p: ArteryHeartbeat received — replying with ArteryHeartbeatRsp (uid=%d)", assoc, assoc.localUid)
		rsp := &gproto_remote.ArteryHeartbeatRsp{Uid: proto.Uint64(assoc.localUid)}
		payload, err := proto.Marshal(rsp)
		if err != nil {
			return fmt.Errorf("failed to marshal ArteryHeartbeatRsp: %w", err)
		}
		frame, err := BuildArteryFrame(int64(assoc.localUid), actor.ArteryInternalSerializerID, "", "", "n", payload, true)
		if err != nil {
			return fmt.Errorf("failed to build ArteryHeartbeatRsp frame: %w", err)
		}
		select {
		case assoc.outbox <- frame:
		default:
			log.Printf("Association %p: ArteryHeartbeatRsp outbox full, dropping response", assoc)
		}
		return nil

	case "n": // ArteryHeartbeatRsp — Pekko's reply to a heartbeat we sent
		hb := &gproto_remote.ArteryHeartbeatRsp{}
		if err := proto.Unmarshal(meta.Payload, hb); err != nil {
			return err
		}
		assoc.mu.Lock()
		if !assoc.lastHeartbeatSentAt.IsZero() {
			assoc.lastRTT = time.Since(assoc.lastHeartbeatSentAt)
			log.Printf("Association %p: ArteryHeartbeatRsp received (uid=%d) RTT=%v", assoc, hb.GetUid(), assoc.lastRTT)
		} else {
			log.Printf("Association %p: ArteryHeartbeatRsp received (uid=%d)", assoc, hb.GetUid())
		}
		assoc.mu.Unlock()
		return nil

	case "ActorRefCompressionAdvertisement", "ClassManifestCompressionAdvertisement":
		if assoc.nodeMgr.compressionMgr != nil {
			adv := &gproto_remote.CompressionTableAdvertisement{}
			if err := proto.Unmarshal(meta.Payload, adv); err != nil {
				return err
			}
			isActorRef := manifest == "ActorRefCompressionAdvertisement"
			// Get local address from NodeManager to use in the Ack
			localUA := &gproto_remote.UniqueAddress{
				Address: assoc.nodeMgr.LocalAddr,
				Uid:     proto.Uint64(assoc.nodeMgr.localUid),
			}
			return assoc.nodeMgr.compressionMgr.HandleAdvertisement(ctx, adv, isActorRef, localUA)
		}
		return nil

	case "Quarantined":
		// Remote has detected a UID conflict and is notifying us. Quarantine the association.
		quar := &gproto_remote.Quarantined{}
		if err := proto.Unmarshal(meta.Payload, quar); err != nil {
			return err
		}
		log.Printf("Association %p: received Quarantined from %v", assoc, quar.From)
		assoc.mu.Lock()
		assoc.state = QUARANTINED
		assoc.mu.Unlock()
		return nil

	case "ActorRefCompressionAdvertisementAck", "ClassManifestCompressionAdvertisementAck":
		// We log the ack, but we don't block on receiving it yet.
		// In a full implementation, we'd wait for this before transitioning to using the compressed IDs.
		ack := &gproto_remote.CompressionTableAdvertisementAck{}
		if err := proto.Unmarshal(meta.Payload, ack); err != nil {
			return err
		}
		log.Printf("Association: Received compression table ack %s version %d from %v", manifest, ack.GetVersion(), ack.GetFrom())
		return nil

	default:
		if meta.SerializerId == 6 {
			// MessageContainerSerializer (ID=6) wraps ActorSelectionMessage.
			// Akka sends cluster heartbeats via actorSelection using serializer 6.
			// Decode the SelectionEnvelope and forward inner cluster messages so
			// our failure-detector keeps receiving HBR responses from Go.
			if assoc.nodeMgr.clusterMgr != nil {
				env := &gproto_remote.SelectionEnvelope{}
				if err := proto.Unmarshal(meta.Payload, env); err == nil {
					innerManifest := string(env.GetMessageManifest())
					if env.GetSerializerId() == 5 && innerManifest != "" {
						assoc.mu.RLock()
						remote := assoc.remote
						assoc.mu.RUnlock()
						log.Printf("Association: forwarding actorSelection cluster message manifest=%q to clusterMgr", innerManifest)
						return assoc.nodeMgr.clusterMgr.HandleIncomingClusterMessage(
							ctx, env.GetEnclosedMessage(), innerManifest, ToClusterUniqueAddress(remote))
					}
				}
			}
			return nil
		}
		log.Printf("Association: unidentified control message with manifest %q (id=%d)", manifest, meta.SerializerId)
		return nil
	}
}

func (assoc *GekkaAssociation) handleHandshakeReq(req *gproto_remote.HandshakeReq) error {
	log.Printf("Association %p: received HandshakeReq from %s (role=%v)", assoc, req.From.String(), assoc.role)

	// Validate that the 'To' address matches our local node (Pekko protocol requirement).
	if toSys := req.GetTo().GetSystem(); toSys != "" {
		if localSys := assoc.nodeMgr.LocalAddr.GetSystem(); toSys != localSys {
			return fmt.Errorf("Handshake rejected: To system %q != local system %q", toSys, localSys)
		}
	}

	assoc.mu.Lock()
	assoc.remote = req.From
	assoc.state = ASSOCIATED
	assoc.mu.Unlock()

	assoc.nodeMgr.RegisterAssociation(req.From, assoc)

	// Symmetric Handshake: check if there's an outbound association waiting for a Handshake from this same remote node.
	if assoc.role == INBOUND {
		addr := req.From.GetAddress()
		nm := assoc.nodeMgr
		nm.mu.RLock()
		var matched *GekkaAssociation          // OUTBOUND in WAITING/INITIATED state
		var outboundToRemote *GekkaAssociation // any OUTBOUND to this remote (any state)

		normalize := func(h string) string {
			if h == "localhost" || h == "127.0.0.1" || h == "::1" {
				return "localhost"
			}
			return h
		}

		for k, a := range nm.associations {
			a.mu.RLock()
			isOutbound := a.role == OUTBOUND
			isWaiting := a.state == INITIATED || a.state == WAITING_FOR_HANDSHAKE
			var hostMatch bool
			var aHost string
			var aPort uint32
			if a.remote != nil && a.remote.Address != nil {
				aHost = a.remote.Address.GetHostname()
				aPort = a.remote.Address.GetPort()
				hostMatch = normalize(aHost) == normalize(addr.GetHostname()) &&
					aPort == addr.GetPort()
			}
			a.mu.RUnlock()

			log.Printf("handleHandshakeReq candidate: k=%s role=%v state=%v host=%s port=%d match=%v", k, a.role, a.state, aHost, aPort, hostMatch)

			if isOutbound && hostMatch {
				if outboundToRemote == nil {
					outboundToRemote = a
				}
				if isWaiting && matched == nil {
					matched = a
					log.Printf("handleHandshakeReq: matched Key=%s Association=%p", k, a)
				}
			}
		}
		nm.mu.RUnlock()

		// Build HandshakeRsp frame.
		localUA := &gproto_remote.UniqueAddress{Address: assoc.nodeMgr.LocalAddr, Uid: proto.Uint64(assoc.localUid)}
		rspProto := &gproto_remote.MessageWithAddress{Address: localUA}
		if rspPayload, err2 := proto.Marshal(rspProto); err2 == nil {
			if frame, err2 := BuildArteryFrame(int64(assoc.localUid), actor.ArteryInternalSerializerID, "", "", "e", rspPayload, true); err2 == nil {
				// Artery TCP synchronous handshake: the HandshakeRsp MUST be sent
				// back on the same TCP connection that received the HandshakeReq.
				// This applies to ALL streams (Control, Ordinary, Large) opened by the remote.
				log.Printf("Association %p (INBOUND): sending HandshakeRsp via INBOUND stream %d", assoc, assoc.streamId)
				select {
				case assoc.outbox <- frame:
				default:
					log.Printf("Association %p (INBOUND): outbox full, dropping HandshakeRsp", assoc)
				}
			}
		}

		// Symmetric optimization: if this node also has an OUTBOUND assoc to
		// the same remote (e.g. both nodes dialled each other simultaneously),
		// complete it directly without waiting for an extra round-trip.
		if matched != nil {
			log.Printf("Association %p (INBOUND): completing matching OUTBOUND association %p", assoc, matched)
			matched.mu.Lock()
			matched.remote = req.From
			matched.state = ASSOCIATED
			select {
			case <-matched.Handshake:
			default:
				close(matched.Handshake)
			}
			for _, msg := range matched.pending {
				select {
				case matched.outbox <- msg:
				default:
					log.Printf("Association %p: outbox full, dropping pending frame during handshake flush", matched)
				}
			}
			matched.pending = nil
			matched.mu.Unlock()
			nm.RegisterAssociation(req.From, matched)
		}

		return nil
	}

	// OUTBOUND: flush pending and send HandshakeRsp (if we were the one initiating)
	assoc.mu.Lock()
	for _, msg := range assoc.pending {
		select {
		case assoc.outbox <- msg:
		default:
			log.Printf("Association %p: outbox full, dropping pending frame during handshake flush", assoc)
		}
	}
	assoc.pending = nil
	assoc.mu.Unlock()

	localUA := &gproto_remote.UniqueAddress{Address: assoc.nodeMgr.LocalAddr, Uid: proto.Uint64(assoc.localUid)}
	rsp := &gproto_remote.MessageWithAddress{Address: localUA}

	// Write HandshakeRsp to outbox
	payload, err := proto.Marshal(rsp)
	if err != nil {
		return err
	}
	frame, err := BuildArteryFrame(int64(assoc.localUid), actor.ArteryInternalSerializerID, "", "", "e", payload, true)
	if err != nil {
		return err
	}
	log.Printf("Association %p: sending HandshakeRsp (e) for UID %d", assoc, assoc.localUid)
	select {
	case assoc.outbox <- frame:
	default:
		log.Printf("Association %p: outbox full, dropping HandshakeRsp frame", assoc)
	}
	return nil
}

func (assoc *GekkaAssociation) handleHandshakeRsp(mwa *gproto_remote.MessageWithAddress) error {
	log.Printf("Association %p: received HandshakeRsp from %s", assoc, mwa.Address.String())

	assoc.nodeMgr.mu.RLock()
	// Create a list of candidates to avoid holding nodeMgr lock while locking individuals
	candidates := make([]*GekkaAssociation, 0, len(assoc.nodeMgr.associations))
	for _, a := range assoc.nodeMgr.associations {
		candidates = append(candidates, a)
	}
	assoc.nodeMgr.mu.RUnlock()

	var matched *GekkaAssociation
	for _, a := range candidates {
		a.mu.RLock()
		isOutbound := a.role == OUTBOUND
		isWaiting := a.state == INITIATED || a.state == WAITING_FOR_HANDSHAKE || a.state == ASSOCIATED

		var match bool
		if a.remote != nil && mwa.Address != nil && mwa.Address.Address != nil {
			aHost, aPort := a.remote.Address.GetHostname(), a.remote.Address.GetPort()
			mHost, mPort := mwa.Address.Address.GetHostname(), mwa.Address.Address.GetPort()

			normalize := func(h string) string {
				if h == "localhost" || h == "127.0.0.1" {
					return "127.0.0.1"
				}
				return h
			}
			match = normalize(aHost) == normalize(mHost) && aPort == mPort
			log.Printf("handleHandshakeRsp: Candidate %p: role=%v state=%v match=%v (A:%s:%d vs M:%s:%d)",
				a, a.role, a.state, match, aHost, aPort, mHost, mPort)
		} else {
			log.Printf("handleHandshakeRsp: Candidate %p: role=%v state=%v match=false (a.remote=%v mwa.Address=%p)",
				a, a.role, a.state, a.remote != nil, mwa.Address)
		}
		a.mu.RUnlock()

		if isOutbound && isWaiting && match {
			log.Printf("handleHandshakeRsp: FOUND matching outbound association %p (state=%v)", a, a.state)
			matched = a
			break
		}
	}

	if matched != nil {
		matched.mu.Lock()
		matched.remote = mwa.Address
		matched.state = ASSOCIATED

		// Unblock anybody waiting on this
		select {
		case <-matched.Handshake:
			// already closed
		default:
			close(matched.Handshake)
		}

		// Flush pending messages
		for _, msg := range matched.pending {
			select {
			case matched.outbox <- msg:
			default:
				log.Printf("Association %p: outbox full, dropping pending frame during handshakeRsp flush", matched)
			}
		}
		matched.pending = nil
		matched.mu.Unlock()

		// Re-register with full UniqueAddress (including UID from Pekko)
		assoc.nodeMgr.RegisterAssociation(mwa.Address, matched)
	} else {
		log.Printf("handleHandshakeRsp: WARNING - no matching outbound association found")
	}

	return nil
}
