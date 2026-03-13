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
	}
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

// routePendingReply delivers meta to a waiting Ask call if path is registered.
// Returns true when a waiting caller was found and the message routed.
func (nm *NodeManager) routePendingReply(path string, meta *ArteryMetadata) bool {
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

// GetAssociation looks up an existing association by unique address.
func (nm *NodeManager) GetAssociation(remote *gproto_remote.UniqueAddress) (*GekkaAssociation, bool) {
	if remote == nil || remote.Address == nil {
		return nil, false
	}
	addr := remote.GetAddress()
	key := fmt.Sprintf("%s:%d-%d", addr.GetHostname(), addr.GetPort(), remote.GetUid())
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
	newKey := fmt.Sprintf("%s:%d-%d", addr.GetHostname(), addr.GetPort(), remote.GetUid())

	nm.mu.Lock()
	defer nm.mu.Unlock()

	// UID Check: Check if there's an existing association for the same Host:Port but different UID.
	// UID=0 (newKey) is an early placeholder — the confirmed remote UID is not yet known, so
	// we cannot make a node-restart determination and must skip the quarantine check entirely.
	hostPortKey := fmt.Sprintf("%s:%d-", addr.GetHostname(), addr.GetPort())
	if !strings.HasSuffix(newKey, "-0") {
		for k, existing := range nm.associations {
			if strings.HasPrefix(k, hostPortKey) && k != newKey {
				// Skip early registrations (UID=0 placeholders).
				if strings.HasSuffix(k, "-0") {
					log.Printf("NodeManager: existing early registration %s found for host-port %s", k, hostPortKey)
					continue
				}

				log.Printf("NodeManager: detected node restart for %s. Old UID association %s being quarantined.", hostPortKey, k)
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

	// Never let an INBOUND association overwrite an existing OUTBOUND one.
	// The associations map is used by getAssociationByHost for routing (sending),
	// so OUTBOUND associations must take precedence.
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
}

// ProcessConnection is the unified entry point for both inbound and outbound connections.
func (nm *NodeManager) ProcessConnection(ctx context.Context, conn net.Conn, role AssociationRole, remote *gproto_remote.Address, streamId int32) error {
	if role == INBOUND {
		magic := make([]byte, 5)
		if _, err := io.ReadFull(conn, magic); err != nil {
			return fmt.Errorf("failed to read artery magic: %w", err)
		}
		if string(magic[:4]) != "AKKA" {
			return fmt.Errorf("invalid artery magic: %q", magic[:4])
		}
		streamId = int32(magic[4])
		log.Printf("ProcessConnection: INBOUND magic read, streamId=%d", streamId)
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

	// Start background write loop
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-assoc.outbox:
				if !ok {
					return
				}
				log.Printf("Association %p: SENDING frame of %d bytes", assoc, len(msg))
				if err := WriteFrame(assoc.conn, msg); err != nil {
					log.Printf("Association %p: write error: %v", assoc, err)
					return
				}
			}
		}
	}()

	if role == OUTBOUND && remote != nil {
		go func() {
			// Give handler a moment to start and send magic header
			time.Sleep(500 * time.Millisecond)
			if err := assoc.initiateHandshake(remote); err != nil {
				log.Printf("Association %p: initiateHandshake error: %v", assoc, err)
			}
		}()
	}

	return assoc.Process(ctx)
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

func (assoc *GekkaAssociation) Process(ctx context.Context) error {
	dispatch := func(ctx context.Context, meta *ArteryMetadata) error {
		return assoc.dispatch(ctx, meta)
	}
	remoteUid := uint64(0)
	if assoc.remote != nil {
		remoteUid = assoc.remote.GetUid()
	}
	if assoc.role == OUTBOUND {
		return TcpArteryOutboundHandler(ctx, assoc.conn, dispatch, assoc.nodeMgr.compressionMgr, remoteUid, assoc.streamId)
	}
	return TcpArteryHandlerWithCallback(ctx, assoc.conn, dispatch, assoc.nodeMgr.compressionMgr, remoteUid, assoc.streamId)
}

func (assoc *GekkaAssociation) dispatch(ctx context.Context, meta *ArteryMetadata) error {
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
	return SendArteryMessageWithAck(assoc.conn, int64(assoc.localUid), actor.ArteryInternalSerializerID, "SystemMessageDeliveryAck", ack, to, true)
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
			if idx := strings.LastIndex(recipientPath, "/"); idx != -1 {
				if recipientPath[idx:] != "/" { // e.g. "pekko://System@host:port/user/foo" -> "/user/foo"
					// Find the start of the path part (after the authority)
					// URI format: scheme://authority/path
					// authority is System@host:port
					if firstSlash := strings.Index(recipientPath[strings.Index(recipientPath, "://")+3:], "/"); firstSlash != -1 {
						recipientPath = recipientPath[strings.Index(recipientPath, "://")+3+firstSlash:]
					}
				}
			}
		}
		if recipientPath != "" && assoc.nodeMgr.routePendingReply(recipientPath, meta) {
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

	frame, err := BuildArteryFrame(int64(assoc.localUid), serializerId, senderPath, recipient, manifest, payload, false)
	if err != nil {
		return err
	}
	log.Printf("Association %p: SendWithSender frame of %d bytes, sender=%q", assoc, len(frame), senderPath)

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

	sender := fmt.Sprintf("%s://%s@%s:%d/user/echo",
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
		log.Printf("Association %p: ArteryHeartbeatRsp received (uid=%d)", assoc, hb.GetUid())
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
		// If ID 6 and no manifest, it's likely a CompressionTableAdvertisement
		if meta.SerializerId == 6 && manifest == "" {
			// Try to unmarshal as CompressionTableAdvertisement
			adv := &gproto_remote.CompressionTableAdvertisement{}
			if err := proto.Unmarshal(meta.Payload, adv); err == nil {
				if assoc.nodeMgr.compressionMgr != nil {
					// Guess if it's ActorRef or ClassManifest based on typical Pekko behavior
					// Actually, the Advertisement message itself doesn't say.
					// But we can update both or peek at keys.
					// For now, let's assume if it contains '/' it's ActorRef, otherwise it might be ClassManifest.
					isActorRef := false
					if len(adv.Keys) > 0 && strings.Contains(adv.Keys[0], "/") {
						isActorRef = true
					}
					// If we can't tell, we might have to update both or store it specially.
					// Pekko actually sends them separately.
					// Let's just try to update both if we are unsure, but ideally we'd know.
					// Actually, the manifest is usually "ActorRefCompressionAdvertisement" or "ClassManifestCompressionAdvertisement".
					// If it's missing, Pekko might be using a different scheme.

					log.Printf("Association: Received Advertisement with ID 6 and empty manifest. Guessing isActorRef=%v", isActorRef)
					localUA := &gproto_remote.UniqueAddress{
						Address: assoc.nodeMgr.LocalAddr,
						Uid:     proto.Uint64(assoc.nodeMgr.localUid),
					}
					return assoc.nodeMgr.compressionMgr.HandleAdvertisement(ctx, adv, isActorRef, localUA)
				}
			}
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
		var matched *GekkaAssociation

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

			if isOutbound && isWaiting && hostMatch {
				matched = a
				log.Printf("handleHandshakeReq: matched Key=%s Association=%p", k, a)
				break
			}
		}
		nm.mu.RUnlock()

		// Send HandshakeRsp back to the remote so its OUTBOUND association can
		// transition to ASSOCIATED.  This is correct Artery protocol for both
		// Pekko and Go peers: the outbound side reads HandshakeRsp after sending
		// HandshakeReq.
		localUA := &gproto_remote.UniqueAddress{Address: assoc.nodeMgr.LocalAddr, Uid: proto.Uint64(assoc.localUid)}
		rspProto := &gproto_remote.MessageWithAddress{Address: localUA}
		if rspPayload, err2 := proto.Marshal(rspProto); err2 == nil {
			if frame, err2 := BuildArteryFrame(int64(assoc.localUid), actor.ArteryInternalSerializerID, "", "", "e", rspPayload, true); err2 == nil {
				log.Printf("Association %p (INBOUND): sending HandshakeRsp to remote", assoc)
				assoc.outbox <- frame
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
				matched.outbox <- msg
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
		assoc.outbox <- msg
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
	assoc.outbox <- frame
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
			matched.outbox <- msg
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
