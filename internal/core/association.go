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
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster"
	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// Pekko defaults for pekko.remote.artery.advanced.* knobs. Used when a
// NodeManager field is left zero-valued.
const (
	DefaultInboundLanes                  = 4
	DefaultOutboundLanes                 = 1
	DefaultOutboundMessageQueueSize      = 3072
	DefaultSystemMessageBufferSize       = 20000
	DefaultOutboundControlQueueSize      = 20000
	DefaultInboundMaxRestarts            = 5
	DefaultOutboundMaxRestarts           = 5
	DefaultCompressionActorRefsMax       = 256
	DefaultCompressionManifestsMax       = 256
	DefaultBufferPoolSize                = 128
	DefaultMaximumLargeFrameSize         = 2 * 1024 * 1024
	DefaultLargeBufferPoolSize           = 32
	DefaultOutboundLargeMessageQueueSize = 256
)

// Pekko defaults for pekko.remote.artery.advanced.* duration knobs.
var (
	DefaultHandshakeTimeout                          = 20 * time.Second
	DefaultHandshakeRetryInterval                    = 1 * time.Second
	DefaultSystemMessageResendInterval               = 1 * time.Second
	DefaultGiveUpSystemMessageAfter                  = 6 * time.Hour
	DefaultStopIdleOutboundAfter                     = 5 * time.Minute
	DefaultQuarantineIdleOutboundAfter               = 6 * time.Hour
	DefaultStopQuarantinedAfterIdle                  = 3 * time.Second
	DefaultRemoveQuarantinedAssociationAfter         = 1 * time.Hour
	DefaultShutdownFlushTimeout                      = 1 * time.Second
	DefaultDeathWatchNotificationFlushTimeout        = 3 * time.Second
	DefaultInboundRestartTimeout                     = 5 * time.Second
	DefaultOutboundRestartBackoff                    = 1 * time.Second
	DefaultOutboundRestartTimeout                    = 5 * time.Second
	DefaultCompressionActorRefsAdvertisementInterval = 1 * time.Minute
	DefaultCompressionManifestsAdvertisementInterval = 1 * time.Minute
	DefaultTcpConnectionTimeout                      = 5 * time.Second
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

// outboundLane is one TCP connection in a multi-lane outbound association.
// Sub-plan 8f outbound half: when EffectiveOutboundLanes() > 1 (or even == 1
// to match Pekko's default of one TCP per ordinary stream), an OUTBOUND
// streamId=2 *GekkaAssociation* holds N parallel lanes, each with its own
// TCP connection, outbox, writer goroutine, and per-lane handshake state.
// Each lane carries a fraction of the user-message traffic, hashed by
// recipient actor path. This delivers true Pekko parity for
// pekko.remote.artery.advanced.outbound-lanes: independent kernel send
// buffers, no TCP head-of-line blocking across recipients on different
// lanes, parallel writer goroutines.
type outboundLane struct {
	idx           int
	conn          net.Conn
	outbox        chan []byte
	state         AssociationState
	handshakeDone chan struct{}
	localUid      uint64
	remoteUid     uint64
	writeMu       sync.Mutex // serializes WriteFrame on lane.conn
}

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

	// UDP transport fields — set when this association uses the Aeron-UDP
	// transport instead of a TCP connection.  When udpHandler is non-nil the
	// outbox drainer sends frames via udpHandler.SendFrame rather than
	// WriteFrame(conn, …).
	udpHandler        *UdpArteryHandler
	udpDst            *net.UDPAddr
	// udpOrdinaryOutbox carries user messages destined for Aeron stream 2
	// (OrdinaryStream).  Control/cluster/artery-internal messages continue to
	// use the primary outbox (stream 1).  Nil for TCP associations.
	udpOrdinaryOutbox chan []byte
	// udpLargeOutbox carries user messages destined for Aeron stream 3
	// (LargeStream). Used when the recipient path matches a configured
	// large-message-destinations glob. Nil for TCP associations.
	udpLargeOutbox chan []byte

	// inboundLanes is the per-association inbound dispatch fan-out. When
	// pekko.remote.artery.advanced.inbound-lanes > 1 and streamId != 1
	// (control), ProcessConnection allocates N lane channels and starts N
	// drain goroutines that call assoc.dispatch on each frame, hashing by
	// recipient actor path. When nil, dispatch runs single-threaded on the
	// read goroutine (legacy behavior). See dispatchSharded.
	inboundLanes   []chan *ArteryMetadata
	inboundLanesWg sync.WaitGroup

	// lanes is the per-association OUTBOUND lane fan-out for streamId=2
	// (Ordinary). Populated only on streamId=2 OUTBOUND associations
	// constructed by EnsureOrdinarySibling. Nil for streamId=1 (control)
	// and streamId=3 (large) which stay single-conn. Sub-plan 8f outbound
	// half — pekko.remote.artery.advanced.outbound-lanes.
	lanes []*outboundLane
	// inboundConns lists all inbound TCPs that have been coalesced into a
	// single logical INBOUND streamId=2 association by remote UID. The
	// initial accept becomes inboundConns[0]; subsequent inbound TCPs from
	// the same peer UID for streamId=2 attach via handleHandshakeReq's
	// coalescence path. Sub-plan 8f outbound half.
	inboundConns []net.Conn
	// ordinarySibling links a control assoc (streamId=1) to its ordinary
	// lane assoc (streamId=2) and vice versa. Set by EnsureOrdinarySibling.
	// outboxFor uses this pointer to redirect user-message traffic from the
	// control stream onto the ordinary lanes. When nil, user traffic falls
	// back to the control stream (the convergence-window pre-sibling
	// behaviour, also the default when outbound-lanes is unset).
	ordinarySibling     *GekkaAssociation
	ordinarySiblingOnce sync.Once
	// delegate, when non-nil, redirects this assoc's dispatch path to the
	// pointed-to assoc. Used by inbound coalescence: the temporary assoc
	// constructed for the second/third inbound streamId=2 TCP from the
	// same peer UID delegates to the primary association so its inbound
	// lanes machinery handles all frames uniformly. Sub-plan 8f outbound
	// half (inbound coalescence).
	delegate *GekkaAssociation
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
	SystemMessageCallback func(remote *gproto_remote.UniqueAddress, env *gproto_remote.SystemMessageEnvelope, msg *gproto_remote.SystemMessage) error

	pendingRepliesMu sync.RWMutex
	pendingReplies   map[string]chan *ArteryMetadata // keyed by temp actor path

	// quarantinedMu guards quarantinedUIDs.
	quarantinedMu sync.RWMutex
	// quarantinedUIDs is a permanent registry of remote node UIDs that have been
	// quarantined.  Unlike the associations map (which removes the entry upon
	// quarantine), this registry persists so that a restarting remote node with
	// the same UID cannot re-associate until the local process restarts.
	// Value is the UniqueAddress of the remote node at the time of quarantine.
	quarantinedUIDs map[uint64]*gproto_remote.UniqueAddress

	// mutedMu guards mutedNodes.
	mutedMu sync.RWMutex
	// mutedNodes is a set of "host:port" strings for which all outbound
	// frames are silently dropped and all inbound frames are ignored.
	// Populate via MuteNode / UnmuteNode to simulate a network partition
	// in tests without terminating the process.
	mutedNodes map[string]struct{}

	// pendingDials tracks in-progress DialRemote calls initiated by
	// handleHandshakeReq when Go is the seed and has no outbound to
	// a newly-connecting Pekko node. Keyed by "host:port". Protected
	// by mu (the NodeManager-level RWMutex).
	pendingDials map[string]bool

	// TLSConfig is the *tls.Config to use for outbound connections.
	// Nil means plain TCP (default).
	TLSConfig *tls.Config

	// NodeMetrics is the shared NodeMetrics instance (set by Cluster.Spawn).
	// Nil-safe: all callers check before touching.
	NodeMetrics *NodeMetrics

	// FlightRec is the Artery Flight Recorder for this node. Nil-safe.
	FlightRec *FlightRecorder

	// MaxFrameSize is the maximum Artery frame payload size accepted by the
	// read loop.  Frames larger than this are rejected.
	// Corresponds to pekko.remote.artery.advanced.maximum-frame-size.
	// Default: DefaultMaxFrameSize (256 KiB).  Zero means use the default.
	MaxFrameSize int

	// InboundLanes is the number of parallel inbound dispatch lanes per
	// association (pekko.remote.artery.advanced.inbound-lanes).
	// Zero means use DefaultInboundLanes (4).
	InboundLanes int

	// OutboundLanes is the number of parallel outbound lanes per association
	// (pekko.remote.artery.advanced.outbound-lanes).
	// Zero means use DefaultOutboundLanes (1).
	OutboundLanes int

	// OutboundMessageQueueSize is the capacity of each association's
	// outbound message queue (pekko.remote.artery.advanced.outbound-message-queue-size).
	// Zero means use DefaultOutboundMessageQueueSize (3072).
	OutboundMessageQueueSize int

	// SystemMessageBufferSize is the capacity of the sender-side buffer of
	// unacknowledged system messages per association
	// (pekko.remote.artery.advanced.system-message-buffer-size).
	// Zero means use DefaultSystemMessageBufferSize (20000).
	SystemMessageBufferSize int

	// HandshakeTimeout is the hard deadline for an outbound association to
	// reach the ASSOCIATED state before the handshake loop gives up.
	// (pekko.remote.artery.advanced.handshake-timeout).
	// Zero means use DefaultHandshakeTimeout (20s).
	HandshakeTimeout time.Duration

	// HandshakeRetryInterval is the cadence at which HandshakeReq is
	// re-transmitted while the outbound association is waiting for a
	// HandshakeRsp.
	// (pekko.remote.artery.advanced.handshake-retry-interval).
	// Zero means use DefaultHandshakeRetryInterval (1s).
	HandshakeRetryInterval time.Duration

	// SystemMessageResendInterval is the cadence at which unacknowledged
	// system messages are re-sent. Recorded for the sender-side redelivery
	// consumer.
	// (pekko.remote.artery.advanced.system-message-resend-interval).
	// Zero means use DefaultSystemMessageResendInterval (1s).
	SystemMessageResendInterval time.Duration

	// GiveUpSystemMessageAfter is the ultimatum after which an unacknowledged
	// system message triggers association quarantine. Recorded for the
	// sender-side redelivery consumer.
	// (pekko.remote.artery.advanced.give-up-system-message-after).
	// Zero means use DefaultGiveUpSystemMessageAfter (6h).
	GiveUpSystemMessageAfter time.Duration

	// OutboundControlQueueSize is the capacity of each outbound control-stream
	// (streamId=1) association's outbox (handshake, heartbeat, system
	// messages). Separate knob from OutboundMessageQueueSize so control
	// traffic never starves when ordinary traffic saturates.
	// (pekko.remote.artery.advanced.outbound-control-queue-size).
	// Zero means use DefaultOutboundControlQueueSize (20000).
	OutboundControlQueueSize int

	// StopIdleOutboundAfter is the idle duration after which an outbound
	// association that has not been used is stopped.
	// (pekko.remote.artery.advanced.stop-idle-outbound-after).
	// Zero means use DefaultStopIdleOutboundAfter (5m).
	StopIdleOutboundAfter time.Duration

	// QuarantineIdleOutboundAfter is the idle duration after which an unused
	// outbound association is quarantined.  Consumed by
	// SweepIdleOutboundQuarantine.
	// (pekko.remote.artery.advanced.quarantine-idle-outbound-after).
	// Zero means use DefaultQuarantineIdleOutboundAfter (6h).
	QuarantineIdleOutboundAfter time.Duration

	// StopQuarantinedAfterIdle is the idle duration after which the outbound
	// stream of a quarantined association is stopped.
	// (pekko.remote.artery.advanced.stop-quarantined-after-idle).
	// Zero means use DefaultStopQuarantinedAfterIdle (3s).
	StopQuarantinedAfterIdle time.Duration

	// RemoveQuarantinedAssociationAfter is the duration after which a
	// quarantined association is removed from the registry.
	// (pekko.remote.artery.advanced.remove-quarantined-association-after).
	// Zero means use DefaultRemoveQuarantinedAssociationAfter (1h).
	RemoveQuarantinedAssociationAfter time.Duration

	// ShutdownFlushTimeout is how long ActorSystem termination waits for
	// pending outbound messages to flush before closing the transport.
	// (pekko.remote.artery.advanced.shutdown-flush-timeout).
	// Zero means use DefaultShutdownFlushTimeout (1s).
	ShutdownFlushTimeout time.Duration

	// DeathWatchNotificationFlushTimeout is how long the remote layer waits
	// before sending a DeathWatchNotification, to let prior messages flush.
	// (pekko.remote.artery.advanced.death-watch-notification-flush-timeout).
	// Zero means use DefaultDeathWatchNotificationFlushTimeout (3s).
	DeathWatchNotificationFlushTimeout time.Duration

	// InboundRestartTimeout is the rolling window in which InboundMaxRestarts
	// caps inbound-stream restarts. Consumed by the RestartTracker.
	// (pekko.remote.artery.advanced.inbound-restart-timeout).
	// Zero means use DefaultInboundRestartTimeout (5s).
	InboundRestartTimeout time.Duration

	// InboundMaxRestarts is the maximum number of inbound-stream restarts
	// permitted inside InboundRestartTimeout. Consumed by the RestartTracker
	// (TryRecordInboundRestart returns false when the cap is exceeded).
	// (pekko.remote.artery.advanced.inbound-max-restarts).
	// Zero means use DefaultInboundMaxRestarts (5).
	InboundMaxRestarts int

	// OutboundRestartBackoff is the backoff applied before retrying an
	// outbound TCP connection.  Recorded for the dialer consumer.
	// (pekko.remote.artery.advanced.outbound-restart-backoff).
	// Zero means use DefaultOutboundRestartBackoff (1s).
	OutboundRestartBackoff time.Duration

	// OutboundRestartTimeout is the rolling window in which OutboundMaxRestarts
	// caps outbound-stream restarts. Consumed by the RestartTracker.
	// (pekko.remote.artery.advanced.outbound-restart-timeout).
	// Zero means use DefaultOutboundRestartTimeout (5s).
	OutboundRestartTimeout time.Duration

	// OutboundMaxRestarts is the maximum number of outbound-stream restarts
	// permitted inside OutboundRestartTimeout. Consumed by the RestartTracker
	// (TryRecordOutboundRestart returns false when the cap is exceeded).
	// (pekko.remote.artery.advanced.outbound-max-restarts).
	// Zero means use DefaultOutboundMaxRestarts (5).
	OutboundMaxRestarts int

	// CompressionActorRefsMax is the maximum number of compressed actor-ref
	// entries per received advertisement. Updates whose key count exceeds
	// the cap are rejected by the CompressionTableManager.
	// (pekko.remote.artery.advanced.compression.actor-refs.max).
	// Zero means use DefaultCompressionActorRefsMax (256).
	CompressionActorRefsMax int

	// CompressionActorRefsAdvertisementInterval is the cadence at which the
	// local actor-ref compression table is advertised to remote peers.
	// Consumed by CompressionTableManager.StartAdvertisementScheduler.
	// (pekko.remote.artery.advanced.compression.actor-refs.advertisement-interval).
	// Zero means use DefaultCompressionActorRefsAdvertisementInterval (1m).
	CompressionActorRefsAdvertisementInterval time.Duration

	// CompressionManifestsMax is the maximum number of compressed manifest
	// entries per received advertisement. Updates whose key count exceeds
	// the cap are rejected by the CompressionTableManager.
	// (pekko.remote.artery.advanced.compression.manifests.max).
	// Zero means use DefaultCompressionManifestsMax (256).
	CompressionManifestsMax int

	// CompressionManifestsAdvertisementInterval is the cadence at which the
	// local manifest compression table is advertised to remote peers.
	// Consumed by CompressionTableManager.StartAdvertisementScheduler.
	// (pekko.remote.artery.advanced.compression.manifests.advertisement-interval).
	// Zero means use DefaultCompressionManifestsAdvertisementInterval (1m).
	CompressionManifestsAdvertisementInterval time.Duration

	// TcpConnectionTimeout is the TCP dial timeout used by DialRemote. Also
	// applied to the "wait for association to appear" poll loop.
	// (pekko.remote.artery.advanced.tcp.connection-timeout).
	// Zero means use DefaultTcpConnectionTimeout (5s).
	TcpConnectionTimeout time.Duration

	// TcpOutboundClientHostname, when non-empty, is used as the local source
	// hostname for outbound Artery TCP connections (net.Dialer.LocalAddr).
	// Empty means the OS picks the local address.
	// (pekko.remote.artery.advanced.tcp.outbound-client-hostname).
	TcpOutboundClientHostname string

	// BufferPoolSize is the size of the shared receive buffer pool per stream.
	// Recorded on NodeManager for future buffer-pool consumers.
	// (pekko.remote.artery.advanced.buffer-pool-size).
	// Zero means use DefaultBufferPoolSize (128).
	BufferPoolSize int

	// MaximumLargeFrameSize is the max frame payload for the large-message
	// stream (streamId=3). Consumed by the large-stream read/write paths.
	// (pekko.remote.artery.advanced.maximum-large-frame-size).
	// Zero means use DefaultMaximumLargeFrameSize (2 MiB).
	MaximumLargeFrameSize int

	// LargeBufferPoolSize is the size of the shared receive buffer pool for
	// the large-message stream. Recorded on NodeManager for future consumers.
	// (pekko.remote.artery.advanced.large-buffer-pool-size).
	// Zero means use DefaultLargeBufferPoolSize (32).
	LargeBufferPoolSize int

	// OutboundLargeMessageQueueSize is the outbox capacity for the
	// large-message stream (streamId=3). Sizes the per-association outbox
	// channel when the large stream is opened.
	// (pekko.remote.artery.advanced.outbound-large-message-queue-size).
	// Zero means use DefaultOutboundLargeMessageQueueSize (256).
	OutboundLargeMessageQueueSize int

	// restartMu guards inboundRestarts and outboundRestarts.
	restartMu        sync.Mutex
	inboundRestarts  []time.Time
	outboundRestarts []time.Time

	// AcceptProtocolNames lists protocol names accepted during Artery handshake.
	// Default: ["pekko", "akka"].
	AcceptProtocolNames []string

	// LogReceivedMessages enables DEBUG-level logging of inbound user messages.
	// Corresponds to pekko.remote.artery.log-received-messages.
	LogReceivedMessages bool

	// LogSentMessages enables DEBUG-level logging of outbound user messages.
	// Corresponds to pekko.remote.artery.log-sent-messages.
	LogSentMessages bool

	// LogFrameSizeExceeding emits a warning when an outbound payload exceeds
	// this many bytes (0 = off).
	// Corresponds to pekko.remote.artery.log-frame-size-exceeding.
	LogFrameSizeExceeding int64

	// PropagateHarmlessQuarantineEvents propagates harmless quarantine events
	// (legacy Pekko 1.x behavior). Default off.
	// Corresponds to pekko.remote.artery.propagate-harmless-quarantine-events.
	PropagateHarmlessQuarantineEvents bool

	// UntrustedMode rejects PossiblyHarmful and DeathWatch system messages
	// from remote senders, drops messages addressed to /system/* paths, and
	// (paired with TrustedSelectionPaths) restricts inbound ActorSelection.
	// Corresponds to pekko.remote.artery.untrusted-mode. Default off.
	UntrustedMode bool

	// TrustedSelectionPaths is the allowlist of actor-path prefixes that may
	// receive inbound ActorSelection messages when UntrustedMode is on.
	// Empty list under untrusted-mode blocks all selections.
	// Corresponds to pekko.remote.artery.trusted-selection-paths.
	TrustedSelectionPaths []string

	// frameSizeMu guards frameSizeMaxByType.
	frameSizeMu sync.Mutex
	// frameSizeMaxByType tracks the largest payload seen per (serializerId,
	// manifest) tuple to honor Pekko's "log once, then only on +10% growth"
	// rule for log-frame-size-exceeding.
	frameSizeMaxByType map[string]int64

	// UDPHandler is set when the Aeron-UDP transport is active.  When non-nil,
	// DialRemote switches to the UDP path automatically.
	UDPHandler *UdpArteryHandler

	// udpSrcAssoc maps the physical UDP source address (e.g. "127.0.0.1:62159")
	// to an association.  This is separate from `associations` because
	// handleHandshakeReq overwrites assoc.remote with the canonical Akka
	// address (port 2561), making the port-based lookup in getAnyAssociationByHost
	// fail for subsequent frames from the ephemeral media-driver port.
	// Guarded by mu.
	udpSrcAssoc map[string]*GekkaAssociation

	// largeRouter resolves outbound recipient paths to streamId 3 when they
	// match a configured glob in pekko.remote.artery.large-message-destinations.
	// Nil-safe: a nil router treats everything as non-large.
	// Guarded by mu (read-mostly; replaced via SetLargeMessageDestinations).
	largeRouter *LargeMessageRouter
}

func NewNodeManager(local *gproto_remote.Address, uid uint64) *NodeManager {
	return &NodeManager{
		LocalAddr:          local,
		associations:       make(map[string]*GekkaAssociation),
		localUid:           uid,
		SerializerRegistry: NewSerializationRegistry(),
		pendingReplies:     make(map[string]chan *ArteryMetadata),
		mutedNodes:         make(map[string]struct{}),
		udpSrcAssoc:        make(map[string]*GekkaAssociation),
		quarantinedUIDs:    make(map[uint64]*gproto_remote.UniqueAddress),
		FlightRec:          NewFlightRecorder(true, LevelLifecycle),
	}
}

// EffectiveInboundLanes returns the configured inbound-lanes or the Pekko default.
func (nm *NodeManager) EffectiveInboundLanes() int {
	if nm.InboundLanes > 0 {
		return nm.InboundLanes
	}
	return DefaultInboundLanes
}

// SetLargeMessageDestinations replaces the large-message router with one
// compiled from the supplied actor-path globs. Round-2 session 29 hooks this
// into the Cluster spawn pipeline.
func (nm *NodeManager) SetLargeMessageDestinations(patterns []string) {
	router := NewLargeMessageRouter(patterns)
	nm.mu.Lock()
	nm.largeRouter = router
	nm.mu.Unlock()
}

// IsLargeRecipient reports whether the supplied recipient path is configured
// to route over the dedicated large-message stream.
func (nm *NodeManager) IsLargeRecipient(recipient string) bool {
	nm.mu.RLock()
	r := nm.largeRouter
	nm.mu.RUnlock()
	return r.IsLarge(recipient)
}

// EmitHarmlessQuarantineEvent logs a quarantine event whose severity is
// controlled by pekko.remote.artery.propagate-harmless-quarantine-events.
// When the flag is on, the event is logged at WARN (legacy Pekko 1.x
// behavior); otherwise it is downgraded to DEBUG so the cluster does not
// emit noisy warnings for already-handled quarantine cases.
func (nm *NodeManager) EmitHarmlessQuarantineEvent(msg string, attrs ...any) {
	if nm.PropagateHarmlessQuarantineEvents {
		slog.Warn(msg, attrs...)
		return
	}
	slog.Debug(msg, attrs...)
}

// recordOversizedFrame logs a warning when an outbound payload exceeds
// LogFrameSizeExceeding. To match Pekko semantics, the maximum size is logged
// once per (serializerId, manifest) and only re-logged if the new size grows
// at least 10% beyond the previous high-water mark.
func (nm *NodeManager) recordOversizedFrame(serializerId int32, manifest string, size int64) {
	if nm.LogFrameSizeExceeding <= 0 || size <= nm.LogFrameSizeExceeding {
		return
	}
	key := fmt.Sprintf("%d|%s", serializerId, manifest)
	nm.frameSizeMu.Lock()
	if nm.frameSizeMaxByType == nil {
		nm.frameSizeMaxByType = make(map[string]int64)
	}
	prev := nm.frameSizeMaxByType[key]
	threshold := prev + prev/10 // +10%
	if size <= prev || (prev > 0 && size <= threshold) {
		nm.frameSizeMu.Unlock()
		return
	}
	nm.frameSizeMaxByType[key] = size
	nm.frameSizeMu.Unlock()
	slog.Warn("artery: frame size exceeds threshold",
		"serializerId", serializerId,
		"manifest", manifest,
		"payload_bytes", size,
		"threshold_bytes", nm.LogFrameSizeExceeding,
		"previous_max", prev)
}

// IsPossiblyHarmfulManifest reports whether the given (serializerId, manifest)
// pair identifies an inbound message that Pekko's RemoteActorRefProvider would
// classify as PossiblyHarmful under untrusted-mode. Pekko treats PoisonPill and
// Kill (MiscMessageSerializer manifests "P"/"K") plus the DeathWatch system
// messages (ArteryInternalSerializer manifest "SystemMessage" wrapping
// Watch/Unwatch) as PossiblyHarmful and drops them when untrusted-mode is on.
func IsPossiblyHarmfulManifest(serializerId int32, manifest string) bool {
	if serializerId == MiscMessageSerializerID && (manifest == "P" || manifest == "K") {
		return true
	}
	if serializerId == actor.ArteryInternalSerializerID && manifest == "SystemMessage" {
		return true
	}
	return false
}

// untrustedModeDrop reports whether UntrustedMode is on and the inbound message
// must be dropped per Pekko Artery untrusted-mode semantics. The caller logs
// the drop at WARN exactly once per (serializerId, manifest) tuple via
// recordUntrustedDrop. ActorSelection enforcement is performed at the
// SelectionEnvelope dispatch site (see trusted-selection-paths).
func (nm *NodeManager) untrustedModeDrop(serializerId int32, manifest string) bool {
	if !nm.UntrustedMode {
		return false
	}
	return IsPossiblyHarmfulManifest(serializerId, manifest)
}

// recordUntrustedDrop logs a WARN once per (serializerId, manifest) drop tuple.
// Subsequent drops of the same tuple are downgraded to DEBUG to avoid log
// flooding when a misbehaving peer is repeatedly probed.
func (nm *NodeManager) recordUntrustedDrop(serializerId int32, manifest, reason, recipientPath, senderPath string) {
	key := fmt.Sprintf("untrusted|%d|%s", serializerId, manifest)
	nm.frameSizeMu.Lock()
	if nm.frameSizeMaxByType == nil {
		nm.frameSizeMaxByType = make(map[string]int64)
	}
	first := nm.frameSizeMaxByType[key] == 0
	if first {
		nm.frameSizeMaxByType[key] = 1
	}
	nm.frameSizeMu.Unlock()
	if first {
		slog.Warn("artery: untrusted-mode dropping inbound message",
			"reason", reason,
			"serializerId", serializerId,
			"manifest", manifest,
			"recipient", recipientPath,
			"sender", senderPath)
	} else {
		slog.Debug("artery: untrusted-mode dropping inbound message",
			"reason", reason,
			"serializerId", serializerId,
			"manifest", manifest,
			"recipient", recipientPath,
			"sender", senderPath)
	}
}

// IsTrustedSelectionPath reports whether path is allowlisted for inbound
// ActorSelection delivery under untrusted-mode. Matching is exact-prefix on
// the bare actor path (the URI scheme/authority are stripped before
// comparison) — mirroring Pekko's Endpoint.scala behaviour where the path is
// composed via `sel.elements.mkString("/", "/", "")` and tested with
// `TrustedSelectionPaths.contains(...)`. With an empty allowlist no selection
// is trusted.
func (nm *NodeManager) IsTrustedSelectionPath(path string) bool {
	if len(nm.TrustedSelectionPaths) == 0 {
		return false
	}
	bare := path
	if i := strings.Index(bare, "://"); i != -1 {
		if j := strings.Index(bare[i+3:], "/"); j != -1 {
			bare = bare[i+3+j:]
		}
	}
	for _, allowed := range nm.TrustedSelectionPaths {
		if allowed == bare {
			return true
		}
	}
	return false
}

// EffectiveOutboundLanes returns the configured outbound-lanes or the Pekko default.
func (nm *NodeManager) EffectiveOutboundLanes() int {
	if nm.OutboundLanes > 0 {
		return nm.OutboundLanes
	}
	return DefaultOutboundLanes
}

// EffectiveOutboundMessageQueueSize returns the configured outbox capacity or the Pekko default.
func (nm *NodeManager) EffectiveOutboundMessageQueueSize() int {
	if nm.OutboundMessageQueueSize > 0 {
		return nm.OutboundMessageQueueSize
	}
	return DefaultOutboundMessageQueueSize
}

// EffectiveSystemMessageBufferSize returns the configured system-message-buffer or the Pekko default.
func (nm *NodeManager) EffectiveSystemMessageBufferSize() int {
	if nm.SystemMessageBufferSize > 0 {
		return nm.SystemMessageBufferSize
	}
	return DefaultSystemMessageBufferSize
}

// EffectiveHandshakeTimeout returns the configured handshake-timeout or the Pekko default.
func (nm *NodeManager) EffectiveHandshakeTimeout() time.Duration {
	if nm.HandshakeTimeout > 0 {
		return nm.HandshakeTimeout
	}
	return DefaultHandshakeTimeout
}

// EffectiveHandshakeRetryInterval returns the configured handshake-retry-interval or the Pekko default.
func (nm *NodeManager) EffectiveHandshakeRetryInterval() time.Duration {
	if nm.HandshakeRetryInterval > 0 {
		return nm.HandshakeRetryInterval
	}
	return DefaultHandshakeRetryInterval
}

// EffectiveSystemMessageResendInterval returns the configured system-message-resend-interval or the Pekko default.
func (nm *NodeManager) EffectiveSystemMessageResendInterval() time.Duration {
	if nm.SystemMessageResendInterval > 0 {
		return nm.SystemMessageResendInterval
	}
	return DefaultSystemMessageResendInterval
}

// EffectiveGiveUpSystemMessageAfter returns the configured give-up-system-message-after or the Pekko default.
func (nm *NodeManager) EffectiveGiveUpSystemMessageAfter() time.Duration {
	if nm.GiveUpSystemMessageAfter > 0 {
		return nm.GiveUpSystemMessageAfter
	}
	return DefaultGiveUpSystemMessageAfter
}

// EffectiveOutboundControlQueueSize returns the configured outbound-control-queue-size or the Pekko default.
func (nm *NodeManager) EffectiveOutboundControlQueueSize() int {
	if nm.OutboundControlQueueSize > 0 {
		return nm.OutboundControlQueueSize
	}
	return DefaultOutboundControlQueueSize
}

// EffectiveStopIdleOutboundAfter returns the configured stop-idle-outbound-after or the Pekko default.
func (nm *NodeManager) EffectiveStopIdleOutboundAfter() time.Duration {
	if nm.StopIdleOutboundAfter > 0 {
		return nm.StopIdleOutboundAfter
	}
	return DefaultStopIdleOutboundAfter
}

// EffectiveQuarantineIdleOutboundAfter returns the configured quarantine-idle-outbound-after or the Pekko default.
func (nm *NodeManager) EffectiveQuarantineIdleOutboundAfter() time.Duration {
	if nm.QuarantineIdleOutboundAfter > 0 {
		return nm.QuarantineIdleOutboundAfter
	}
	return DefaultQuarantineIdleOutboundAfter
}

// EffectiveStopQuarantinedAfterIdle returns the configured stop-quarantined-after-idle or the Pekko default.
func (nm *NodeManager) EffectiveStopQuarantinedAfterIdle() time.Duration {
	if nm.StopQuarantinedAfterIdle > 0 {
		return nm.StopQuarantinedAfterIdle
	}
	return DefaultStopQuarantinedAfterIdle
}

// EffectiveRemoveQuarantinedAssociationAfter returns the configured remove-quarantined-association-after or the Pekko default.
func (nm *NodeManager) EffectiveRemoveQuarantinedAssociationAfter() time.Duration {
	if nm.RemoveQuarantinedAssociationAfter > 0 {
		return nm.RemoveQuarantinedAssociationAfter
	}
	return DefaultRemoveQuarantinedAssociationAfter
}

// EffectiveShutdownFlushTimeout returns the configured shutdown-flush-timeout or the Pekko default.
func (nm *NodeManager) EffectiveShutdownFlushTimeout() time.Duration {
	if nm.ShutdownFlushTimeout > 0 {
		return nm.ShutdownFlushTimeout
	}
	return DefaultShutdownFlushTimeout
}

// EffectiveDeathWatchNotificationFlushTimeout returns the configured death-watch-notification-flush-timeout or the Pekko default.
func (nm *NodeManager) EffectiveDeathWatchNotificationFlushTimeout() time.Duration {
	if nm.DeathWatchNotificationFlushTimeout > 0 {
		return nm.DeathWatchNotificationFlushTimeout
	}
	return DefaultDeathWatchNotificationFlushTimeout
}

// EffectiveInboundRestartTimeout returns the configured inbound-restart-timeout or the Pekko default.
func (nm *NodeManager) EffectiveInboundRestartTimeout() time.Duration {
	if nm.InboundRestartTimeout > 0 {
		return nm.InboundRestartTimeout
	}
	return DefaultInboundRestartTimeout
}

// EffectiveInboundMaxRestarts returns the configured inbound-max-restarts or the Pekko default.
func (nm *NodeManager) EffectiveInboundMaxRestarts() int {
	if nm.InboundMaxRestarts > 0 {
		return nm.InboundMaxRestarts
	}
	return DefaultInboundMaxRestarts
}

// EffectiveOutboundRestartBackoff returns the configured outbound-restart-backoff or the Pekko default.
func (nm *NodeManager) EffectiveOutboundRestartBackoff() time.Duration {
	if nm.OutboundRestartBackoff > 0 {
		return nm.OutboundRestartBackoff
	}
	return DefaultOutboundRestartBackoff
}

// EffectiveOutboundRestartTimeout returns the configured outbound-restart-timeout or the Pekko default.
func (nm *NodeManager) EffectiveOutboundRestartTimeout() time.Duration {
	if nm.OutboundRestartTimeout > 0 {
		return nm.OutboundRestartTimeout
	}
	return DefaultOutboundRestartTimeout
}

// EffectiveOutboundMaxRestarts returns the configured outbound-max-restarts or the Pekko default.
func (nm *NodeManager) EffectiveOutboundMaxRestarts() int {
	if nm.OutboundMaxRestarts > 0 {
		return nm.OutboundMaxRestarts
	}
	return DefaultOutboundMaxRestarts
}

// EffectiveCompressionActorRefsMax returns the configured compression.actor-refs.max or the Pekko default.
func (nm *NodeManager) EffectiveCompressionActorRefsMax() int {
	if nm.CompressionActorRefsMax > 0 {
		return nm.CompressionActorRefsMax
	}
	return DefaultCompressionActorRefsMax
}

// EffectiveCompressionActorRefsAdvertisementInterval returns the configured compression.actor-refs.advertisement-interval or the Pekko default.
func (nm *NodeManager) EffectiveCompressionActorRefsAdvertisementInterval() time.Duration {
	if nm.CompressionActorRefsAdvertisementInterval > 0 {
		return nm.CompressionActorRefsAdvertisementInterval
	}
	return DefaultCompressionActorRefsAdvertisementInterval
}

// EffectiveCompressionManifestsMax returns the configured compression.manifests.max or the Pekko default.
func (nm *NodeManager) EffectiveCompressionManifestsMax() int {
	if nm.CompressionManifestsMax > 0 {
		return nm.CompressionManifestsMax
	}
	return DefaultCompressionManifestsMax
}

// EffectiveCompressionManifestsAdvertisementInterval returns the configured compression.manifests.advertisement-interval or the Pekko default.
func (nm *NodeManager) EffectiveCompressionManifestsAdvertisementInterval() time.Duration {
	if nm.CompressionManifestsAdvertisementInterval > 0 {
		return nm.CompressionManifestsAdvertisementInterval
	}
	return DefaultCompressionManifestsAdvertisementInterval
}

// EffectiveTcpConnectionTimeout returns the configured tcp.connection-timeout or the Pekko default.
func (nm *NodeManager) EffectiveTcpConnectionTimeout() time.Duration {
	if nm.TcpConnectionTimeout > 0 {
		return nm.TcpConnectionTimeout
	}
	return DefaultTcpConnectionTimeout
}

// EffectiveTcpOutboundClientHostname returns the configured tcp.outbound-client-hostname.
// Empty means the OS picks the local source address.
func (nm *NodeManager) EffectiveTcpOutboundClientHostname() string {
	return nm.TcpOutboundClientHostname
}

// EffectiveBufferPoolSize returns the configured buffer-pool-size or the Pekko default.
func (nm *NodeManager) EffectiveBufferPoolSize() int {
	if nm.BufferPoolSize > 0 {
		return nm.BufferPoolSize
	}
	return DefaultBufferPoolSize
}

// EffectiveMaximumLargeFrameSize returns the configured maximum-large-frame-size or the Pekko default.
func (nm *NodeManager) EffectiveMaximumLargeFrameSize() int {
	if nm.MaximumLargeFrameSize > 0 {
		return nm.MaximumLargeFrameSize
	}
	return DefaultMaximumLargeFrameSize
}

// EffectiveLargeBufferPoolSize returns the configured large-buffer-pool-size or the Pekko default.
func (nm *NodeManager) EffectiveLargeBufferPoolSize() int {
	if nm.LargeBufferPoolSize > 0 {
		return nm.LargeBufferPoolSize
	}
	return DefaultLargeBufferPoolSize
}

// EffectiveOutboundLargeMessageQueueSize returns the configured outbound-large-message-queue-size or the Pekko default.
func (nm *NodeManager) EffectiveOutboundLargeMessageQueueSize() int {
	if nm.OutboundLargeMessageQueueSize > 0 {
		return nm.OutboundLargeMessageQueueSize
	}
	return DefaultOutboundLargeMessageQueueSize
}

// pruneRestartTimestamps drops timestamps older than now-window from stamps
// and returns the remaining slice.  Caller must hold restartMu.
func pruneRestartTimestamps(stamps []time.Time, now time.Time, window time.Duration) []time.Time {
	cutoff := now.Add(-window)
	i := 0
	for ; i < len(stamps); i++ {
		if stamps[i].After(cutoff) {
			break
		}
	}
	if i == 0 {
		return stamps
	}
	pruned := make([]time.Time, len(stamps)-i)
	copy(pruned, stamps[i:])
	return pruned
}

// TryRecordInboundRestart records an inbound-stream restart and returns true
// when the count within the rolling inbound-restart-timeout window is still
// at or below inbound-max-restarts.  When the cap is exceeded the method
// returns false and the caller is expected to treat the restart as fatal
// (matches Pekko's "ActorSystem terminated on exceeded restarts" behaviour).
func (nm *NodeManager) TryRecordInboundRestart() bool {
	window := nm.EffectiveInboundRestartTimeout()
	max := nm.EffectiveInboundMaxRestarts()
	now := time.Now()
	nm.restartMu.Lock()
	defer nm.restartMu.Unlock()
	nm.inboundRestarts = pruneRestartTimestamps(nm.inboundRestarts, now, window)
	nm.inboundRestarts = append(nm.inboundRestarts, now)
	return len(nm.inboundRestarts) <= max
}

// TryRecordOutboundRestart records an outbound-stream restart and returns true
// when the count within the rolling outbound-restart-timeout window is still
// at or below outbound-max-restarts.  When the cap is exceeded the method
// returns false and the caller is expected to stop attempting further
// reconnects (matches Pekko's outbound-max-restarts behaviour).
func (nm *NodeManager) TryRecordOutboundRestart() bool {
	window := nm.EffectiveOutboundRestartTimeout()
	max := nm.EffectiveOutboundMaxRestarts()
	now := time.Now()
	nm.restartMu.Lock()
	defer nm.restartMu.Unlock()
	nm.outboundRestarts = pruneRestartTimestamps(nm.outboundRestarts, now, window)
	nm.outboundRestarts = append(nm.outboundRestarts, now)
	return len(nm.outboundRestarts) <= max
}

// ResetRestartCounters clears both inbound and outbound restart windows.
// Used by tests to isolate restart-cap scenarios.
func (nm *NodeManager) ResetRestartCounters() {
	nm.restartMu.Lock()
	nm.inboundRestarts = nil
	nm.outboundRestarts = nil
	nm.restartMu.Unlock()
}

// SweepIdleOutboundQuarantine quarantines any OUTBOUND associations whose
// lastSeen timestamp is older than quarantine-idle-outbound-after.  The
// association is transitioned to QUARANTINED state, its connection is closed,
// and its UID is recorded in the permanent quarantine registry so the remote
// cannot re-associate with the same UID.  Returns the number of associations
// quarantined by this sweep (callers use it to drive metrics/flight events).
func (nm *NodeManager) SweepIdleOutboundQuarantine() int {
	threshold := nm.EffectiveQuarantineIdleOutboundAfter()
	cutoff := time.Now().Add(-threshold)
	nm.mu.Lock()
	defer nm.mu.Unlock()

	quarantined := 0
	for key, assoc := range nm.associations {
		assoc.mu.Lock()
		role := assoc.role
		state := assoc.state
		lastSeen := assoc.lastSeen
		remote := assoc.remote
		conn := assoc.conn
		if role != OUTBOUND || state == QUARANTINED {
			assoc.mu.Unlock()
			continue
		}
		if !lastSeen.Before(cutoff) {
			assoc.mu.Unlock()
			continue
		}
		assoc.state = QUARANTINED
		if conn != nil {
			_ = conn.Close()
		}
		assoc.mu.Unlock()

		assoc.emitFlight(SeverityWarn, CatQuarantine, "idle_outbound_quarantined", map[string]any{
			"remote":    assoc.remoteKey(),
			"threshold": threshold.String(),
		})
		if remote != nil {
			nm.quarantinedMu.Lock()
			nm.quarantinedUIDs[remote.GetUid()] = remote
			nm.quarantinedMu.Unlock()
		}
		delete(nm.associations, key)
		quarantined++
	}
	return quarantined
}

// IsQuarantined returns true when uid has been permanently quarantined.
// Thread-safe.
func (nm *NodeManager) IsQuarantined(uid uint64) bool {
	if uid == 0 {
		return false
	}
	nm.quarantinedMu.RLock()
	_, ok := nm.quarantinedUIDs[uid]
	nm.quarantinedMu.RUnlock()
	return ok
}

// RegisterQuarantinedUID permanently records remote as quarantined.
// Subsequent connection attempts from that UID will be rejected.
func (nm *NodeManager) RegisterQuarantinedUID(remote *gproto_remote.UniqueAddress) {
	if remote == nil {
		return
	}
	uid := remote.GetUid()
	if uid == 0 {
		return
	}
	nm.quarantinedMu.Lock()
	nm.quarantinedUIDs[uid] = remote
	nm.quarantinedMu.Unlock()
	slog.Warn("node manager: registered permanently quarantined UID", "uid", uid, "address", remote.GetAddress())
	if nm.FlightRec != nil {
		key := fmt.Sprintf("%s:%d", remote.GetAddress().GetHostname(), remote.GetAddress().GetPort())
		nm.FlightRec.Emit(key, FlightEvent{
			Timestamp: time.Now(),
			Severity:  SeverityWarn,
			Category:  CatQuarantine,
			Message:   "uid_registered",
			Fields:    map[string]any{"uid": uid},
		})
	}
}

// QuarantinedUIDs returns a snapshot of all permanently quarantined UIDs.
// Used by diagnostics and tests.
func (nm *NodeManager) QuarantinedUIDs() []uint64 {
	nm.quarantinedMu.RLock()
	defer nm.quarantinedMu.RUnlock()
	out := make([]uint64, 0, len(nm.quarantinedUIDs))
	for uid := range nm.quarantinedUIDs {
		out = append(out, uid)
	}
	return out
}

// MuteNode silently drops all outbound frames to and all inbound frames from
// the node at host:port. This simulates a one-sided or full network partition
// without terminating either process. Safe to call concurrently.
func (nm *NodeManager) MuteNode(host string, port uint32) {
	key := fmt.Sprintf("%s:%d", host, port)
	nm.mutedMu.Lock()
	nm.mutedNodes[key] = struct{}{}
	nm.mutedMu.Unlock()
	slog.Info("node manager: muted node", "address", key)
}

// UnmuteNode reverses a previous MuteNode call. Safe to call even if the node
// was never muted.
func (nm *NodeManager) UnmuteNode(host string, port uint32) {
	key := fmt.Sprintf("%s:%d", host, port)
	nm.mutedMu.Lock()
	delete(nm.mutedNodes, key)
	nm.mutedMu.Unlock()
	slog.Info("node manager: unmuted node", "address", key)
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
		slog.Warn("node manager: Ask reply channel full, dropping", "path", path)
	}
	return true
}

var _ actor.RemoteMessagingProvider = (*NodeManager)(nil)

func (nm *NodeManager) LocalAddress() *gproto_remote.Address {
	return nm.LocalAddr
}

func (nm *NodeManager) GetAssociationByHost(host string, port uint32) (actor.RemoteAssociation, bool) {
	return nm.GetGekkaAssociationByHost(host, port)
}

// GetGekkaAssociationByHost returns a *GekkaAssociation for a remote node specified by host and port.
func (nm *NodeManager) GetGekkaAssociationByHost(host string, port uint32) (*GekkaAssociation, bool) {
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
	// Aeron-UDP path: delegate to the UDP handler when configured.
	if nm.UDPHandler != nil {
		return nm.DialRemoteUDP(ctx, nm.UDPHandler, target.GetHostname(), target.GetPort())
	}

	addrStr := fmt.Sprintf("%s:%d", target.GetHostname(), target.GetPort())

	connectTimeout := nm.EffectiveTcpConnectionTimeout()

	// Optional outbound-client-hostname: bind the dialer's source address.
	var localAddr *net.TCPAddr
	if src := nm.EffectiveTcpOutboundClientHostname(); src != "" {
		if ip := net.ParseIP(src); ip != nil {
			localAddr = &net.TCPAddr{IP: ip}
		} else if addrs, resolveErr := net.LookupIP(src); resolveErr == nil && len(addrs) > 0 {
			localAddr = &net.TCPAddr{IP: addrs[0]}
		} else {
			slog.Warn("artery: tcp.outbound-client-hostname unresolved", "hostname", src, "error", resolveErr)
		}
	}

	client, err := NewTcpClient(TcpClientConfig{
		Addr: addrStr,
		Handler: func(ctx context.Context, conn net.Conn) error {
			return nm.ProcessConnection(ctx, conn, OUTBOUND, target, 1) // Default to Control stream for outbound
		},
		TLSConfig:   nm.TLSConfig,
		DialTimeout: connectTimeout,
		LocalAddr:   localAddr,
	})
	if err != nil {
		return nil, err
	}

	// Start connection in a goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- client.Connect(ctx)
	}()

	// Wait for the association to appear or for an error.
	timeout := time.After(connectTimeout)
	for {
		select {
		case err := <-errChan:
			return nil, err
		case <-timeout:
			if assoc, ok := nm.GetAssociationByHost(target.GetHostname(), target.GetPort()); ok {
				return assoc, nil
			}
			return nil, fmt.Errorf("dial timeout after %s", connectTimeout)
		case <-time.After(100 * time.Millisecond):
			if assoc, ok := nm.GetAssociationByHost(target.GetHostname(), target.GetPort()); ok {
				return assoc, nil
			}
		}
	}
}

// DialRemoteWithRestart wraps DialRemote with Pekko-style outbound
// restart semantics: the first dial is the initial start; each
// post-failure retry is a "restart" gated by TryRecordOutboundRestart
// (rolling cap of OutboundMaxRestarts within OutboundRestartTimeout)
// and separated by EffectiveOutboundRestartBackoff.
//
// On the first success, the established association is returned with
// no counter increment. When DialRemote returns an error the helper
// records a restart and, if the cap is still respected, sleeps for
// the configured backoff (interruptible by ctx) and retries. Once the
// cap is exceeded the most recent dial error is wrapped in
// "outbound restart cap exceeded" and returned to the caller, matching
// Pekko's outbound-max-restarts behaviour. Consumes
// pekko.remote.artery.advanced.{outbound-max-restarts,
// outbound-restart-timeout, outbound-restart-backoff}.
func (nm *NodeManager) DialRemoteWithRestart(ctx context.Context, target *gproto_remote.Address) (actor.RemoteAssociation, error) {
	backoff := nm.EffectiveOutboundRestartBackoff()
	maxRestarts := nm.EffectiveOutboundMaxRestarts()
	var lastErr error
	// Initial attempt + at most maxRestarts retries; the counter is the
	// primary guard, the loop bound is a defensive belt-and-braces to
	// prevent runaway iteration if the helper is mis-tuned.
	for attempt := 0; attempt <= maxRestarts; attempt++ {
		if ctx.Err() != nil {
			if lastErr != nil {
				return nil, fmt.Errorf("outbound dial cancelled after %d attempts: %w", attempt, ctx.Err())
			}
			return nil, ctx.Err()
		}
		assoc, err := nm.DialRemote(ctx, target)
		if err == nil {
			return assoc, nil
		}
		lastErr = err
		// First failure (attempt == 0) and every subsequent retry both
		// count as restart events — the parsed cap covers all of them.
		if !nm.TryRecordOutboundRestart() {
			return nil, fmt.Errorf("outbound restart cap exceeded after %d attempt(s): %w", attempt+1, err)
		}
		// Sleep the configured backoff before the next attempt; honor ctx.
		if backoff > 0 {
			t := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				t.Stop()
				return nil, fmt.Errorf("outbound dial cancelled during backoff: %w", ctx.Err())
			case <-t.C:
			}
		}
	}
	return nil, fmt.Errorf("outbound restart cap exceeded after %d attempt(s): %w", maxRestarts+1, lastErr)
}

// dialOrdinaryLane opens a single TCP connection to target, performs the
// optional TLS upgrade, and writes the streamId=2 (Ordinary) Artery preamble.
// It does NOT enter a read or write loop — the caller wires the conn into
// an outboundLane and starts those loops (see EnsureOrdinarySibling).
// Sub-plan 8f outbound half.
func (nm *NodeManager) dialOrdinaryLane(ctx context.Context, target *gproto_remote.Address) (net.Conn, error) {
	addrStr := fmt.Sprintf("%s:%d", target.GetHostname(), target.GetPort())
	connectTimeout := nm.EffectiveTcpConnectionTimeout()

	var localAddr *net.TCPAddr
	if src := nm.EffectiveTcpOutboundClientHostname(); src != "" {
		if ip := net.ParseIP(src); ip != nil {
			localAddr = &net.TCPAddr{IP: ip}
		} else if addrs, resolveErr := net.LookupIP(src); resolveErr == nil && len(addrs) > 0 {
			localAddr = &net.TCPAddr{IP: addrs[0]}
		} else {
			slog.Warn("artery: tcp.outbound-client-hostname unresolved", "hostname", src, "error", resolveErr)
		}
	}

	d := net.Dialer{Timeout: connectTimeout}
	if localAddr != nil {
		d.LocalAddr = localAddr
	}
	conn, err := d.DialContext(ctx, "tcp", addrStr)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	var c net.Conn = conn
	if nm.TLSConfig != nil {
		serverName := nm.TLSConfig.ServerName
		if serverName == "" {
			serverName, _, _ = net.SplitHostPort(addrStr)
		}
		tlsCfg := nm.TLSConfig.Clone()
		tlsCfg.ServerName = serverName
		tlsConn := tls.Client(conn, tlsCfg)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("tls handshake: %w", err)
		}
		c = tlsConn
	}
	// Write the streamId=2 (Ordinary) Pekko/Akka preamble. Both Pekko 1.x
	// and Akka 2.6.x use the AKKA preamble on the wire.
	preamble := []byte{'A', 'K', 'K', 'A', 2}
	if _, err := c.Write(preamble); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("preamble write: %w", err)
	}
	return c, nil
}

// DialRemoteOrdinaryLanes opens N parallel TCP connections to target for
// streamId=2 (Ordinary), where N = EffectiveOutboundLanes(). Each lane uses
// per-lane Pekko-style restart-counter gating (sub-plan 8e parity): every
// failed dial counts against the per-lane restart budget and the dial is
// retried after EffectiveOutboundRestartBackoff. Returns the conns in
// lane-index order. On any cap-exceeded error from a lane the helper
// closes any successfully-opened conns and returns the error to the
// caller. Sub-plan 8f outbound half.
func (nm *NodeManager) DialRemoteOrdinaryLanes(ctx context.Context, target *gproto_remote.Address) ([]net.Conn, error) {
	n := nm.EffectiveOutboundLanes()
	if n < 1 {
		n = 1
	}

	conns := make([]net.Conn, n)
	errs := make([]error, n)
	gctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			backoff := nm.EffectiveOutboundRestartBackoff()
			maxRestarts := nm.EffectiveOutboundMaxRestarts()
			var lastErr error
			for attempt := 0; attempt <= maxRestarts; attempt++ {
				if gctx.Err() != nil {
					if lastErr != nil {
						errs[i] = fmt.Errorf("lane %d cancelled after %d attempts: %w", i, attempt, gctx.Err())
					} else {
						errs[i] = gctx.Err()
					}
					return
				}
				c, err := nm.dialOrdinaryLane(gctx, target)
				if err == nil {
					conns[i] = c
					return
				}
				lastErr = err
				if !nm.TryRecordOutboundRestart() {
					errs[i] = fmt.Errorf("lane %d: outbound restart cap exceeded after %d attempt(s): %w", i, attempt+1, err)
					cancel()
					return
				}
				if backoff > 0 {
					t := time.NewTimer(backoff)
					select {
					case <-gctx.Done():
						t.Stop()
						errs[i] = fmt.Errorf("lane %d cancelled during backoff: %w", i, gctx.Err())
						return
					case <-t.C:
					}
				}
			}
			errs[i] = fmt.Errorf("lane %d: outbound restart cap exceeded after %d attempt(s): %w", i, maxRestarts+1, lastErr)
			cancel()
		}()
	}
	wg.Wait()

	// On any error: close any successfully-opened conns and surface the
	// first error.
	var firstErr error
	for i := range errs {
		if errs[i] != nil && firstErr == nil {
			firstErr = errs[i]
		}
	}
	if firstErr != nil {
		for i := range conns {
			if conns[i] != nil {
				_ = conns[i].Close()
				conns[i] = nil
			}
		}
		return nil, firstErr
	}
	return conns, nil
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
	if ctm != nil {
		ctm.flightRec = nm.FlightRec
	}
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

// GetAssociationByRemote returns an association for a specific remote node by its UID string.
func (nm *NodeManager) GetAssociationByRemote(remoteID string, streamId int32) (*GekkaAssociation, bool) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	// The key format in RegisterAssociation is host:port-uid-sStreamId
	// We scan for the -uid-sStreamId suffix.
	suffix := fmt.Sprintf("-%s-s%d", remoteID, streamId)
	for k, assoc := range nm.associations {
		if strings.HasSuffix(k, suffix) {
			return assoc, true
		}
	}
	return nil, false
}

// UniqueAddressToString converts a UniqueAddress to a stable string identifier for maps.
func UniqueAddressToString(ua *gproto_remote.UniqueAddress) string {
	if ua == nil {
		return ""
	}
	return fmt.Sprintf("%d", ua.GetUid())
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
					slog.Debug("node manager: existing early registration found for host-port", "key", k, "hostPort", hostPortKey)
					continue
				}

				slog.Warn("node manager: detected node restart, quarantining old association", "hostPort", hostPortKey, "oldKey", k)
				existing.mu.Lock()
				existingRemote := existing.remote
				existing.state = QUARANTINED
				if existing.conn != nil {
					existing.conn.Close()
				}
				existing.mu.Unlock()
				existing.emitFlight(SeverityError, CatQuarantine, "QUARANTINED", map[string]any{
					"remote":  existing.remoteKey(),
					"new_uid": assoc.remote.GetUid(),
				})
				if nm.FlightRec != nil {
					nm.FlightRec.DumpOnQuarantine(existing.remoteKey())
				}
				delete(nm.associations, k)
				// Register the old UID permanently so the remote cannot re-associate
				// with that UID after we drop the connection.
				if existingRemote != nil {
					nm.quarantinedMu.Lock()
					nm.quarantinedUIDs[existingRemote.GetUid()] = existingRemote
					nm.quarantinedMu.Unlock()
				}
				// Notify the remote that we have quarantined it, using the new
				// association's outbox so the frame reaches the remote over the
				// live transport.
				go existing.SendQuarantined(existingRemote)
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
				slog.Debug("node manager: INBOUND skipping registration — OUTBOUND already exists", "key", newKey)
				return
			}
		}
	}

	nm.associations[newKey] = assoc
	slog.Debug("node manager: registered association", "key", newKey)
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
			slog.Info("artery: INBOUND Pekko preamble read", "streamId", streamId)
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
			slog.Info("artery: INBOUND Akka preamble read", "streamId", streamId)
		}

		// Validate protocol against accepted list
		if len(nm.AcceptProtocolNames) > 0 {
			accepted := false
			for _, name := range nm.AcceptProtocolNames {
				if name == protocol {
					accepted = true
					break
				}
			}
			if !accepted {
				return fmt.Errorf("artery: protocol %q not in accept-protocol-names %v", protocol, nm.AcceptProtocolNames)
			}
		}
	}

	// Control stream (streamId=1) gets the dedicated control-queue capacity;
	// ordinary streams use the user-message queue capacity.
	outboxCap := nm.EffectiveOutboundMessageQueueSize()
	if streamId == 1 {
		outboxCap = nm.EffectiveOutboundControlQueueSize()
	}
	assoc := &GekkaAssociation{
		state:     INITIATED,
		role:      role,
		conn:      conn,
		nodeMgr:   nm,
		lastSeen:  time.Now(),
		Handshake: make(chan struct{}),
		localUid:  nm.localUid,
		outbox:    make(chan []byte, outboxCap),
		remote:    &gproto_remote.UniqueAddress{Address: remote, Uid: proto.Uint64(0)},
		streamId:  streamId,
	}
	assoc.emitFlight(SeverityInfo, CatHandshake, "INITIATED", map[string]any{
		"remote": assoc.remoteKey(),
		"role":   role,
	})

	// Inbound-lanes fan-out: when configured (>1) and not on the control
	// stream, allocate per-lane channels and start drain goroutines that
	// call assoc.dispatch hashed by recipient path. Control-stream traffic
	// (streamId=1) bypasses the lanes to preserve handshake/heartbeat
	// ordering. Sub-plan 8f.
	if inboundN := nm.EffectiveInboundLanes(); inboundN > 1 && streamId != 1 {
		assoc.inboundLanes = make([]chan *ArteryMetadata, inboundN)
		laneCap := nm.EffectiveOutboundMessageQueueSize()
		for i := range assoc.inboundLanes {
			assoc.inboundLanes[i] = make(chan *ArteryMetadata, laneCap)
		}
		for i := range assoc.inboundLanes {
			lane := assoc.inboundLanes[i]
			assoc.inboundLanesWg.Add(1)
			go func() {
				defer assoc.inboundLanesWg.Done()
				for {
					select {
					case <-assocCtx.Done():
						return
					case meta, ok := <-lane:
						if !ok {
							return
						}
						if err := assoc.dispatch(assocCtx, meta); err != nil {
							slog.Debug("artery: lane dispatch error", "error", err)
						}
					}
				}
			}()
		}
	}

	// Register early so handleHandshakeRsp can find it
	if remote != nil {
		nm.RegisterAssociation(assoc.remote, assoc)
	}

	// Start background write loop. The single-conn path (streamId=1
	// control, streamId=3 large, and UDP associations) uses an "implicit
	// lane" backed by assoc.conn / assoc.outbox so the writer logic is
	// shared with the multi-lane outbound case (sub-plan 8f outbound half).
	implicitLane := &outboundLane{
		idx:    0,
		conn:   conn,
		outbox: assoc.outbox,
	}
	go assoc.startLaneWriter(ctx, implicitLane)

	if role == OUTBOUND && remote != nil {
		go func() {
			// Give handler a moment to start and send magic header
			time.Sleep(200 * time.Millisecond)
			if err := assoc.initiateHandshake(remote); err != nil {
				slog.Error("artery: initiateHandshake error", "error", err)
			}

			// Retry loop: re-send HandshakeReq at handshake-retry-interval
			// until the association reaches ASSOCIATED or handshake-timeout
			// expires. Pekko parity: advanced.handshake-timeout /
			// advanced.handshake-retry-interval.
			retryInterval := nm.EffectiveHandshakeRetryInterval()
			deadline := time.Now().Add(nm.EffectiveHandshakeTimeout())
			retryTicker := time.NewTicker(retryInterval)
		retryLoop:
			for {
				select {
				case <-assocCtx.Done():
					retryTicker.Stop()
					return
				case <-retryTicker.C:
					assoc.mu.RLock()
					state := assoc.state
					assoc.mu.RUnlock()
					if state == ASSOCIATED {
						break retryLoop
					}
					if time.Now().After(deadline) {
						slog.Warn("artery: handshake timed out",
							"remote", assoc.remoteKey(),
							"timeout", nm.EffectiveHandshakeTimeout())
						assoc.emitFlight(SeverityWarn, CatHandshake, "TIMEOUT", map[string]any{
							"remote":  assoc.remoteKey(),
							"timeout": nm.EffectiveHandshakeTimeout().String(),
						})
						retryTicker.Stop()
						return
					}
					if err := assoc.initiateHandshake(remote); err != nil {
						slog.Debug("artery: handshake retry error", "error", err)
					}
				}
			}
			retryTicker.Stop()

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
							slog.Debug("artery: failed to send heartbeat", "error", err)
						}
					}
				}
			}
		}()
	}

	return assoc.Process(assocCtx, protocol)
}

func (assoc *GekkaAssociation) LocalUid() uint64 {
	return assoc.localUid
}

// Remote returns the remote UniqueAddress for this association. Thread-safe.
func (assoc *GekkaAssociation) Remote() *gproto_remote.UniqueAddress {
	assoc.mu.RLock()
	defer assoc.mu.RUnlock()
	return assoc.remote
}

func (assoc *GekkaAssociation) Outbox() chan []byte {
	return assoc.outbox
}

func (assoc *GekkaAssociation) initiateHandshake(to *gproto_remote.Address) error {
	assoc.mu.Lock()
	assoc.state = WAITING_FOR_HANDSHAKE
	uid := assoc.localUid
	assoc.mu.Unlock()
	assoc.emitFlight(SeverityInfo, CatHandshake, "WAITING_FOR_HANDSHAKE", map[string]any{
		"remote": assoc.remoteKey(),
	})

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

	// For UDP associations (conn == nil) build the frame and route it through
	// the outbox so the correct transport layer (Aeron DATA frame) is used.
	assoc.mu.RLock()
	isUDP := assoc.udpHandler != nil
	assoc.mu.RUnlock()

	if isUDP {
		senderPath := fmt.Sprintf("%s://%s@%s:%d",
			req.From.GetAddress().GetProtocol(),
			req.From.GetAddress().GetSystem(),
			req.From.GetAddress().GetHostname(),
			req.From.GetAddress().GetPort())
		msgPayload, err := proto.Marshal(req)
		if err != nil {
			return fmt.Errorf("artery: marshal HandshakeReq: %w", err)
		}
		frame, err := BuildArteryFrame(int64(uid), actor.ArteryInternalSerializerID, senderPath, "", "d", msgPayload, true)
		if err != nil {
			return err
		}
		select {
		case assoc.outbox <- frame:
			return nil
		default:
			return fmt.Errorf("artery: outbox full during HandshakeReq (UDP)")
		}
	}

	return SendArteryMessageWithAck(assoc.conn, int64(uid), actor.ArteryInternalSerializerID, "d", req, req.From, true)
}

func (assoc *GekkaAssociation) Process(ctx context.Context, protocol string) error {
	dispatch := func(ctx context.Context, meta *ArteryMetadata) error {
		return assoc.dispatchSharded(ctx, meta)
	}
	remoteUid := uint64(0)
	if assoc.remote != nil {
		remoteUid = assoc.remote.GetUid()
	}
	maxFrame := assoc.effectiveStreamFrameSizeCap()
	if assoc.role == OUTBOUND {
		return TcpArteryOutboundHandler(ctx, assoc.conn, dispatch, assoc.nodeMgr.compressionMgr, remoteUid, assoc.streamId, protocol, maxFrame)
	}
	return TcpArteryHandlerWithCallback(ctx, assoc.conn, dispatch, assoc.nodeMgr.compressionMgr, remoteUid, assoc.streamId, maxFrame)
}

// initiateLaneHandshake writes a HandshakeReq frame on a specific outbound
// lane TCP connection. Sub-plan 8f outbound half: each lane runs its own
// handshake exchange independently — gekka writes HandshakeReq on every
// lane's outbound TCP so the peer's Artery state machine knows our UID for
// that stream. The peer's HandshakeRsp may arrive on any of gekka's
// inbound TCPs (Artery TCP is unidirectional per connection); the
// per-lane handshakeDone channel is signalled when the lane's HandshakeReq
// has been written successfully.
func (assoc *GekkaAssociation) initiateLaneHandshake(lane *outboundLane, to *gproto_remote.Address) error {
	uid := assoc.localUid
	if lane.localUid != 0 {
		uid = lane.localUid
	}
	req := &gproto_remote.HandshakeReq{
		From: &gproto_remote.UniqueAddress{
			Address: assoc.nodeMgr.LocalAddr,
			Uid:     proto.Uint64(uid),
		},
		To: to,
	}
	lane.writeMu.Lock()
	defer lane.writeMu.Unlock()
	return SendArteryMessageWithAck(lane.conn, int64(uid), actor.ArteryInternalSerializerID, "d", req, req.From, true)
}

// ProcessConnectionLane runs the per-lane outbound handshake retry loop and
// the per-lane read loop for an outbound streamId=2 lane. Sub-plan 8f
// outbound half: factored out of the inline handshake-retry block in
// ProcessConnection so each lane has its own retry/heartbeat lifecycle.
//
// The lane is considered ASSOCIATED once its HandshakeReq has been
// successfully written; the lane's handshakeDone channel is closed at that
// point. We do not block the lane's writes on receiving the peer's
// HandshakeRsp — Pekko/Akka tolerate user frames on any associated stream,
// and the streamId=2 sibling is created only after the control assoc has
// already reached ASSOCIATED, so the peer UID is known.
func (nm *NodeManager) ProcessConnectionLane(ctx context.Context, assoc *GekkaAssociation, lane *outboundLane, to *gproto_remote.Address) {
	// Initial HandshakeReq.
	if err := assoc.initiateLaneHandshake(lane, to); err != nil {
		slog.Debug("artery: lane initiateHandshake error", "lane", lane.idx, "error", err)
	}
	// Retry until lane is ASSOCIATED or handshake-timeout expires.
	retryInterval := nm.EffectiveHandshakeRetryInterval()
	deadline := time.Now().Add(nm.EffectiveHandshakeTimeout())
	retryTicker := time.NewTicker(retryInterval)
	defer retryTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-lane.handshakeDone:
			return
		case <-retryTicker.C:
			assoc.mu.RLock()
			s := lane.state
			assoc.mu.RUnlock()
			if s == ASSOCIATED {
				return
			}
			if time.Now().After(deadline) {
				slog.Warn("artery: lane handshake timed out",
					"remote", assoc.remoteKey(),
					"lane", lane.idx,
					"timeout", nm.EffectiveHandshakeTimeout())
				return
			}
			if err := assoc.initiateLaneHandshake(lane, to); err != nil {
				slog.Debug("artery: lane handshake retry error", "lane", lane.idx, "error", err)
			}
		}
	}
}

// runLaneReadLoop drains one outbound lane's inbound bytes (the read side
// is a no-op in Artery-TCP since outbound TCP is write-only on gekka's
// side, but a stray byte from the peer constitutes a protocol violation
// and we want to terminate the lane cleanly when that happens). In
// practice this loop returns immediately on EOF or context cancellation.
func (nm *NodeManager) runLaneReadLoop(ctx context.Context, assoc *GekkaAssociation, lane *outboundLane) {
	dispatch := func(ctx context.Context, meta *ArteryMetadata) error {
		return assoc.dispatchSharded(ctx, meta)
	}
	remoteUid := uint64(0)
	if assoc.remote != nil {
		remoteUid = assoc.remote.GetUid()
	}
	maxFrame := assoc.effectiveStreamFrameSizeCap()
	if err := tcpArteryReadLoop(ctx, lane.conn, dispatch, assoc.nodeMgr.compressionMgr, remoteUid, assoc.streamId, int32(maxFrame)); err != nil {
		slog.Debug("artery: lane read loop ended", "lane", lane.idx, "error", err)
	}
}

// EnsureOrdinarySibling creates the streamId=2 ordinary outbound sibling
// for a control (streamId=1) association. Idempotent: subsequent calls
// after the sibling is already established return nil immediately.
//
// Body: dial N TCP connections (where N = EffectiveOutboundLanes(), default
// 1), construct a streamId=2 *GekkaAssociation* with the dialed conns
// wrapped as outboundLanes, register under (host:port, uid, streamId=2),
// link the sibling pointers both ways, and start one writer + one read +
// one handshake goroutine per lane. Sub-plan 8f outbound half.
func (nm *NodeManager) EnsureOrdinarySibling(ctx context.Context, controlAssoc *GekkaAssociation) error {
	if controlAssoc == nil {
		return fmt.Errorf("nil control assoc")
	}
	controlAssoc.mu.RLock()
	already := controlAssoc.ordinarySibling != nil
	role := controlAssoc.role
	controlAssoc.mu.RUnlock()
	if already {
		return nil
	}
	// Only OUTBOUND control assocs initiate the sibling dial. INBOUND
	// control assocs receive their counterpart sibling via the peer's
	// outbound dial onto our listener.
	if role != OUTBOUND {
		return nil
	}
	var dialErr error
	controlAssoc.ordinarySiblingOnce.Do(func() {
		dialErr = nm.dialOrdinarySibling(ctx, controlAssoc)
	})
	return dialErr
}

func (nm *NodeManager) dialOrdinarySibling(ctx context.Context, controlAssoc *GekkaAssociation) error {
	controlAssoc.mu.RLock()
	remote := controlAssoc.remote
	controlAssoc.mu.RUnlock()
	if remote == nil || remote.Address == nil {
		return fmt.Errorf("control assoc has no remote address")
	}
	// Skip Aeron-UDP associations: the multi-conn refactor is TCP-only.
	controlAssoc.mu.RLock()
	isUDP := controlAssoc.udpHandler != nil
	controlAssoc.mu.RUnlock()
	if isUDP {
		return nil
	}

	// Dial N TCPs in parallel. Each lane has its own restart budget.
	conns, err := nm.DialRemoteOrdinaryLanes(ctx, remote.Address)
	if err != nil {
		if controlAssoc.nodeMgr != nil && controlAssoc.nodeMgr.FlightRec != nil {
			controlAssoc.nodeMgr.FlightRec.Emit(controlAssoc.remoteKey(), FlightEvent{
				Timestamp: time.Now(),
				Severity:  SeverityWarn,
				Category:  CatHandshake,
				Message:   "ordinary_sibling_dial_failed",
				Fields:    map[string]any{"error": err.Error()},
			})
		}
		return err
	}

	outboxCap := nm.EffectiveOutboundMessageQueueSize()
	lanes := make([]*outboundLane, len(conns))
	for i, c := range conns {
		lanes[i] = &outboundLane{
			idx:           i,
			conn:          c,
			outbox:        make(chan []byte, outboxCap),
			state:         INITIATED,
			handshakeDone: make(chan struct{}),
			localUid:      nm.localUid,
			remoteUid:     remote.GetUid(),
		}
	}

	ordinaryAssoc := &GekkaAssociation{
		state:     ASSOCIATED, // sibling inherits assoc-level state from control's already-completed handshake
		role:      OUTBOUND,
		nodeMgr:   nm,
		lastSeen:  time.Now(),
		Handshake: make(chan struct{}),
		localUid:  nm.localUid,
		// outbox is unused for streamId=2 sibling — all writes go on
		// per-lane outboxes via outboxFor's lane pivot. Allocate a small
		// buffer so any stray write that lands here doesn't deadlock.
		outbox:   make(chan []byte, 1),
		remote:   remote,
		streamId: 2,
		lanes:    lanes,
	}
	close(ordinaryAssoc.Handshake)
	// Link siblings both directions.
	controlAssoc.mu.Lock()
	controlAssoc.ordinarySibling = ordinaryAssoc
	controlAssoc.mu.Unlock()
	ordinaryAssoc.mu.Lock()
	ordinaryAssoc.ordinarySibling = controlAssoc
	ordinaryAssoc.mu.Unlock()

	// Register under (host:port, uid, streamId=2).
	nm.RegisterAssociation(remote, ordinaryAssoc)

	// Start per-lane goroutines: writer, read loop, handshake retry.
	// Use context.Background() so lanes survive the caller's ctx (the
	// handshake-completion goroutine that triggered EnsureOrdinarySibling
	// often passes a short-lived ctx). Lanes terminate when the lane conn
	// closes (writer/reader exit naturally) or QuarantinePeer cancels them.
	siblingCtx := context.Background()
	for _, lane := range lanes {
		// Mark lane ASSOCIATED proactively. The control handshake has
		// already completed, so the peer UID is known and any user frame
		// written on this lane is wire-tolerable. The HandshakeReq is
		// still sent (below) so the peer's per-stream Artery state
		// machine acknowledges our UID for streamId=2.
		go ordinaryAssoc.startLaneWriter(siblingCtx, lane)
		go nm.runLaneReadLoop(siblingCtx, ordinaryAssoc, lane)
		go func(l *outboundLane) {
			nm.ProcessConnectionLane(siblingCtx, ordinaryAssoc, l, remote.Address)
		}(lane)
		// Mark the lane ASSOCIATED and signal handshakeDone so any
		// per-lane gating proceeds. The retry goroutine returns once
		// it observes ASSOCIATED.
		ordinaryAssoc.mu.Lock()
		lane.state = ASSOCIATED
		ordinaryAssoc.mu.Unlock()
		select {
		case <-lane.handshakeDone:
		default:
			close(lane.handshakeDone)
		}
	}

	if nm.FlightRec != nil {
		nm.FlightRec.Emit(controlAssoc.remoteKey(), FlightEvent{
			Timestamp: time.Now(),
			Severity:  SeverityInfo,
			Category:  CatHandshake,
			Message:   "ordinary_sibling_established",
			Fields:    map[string]any{"lanes": len(lanes)},
		})
	}
	return nil
}

// effectiveStreamFrameSizeCap returns the maximum-frame-size used by the read
// loop for this association's stream. Round-2 session 30 routes streamId=3
// onto the dedicated maximum-large-frame-size cap (Pekko default 2 MiB) so
// large-message frames are accepted; streams 1/2 keep the ordinary
// maximum-frame-size cap (Pekko default 256 KiB).
func (assoc *GekkaAssociation) effectiveStreamFrameSizeCap() int {
	if assoc.streamId == AeronStreamLarge {
		return assoc.nodeMgr.EffectiveMaximumLargeFrameSize()
	}
	return assoc.nodeMgr.MaxFrameSize
}

func (assoc *GekkaAssociation) dispatch(ctx context.Context, meta *ArteryMetadata) error {
	// Drop all inbound frames from a muted node (simulates network partition).
	if assoc.nodeMgr != nil && assoc.remote != nil {
		a := assoc.remote.GetAddress()
		if assoc.nodeMgr.isNodeMuted(a.GetHostname(), a.GetPort()) {
			return nil
		}
	}

	// Emit flight recorder events for received frames.
	if assoc.nodeMgr != nil && assoc.nodeMgr.FlightRec != nil {
		if assoc.streamId == 3 {
			assoc.nodeMgr.FlightRec.Emit(assoc.remoteKey(), FlightEvent{
				Timestamp: time.Now(),
				Severity:  SeverityInfo,
				Category:  CatLargeFrame,
				Message:   "recv",
				Fields:    map[string]any{"size": len(meta.Payload)},
			})
		}
		assoc.nodeMgr.FlightRec.EmitFrame(assoc.remoteKey(), "recv", len(meta.Payload), meta.SerializerId)
	}

	assoc.mu.Lock()
	assoc.lastSeen = time.Now()
	assoc.mu.Unlock()

	if meta.SeqNo != 0 && meta.AckReplyTo != nil {
		if err := assoc.sendSystemAck(meta.SeqNo, meta.AckReplyTo); err != nil {
			slog.Debug("artery: failed to send ACK", "seq", meta.SeqNo, "error", err)
		}
	}

	switch meta.SerializerId {
	case actor.ArteryInternalSerializerID:
		manifest := string(meta.MessageManifest)
		if manifest == "SystemMessage" {
			return assoc.handleSystemMessage(meta)
		}
		return assoc.handleControlMessage(ctx, meta)

	case 6: // MessageContainerSerializer
		env := &gproto_remote.SelectionEnvelope{}
		if err := proto.Unmarshal(meta.Payload, env); err != nil {
			return fmt.Errorf("failed to unmarshal SelectionEnvelope: %w", err)
		}

		// 1. Resolve target path from Pattern
		path := "/"
		for _, e := range env.Pattern {
			if e.GetType() == gproto_remote.PatternType_PARENT {
				lastSlash := strings.LastIndex(strings.TrimSuffix(path, "/"), "/")
				if lastSlash != -1 {
					path = path[:lastSlash+1]
				}
			} else {
				if !strings.HasSuffix(path, "/") {
					path += "/"
				}
				path += e.GetMatcher()
			}
		}

		// 2. Route cluster messages directly — ClusterSerializer (ID=5) is not
		// registered in SerializerRegistry, so calling DeserializePayload would fail.
		if env.GetSerializerId() == actor.ClusterSerializerID {
			if assoc.nodeMgr.clusterMgr != nil {
				assoc.mu.RLock()
				remote := assoc.remote
				assoc.mu.RUnlock()
				senderPath := meta.Sender.GetPath()
				return assoc.nodeMgr.clusterMgr.HandleIncomingClusterMessage(ctx, env.GetEnclosedMessage(), string(env.GetMessageManifest()), ToClusterUniqueAddress(remote), senderPath)
			}
			return nil
		}

		// 2a-bis. pekko.remote.artery.untrusted-mode — when on, an inbound
		// ActorSelection may only land on a path that the operator has
		// explicitly allowlisted via pekko.remote.artery.trusted-selection-paths.
		// Pekko applies this check before delivering the selection, so even an
		// Identify probe is blocked: an attacker who can send selections must
		// not be able to enumerate the local actor tree by walking paths.
		// Cluster traffic is routed in the bypass above and never reaches this
		// gate.
		if assoc.nodeMgr.UntrustedMode && !assoc.nodeMgr.IsTrustedSelectionPath(path) {
			senderPath := ""
			if meta.Sender != nil {
				senderPath = meta.Sender.GetPath()
			}
			assoc.nodeMgr.recordUntrustedDrop(env.GetSerializerId(), string(env.GetMessageManifest()),
				"selection path not in trusted-selection-paths", path, senderPath)
			return nil
		}

		// 2b. Handle Identify (MiscMessageSerializer, sid=16, manifest "A") —
		// reply with ActorIdentity so Pekko's actorSelection(...).resolveOne() works.
		// Only respond to manifest "A" (Identify); other MiscMessage types are dropped.
		if env.GetSerializerId() == MiscMessageSerializerID && string(env.GetMessageManifest()) == "A" {
			meta.Recipient = &gproto_remote.ActorRefData{Path: proto.String(path)}
			assoc.handleIdentify(meta, env)
			return nil
		}

		// 2c. pekko.remote.artery.untrusted-mode — drop PossiblyHarmful inner
		// messages (PoisonPill/Kill via MiscMessageSerializer manifests "P"/"K")
		// even when wrapped by an ActorSelection. Mirrors Pekko's pattern-match
		// order in MessageDispatcher.scala where PossiblyHarmful is checked
		// before the inner ActorSelection is delivered.
		if assoc.nodeMgr.untrustedModeDrop(env.GetSerializerId(), string(env.GetMessageManifest())) {
			senderPath := ""
			if meta.Sender != nil {
				senderPath = meta.Sender.GetPath()
			}
			assoc.nodeMgr.recordUntrustedDrop(env.GetSerializerId(), string(env.GetMessageManifest()),
				"PossiblyHarmful inside ActorSelection", path, senderPath)
			return nil
		}

		// 3. Deserialize inner message for non-cluster payloads
		innerMsg, err := assoc.nodeMgr.SerializerRegistry.DeserializePayload(env.GetSerializerId(), string(env.GetMessageManifest()), env.GetEnclosedMessage())
		if err != nil {
			// Non-fatal: unknown serializer or manifest collision should not
			// kill the TCP connection.  Log and drop the message.
			slog.Debug("artery: selection failed to deserialize inner message (dropping)", "error", err)
			return nil
		}

		// 4. Update meta and route
		meta.DeserializedMessage = innerMsg
		meta.Recipient = &gproto_remote.ActorRefData{Path: proto.String(path)}

		return assoc.handleUserMessage(meta)

	case actor.ClusterSerializerID:
		if assoc.nodeMgr.clusterMgr != nil {
			assoc.mu.RLock()
			remote := assoc.remote
			assoc.mu.RUnlock()
			senderPath := meta.Sender.GetPath()
			return assoc.nodeMgr.clusterMgr.HandleIncomingClusterMessage(ctx, meta.Payload, string(meta.MessageManifest), ToClusterUniqueAddress(remote), senderPath)
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
	// pekko.remote.artery.untrusted-mode — DeathWatch (Watch/Unwatch) and
	// other PossiblyHarmful system messages must be dropped from remote
	// senders. We treat the "SystemMessage" manifest itself as PossiblyHarmful
	// because Pekko's RemoteActorRefProvider matches the PossiblyHarmful trait
	// before the bare SystemMessage case in MessageDispatcher.scala. This is
	// stricter than Pekko (it would still deliver Resume/Suspend SystemMessages
	// from remote in untrusted-mode) but is safer for the cases gekka exposes
	// today and matches the threat model: a node opting into untrusted-mode
	// does not want remote peers driving its actor lifecycle.
	if assoc.nodeMgr.untrustedModeDrop(actor.ArteryInternalSerializerID, "SystemMessage") {
		var recipient, sender string
		if meta.Recipient != nil {
			recipient = meta.Recipient.GetPath()
		}
		if meta.Sender != nil {
			sender = meta.Sender.GetPath()
		}
		assoc.nodeMgr.recordUntrustedDrop(actor.ArteryInternalSerializerID, "SystemMessage",
			"PossiblyHarmful system message", recipient, sender)
		return nil
	}
	// The Artery payload for manifest "SystemMessage" is a SystemMessageEnvelope
	// which carries the SeqNo, AckReplyTo, and the inner SystemMessage bytes.
	env := &gproto_remote.SystemMessageEnvelope{}
	if err := proto.Unmarshal(meta.Payload, env); err != nil {
		return fmt.Errorf("failed to unmarshal SystemMessageEnvelope: %w", err)
	}
	if env.GetSeqNo() != 0 && env.AckReplyTo != nil {
		if err := assoc.sendSystemAck(env.GetSeqNo(), env.AckReplyTo); err != nil {
			slog.Debug("artery: failed to send ACK", "seq", env.GetSeqNo(), "error", err)
		}
	}
	sm := &gproto_remote.SystemMessage{}
	if err := proto.Unmarshal(env.Message, sm); err != nil {
		return fmt.Errorf("failed to unmarshal inner SystemMessage: %w", err)
	}
	slog.Debug("artery: received system message", "type", sm.GetType())

	if assoc.nodeMgr.SystemMessageCallback != nil {
		return assoc.nodeMgr.SystemMessageCallback(assoc.remote, env, sm)
	}
	return nil
}

func (assoc *GekkaAssociation) handleUserMessage(meta *ArteryMetadata) error {
	// pekko.remote.artery.untrusted-mode — drop PoisonPill/Kill arriving at
	// the top level. PoisonPill nested inside an ActorSelection is dropped at
	// the SelectionEnvelope dispatch site before this handler is reached.
	if assoc.nodeMgr.untrustedModeDrop(meta.SerializerId, string(meta.MessageManifest)) {
		var recipient, sender string
		if meta.Recipient != nil {
			recipient = meta.Recipient.GetPath()
		}
		if meta.Sender != nil {
			sender = meta.Sender.GetPath()
		}
		assoc.nodeMgr.recordUntrustedDrop(meta.SerializerId, string(meta.MessageManifest),
			"PossiblyHarmful user message", recipient, sender)
		return nil
	}
	// Count every incoming user message (cluster-internal messages never
	// reach this handler — they go to handleControlMessage/cluster.ClusterManager).
	if assoc.nodeMgr.NodeMetrics != nil {
		assoc.nodeMgr.NodeMetrics.MessagesReceived.Add(1)
		assoc.nodeMgr.NodeMetrics.BytesReceived.Add(int64(len(meta.Payload)))
	}

	// pekko.remote.artery.log-received-messages — DEBUG inbound logging.
	if assoc.nodeMgr.LogReceivedMessages {
		var recipient, sender string
		if meta.Recipient != nil {
			recipient = meta.Recipient.GetPath()
		}
		if meta.Sender != nil {
			sender = meta.Sender.GetPath()
		}
		slog.Debug("artery: received user message",
			"recipient", recipient,
			"sender", sender,
			"serializerId", meta.SerializerId,
			"manifest", string(meta.MessageManifest),
			"payload_bytes", len(meta.Payload))
	}

	if assoc.nodeMgr.SerializerRegistry != nil {
		obj, err := assoc.nodeMgr.SerializerRegistry.DeserializePayload(meta.SerializerId, string(meta.MessageManifest), meta.Payload)
		if err == nil {
			meta.DeserializedMessage = obj
		} else {
			slog.Debug("artery: failed to deserialize payload",
				"serializerId", meta.SerializerId,
				"manifest", meta.MessageManifest,
				"error", err)
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
	slog.Debug("artery: SendWithSender frame", "total_bytes", len(frame), "sender", senderPath, "recipient", recipient)

	assoc.mu.Lock()
	defer assoc.mu.Unlock()

	if assoc.state != ASSOCIATED {
		assoc.pending = append(assoc.pending, frame)
		return nil
	}

	// User messages go on Aeron stream 2 (Ordinary) when a separate outbox is
	// available (UDP transport).  Control/cluster messages stay on stream 1.
	// Recipients matching a configured large-message-destinations glob route
	// onto stream 3 (Large) — Round-2 session 29.
	outbox := assoc.outboxFor(recipient, serializerId)
	select {
	case outbox <- frame:
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

// SendQuarantined enqueues a "Quarantined" control frame to notify the remote
// node that we have permanently quarantined it.  The frame is sent on the
// outbox so it piggybacks on the existing TCP/UDP transport without blocking
// the caller.
func (assoc *GekkaAssociation) SendQuarantined(to *gproto_remote.UniqueAddress) {
	nm := assoc.nodeMgr
	if nm == nil || nm.LocalAddr == nil {
		return
	}
	assoc.emitFlight(SeverityWarn, CatQuarantine, "notification_sent", map[string]any{
		"remote": assoc.remoteKey(),
	})
	localUA := &gproto_remote.UniqueAddress{
		Address: nm.LocalAddr,
		Uid:     proto.Uint64(assoc.localUid),
	}
	msg := &gproto_remote.Quarantined{
		From: localUA,
		To:   to,
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		slog.Warn("artery: failed to marshal Quarantined", "error", err)
		return
	}
	frame, err := BuildArteryFrame(int64(assoc.localUid), actor.ArteryInternalSerializerID, "", "", "Quarantined", payload, true)
	if err != nil {
		slog.Warn("artery: failed to build Quarantined frame", "error", err)
		return
	}
	select {
	case assoc.outbox <- frame:
		slog.Info("artery: sent Quarantined frame", "to", to)
	default:
		slog.Warn("artery: outbox full, Quarantined frame dropped", "to", to)
	}
}

func (assoc *GekkaAssociation) GetRTT() time.Duration {
	assoc.mu.RLock()
	defer assoc.mu.RUnlock()
	return assoc.lastRTT
}

// remoteKey returns the "host:port" key used to identify this association in the flight recorder.
func (assoc *GekkaAssociation) remoteKey() string {
	if assoc.remote != nil {
		a := assoc.remote.GetAddress()
		if a != nil {
			return fmt.Sprintf("%s:%d", a.GetHostname(), a.GetPort())
		}
	}
	if assoc.conn != nil {
		return assoc.conn.RemoteAddr().String()
	}
	return "unknown"
}

// emitFlight records a flight event if the node manager has a flight recorder.
func (assoc *GekkaAssociation) emitFlight(severity EventSeverity, category EventCategory, msg string, fields map[string]any) {
	if assoc.nodeMgr == nil || assoc.nodeMgr.FlightRec == nil {
		return
	}
	assoc.nodeMgr.FlightRec.Emit(assoc.remoteKey(), FlightEvent{
		Timestamp: time.Now(),
		Severity:  severity,
		Category:  category,
		Message:   msg,
		Fields:    fields,
	})
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
	slog.Debug("artery: sending frame", "total_bytes", len(frame), "remote_uid", assoc.remote.GetUid(), "serializerId", serializerId, "manifest", manifest)

	// pekko.remote.artery.log-sent-messages — DEBUG outbound logging.
	if assoc.nodeMgr != nil && assoc.nodeMgr.LogSentMessages {
		slog.Debug("artery: sending user message",
			"recipient", recipient,
			"sender", sender,
			"serializerId", serializerId,
			"manifest", manifest,
			"payload_bytes", len(payload))
	}

	// pekko.remote.artery.log-frame-size-exceeding — warn on oversized payloads.
	if assoc.nodeMgr != nil && assoc.nodeMgr.LogFrameSizeExceeding > 0 &&
		int64(len(payload)) > assoc.nodeMgr.LogFrameSizeExceeding {
		assoc.nodeMgr.recordOversizedFrame(serializerId, manifest, int64(len(payload)))
	}

	assoc.mu.Lock()
	defer assoc.mu.Unlock()

	if assoc.state != ASSOCIATED {
		slog.Debug("artery: buffering message", "state", assoc.state)
		assoc.pending = append(assoc.pending, frame)
		return nil
	}

	outbox := assoc.outboxFor(recipient, serializerId)
	select {
	case outbox <- frame:
		return nil
	default:
		return fmt.Errorf("association outbox full")
	}
}

// outboxFor selects the outbox channel for a (recipient, serializerId) pair.
// Large-message routing (Round-2 session 29) preempts the ordinary stream
// when the recipient matches a configured large-message-destinations glob
// AND the serializer is a user serializer (cluster/artery-internal control
// traffic always stays on stream 1). Sub-plan 8f outbound half adds the
// streamId=2 sibling pivot: when this assoc is the control stream and an
// ordinary sibling is registered, user-message traffic is partitioned
// across the sibling's lanes by hash(recipient).
//
// IMPORTANT: callers hold assoc.mu (write lock — see Send /
// SendWithSender). This function therefore does NOT re-lock assoc.mu;
// taking RLock would deadlock with sync.RWMutex semantics. The sibling
// pointer is read without locking — it is set once at sibling-registration
// time and the slice header is stable thereafter (lane outboxes are
// allocated up front in dialOrdinarySibling).
func (assoc *GekkaAssociation) outboxFor(recipient string, serializerId int32) chan []byte {
	// Cluster + ArteryInternal: stay on this stream regardless of streamId.
	if serializerId == ClusterSerializerID || serializerId == ArteryInternalSerializerID {
		return assoc.outbox
	}
	// Aeron-UDP large path: unchanged. UDP transport does not multiplex over
	// multiple sockets; the multi-conn refactor is TCP-only.
	if nm := assoc.nodeMgr; nm != nil && assoc.udpLargeOutbox != nil && nm.IsLargeRecipient(recipient) {
		return assoc.udpLargeOutbox
	}
	// Aeron-UDP ordinary path: unchanged.
	if assoc.udpOrdinaryOutbox != nil {
		return assoc.udpOrdinaryOutbox
	}
	// TCP ordinary path with sibling: route user messages onto an outbound
	// lane of the streamId=2 sibling. Sub-plan 8f outbound half.
	if assoc.streamId == 1 {
		sib := assoc.ordinarySibling
		if sib != nil {
			lanes := sib.lanes
			if len(lanes) > 0 {
				return lanes[laneIndex(recipient, len(lanes))].outbox
			}
		}
		// Convergence window: streamId=2 sibling not yet registered.
		// Fall back to control stream — Pekko tolerates user messages on
		// streamId=1 (this is gekka's pre-amendment behaviour).
		return assoc.outbox
	}
	if assoc.streamId == 2 && len(assoc.lanes) > 0 {
		return assoc.lanes[laneIndex(recipient, len(assoc.lanes))].outbox
	}
	return assoc.outbox
}

// startLaneWriter is the per-lane TCP/UDP outbox drainer. Sub-plan 8f
// outbound half: factored out of the inline writer goroutine so both the
// single-conn (control / large / UDP) path and the multi-lane streamId=2
// path share the same logic.
//
// Uses the outer ctx (node lifetime), not assocCtx, so the write loop keeps
// draining the outbox even after the read loop exits. In Artery-TCP the
// outbound TCP stream is unidirectional: Pekko half-closes its write end
// immediately, causing the read loop to get EOF and return, but writes in
// the opposite direction are still valid and must continue.
func (assoc *GekkaAssociation) startLaneWriter(ctx context.Context, lane *outboundLane) {
	nm := assoc.nodeMgr
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-lane.outbox:
			if !ok {
				return
			}
			// Check mute state before writing (catches buffered frames
			// queued before MuteNode was called). Use assoc.remote under
			// read lock since it's updated after the handshake.
			assoc.mu.RLock()
			remoteAddr := assoc.remote.GetAddress()
			assoc.mu.RUnlock()
			if remoteAddr != nil && nm != nil && nm.isNodeMuted(remoteAddr.GetHostname(), remoteAddr.GetPort()) {
				continue
			}
			slog.Debug("artery: sending frame", "total_bytes", len(msg), "lane", lane.idx)
			// UDP path: wrap in Aeron DATA frame and send via UDP socket.
			assoc.mu.RLock()
			udpH := assoc.udpHandler
			udpDst := assoc.udpDst
			assoc.mu.RUnlock()
			if udpH != nil && udpDst != nil {
				if err := udpH.SendFrame(udpDst, AeronStreamControl, msg); err != nil {
					slog.Warn("aeron-udp: outbox send error", "error", err)
				}
				continue
			}
			// TCP path: prepend 4-byte length and write to the lane's
			// connection. writeMu serializes WriteFrame within a single
			// lane (the channel itself serializes across lanes via
			// goroutines, but a single lane may be drained from this loop
			// only — the mutex is defensive against any future fan-out).
			lane.writeMu.Lock()
			err := WriteFrame(lane.conn, msg)
			lane.writeMu.Unlock()
			if err != nil {
				slog.Error("artery: write error", "error", err, "lane", lane.idx)
				if lane.conn != nil {
					_ = lane.conn.Close()
				}
				return
			}
			// Emit flight recorder events for sent frames.
			if nm != nil && nm.FlightRec != nil {
				assoc.mu.RLock()
				sid := assoc.streamId
				assoc.mu.RUnlock()
				if sid == 3 {
					nm.FlightRec.Emit(assoc.remoteKey(), FlightEvent{
						Timestamp: time.Now(),
						Severity:  SeverityInfo,
						Category:  CatLargeFrame,
						Message:   "send",
						Fields:    map[string]any{"size": len(msg)},
					})
				}
				nm.FlightRec.EmitFrame(assoc.remoteKey(), "send", len(msg), 0)
			}
		}
	}
}

func (assoc *GekkaAssociation) handleControlMessage(ctx context.Context, meta *ArteryMetadata) error {
	manifest := string(meta.MessageManifest)
	slog.Debug("artery: handling control message", "manifest", manifest)
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
		slog.Debug("artery: ArteryHeartbeat received — replying with ArteryHeartbeatRsp", "uid", assoc.localUid)
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
			slog.Debug("artery: ArteryHeartbeatRsp outbox full, dropping response")
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
			slog.Debug("artery: ArteryHeartbeatRsp received", "uid", hb.GetUid(), "rtt", assoc.lastRTT)
		} else {
			slog.Debug("artery: ArteryHeartbeatRsp received", "uid", hb.GetUid())
		}
		assoc.mu.Unlock()
		assoc.emitFlight(SeverityInfo, CatHeartbeat, "RTT", map[string]any{
			"remote": assoc.remoteKey(),
			"rtt":    assoc.lastRTT.String(),
		})
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
		slog.Warn("artery: received Quarantined", "from", quar.From, "to", quar.To)
		// Register the sender UID permanently so we also refuse future re-association from them.
		if assoc.nodeMgr != nil {
			assoc.nodeMgr.RegisterQuarantinedUID(quar.From)
		}
		assoc.mu.Lock()
		assoc.state = QUARANTINED
		if assoc.conn != nil {
			assoc.conn.Close()
		}
		assoc.mu.Unlock()
		assoc.emitFlight(SeverityError, CatQuarantine, "QUARANTINED", map[string]any{
			"remote": assoc.remoteKey(),
		})
		if assoc.nodeMgr != nil && assoc.nodeMgr.FlightRec != nil {
			assoc.nodeMgr.FlightRec.DumpOnQuarantine(assoc.remoteKey())
		}
		return nil

	case "ActorRefCompressionAdvertisementAck", "ClassManifestCompressionAdvertisementAck":
		// We log the ack, but we don't block on receiving it yet.
		// In a full implementation, we'd wait for this before transitioning to using the compressed IDs.
		ack := &gproto_remote.CompressionTableAdvertisementAck{}
		if err := proto.Unmarshal(meta.Payload, ack); err != nil {
			return err
		}
		slog.Debug("artery: received compression table ack", "manifest", manifest, "version", ack.GetVersion(), "from", ack.GetFrom())
		return nil

	default:
		if meta.SerializerId == 6 {
			// MessageContainerSerializer (ID=6) wraps ActorSelectionMessage.
			// Akka sends cluster heartbeats via actorSelection using serializer 6.
			// Decode the SelectionEnvelope and forward inner cluster messages so
			// our failure-detector keeps receiving HBR responses from Go.
			env := &gproto_remote.SelectionEnvelope{}
			if err := proto.Unmarshal(meta.Payload, env); err == nil {
				innerSID := env.GetSerializerId()
				innerManifest := string(env.GetMessageManifest())

				// Forward cluster messages (sid=5) to the cluster manager.
				if innerSID == 5 && innerManifest != "" && assoc.nodeMgr.clusterMgr != nil {
					assoc.mu.RLock()
					remote := assoc.remote
					assoc.mu.RUnlock()
					slog.Debug("artery: forwarding actorSelection cluster message", "manifest", innerManifest)
					return assoc.nodeMgr.clusterMgr.HandleIncomingClusterMessage(
						ctx, env.GetEnclosedMessage(), innerManifest, ToClusterUniqueAddress(remote), meta.Sender.GetPath())
				}

				// Handle Identify (sid=16) — respond with ActorIdentity so Pekko's
				// actorSelection(...).resolveOne() can discover Go actors.
				if innerSID == MiscMessageSerializerID {
					assoc.handleIdentify(meta, env)
					return nil
				}
			}
			return nil
		}
		slog.Debug("artery: unidentified control message", "manifest", manifest, "serializerId", meta.SerializerId)
		return nil
	}
}

// handleIdentify responds to Pekko's Identify messages inside ActorSelectionMessages.
// This enables actorSelection(...).resolveOne() from Pekko to discover Go actors.
func (assoc *GekkaAssociation) handleIdentify(meta *ArteryMetadata, env *gproto_remote.SelectionEnvelope) {
	// Decode the Identify proto from the enclosed message.
	identify := &gproto_remote.Identify{}
	if err := proto.Unmarshal(env.GetEnclosedMessage(), identify); err != nil {
		slog.Debug("artery: failed to decode Identify", "error", err)
		return
	}

	// Resolve the recipient path from the ActorSelection elements.
	recipientPath := meta.Recipient.GetPath()

	// Build the actor ref path for the reply.
	la := assoc.nodeMgr.LocalAddr
	actorRefPath := fmt.Sprintf("%s://%s@%s:%d%s",
		la.GetProtocol(), la.GetSystem(), la.GetHostname(), la.GetPort(), recipientPath)

	// Build ActorIdentity reply.  Ref is always set (the actor exists from
	// Pekko's perspective — Go's registered actors serve as remote endpoints).
	identity := &gproto_remote.ActorIdentity{
		CorrelationId: identify.GetMessageId(),
		Ref:           &gproto_remote.ProtoActorRef{Path: proto.String(actorRefPath)},
	}

	identityBytes, err := proto.Marshal(identity)
	if err != nil {
		slog.Warn("artery: failed to marshal ActorIdentity", "error", err)
		return
	}

	// Send ActorIdentity back to the sender.  The sender path is in the Artery
	// envelope's sender field — that's where Pekko's resolveOne() waits.
	senderPath := ""
	if meta.Sender != nil {
		senderPath = meta.Sender.GetPath()
	}
	if senderPath == "" {
		slog.Debug("artery: Identify has no sender, cannot reply")
		return
	}

	// Send ActorIdentity via the outbound association to Pekko.
	// In Artery TCP, connections are unidirectional.
	remoteAddr := assoc.remote.GetAddress()
	outAssoc, ok := assoc.nodeMgr.GetGekkaAssociationByHost(remoteAddr.GetHostname(), remoteAddr.GetPort())
	if !ok {
		slog.Warn("artery: no outbound association for ActorIdentity reply")
		return
	}

	// Use SendWithSender to properly frame the reply with the actor ref as sender.
	if err := outAssoc.SendWithSender(senderPath, actorRefPath, identityBytes, MiscMessageSerializerID, "B"); err != nil {
		slog.Warn("artery: failed to send ActorIdentity", "error", err)
		return
	}
	slog.Debug("artery: sent ActorIdentity", "recipient", senderPath, "actorRef", actorRefPath)
}

// inboundLaneIndexOf returns the lane index of this assoc's conn within
// either its own inboundConns or its primary's (when delegated). Returns
// -1 if not found. Used by lane-aware HandshakeRsp routing in
// handleHandshakeReq. Sub-plan 8f outbound half.
func (assoc *GekkaAssociation) inboundLaneIndexOf() int {
	assoc.mu.RLock()
	myConn := assoc.conn
	delegate := assoc.delegate
	conns := assoc.inboundConns
	assoc.mu.RUnlock()
	primary := assoc
	if delegate != nil {
		primary = delegate
		primary.mu.RLock()
		conns = primary.inboundConns
		primary.mu.RUnlock()
	}
	for i, c := range conns {
		if c == myConn {
			return i
		}
	}
	return -1
}

func (assoc *GekkaAssociation) handleHandshakeReq(req *gproto_remote.HandshakeReq) error {
	slog.Debug("artery: received HandshakeReq", "from", req.From.String(), "role", assoc.role)

	// Validate that the 'To' address matches our local node (Pekko protocol requirement).
	if toSys := req.GetTo().GetSystem(); toSys != "" {
		if localSys := assoc.nodeMgr.LocalAddr.GetSystem(); toSys != localSys {
			return fmt.Errorf("Handshake rejected: To system %q != local system %q", toSys, localSys)
		}
	}

	// Quarantine guard: reject reconnection from a permanently quarantined UID.
	if uid := req.GetFrom().GetUid(); uid != 0 && assoc.nodeMgr.IsQuarantined(uid) {
		// Severity gated by pekko.remote.artery.propagate-harmless-quarantine-events.
		assoc.nodeMgr.EmitHarmlessQuarantineEvent("artery: rejecting HandshakeReq from quarantined UID", "uid", uid)
		assoc.emitFlight(SeverityWarn, CatQuarantine, "uid_rejected", map[string]any{
			"remote": assoc.remoteKey(),
			"uid":    uid,
		})
		// Inform the remote that it is quarantined and close.
		assoc.SendQuarantined(req.From)
		assoc.mu.Lock()
		assoc.state = QUARANTINED
		if assoc.conn != nil {
			assoc.conn.Close()
		}
		assoc.mu.Unlock()
		return fmt.Errorf("artery: HandshakeReq rejected — UID %d is quarantined", uid)
	}

	assoc.mu.Lock()
	assoc.remote = req.From
	assoc.state = ASSOCIATED
	assoc.mu.Unlock()
	assoc.emitFlight(SeverityInfo, CatHandshake, "ASSOCIATED", map[string]any{
		"remote": assoc.remoteKey(),
		"uid":    req.GetFrom().GetUid(),
	})

	// Sub-plan 8f outbound half — inbound coalescence by (UID, streamId=2).
	// When peer X opens N inbound streamId=2 TCPs from the same UID, every
	// TCP after the first attaches to the existing streamId=2 logical
	// association as an additional entry in inboundConns and sets its
	// dispatch delegate to the primary so all frames flow through the
	// single shared inbound-lanes fan-out.
	//
	// "Existing streamId=2 assoc" here is found by scanning the registry
	// for ANY streamId=2 entry whose remote UID matches — INBOUND (an
	// earlier inbound primary) OR OUTBOUND (gekka's own ordinary sibling
	// for that peer). The OUTBOUND sibling typically owns the registry
	// key (RegisterAssociation suppresses INBOUND when OUTBOUND already
	// occupies the (host:port, uid, streamId) slot), so inbound TCPs
	// from the peer attach onto the sibling and become the bidirectional
	// "logical" streamId=2 association.
	coalesced := false
	if assoc.role == INBOUND && assoc.streamId == 2 {
		nm := assoc.nodeMgr
		nm.mu.RLock()
		var existing *GekkaAssociation
		for _, a := range nm.associations {
			if a == assoc {
				continue
			}
			a.mu.RLock()
			match := a.streamId == 2 &&
				a.remote != nil &&
				a.remote.GetUid() == req.From.GetUid()
			a.mu.RUnlock()
			if match {
				existing = a
				break
			}
		}
		nm.mu.RUnlock()
		if existing != nil {
			existing.mu.Lock()
			existing.inboundConns = append(existing.inboundConns, assoc.conn)
			existing.mu.Unlock()
			assoc.mu.Lock()
			assoc.delegate = existing
			assoc.mu.Unlock()
			if nm.FlightRec != nil {
				nm.FlightRec.Emit(existing.remoteKey(), FlightEvent{
					Timestamp: time.Now(),
					Severity:  SeverityInfo,
					Category:  CatHandshake,
					Message:   "inbound_coalesced",
					Fields:    map[string]any{"conns": len(existing.inboundConns)},
				})
			}
			slog.Debug("artery: inbound coalesced into existing streamId=2 assoc",
				"remote", existing.remoteKey(),
				"conns", len(existing.inboundConns))
			coalesced = true
			// Skip registration below to avoid overwriting the primary,
			// but fall through to the HandshakeRsp routing block so the
			// peer's HandshakeReq still gets a response on the correct
			// outbound lane.
		} else {
			// First INBOUND streamId=2 from this peer UID — becomes
			// inboundConns[0] of itself (registered as primary below
			// by the RegisterAssociation call).
			assoc.mu.Lock()
			if len(assoc.inboundConns) == 0 {
				assoc.inboundConns = []net.Conn{assoc.conn}
			}
			assoc.mu.Unlock()
		}
	}

	if !coalesced {
		assoc.nodeMgr.RegisterAssociation(req.From, assoc)
	}

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

			slog.Debug("artery: handleHandshakeReq candidate", "key", k, "role", a.role, "state", a.state, "host", aHost, "port", aPort, "match", hostMatch)

			if isOutbound && hostMatch {
				if outboundToRemote == nil {
					outboundToRemote = a
				}
				if isWaiting && matched == nil {
					matched = a
					slog.Debug("artery: handleHandshakeReq matched association", "key", k)
				}
			}
		}
		nm.mu.RUnlock()

		// HandshakeRsp must be sent on Go's OUTBOUND TCP connection to Pekko.
		// In Artery-TCP, Pekko's outbound sockets are WRITE-ONLY — writing bytes
		// back on them causes "Unexpected incoming bytes" and quarantine/reset.
		// Pekko reads HandshakeRsp on its INBOUND (= Go's OUTBOUND to Pekko).
		//
		// If no OUTBOUND exists (e.g. when Go is the seed and Pekko joins for
		// the first time), we initiate one now. Without this, the fallback of
		// writing HandshakeRsp on the INBOUND socket causes Pekko to reset
		// the connection and Pekko can never join Go-seeded clusters.
		// When no OUTBOUND exists (Go is seed, Pekko joining for first time),
		// initiate a reverse outbound connection. The outbound's own handshake
		// (Go→Pekko HandshakeReq + Pekko→Go HandshakeRsp) will establish the
		// bidirectional association. We MUST NOT write HandshakeRsp on the
		// INBOUND connection — Pekko's outbound sockets are write-only and
		// any bytes from Go trigger "Unexpected incoming bytes" + quarantine.
		// Initiate reverse outbound when none exists (Go-as-seed scenario).
		// The outbound's handshake completes the bidirectional association.
		if outboundToRemote == nil {
			fromAddr := req.From.GetAddress()
			nm.mu.Lock()
			if nm.pendingDials == nil {
				nm.pendingDials = make(map[string]bool)
			}
			dialKey := fmt.Sprintf("%s:%d", fromAddr.GetHostname(), fromAddr.GetPort())
			if !nm.pendingDials[dialKey] {
				nm.pendingDials[dialKey] = true
				slog.Info("artery: no OUTBOUND to remote — initiating reverse outbound",
					"host", fromAddr.GetHostname(), "port", fromAddr.GetPort())
				go nm.DialRemoteWithRestart(context.Background(), fromAddr)
			}
			nm.mu.Unlock()
		}
		// Always send HandshakeRsp. Use OUTBOUND if available, INBOUND as
		// fallback (needed for unit tests; Pekko may reset the INBOUND but
		// the async DialRemote above will establish a working OUTBOUND for
		// subsequent messages).
		//
		// Sub-plan 8f outbound half — lane-aware HandshakeRsp routing.
		// When the inbound HandshakeReq arrived on a streamId=2 INBOUND
		// TCP at lane index L, prefer routing the response to the OUTBOUND
		// streamId=2 sibling's lane[L] (matching the lane index on which
		// the request landed). Falls back to the OUTBOUND control assoc's
		// outbox (streamId=1) when the sibling is not yet registered or
		// L >= len(lanes) — Pekko/Akka tolerate HandshakeRsp on streamId=1.
		{
			localUA := &gproto_remote.UniqueAddress{Address: assoc.nodeMgr.LocalAddr, Uid: proto.Uint64(assoc.localUid)}
			rspProto := &gproto_remote.MessageWithAddress{Address: localUA}
			if rspPayload, err2 := proto.Marshal(rspProto); err2 == nil {
				if frame, err2 := BuildArteryFrame(int64(assoc.localUid), actor.ArteryInternalSerializerID, "", "", "e", rspPayload, true); err2 == nil {
					var rspOutbox chan []byte
					var routed string
					if outboundToRemote != nil {
						// Prefer the streamId=1 control outbox for the
						// "OUTBOUND control" baseline. outboundToRemote
						// may itself be the streamId=2 sibling — pivot
						// to its sibling pointer when needed so the
						// fallback is the control outbox, not the
						// sibling outbox (which is unused for streamId=2
						// and would silently swallow the rsp).
						controlAssoc := outboundToRemote
						if outboundToRemote.streamId == 2 {
							outboundToRemote.mu.RLock()
							p := outboundToRemote.ordinarySibling
							outboundToRemote.mu.RUnlock()
							if p != nil {
								controlAssoc = p
							}
						}
						rspOutbox = controlAssoc.outbox
						routed = "OUTBOUND control"
						// If the inbound HandshakeReq came on a
						// streamId=2 TCP, route the response onto the
						// matching outbound lane of the streamId=2
						// sibling (looked up via control's sibling
						// pointer, which always points at the lanes-
						// bearing assoc).
						if assoc.streamId == 2 {
							laneIdx := assoc.inboundLaneIndexOf()
							controlAssoc.mu.RLock()
							sib := controlAssoc.ordinarySibling
							controlAssoc.mu.RUnlock()
							if sib != nil {
								sib.mu.RLock()
								lanes := sib.lanes
								sib.mu.RUnlock()
								if laneIdx >= 0 && laneIdx < len(lanes) {
									rspOutbox = lanes[laneIdx].outbox
									routed = "OUTBOUND sibling lane"
								} else if len(lanes) > 0 {
									rspOutbox = lanes[0].outbox
									routed = "OUTBOUND sibling lane[0]"
								}
							}
						}
					} else {
						rspOutbox = assoc.outbox
						routed = "INBOUND fallback"
					}
					slog.Debug("artery: sending HandshakeRsp", "via", routed)
					select {
					case rspOutbox <- frame:
					default:
						slog.Warn("artery: HandshakeRsp outbox full, dropping response")
					}
				}
			}
		}

		// Symmetric optimization: if this node also has an OUTBOUND assoc to
		// the same remote (e.g. both nodes dialled each other simultaneously),
		// complete it directly without waiting for an extra round-trip.
		if matched != nil {
			slog.Info("artery: completing matching OUTBOUND association")
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
					slog.Warn("artery: outbox full, dropping pending frame during handshake flush")
				}
			}
			matched.pending = nil
			matched.mu.Unlock()
			matched.emitFlight(SeverityInfo, CatHandshake, "ASSOCIATED", map[string]any{
				"remote": matched.remoteKey(),
			})
			nm.RegisterAssociation(req.From, matched)
			// Sub-plan 8f outbound half: dial the streamId=2 ordinary
			// sibling once the OUTBOUND control handshake is complete.
			// On error, log + flight event but do not fail the assoc —
			// ordinary traffic falls back to the control stream during
			// the convergence window.
			if matched.streamId == 1 {
				go func(c *GekkaAssociation) {
					if err := nm.EnsureOrdinarySibling(context.Background(), c); err != nil {
						slog.Debug("artery: EnsureOrdinarySibling error", "remote", c.remoteKey(), "error", err)
					}
				}(matched)
			}
		}

		return nil
	}

	// OUTBOUND: flush pending and send HandshakeRsp (if we were the one initiating)
	assoc.mu.Lock()
	for _, msg := range assoc.pending {
		select {
		case assoc.outbox <- msg:
		default:
			slog.Warn("artery: outbox full, dropping pending frame during handshake flush")
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
	slog.Debug("artery: sending HandshakeRsp (e)", "uid", assoc.localUid)
	select {
	case assoc.outbox <- frame:
	default:
		slog.Warn("artery: outbox full, dropping HandshakeRsp frame")
	}
	return nil
}

func (assoc *GekkaAssociation) handleHandshakeRsp(mwa *gproto_remote.MessageWithAddress) error {
	slog.Debug("artery: received HandshakeRsp", "from", mwa.Address.String())

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
			slog.Debug("artery: handleHandshakeRsp candidate", "match", match, "host", aHost, "port", aPort)
		} else {
			slog.Debug("artery: handleHandshakeRsp candidate", "match", false, "hasRemote", a.remote != nil, "hasMwaAddr", mwa.Address != nil)
		}
		a.mu.RUnlock()

		if isOutbound && isWaiting && match {
			slog.Info("artery: found matching outbound association", "state", a.state)
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
				slog.Warn("artery: outbox full, dropping pending frame during handshakeRsp flush")
			}
		}
		matched.pending = nil
		matched.mu.Unlock()
		matched.emitFlight(SeverityInfo, CatHandshake, "ASSOCIATED", map[string]any{
			"remote": matched.remoteKey(),
		})

		// Re-register with full UniqueAddress (including UID from Pekko)
		assoc.nodeMgr.RegisterAssociation(mwa.Address, matched)
		// Sub-plan 8f outbound half: dial the streamId=2 ordinary sibling
		// once the OUTBOUND control handshake is complete. On error log +
		// flight event but do not fail the control assoc — ordinary
		// traffic falls back to the control stream during the convergence
		// window.
		if matched.streamId == 1 {
			nm := assoc.nodeMgr
			go func(c *GekkaAssociation) {
				if err := nm.EnsureOrdinarySibling(context.Background(), c); err != nil {
					slog.Debug("artery: EnsureOrdinarySibling error", "remote", c.remoteKey(), "error", err)
				}
			}(matched)
		}
	} else {
		slog.Warn("artery: no matching outbound association found for HandshakeRsp")
	}

	return nil
}

// ---------------------------------------------------------------------------
// UDP transport extensions
// ---------------------------------------------------------------------------

// Dispatch is an exported wrapper around the unexported dispatch method.
// It is called by the UdpArteryHandler to route inbound Aeron DATA frames
// through the same Artery message pipeline used by the TCP transport.
func (assoc *GekkaAssociation) Dispatch(ctx context.Context, meta *ArteryMetadata) error {
	return assoc.dispatch(ctx, meta)
}

// GetOrCreateUDPAssociation returns an existing association for src, or creates
// a new INBOUND UDP association wired to udpH for outbound responses.
//
// The new association has no TCP conn; its outbox drainer forwards frames via
// udpH.SendFrame(src, AeronStreamControl, frame).
// getAnyAssociationByHost returns the first association (INBOUND or OUTBOUND)
// for the given host:port pair.  Used by the UDP inbound path to find previously
// created inbound associations without filtering to OUTBOUND-only.
func (nm *NodeManager) getAnyAssociationByHost(host string, port uint32) (*GekkaAssociation, bool) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	for _, assoc := range nm.associations {
		assoc.mu.RLock()
		remote := assoc.remote
		assoc.mu.RUnlock()
		if remote != nil && remote.Address.GetHostname() == host && remote.Address.GetPort() == port {
			return assoc, true
		}
	}
	return nil, false
}

func (nm *NodeManager) GetOrCreateUDPAssociation(src *net.UDPAddr, udpH *UdpArteryHandler) *GekkaAssociation {
	// Fast path: look up by physical UDP source address.  This map is keyed by
	// the ephemeral media-driver port (e.g. "127.0.0.1:62159") and is never
	// overwritten when handleHandshakeReq updates assoc.remote to the canonical
	// Akka address, so it remains valid for the lifetime of the session.
	srcKey := src.String()
	nm.mu.RLock()
	if assoc, ok := nm.udpSrcAssoc[srcKey]; ok {
		nm.mu.RUnlock()
		return assoc
	}
	nm.mu.RUnlock()

	// Also check whether the src is the canonical Akka address (e.g. a direct
	// OUTBOUND association to 127.0.0.1:2561).
	if assoc, ok := nm.getAnyAssociationByHost(src.IP.String(), uint32(src.Port)); ok {
		return assoc
	}

	assoc := &GekkaAssociation{
		state:      INITIATED,
		role:       INBOUND,
		nodeMgr:    nm,
		lastSeen:   time.Now(),
		Handshake:  make(chan struct{}),
		localUid:   nm.localUid,
		outbox:     make(chan []byte, nm.EffectiveOutboundMessageQueueSize()),
		streamId:   AeronStreamControl,
		udpHandler: udpH,
		udpDst:     src,
		remote: &gproto_remote.UniqueAddress{
			Address: &gproto_remote.Address{
				Protocol: nm.LocalAddr.Protocol,
				System:   nm.LocalAddr.System,
				Hostname: proto.String(src.IP.String()),
				Port:     proto.Uint32(uint32(src.Port)),
			},
			Uid: proto.Uint64(0),
		},
	}
	nm.RegisterAssociation(assoc.remote, assoc)

	// Pin the ephemeral UDP source address to this association permanently.
	// handleHandshakeReq will later overwrite assoc.remote with the canonical
	// Akka address, but udpSrcAssoc keeps the original src → assoc binding.
	nm.mu.Lock()
	nm.udpSrcAssoc[srcKey] = assoc
	nm.mu.Unlock()

	// Start outbox drainer: sends frames via Aeron UDP back to src.
	go func() {
		for frame := range assoc.outbox {
			if err := udpH.SendFrame(src, AeronStreamControl, frame); err != nil {
				slog.Warn("aeron-udp: inbound assoc outbox error", "src", src, "error", err)
			}
		}
	}()

	slog.Info("aeron-udp: new inbound UDP association created", "src", src)
	return assoc
}
