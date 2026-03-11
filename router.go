/*
 * router.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"fmt"
	"gekka/cluster"
	"log"
	"net"
	reflect "reflect"
	"time"

	"google.golang.org/protobuf/proto"
)

// Router handles actor path resolution and message delivery.
type Router struct {
	nodeMgr *NodeManager
}

func NewRouter(nm *NodeManager) *Router {
	return &Router{nodeMgr: nm}
}

// Send resolves the path and delivers the message.
// It uses SerializationRegistry to automatically detect serializerId and manifest.
func (r *Router) Send(ctx context.Context, path string, msg interface{}) error {
	ap, err := ParseActorPath(path)
	if err != nil {
		return err
	}

	// 1. Check if local
	local := r.nodeMgr.localAddress
	if ap.System == local.GetSystem() && ap.Host == local.GetHostname() && ap.Port == local.GetPort() {
		log.Printf("Router: local delivery to %s", ap.Path)
		// In a real system, this would go to the local actor's mailbox.
		return nil
	}

	// 2. Remote delivery
	targetAddr := ap.ToAddress()

	// Check NodeManager for existing association
	// Note: We don't have the UID yet, so we look by Host:Port if we had such an index.
	// Our current NodeManager uses HOST-UID. Let's add a host-based lookup or use a dummy UID for now.
	// In Artery, we typically dial the host:port and get the UID during handshake.

	assoc, ok := r.getAssociationByHost(targetAddr.GetHostname(), targetAddr.GetPort())
	if !ok {
		// Initiate new connection
		log.Printf("Router: initiating new connection to %s:%d", targetAddr.GetHostname(), targetAddr.GetPort())
		var err error
		assoc, err = r.dialRemote(ctx, targetAddr)
		if err != nil {
			return fmt.Errorf("failed to dial remote: %w", err)
		}
	}

	// 3. Serialize and send using Registry
	var payload []byte
	var errSerialize error
	var finalSerializerId int32
	var finalManifest string

	// Check cluster message registry first: Pekko's ClusterMessageSerializer (ID=5) uses short manifests.
	msgType := reflect.TypeOf(msg)
	switch msgType {
	case reflect.TypeOf((*cluster.InitJoin)(nil)):
		finalSerializerId, finalManifest = ClusterSerializerID, "IJ"
	case reflect.TypeOf((*cluster.InitJoinAck)(nil)):
		finalSerializerId, finalManifest = ClusterSerializerID, "IJA"
	case reflect.TypeOf((*cluster.Join)(nil)):
		finalSerializerId, finalManifest = ClusterSerializerID, "J"
	case reflect.TypeOf((*cluster.Welcome)(nil)):
		finalSerializerId, finalManifest = ClusterSerializerID, "W"
	case reflect.TypeOf((*cluster.Heartbeat)(nil)):
		finalSerializerId, finalManifest = ClusterSerializerID, "HB"
	case reflect.TypeOf((*cluster.HeartBeatResponse)(nil)):
		finalSerializerId, finalManifest = ClusterSerializerID, "HBR"
	case reflect.TypeOf((*cluster.GossipStatus)(nil)):
		finalSerializerId, finalManifest = ClusterSerializerID, "GS"
	case reflect.TypeOf((*cluster.GossipEnvelope)(nil)):
		finalSerializerId, finalManifest = ClusterSerializerID, "GE"
	case reflect.TypeOf((*cluster.Address)(nil)):
		// Leave message — serialized as an Address proto
		finalSerializerId, finalManifest = ClusterSerializerID, "L"
	default:
		if _, isProto := msg.(proto.Message); isProto {
			finalSerializerId = 2
			finalManifest = msgType.String()
		} else if _, isBytes := msg.([]byte); isBytes {
			// Raw bytes use Pekko's ByteArraySerializer (ID=4) with empty manifest.
			finalSerializerId = 4
			finalManifest = ""
		} else {
			// Arbitrary Go struct: use the built-in JSONSerializer so that
			// ToBinary succeeds (ByteArraySerializer only accepts []byte).
			finalSerializerId = JSONSerializerID
			finalManifest = msgType.String()
		}
	}

	// Serialize: Welcome uses gzip(proto) per Pekko wire format; all other protos use plain proto.Marshal.
	if finalManifest == "W" {
		if pmsg, ok := msg.(proto.Message); ok {
			raw, err := proto.Marshal(pmsg)
			if err != nil {
				return fmt.Errorf("marshal Welcome: %w", err)
			}
			payload, errSerialize = gzipCompress(raw)
		}
	} else if pmsg, ok := msg.(proto.Message); ok {
		payload, errSerialize = proto.Marshal(pmsg)
	} else if b, ok := msg.([]byte); ok {
		payload = b
	} else if r.nodeMgr.SerializerRegistry != nil {
		s, err := r.nodeMgr.SerializerRegistry.GetSerializer(finalSerializerId)
		if err == nil {
			payload, errSerialize = s.ToBinary(msg)
		} else {
			errSerialize = err
		}
	} else {
		errSerialize = fmt.Errorf("cannot serialize non-proto message without registry")
	}

	if errSerialize != nil {
		return fmt.Errorf("serialize error: %w", errSerialize)
	}

	if assoc == nil {
		return fmt.Errorf("failed to dial remote: association is nil")
	}

	// Track user message metrics (cluster-internal messages are excluded).
	if r.nodeMgr.metrics != nil &&
		finalSerializerId != ClusterSerializerID &&
		finalSerializerId != ArteryInternalSerializerID {
		r.nodeMgr.metrics.MessagesSent.Add(1)
		r.nodeMgr.metrics.BytesSent.Add(int64(len(payload)))
	}

	return assoc.Send(path, payload, finalSerializerId, finalManifest)
}

// SendWithSender resolves the path, serializes msg, and delivers it to the remote
// actor with senderPath set as the Artery sender (used by the Ask pattern so the
// remote actor can reply to the temporary sender path).
func (r *Router) SendWithSender(ctx context.Context, path string, senderPath string, msg interface{}) error {
	ap, err := ParseActorPath(path)
	if err != nil {
		return err
	}

	targetAddr := ap.ToAddress()
	assoc, ok := r.getAssociationByHost(targetAddr.GetHostname(), targetAddr.GetPort())
	if !ok {
		assoc, err = r.dialRemote(ctx, targetAddr)
		if err != nil {
			return fmt.Errorf("failed to dial remote: %w", err)
		}
	}

	var payload []byte
	var errSerialize error
	var finalSerializerId int32
	var finalManifest string

	msgType := reflect.TypeOf(msg)
	switch msgType {
	case reflect.TypeOf((*cluster.InitJoin)(nil)):
		finalSerializerId, finalManifest = ClusterSerializerID, "IJ"
	case reflect.TypeOf((*cluster.InitJoinAck)(nil)):
		finalSerializerId, finalManifest = ClusterSerializerID, "IJA"
	case reflect.TypeOf((*cluster.Join)(nil)):
		finalSerializerId, finalManifest = ClusterSerializerID, "J"
	case reflect.TypeOf((*cluster.Welcome)(nil)):
		finalSerializerId, finalManifest = ClusterSerializerID, "W"
	case reflect.TypeOf((*cluster.Heartbeat)(nil)):
		finalSerializerId, finalManifest = ClusterSerializerID, "HB"
	case reflect.TypeOf((*cluster.HeartBeatResponse)(nil)):
		finalSerializerId, finalManifest = ClusterSerializerID, "HBR"
	case reflect.TypeOf((*cluster.GossipStatus)(nil)):
		finalSerializerId, finalManifest = ClusterSerializerID, "GS"
	case reflect.TypeOf((*cluster.GossipEnvelope)(nil)):
		finalSerializerId, finalManifest = ClusterSerializerID, "GE"
	case reflect.TypeOf((*cluster.Address)(nil)):
		finalSerializerId, finalManifest = ClusterSerializerID, "L"
	default:
		if _, isProto := msg.(proto.Message); isProto {
			finalSerializerId = 2
			finalManifest = msgType.String()
		} else if _, isBytes := msg.([]byte); isBytes {
			finalSerializerId = 4
			finalManifest = ""
		} else {
			// Arbitrary Go struct: use the built-in JSONSerializer so that
			// ToBinary succeeds (ByteArraySerializer only accepts []byte).
			finalSerializerId = JSONSerializerID
			finalManifest = msgType.String()
		}
	}

	if finalManifest == "W" {
		if pmsg, ok := msg.(proto.Message); ok {
			raw, err := proto.Marshal(pmsg)
			if err != nil {
				return fmt.Errorf("marshal Welcome: %w", err)
			}
			payload, errSerialize = gzipCompress(raw)
		}
	} else if pmsg, ok := msg.(proto.Message); ok {
		payload, errSerialize = proto.Marshal(pmsg)
	} else if b, ok := msg.([]byte); ok {
		payload = b
	} else if r.nodeMgr.SerializerRegistry != nil {
		s, err := r.nodeMgr.SerializerRegistry.GetSerializer(finalSerializerId)
		if err == nil {
			payload, errSerialize = s.ToBinary(msg)
		} else {
			errSerialize = err
		}
	} else {
		errSerialize = fmt.Errorf("cannot serialize non-proto message without registry")
	}

	if errSerialize != nil {
		return fmt.Errorf("serialize error: %w", errSerialize)
	}

	if assoc == nil {
		return fmt.Errorf("failed to dial remote: association is nil")
	}

	// SendWithSender is only used by the Ask pattern — always a user message.
	if r.nodeMgr.metrics != nil {
		r.nodeMgr.metrics.MessagesSent.Add(1)
		r.nodeMgr.metrics.BytesSent.Add(int64(len(payload)))
	}

	return assoc.SendWithSender(path, senderPath, payload, finalSerializerId, finalManifest)
}

func (r *Router) getAssociationByHost(host string, port uint32) (*GekkaAssociation, bool) {
	r.nodeMgr.mu.RLock()
	defer r.nodeMgr.mu.RUnlock()

	for _, assoc := range r.nodeMgr.associations {
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

func (r *Router) dialRemote(ctx context.Context, target *Address) (*GekkaAssociation, error) {
	addrStr := fmt.Sprintf("%s:%d", target.GetHostname(), target.GetPort())

	// Create a temporary client to perform the handshake.
	// The actual processing will be handed off to Association.Process.

	client, err := NewTcpClient(TcpClientConfig{
		Addr: addrStr,
		Handler: func(ctx context.Context, conn net.Conn) error {
			return r.nodeMgr.ProcessConnection(ctx, conn, OUTBOUND, target, 1) // Default to Control stream for outbound
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

	// Wait for the association to appear in NodeManager or for an error
	// This is a simplified waiter.
	timeout := time.After(5 * time.Second)
	for {
		select {
		case err := <-errChan:
			return nil, err
		case <-timeout:
			return nil, fmt.Errorf("dial timeout")
		case <-time.After(100 * time.Millisecond):
			if assoc, ok := r.getAssociationByHost(target.GetHostname(), target.GetPort()); ok {
				return assoc, nil
			}
		}
	}
}
