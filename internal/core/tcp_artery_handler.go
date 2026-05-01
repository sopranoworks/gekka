/*
 * tcp_artery_handler.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/sopranoworks/gekka/actor"
	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

const (
	// DefaultMaxFrameSize is the default maximum allowed payload length.
	// Pekko's default is 256 KiB (pekko.remote.artery.advanced.maximum-frame-size).
	DefaultMaxFrameSize = 256 * 1024

	// MaxArteryPayloadLength is the absolute upper bound for validation.
	// Even with a configurable frame size we never accept more than 128 MB.
	MaxArteryPayloadLength = 128 * 1024 * 1024
)

// ArteryMetadata holds decoded information from an Artery envelope.
type ArteryMetadata struct {
	Sender              *gproto_remote.ActorRefData
	Recipient           *gproto_remote.ActorRefData
	SeqNo               uint64
	SerializerId        int32
	MessageManifest     []byte
	Payload             []byte
	AckReplyTo          *gproto_remote.UniqueAddress // Only for SystemMessageEnvelope
	DeserializedMessage interface{}
}

// FrameHandler is a function that processes an Artery frame.
type FrameHandler func(ctx context.Context, meta *ArteryMetadata) error

// DefaultFrameHandler is the default implementation that logs the message.
var DefaultFrameHandler FrameHandler = ArteryDispatcher

// TcpArteryHandlerWithNodeManager is a compatibility wrapper that uses NodeManager.ProcessConnection.
//
// On a non-nil error from ProcessConnection the wrapper invokes
// nm.TryRecordInboundRestart() — the gekka analogue of Pekko's inbound
// stream restart, since the next inbound connection is a fresh
// "stream materialization". When the rolling cap
// (pekko.remote.artery.advanced.inbound-max-restarts within
// inbound-restart-timeout) is exceeded, a slog.Warn is emitted along
// with a flight-recorder event under CatInboundRestartExceeded so the
// saturation signal is observable. The listener is intentionally NOT
// terminated — Pekko terminates the ActorSystem on cap-exceed, but
// gekka chooses the more conservative route of surfacing the
// saturation without forcing service-impacting shutdown.
func TcpArteryHandlerWithNodeManager(nm *NodeManager) TcpHandler {
	return func(ctx context.Context, conn net.Conn) error {
		// Existing tests assume INBOUND behavior for this handler.
		err := nm.ProcessConnection(ctx, conn, INBOUND, nil, 0) // Unknown streamId
		if err != nil {
			if !nm.TryRecordInboundRestart() {
				slog.Warn("artery: inbound-restart cap exceeded",
					"error", err,
					"max", nm.EffectiveInboundMaxRestarts(),
					"window", nm.EffectiveInboundRestartTimeout())
				if nm.FlightRec != nil {
					remoteKey := ""
					if conn != nil && conn.RemoteAddr() != nil {
						remoteKey = conn.RemoteAddr().String()
					}
					nm.FlightRec.Emit(remoteKey, FlightEvent{
						Timestamp: time.Now(),
						Severity:  SeverityWarn,
						Category:  CatInboundRestartExceeded,
						Message:   "inbound_restart_cap_exceeded",
						Fields: map[string]any{
							"error":  err.Error(),
							"max":    nm.EffectiveInboundMaxRestarts(),
							"window": nm.EffectiveInboundRestartTimeout().String(),
						},
					})
				}
			}
		}
		return err
	}
}

// TcpArteryHandlerWithCallback is an inbound handler that proceeds directly to the read loop with a known streamId.
func TcpArteryHandlerWithCallback(ctx context.Context, conn net.Conn, handler FrameHandler, ctm *CompressionTableManager, remoteUid uint64, streamId int32, maxFrameSize ...int) error {
	limit := resolveMaxFrameSize(maxFrameSize)
	return tcpArteryReadLoop(ctx, conn, handler, ctm, remoteUid, streamId, limit)
}

func TcpArteryOutboundHandler(ctx context.Context, conn net.Conn, handler FrameHandler, ctm *CompressionTableManager, remoteUid uint64, streamId int32, protocol string, maxFrameSize ...int) error {
	// Both Pekko 1.x and Akka 2.6.x use the AKKA preamble on the wire.
	// Pekko kept the same Artery TCP framing for backwards compatibility;
	// the "pekko://" vs "akka://" distinction is only in actor-path URIs,
	// not in the TCP stream header.
	magicHeader := []byte{'A', 'K', 'K', 'A', byte(streamId)}
	slog.Info("artery: writing Pekko preamble", "streamId", streamId)

	if _, err := conn.Write(magicHeader); err != nil {
		return fmt.Errorf("failed to write stream magic header: %w", err)
	}

	limit := resolveMaxFrameSize(maxFrameSize)
	return tcpArteryReadLoop(ctx, conn, handler, ctm, remoteUid, streamId, limit)
}

// resolveMaxFrameSize returns the first element of the variadic slice, or
// DefaultMaxFrameSize when empty/zero.
func resolveMaxFrameSize(vals []int) int32 {
	if len(vals) > 0 && vals[0] > 0 {
		return int32(vals[0])
	}
	return DefaultMaxFrameSize
}

func tcpArteryReadLoop(ctx context.Context, conn net.Conn, handler FrameHandler, ctm *CompressionTableManager, remoteUid uint64, streamId int32, maxFrameSize int32) error {
	headerBuf := make([]byte, 4)
	payloadBuf := make([]byte, 64*1024)

	for {
		// Respect context cancellation.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// First, read exactly 4 bytes to determine the payloadLength.
		_, err := io.ReadFull(conn, headerBuf)
		if err != nil {
			if err == io.EOF {
				return nil // connection closed by remote
			}
			return fmt.Errorf("read header error: %w", err)
		}

		payloadLength := int32(binary.LittleEndian.Uint32(headerBuf))

		// Validate the payloadLength.
		if payloadLength < 0 {
			return fmt.Errorf("invalid payload length: %d", payloadLength)
		}
		if payloadLength > maxFrameSize {
			return fmt.Errorf("payload length exceeds limit: %d > %d", payloadLength, maxFrameSize)
		}

		if payloadLength == 0 {
			continue // Artery doesn't typically send empty frames
		}

		// Ensure payloadBuf is large enough.
		if int(payloadLength) > len(payloadBuf) {
			payloadBuf = make([]byte, payloadLength)
		}

		// Second, read exactly payloadLength bytes into a buffer.
		targetBuf := payloadBuf[:payloadLength]
		_, err = io.ReadFull(conn, targetBuf)
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("unexpected EOF while reading payload")
			}
			return fmt.Errorf("read payload error: %w", err)
		}

		// Decode the envelope.
		meta, err := DecodeArteryEnvelope(targetBuf, ctm, remoteUid)
		if err != nil {
			slog.Warn("artery: failed to decode envelope", "error", err)
			continue
		}
		slog.Debug("artery: decoded frame",
			"serializerId", meta.SerializerId,
			"manifest", string(meta.MessageManifest),
			"seq", meta.SeqNo)

		if err := handler(ctx, meta); err != nil {
			slog.Warn("artery: handler error", "error", err)
			return fmt.Errorf("handler error: %w", err)
		}
	}
}

// DecodeArteryEnvelope decodes the binary Artery EnvelopeBuffer frame.
func DecodeArteryEnvelope(payload []byte, ctm *CompressionTableManager, remoteUid uint64) (*ArteryMetadata, error) {
	return ParseArteryFrame(payload, ctm, remoteUid)
}

// SendArteryMessage is a convenience wrapper for SendArteryMessageWithAck without an ack.
func SendArteryMessage(conn net.Conn, localUid int64, serializerId int32, manifest string, message proto.Message, control bool) error {
	return SendArteryMessageWithAck(conn, localUid, serializerId, manifest, message, nil, control)
}

// SendArteryMessageWithAck marshals the message and sends it as a proper Artery binary frame.
func SendArteryMessageWithAck(conn net.Conn, localUid int64, serializerId int32, manifest string, message proto.Message, sender *gproto_remote.UniqueAddress, control bool) error {
	msgPayload, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal artery message: %w", err)
	}

	var senderPath string
	if sender != nil {
		a := sender.GetAddress()
		senderPath = fmt.Sprintf("%s://%s@%s:%d", a.GetProtocol(), a.GetSystem(), a.GetHostname(), a.GetPort())
	}

	frame, err := BuildArteryFrame(localUid, serializerId, senderPath, "", manifest, msgPayload, control)
	if err != nil {
		return err
	}

	return WriteFrame(conn, frame)
}

// SendArteryHeartbeat sends a transport-level heartbeat and records the sent time.
// It uses the association's outbox to ensure thread-safety with other outbound frames.
func SendArteryHeartbeat(assoc *GekkaAssociation) error {
	assoc.mu.Lock()
	assoc.lastHeartbeatSentAt = time.Now()
	uid := assoc.localUid
	assoc.mu.Unlock()

	// ArteryHeartbeat (manifest "m") is an empty payload message.
	frame, err := BuildArteryFrame(int64(uid), actor.ArteryInternalSerializerID, "", "", "m", nil, true)
	if err != nil {
		return err
	}

	select {
	case assoc.outbox <- frame:
		return nil
	default:
		return fmt.Errorf("association outbox full")
	}
}

// BuildRemoteEnvelope constructs a 3-layer remote envelope for actor messages.
func BuildRemoteEnvelope(recipient string, message []byte, serializerId int32, manifest string, seq uint64, sender *gproto_remote.UniqueAddress) ([]byte, error) {
	serMsg := &gproto_remote.SerializedMessage{
		Message:         message,
		SerializerId:    proto.Int32(serializerId),
		MessageManifest: []byte(manifest),
	}

	env := &gproto_remote.RemoteEnvelope{
		Recipient: &gproto_remote.ActorRefData{Path: proto.String(recipient)},
		Message:   serMsg,
		Seq:       proto.Uint64(seq),
	}

	if sender != nil {
		// Artery uses ActorRefData for sender in RemoteEnvelope, but it often contains UniqueAddress logic.
		// For simplicity, we just put the path if available or a dummy.
		env.Sender = &gproto_remote.ActorRefData{Path: proto.String(sender.GetAddress().GetHostname())}
	}
	return proto.Marshal(env)
}

// BuildSystemEnvelope constructs a system message envelope with a sequence number and optional ack reply.
func BuildSystemEnvelope(message []byte, serializerId int32, manifest string, seqNo uint64, ackReplyTo *gproto_remote.UniqueAddress) ([]byte, error) {
	env := &gproto_remote.SystemMessageEnvelope{
		Message:         message,
		SerializerId:    proto.Int32(serializerId),
		MessageManifest: []byte(manifest),
		SeqNo:           proto.Uint64(seqNo),
		AckReplyTo:      ackReplyTo,
	}

	return proto.Marshal(env)
}

// WriteFrame prepends the 4-byte length header and writes the payload to the writer.
func WriteFrame(writer io.Writer, payload []byte) error {
	frame := make([]byte, 4+len(payload))
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)

	// Trace-level hex dump of the first 64 bytes (header + start of Artery envelope).
	// Enable with slog at Debug level to compare wire bytes against a real Pekko/Akka node.
	if slog.Default().Enabled(context.TODO(), slog.LevelDebug) {
		end := 64
		if end > len(frame) {
			end = len(frame)
		}
		slog.Debug("artery: outbound frame",
			"total_bytes", len(frame),
			"payload_bytes", len(payload),
			"hex64", hex.EncodeToString(frame[:end]),
		)
	}

	if _, err := writer.Write(frame); err != nil {
		return fmt.Errorf("failed to write frame: %w", err)
	}
	slog.Debug("artery: wrote frame", "total_bytes", 4+len(payload))
	return nil
}

// ArteryDispatcher routes the message once the association is established.
func ArteryDispatcher(ctx context.Context, meta *ArteryMetadata) error {
	switch meta.SerializerId {
	case 1: // Example: Java Serializer
		slog.Debug("artery: routing to Java Serializer", "seq", meta.SeqNo)
	case 2: // Example: JSON Serializer
		slog.Debug("artery: routing to JSON Serializer", "seq", meta.SeqNo)
	default:
		slog.Debug("artery: unrecognized serializerId", "serializerId", meta.SerializerId)
		return fmt.Errorf("unrecognized serializerId: %d", meta.SerializerId)
	}
	return nil
}
