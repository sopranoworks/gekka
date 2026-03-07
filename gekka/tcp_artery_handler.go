/*
 * tcp_artery_handler.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"

	"google.golang.org/protobuf/proto"
)

const (
	// MaxArteryPayloadLength is the maximum allowed payload length (128MB).
	MaxArteryPayloadLength = 128 * 1024 * 1024
)

// ArteryMetadata holds decoded information from an Artery envelope.
type ArteryMetadata struct {
	Sender              *ActorRefData
	Recipient           *ActorRefData
	SeqNo               uint64
	SerializerId        int32
	MessageManifest     []byte
	Payload             []byte
	AckReplyTo          *UniqueAddress // Only for SystemMessageEnvelope
	DeserializedMessage interface{}
}

// FrameHandler is a function that processes an Artery frame.
type FrameHandler func(ctx context.Context, meta *ArteryMetadata) error

// DefaultFrameHandler is the default implementation that logs the message.
var DefaultFrameHandler FrameHandler = ArteryDispatcher

// TcpArteryHandlerWithNodeManager is a compatibility wrapper that uses NodeManager.ProcessConnection.
func TcpArteryHandlerWithNodeManager(nm *NodeManager) TcpHandler {
	return func(ctx context.Context, conn net.Conn) error {
		// Existing tests assume INBOUND behavior for this handler.
		return nm.ProcessConnection(ctx, conn, INBOUND, nil)
	}
}

// TcpArteryHandlerWithCallback is an inbound handler (reads magic from peer first).
func TcpArteryHandlerWithCallback(ctx context.Context, conn net.Conn, handler FrameHandler, ctm *CompressionTableManager, remoteUid uint64) error {
	// Inbound: read and validate magic header from peer
	peerMagic := make([]byte, 5)
	if _, err := io.ReadFull(conn, peerMagic); err != nil {
		return fmt.Errorf("failed to read stream magic header: %w", err)
	}
	if string(peerMagic[:4]) != "AKKA" {
		return fmt.Errorf("invalid stream magic header: %x", peerMagic)
	}
	log.Printf("TcpArteryHandler: inbound connection accepted, streamId=%d", peerMagic[4])
	return tcpArteryReadLoop(ctx, conn, handler, ctm, remoteUid)
}

func TcpArteryOutboundHandler(ctx context.Context, conn net.Conn, handler FrameHandler, ctm *CompressionTableManager, remoteUid uint64) error {
	// Outbound: write magic header (streamId=1 for Control stream), then enter read loop
	// Pekko Artery: ControlStreamId = 1, OrdinaryStreamId = 2, LargeStreamId = 3
	magicHeader := []byte{'A', 'K', 'K', 'A', 2}
	if _, err := conn.Write(magicHeader); err != nil {
		return fmt.Errorf("failed to write stream magic header: %w", err)
	}
	log.Printf("TcpArteryOutboundHandler: magic header [AKKA\\x02] written, entering read loop")

	return tcpArteryReadLoop(ctx, conn, handler, ctm, remoteUid)
}

func tcpArteryReadLoop(ctx context.Context, conn net.Conn, handler FrameHandler, ctm *CompressionTableManager, remoteUid uint64) error {
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
		if payloadLength > MaxArteryPayloadLength {
			return fmt.Errorf("payload length exceeds limit: %d > %d", payloadLength, MaxArteryPayloadLength)
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
			log.Printf("TcpArteryHandler: failed to decode envelope: %v", err)
			continue
		}
		log.Printf("TcpArteryHandler: decoded frame: serializerId=%d manifest=%q seq=%d", meta.SerializerId, string(meta.MessageManifest), meta.SeqNo)

		if err := handler(ctx, meta); err != nil {
			log.Printf("TcpArteryHandler: handler error: %v", err)
			return fmt.Errorf("handler error: %w", err)
		}
	}
}

// DecodeArteryEnvelope decodes the binary Artery EnvelopeBuffer frame.
func DecodeArteryEnvelope(payload []byte, ctm *CompressionTableManager, remoteUid uint64) (*ArteryMetadata, error) {
	return ParseArteryFrame(payload, ctm, remoteUid)
}

// SendArteryMessage is a convenience wrapper for SendArteryMessageWithAck without an ack.
func SendArteryMessage(conn net.Conn, remoteUid int64, serializerId int32, manifest string, message proto.Message, control bool) error {
	return SendArteryMessageWithAck(conn, remoteUid, serializerId, manifest, message, nil, control)
}

// SendArteryMessageWithAck marshals the message and sends it as a proper Artery binary frame.
func SendArteryMessageWithAck(conn net.Conn, remoteUid int64, serializerId int32, manifest string, message proto.Message, sender *UniqueAddress, control bool) error {
	msgPayload, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal artery message: %w", err)
	}

	var senderPath string
	if sender != nil {
		a := sender.GetAddress()
		senderPath = fmt.Sprintf("%s://%s@%s:%d", a.GetProtocol(), a.GetSystem(), a.GetHostname(), a.GetPort())
	}

	frame, err := BuildArteryFrame(remoteUid, serializerId, senderPath, "", manifest, msgPayload, control)
	if err != nil {
		return err
	}

	log.Printf("OUT: len=%d serializerId=%d manifest=%q payload=%x", len(frame), serializerId, manifest, msgPayload)

	return WriteFrame(conn, frame)
}

// BuildRemoteEnvelope constructs a 3-layer remote envelope for actor messages.
func BuildRemoteEnvelope(recipient string, message []byte, serializerId int32, manifest string, seq uint64, sender *UniqueAddress) ([]byte, error) {
	serMsg := &SerializedMessage{
		Message:         message,
		SerializerId:    proto.Int32(serializerId),
		MessageManifest: []byte(manifest),
	}

	env := &RemoteEnvelope{
		Recipient: &ActorRefData{Path: proto.String(recipient)},
		Message:   serMsg,
		Seq:       proto.Uint64(seq),
	}

	if sender != nil {
		// Artery uses ActorRefData for sender in RemoteEnvelope, but it often contains UniqueAddress logic.
		// For simplicity, we just put the path if available or a dummy.
		env.Sender = &ActorRefData{Path: proto.String(sender.GetAddress().GetHostname())}
	}
	return proto.Marshal(env)
}

// BuildSystemEnvelope constructs a system message envelope with a sequence number and optional ack reply.
func BuildSystemEnvelope(message []byte, serializerId int32, manifest string, seqNo uint64, ackReplyTo *UniqueAddress) ([]byte, error) {
	env := &SystemMessageEnvelope{
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
	header := make([]byte, 4)
	binary.LittleEndian.PutUint32(header, uint32(len(payload)))

	if _, err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	if _, err := writer.Write(payload); err != nil {
		return fmt.Errorf("failed to write payload: %w", err)
	}
	log.Printf("WriteFrame: wrote %d bytes (header+payload)", 4+len(payload))
	return nil
}

// ArteryDispatcher routes the message once the association is established.
func ArteryDispatcher(ctx context.Context, meta *ArteryMetadata) error {
	switch meta.SerializerId {
	case 1: // Example: Java Serializer
		log.Printf("ArteryDispatcher: routing to Java Serializer (seq: %d)", meta.SeqNo)
	case 2: // Example: JSON Serializer
		log.Printf("ArteryDispatcher: routing to JSON Serializer (seq: %d)", meta.SeqNo)
	default:
		log.Printf("ArteryDispatcher: unrecognized serializerId: %d", meta.SerializerId)
		return fmt.Errorf("unrecognized serializerId: %d", meta.SerializerId)
	}
	return nil
}
