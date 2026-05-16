/*
 * unified_handler_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"context"
	"net"
	"testing"
	"time"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

func TestUnifiedHandler_FullDuplexHandshake(t *testing.T) {
	// Two-pipe topology mirrors production Artery-TCP: each direction has its
	// own TCP connection (sender writes, receiver reads).  Post-2026-05-16,
	// handleHandshakeReq never writes HandshakeRsp back over the INBOUND TCP
	// (Pekko Artery treats inbound TCPs as write-only from the peer's side
	// and quarantines on any "incoming bytes"); responses go via the peer's
	// OUTBOUND.  The single-pipe version of this test relied on the now-
	// removed INBOUND-fallback write and could not represent that contract.
	//
	// pipeAB carries A → B (A's OUTBOUND, B's INBOUND).
	// pipeBA carries B → A (B's OUTBOUND, A's INBOUND).
	nodeAAddr := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("nodeA"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(52551),
	}
	nodeBAddr := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("nodeB"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(52552),
	}

	nmA := NewNodeManager(nodeAAddr, 0)
	nmB := NewNodeManager(nodeBAddr, 0)

	// pipeAB.client is A's OUTBOUND write end; pipeAB.server is B's INBOUND read end.
	pipeABServer, pipeABClient := net.Pipe()
	defer pipeABServer.Close()
	defer pipeABClient.Close()
	// pipeBA.client is B's OUTBOUND write end; pipeBA.server is A's INBOUND read end.
	pipeBAServer, pipeBAClient := net.Pipe()
	defer pipeBAServer.Close()
	defer pipeBAClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// A → B direction: B accepts INBOUND, A initiates OUTBOUND.
	go func() {
		_ = nmB.ProcessConnection(ctx, pipeABServer, INBOUND, nil, 0)
	}()
	go func() {
		_ = nmA.ProcessConnection(ctx, pipeABClient, OUTBOUND, nodeBAddr, 1)
	}()
	// B → A direction: A accepts INBOUND, B initiates OUTBOUND.  This provides
	// the OUTBOUND channel B needs to send HandshakeRsp back to A.
	go func() {
		_ = nmA.ProcessConnection(ctx, pipeBAServer, INBOUND, nil, 0)
	}()
	go func() {
		_ = nmB.ProcessConnection(ctx, pipeBAClient, OUTBOUND, nodeAAddr, 1)
	}()

	// initiateHandshake sleeps 500 ms before sending HandshakeReq, so wait longer.
	time.Sleep(900 * time.Millisecond)

	// NewNodeManager(addr, 0) assigns localUid=0, so HandshakeReq/Rsp carry Uid=0.
	uniqueB := &gproto_remote.UniqueAddress{Address: nodeBAddr, Uid: proto.Uint64(0)}
	assocA, okA := nmA.GetAssociation(uniqueB, 1)
	if !okA || assocA.GetState() != ASSOCIATED {
		t.Errorf("Node A association failed: ok=%v, state=%v", okA, assocA.GetState())
	}

	uniqueA := &gproto_remote.UniqueAddress{Address: nodeAAddr, Uid: proto.Uint64(0)}
	assocB, okB := nmB.GetAssociation(uniqueA, 1)
	if !okB || assocB.GetState() != ASSOCIATED {
		t.Errorf("Node B association failed: ok=%v, state=%v", okB, assocB.GetState())
	}
}

func TestUnifiedHandler_RegistryReuse(t *testing.T) {
	nodeAAddr := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("nodeA"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	nm := NewNodeManager(nodeAAddr, 0)

	// Pre-register an association
	dummyConn, _ := net.Pipe()
	defer dummyConn.Close()

	remoteUnique := &gproto_remote.UniqueAddress{
		Address: &gproto_remote.Address{
			Hostname: proto.String("10.0.0.1"),
			Port:     proto.Uint32(2552),
		},
		Uid: proto.Uint64(123),
	}

	assoc := &GekkaAssociation{
		state:    ASSOCIATED,
		conn:     dummyConn,
		streamId: 1,
	}
	nm.RegisterAssociation(remoteUnique, assoc)

	// Verify lookup
	found, ok := nm.GetAssociation(remoteUnique, 1)
	if !ok || found != assoc {
		t.Errorf("Registry lookup failed")
	}
}
