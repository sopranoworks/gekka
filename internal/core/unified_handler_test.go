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
	nodeAAddr := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("nodeA"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	nodeBAddr := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("nodeB"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2553),
	}

	nmA := NewNodeManager(nodeAAddr, 0)
	nmB := NewNodeManager(nodeBAddr, 0)

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Node B accepts connection (Inbound)
	go func() {
		_ = nmB.ProcessConnection(ctx, server, INBOUND, nil, 0)
	}()

	// Node A initiates connection (Outbound)
	errChan := make(chan error, 1)
	go func() {
		errChan <- nmA.ProcessConnection(ctx, client, OUTBOUND, nodeBAddr, 1) // Use 1 for test
	}()

	// initiateHandshake sleeps 500 ms before sending HandshakeReq, so wait longer.
	time.Sleep(700 * time.Millisecond)

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
