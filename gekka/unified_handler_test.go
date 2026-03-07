/*
 * unified_handler_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

func TestUnifiedHandler_FullDuplexHandshake(t *testing.T) {
	nodeAAddr := &Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("nodeA"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	nodeBAddr := &Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("nodeB"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2553),
	}

	nmA := NewNodeManager(nodeAAddr)
	nmB := NewNodeManager(nodeBAddr)

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Node B accepts connection (Inbound)
	go func() {
		_ = nmB.ProcessConnection(ctx, server, INBOUND, nil)
	}()

	// Node A initiates connection (Outbound)
	errChan := make(chan error, 1)
	go func() {
		errChan <- nmA.ProcessConnection(ctx, client, OUTBOUND, nodeBAddr)
	}()

	// Wait for associations to be established
	time.Sleep(200 * time.Millisecond)

	uniqueB := &UniqueAddress{Address: nodeBAddr, Uid: proto.Uint64(1)}
	assocA, okA := nmA.GetAssociation(uniqueB)
	if !okA || assocA.GetState() != ASSOCIATED {
		t.Errorf("Node A association failed: ok=%v, state=%v", okA, assocA.GetState())
	}

	uniqueA := &UniqueAddress{Address: nodeAAddr, Uid: proto.Uint64(1)}
	assocB, okB := nmB.GetAssociation(uniqueA)
	if !okB || assocB.GetState() != ASSOCIATED {
		t.Errorf("Node B association failed: ok=%v, state=%v", okB, assocB.GetState())
	}
}

func TestUnifiedHandler_RegistryReuse(t *testing.T) {
	nodeAAddr := &Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("nodeA"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	nm := NewNodeManager(nodeAAddr)

	// Pre-register an association
	dummyConn, _ := net.Pipe()
	defer dummyConn.Close()

	remoteUnique := &UniqueAddress{
		Address: &Address{
			Hostname: proto.String("10.0.0.1"),
			Port:     proto.Uint32(2552),
		},
		Uid: proto.Uint64(123),
	}

	assoc := &GekkaAssociation{
		state: ASSOCIATED,
		conn:  dummyConn,
	}
	nm.RegisterAssociation(remoteUnique, assoc)

	// Verify lookup
	found, ok := nm.GetAssociation(remoteUnique)
	if !ok || found != assoc {
		t.Errorf("Registry lookup failed")
	}
}
