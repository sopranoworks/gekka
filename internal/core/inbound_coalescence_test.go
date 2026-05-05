/*
 * inbound_coalescence_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"net"
	"testing"
	"time"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// makeInboundStream2Assoc builds an INBOUND streamId=2 GekkaAssociation
// suitable for driving handleHandshakeReq directly. The conn becomes
// assoc.conn so inboundLaneIndexOf can locate it within the primary's
// inboundConns slice.
func makeInboundStream2Assoc(t *testing.T, nm *NodeManager, remoteAddr *gproto_remote.Address) (*GekkaAssociation, net.Conn) {
	t.Helper()
	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	assoc := &GekkaAssociation{
		state:     INITIATED,
		role:      INBOUND,
		conn:      client,
		nodeMgr:   nm,
		Handshake: make(chan struct{}),
		localUid:  nm.localUid,
		outbox:    make(chan []byte, 16),
		remote:    &gproto_remote.UniqueAddress{Address: remoteAddr, Uid: proto.Uint64(0)},
		streamId:  2,
	}
	return assoc, client
}

// preregisterOutboundControl registers an OUTBOUND streamId=1 control assoc
// so handleHandshakeReq's "no OUTBOUND, dial reverse" path is skipped (the
// test focuses on coalescence + HandshakeRsp routing, not the seed-dial
// fallback).
func preregisterOutboundControl(t *testing.T, nm *NodeManager, remoteAddr *gproto_remote.Address, remoteUid uint64) *GekkaAssociation {
	t.Helper()
	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	remote := &gproto_remote.UniqueAddress{Address: remoteAddr, Uid: proto.Uint64(remoteUid)}
	control := &GekkaAssociation{
		state:     ASSOCIATED,
		role:      OUTBOUND,
		conn:      client,
		nodeMgr:   nm,
		Handshake: make(chan struct{}),
		localUid:  nm.localUid,
		outbox:    make(chan []byte, 16),
		remote:    remote,
		streamId:  1,
	}
	close(control.Handshake)
	nm.RegisterAssociation(remote, control)
	return control
}

// TestInboundCoalescence_MultiTcpFromSamePeerSharesAssociation verifies
// that N inbound streamId=2 TCPs from the same peer UID coalesce into a
// single logical *GekkaAssociation*: the first becomes the primary and is
// registered in the node manager; the 2nd...N-th attach as additional
// entries in the primary's inboundConns slice and set their delegate to
// the primary so dispatch flows through the primary's lane fan-out.
func TestInboundCoalescence_MultiTcpFromSamePeerSharesAssociation(t *testing.T) {
	const N = 3
	nm, _ := newOutboundLanesNodeManager(t, 1)

	remoteAddr := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Peer"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	const remoteUid = uint64(0xCAFE)
	preregisterOutboundControl(t, nm, remoteAddr, remoteUid)

	from := &gproto_remote.UniqueAddress{Address: remoteAddr, Uid: proto.Uint64(remoteUid)}
	req := &gproto_remote.HandshakeReq{From: from, To: nm.LocalAddr}

	assocs := make([]*GekkaAssociation, N)
	for i := 0; i < N; i++ {
		a, _ := makeInboundStream2Assoc(t, nm, remoteAddr)
		assocs[i] = a
		if err := a.handleHandshakeReq(req); err != nil {
			t.Fatalf("handleHandshakeReq[%d]: %v", i, err)
		}
	}

	primary, ok := nm.GetAssociation(from, 2)
	if !ok {
		t.Fatalf("primary streamId=2 assoc not registered")
	}
	if primary != assocs[0] {
		t.Errorf("primary mismatch: got %p, want %p", primary, assocs[0])
	}
	primary.mu.RLock()
	got := len(primary.inboundConns)
	primary.mu.RUnlock()
	if got != N {
		t.Errorf("primary.inboundConns len = %d, want %d", got, N)
	}

	for i := 1; i < N; i++ {
		assocs[i].mu.RLock()
		d := assocs[i].delegate
		assocs[i].mu.RUnlock()
		if d != primary {
			t.Errorf("assocs[%d].delegate = %p, want primary %p", i, d, primary)
		}
	}

	// Primary was registered (initial assoc); subsequent assocs MUST NOT
	// be registered, otherwise the registry's last-write-wins overwrite
	// would erase the primary.
	if reg, _ := nm.GetAssociation(from, 2); reg != primary {
		t.Errorf("registry overwritten: got %p, want primary %p", reg, primary)
	}
}

// TestOutboundLanes_HandshakeRspRoutesToCorrectLane verifies the lane-
// aware HandshakeRsp routing rule: when the inbound HandshakeReq arrives
// on the L-th coalesced inbound conn of a streamId=2 INBOUND association,
// the response is written on the OUTBOUND ordinary sibling's lane[L]
// outbox (matching the source lane index). When the sibling is not yet
// registered or L >= len(lanes), the response falls back to the
// streamId=1 control outbox.
func TestOutboundLanes_HandshakeRspRoutesToCorrectLane(t *testing.T) {
	const N = 3
	nm, _ := newOutboundLanesNodeManager(t, N)

	remoteAddr := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Peer"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	const remoteUid = uint64(0xBEEF)
	control := preregisterOutboundControl(t, nm, remoteAddr, remoteUid)

	// Build OUTBOUND streamId=2 sibling with N lanes (no real conns —
	// outboxes are inspected directly).
	lanes := make([]*outboundLane, N)
	for i := 0; i < N; i++ {
		lanes[i] = &outboundLane{
			idx:           i,
			outbox:        make(chan []byte, 4),
			state:         ASSOCIATED,
			handshakeDone: make(chan struct{}),
		}
		close(lanes[i].handshakeDone)
	}
	remote := &gproto_remote.UniqueAddress{Address: remoteAddr, Uid: proto.Uint64(remoteUid)}
	sib := &GekkaAssociation{
		state:     ASSOCIATED,
		role:      OUTBOUND,
		nodeMgr:   nm,
		Handshake: make(chan struct{}),
		localUid:  nm.localUid,
		outbox:    make(chan []byte, 1),
		remote:    remote,
		streamId:  2,
		lanes:     lanes,
	}
	close(sib.Handshake)
	control.mu.Lock()
	control.ordinarySibling = sib
	control.mu.Unlock()
	sib.mu.Lock()
	sib.ordinarySibling = control
	sib.mu.Unlock()
	nm.RegisterAssociation(remote, sib)

	// Open N INBOUND streamId=2 TCPs sequentially. Each runs
	// handleHandshakeReq, coalesces onto the OUTBOUND sibling, and writes
	// a HandshakeRsp onto the corresponding outbound lane. We assert per
	// shadow: response landed on lane[i] and not on any other lane (the
	// other lanes' outboxes must be empty between shadows).
	from := &gproto_remote.UniqueAddress{Address: remoteAddr, Uid: proto.Uint64(remoteUid)}
	req := &gproto_remote.HandshakeReq{From: from, To: nm.LocalAddr}
	shadows := make([]*GekkaAssociation, N)
	for i := 0; i < N; i++ {
		s, _ := makeInboundStream2Assoc(t, nm, remoteAddr)
		shadows[i] = s
		if err := s.handleHandshakeReq(req); err != nil {
			t.Fatalf("shadow[%d] handleHandshakeReq: %v", i, err)
		}
		idx := s.inboundLaneIndexOf()
		if idx != i {
			t.Fatalf("shadow[%d] lane index = %d, want %d", i, idx, i)
		}
		select {
		case <-lanes[i].outbox:
			// OK — HandshakeRsp landed on lane[i].
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("shadow[%d] HandshakeRsp did not land on lane[%d]", i, i)
		}
		// Other lanes must NOT have received this rsp.
		for j, ln := range lanes {
			if j == i {
				continue
			}
			select {
			case <-ln.outbox:
				t.Fatalf("shadow[%d] HandshakeRsp leaked to lane[%d]", i, j)
			default:
			}
		}
	}
	sib.mu.RLock()
	if got := len(sib.inboundConns); got != N {
		t.Fatalf("sib.inboundConns len = %d, want %d", got, N)
	}
	sib.mu.RUnlock()

	// Fallback: if we manually clear the sibling pointer, response goes
	// to control.outbox.
	control.mu.Lock()
	control.ordinarySibling = nil
	control.mu.Unlock()
	fallback, _ := makeInboundStream2Assoc(t, nm, remoteAddr)
	if err := fallback.handleHandshakeReq(req); err != nil {
		t.Fatalf("fallback handleHandshakeReq: %v", err)
	}
	select {
	case <-control.outbox:
		// OK
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("fallback HandshakeRsp did not land on control outbox")
	}
}
