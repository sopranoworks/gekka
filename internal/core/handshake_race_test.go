/*
 * handshake_race_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"net"
	"sync"
	"testing"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// TestHandshakeReqRsp_ConcurrentSymmetricCompletion_NoRace forces
// handleHandshakeReq and handleHandshakeRsp to race on the *same*
// GekkaAssociation ("matched" in both handlers' symmetric-handshake
// completion blocks) — the scenario Pekko/gekka hits whenever two nodes
// dial each other at roughly the same time: node A's INBOUND connection
// concurrently receives peer B's HandshakeReq (processed on the INBOUND
// TCP's read goroutine) while node A's OUTBOUND connection to B
// concurrently receives B's HandshakeRsp (processed on the OUTBOUND TCP's
// own read goroutine). Both handlers find and complete the very same
// OUTBOUND association object.
//
// Run with -race; any unsynchronized read/write of shared
// *GekkaAssociation state touched by both handlers is expected to be
// flagged by the race detector.
func TestHandshakeReqRsp_ConcurrentSymmetricCompletion_NoRace(t *testing.T) {
	nm, _ := newOutboundLanesNodeManager(t, 0)
	nm.FlightRec = NewFlightRecorder(true, LevelLifecycle)

	const iterations = 200
	for i := 0; i < iterations; i++ {
		remoteAddr := &gproto_remote.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("Peer"),
			Hostname: proto.String("127.0.0.1"),
			Port:     proto.Uint32(2552),
		}
		remoteUA := &gproto_remote.UniqueAddress{Address: remoteAddr, Uid: proto.Uint64(0xBEEF)}

		_, outConn := net.Pipe()
		t.Cleanup(func() { _ = outConn.Close() })
		outAssoc := &GekkaAssociation{
			state:     WAITING_FOR_HANDSHAKE,
			role:      OUTBOUND,
			conn:      outConn,
			nodeMgr:   nm,
			Handshake: make(chan struct{}),
			localUid:  nm.localUid,
			outbox:    make(chan []byte, 16),
			streamId:  1,
		}
		outAssoc.remote.Store(remoteUA)
		nm.RegisterAssociation(remoteUA, outAssoc)

		_, inConn := net.Pipe()
		t.Cleanup(func() { _ = inConn.Close() })
		inAssoc := &GekkaAssociation{
			state:     INITIATED,
			role:      INBOUND,
			conn:      inConn,
			nodeMgr:   nm,
			Handshake: make(chan struct{}),
			localUid:  nm.localUid,
			outbox:    make(chan []byte, 16),
			streamId:  1,
		}

		req := &gproto_remote.HandshakeReq{From: remoteUA, To: nm.LocalAddr}
		mwa := &gproto_remote.MessageWithAddress{Address: remoteUA}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = inAssoc.handleHandshakeReq(req)
		}()
		go func() {
			defer wg.Done()
			_ = outAssoc.handleHandshakeRsp(mwa)
		}()
		wg.Wait()
	}
}
