/*
 * outbound_lanes_multiconn_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

func itoaLocal(i int) string { return strconv.Itoa(i) }

// TestOutboundLanes_KernelBackpressureIsolatedAcrossLanes verifies that
// saturating one outbound lane does not block traffic on a different
// outbound lane. With OutboundLanes=2 and a peer that pauses reading on
// lane 0, frames hashed to lane 1 must continue to drain.
func TestOutboundLanes_KernelBackpressureIsolatedAcrossLanes(t *testing.T) {
	const N = 2
	pl := newPreambleAcceptingListener(t)
	pl.mu.Lock()
	pl.drainAfterPreamble = false
	pl.mu.Unlock()
	nm, _ := newOutboundLanesNodeManager(t, N)
	// Tiny outbox capacity so saturation happens quickly.
	nm.OutboundMessageQueueSize = 4

	host, portStr, _ := net.SplitHostPort(pl.addr())
	var port uint32
	for _, c := range portStr {
		port = port*10 + uint32(c-'0')
	}
	remoteAddr := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Peer"),
		Hostname: proto.String(host),
		Port:     proto.Uint32(port),
	}
	control, _, _ := makeControlAssoc(t, nm, remoteAddr, 1234)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := nm.EnsureOrdinarySibling(ctx, control); err != nil {
		t.Fatalf("EnsureOrdinarySibling: %v", err)
	}
	control.mu.RLock()
	sib := control.ordinarySibling
	control.mu.RUnlock()
	if !pl.waitFor(N, 1*time.Second) {
		t.Fatalf("peer did not observe N=%d accepts", N)
	}

	// Pin two recipients to distinct lanes.
	var rLane0, rLane1 string
	for i := 0; i < 1000; i++ {
		s := "/user/recipient-" + itoaLocal(i)
		idx := laneIndex(s, N)
		if idx == 0 && rLane0 == "" {
			rLane0 = s
		}
		if idx == 1 && rLane1 == "" {
			rLane1 = s
		}
		if rLane0 != "" && rLane1 != "" {
			break
		}
	}
	if rLane0 == "" || rLane1 == "" {
		t.Fatalf("could not pin recipients for both lanes")
	}

	// Don't read from peerConns[0] — kernel send buffer on lane 0 will
	// fill, then the lane.outbox will fill, then sends to that lane will
	// fail (TrySendOnLane returns ErrFull). Lane 1 stays unaffected.
	_ = sib

	// Send many frames to lane 0 — saturate it. We don't care if they
	// all land; the goal is to make lane 0 write-blocked.
	saturatePayload := make([]byte, 8192)
	for i := 0; i < 200; i++ {
		select {
		case sib.lanes[0].outbox <- saturatePayload:
		default:
			// outbox full — saturated.
			break
		}
	}

	// Send a small frame to lane 1 with a recognizable marker.
	marker := []byte{0xAB, 0xCD, 0xEF, 0x01}
	frame := append([]byte{0, 0, 0, byte(len(marker))}, marker...) // 4-byte LE length prefix is what WriteFrame produces; we use raw form
	// Use WriteFrame-equivalent: prepend 4-byte LE length.
	out := make([]byte, 4+len(marker))
	binary.LittleEndian.PutUint32(out[:4], uint32(len(marker)))
	copy(out[4:], marker)
	select {
	case sib.lanes[1].outbox <- marker:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("lane 1 outbox blocked despite lane 0 saturation")
	}
	_ = frame

	// Match peer-side conns to lane indices via source port. Accepts can
	// land in any order on the listener, so peerConns[i] does not
	// necessarily correspond to lane i. lane.conn.LocalAddr() (gekka
	// side) == peerConn.RemoteAddr() for the matching peer.
	peerConns, _ := pl.snapshot()
	if len(peerConns) < 2 {
		t.Fatalf("peer conns < 2: %d", len(peerConns))
	}
	peerForLane := make(map[int]net.Conn)
	for laneIdx, lane := range sib.lanes {
		want := lane.conn.LocalAddr().String()
		for _, p := range peerConns {
			if p.RemoteAddr().String() == want {
				peerForLane[laneIdx] = p
				break
			}
		}
	}
	if peerForLane[0] == nil || peerForLane[1] == nil {
		t.Fatalf("could not match peer conns to lanes: %v", peerForLane)
	}

	// Skip the initial HandshakeReq frame written by ProcessConnectionLane
	// then read the next frame, which is our marker.
	_ = peerForLane[1].SetReadDeadline(time.Now().Add(1 * time.Second))
	hdr := make([]byte, 4)
	for {
		if _, err := io.ReadFull(peerForLane[1], hdr); err != nil {
			t.Fatalf("lane-1 peer read header: %v", err)
		}
		plen := binary.LittleEndian.Uint32(hdr)
		body := make([]byte, plen)
		if _, err := io.ReadFull(peerForLane[1], body); err != nil {
			t.Fatalf("lane-1 peer read body: %v", err)
		}
		if int(plen) == len(marker) {
			match := true
			for i, b := range marker {
				if body[i] != b {
					match = false
					break
				}
			}
			if match {
				return
			}
		}
		// Not the marker — keep reading (must be a stray handshake frame).
	}
}
