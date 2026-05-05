/*
 * outbound_lanes_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

func dispatchHashRecipientItoa(i int) string { return strconv.Itoa(i) }

// preambleAcceptingListener accepts TCP connections, reads the 5-byte
// Artery preamble, and records (conn, observed-streamId) for assertions.
// Subsequent bytes are drained but not parsed.
type preambleAcceptingListener struct {
	ln                 net.Listener
	t                  *testing.T
	mu                 sync.Mutex
	accepted           []net.Conn
	streams            []byte // observed streamId byte per accepted conn
	drainAfterPreamble bool
	wg                 sync.WaitGroup
}

func newPreambleAcceptingListener(t *testing.T) *preambleAcceptingListener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	pl := &preambleAcceptingListener{ln: ln, t: t, drainAfterPreamble: true}
	pl.wg.Add(1)
	go pl.run()
	t.Cleanup(func() {
		_ = ln.Close()
		pl.mu.Lock()
		for _, c := range pl.accepted {
			_ = c.Close()
		}
		pl.mu.Unlock()
		pl.wg.Wait()
	})
	return pl
}

func (pl *preambleAcceptingListener) run() {
	defer pl.wg.Done()
	for {
		c, err := pl.ln.Accept()
		if err != nil {
			return
		}
		pl.wg.Add(1)
		go func(c net.Conn) {
			defer pl.wg.Done()
			preamble := make([]byte, 5)
			if _, err := io.ReadFull(c, preamble); err != nil {
				_ = c.Close()
				return
			}
			pl.mu.Lock()
			pl.accepted = append(pl.accepted, c)
			pl.streams = append(pl.streams, preamble[4])
			drain := pl.drainAfterPreamble
			pl.mu.Unlock()
			if !drain {
				return
			}
			// Drain any subsequent bytes (HandshakeReq frames) so the
			// conn doesn't error. Drain mode is opt-in; tests that
			// inspect post-preamble bytes turn it off.
			buf := make([]byte, 4096)
			for {
				if _, err := c.Read(buf); err != nil {
					return
				}
			}
		}(c)
	}
}

func (pl *preambleAcceptingListener) addr() string { return pl.ln.Addr().String() }

func (pl *preambleAcceptingListener) waitFor(n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pl.mu.Lock()
		got := len(pl.accepted)
		pl.mu.Unlock()
		if got >= n {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func (pl *preambleAcceptingListener) snapshot() ([]net.Conn, []byte) {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	c := make([]net.Conn, len(pl.accepted))
	copy(c, pl.accepted)
	s := make([]byte, len(pl.streams))
	copy(s, pl.streams)
	return c, s
}

func newOutboundLanesNodeManager(t *testing.T, lanes int) (*NodeManager, *gproto_remote.Address) {
	t.Helper()
	nm := NewNodeManager(&gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Test"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(0),
	}, 1)
	if lanes > 0 {
		nm.OutboundLanes = lanes
	}
	nm.FlightRec = NewFlightRecorder(true, LevelLifecycle)
	return nm, nm.LocalAddr
}

// TestOutboundLanes_DialsNTcpConnectionsForStreamOrdinary asserts that
// DialRemoteOrdinaryLanes opens N parallel TCP connections to the target
// when OutboundLanes = 4, each carrying the streamId=2 preamble.
func TestOutboundLanes_DialsNTcpConnectionsForStreamOrdinary(t *testing.T) {
	pl := newPreambleAcceptingListener(t)
	nm, _ := newOutboundLanesNodeManager(t, 4)

	host, portStr, _ := net.SplitHostPort(pl.addr())
	var port uint32
	for _, c := range portStr {
		port = port*10 + uint32(c-'0')
	}
	target := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Peer"),
		Hostname: proto.String(host),
		Port:     proto.Uint32(port),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conns, err := nm.DialRemoteOrdinaryLanes(ctx, target)
	if err != nil {
		t.Fatalf("DialRemoteOrdinaryLanes: %v", err)
	}
	t.Cleanup(func() {
		for _, c := range conns {
			if c != nil {
				_ = c.Close()
			}
		}
	})
	if len(conns) != 4 {
		t.Fatalf("conns len = %d, want 4", len(conns))
	}
	for i, c := range conns {
		if c == nil {
			t.Fatalf("conns[%d] is nil", i)
		}
	}
	if !pl.waitFor(4, 1*time.Second) {
		t.Fatalf("listener did not observe 4 accepts within 1s")
	}
	_, streams := pl.snapshot()
	for i, sid := range streams {
		if sid != 2 {
			t.Errorf("accept[%d] streamId = %d, want 2", i, sid)
		}
	}
}

// TestOutboundLanes_DefaultIsOneSingleConn asserts that with OutboundLanes
// unset (default 1), DialRemoteOrdinaryLanes opens exactly one ordinary
// TCP. This matches Pekko's default outbound-lanes=1 which still opens a
// dedicated streamId=2 socket separate from control.
func TestOutboundLanes_DefaultIsOneSingleConn(t *testing.T) {
	pl := newPreambleAcceptingListener(t)
	nm, _ := newOutboundLanesNodeManager(t, 0) // default

	host, portStr, _ := net.SplitHostPort(pl.addr())
	var port uint32
	for _, c := range portStr {
		port = port*10 + uint32(c-'0')
	}
	target := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Peer"),
		Hostname: proto.String(host),
		Port:     proto.Uint32(port),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conns, err := nm.DialRemoteOrdinaryLanes(ctx, target)
	if err != nil {
		t.Fatalf("DialRemoteOrdinaryLanes: %v", err)
	}
	t.Cleanup(func() {
		for _, c := range conns {
			if c != nil {
				_ = c.Close()
			}
		}
	})
	if len(conns) != 1 {
		t.Fatalf("conns len = %d, want 1 (default outbound-lanes=1)", len(conns))
	}
	if !pl.waitFor(1, 1*time.Second) {
		t.Fatalf("listener did not observe 1 accept")
	}
	_, streams := pl.snapshot()
	if len(streams) != 1 || streams[0] != 2 {
		t.Errorf("streams = %v, want [2]", streams)
	}
}

// makeControlAssoc constructs an OUTBOUND streamId=1 control association
// in ASSOCIATED state with a fixed remote, suitable for driving
// EnsureOrdinarySibling without going through a real handshake. The
// supplied conn becomes assoc.conn (control channel placeholder).
func makeControlAssoc(t *testing.T, nm *NodeManager, remoteAddr *gproto_remote.Address, remoteUid uint64) (*GekkaAssociation, net.Conn, net.Conn) {
	t.Helper()
	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	remote := &gproto_remote.UniqueAddress{Address: remoteAddr, Uid: proto.Uint64(remoteUid)}
	assoc := &GekkaAssociation{
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
	close(assoc.Handshake)
	nm.RegisterAssociation(remote, assoc)
	return assoc, server, client
}

// TestOutboundLanes_PerLaneHandshake asserts EnsureOrdinarySibling
// constructs N lanes, each in ASSOCIATED state with a closed
// handshakeDone channel, and registers the streamId=2 sibling under the
// composite (host:port, uid, streamId=2) registry key.
func TestOutboundLanes_PerLaneHandshake(t *testing.T) {
	pl := newPreambleAcceptingListener(t)
	nm, _ := newOutboundLanesNodeManager(t, 3)

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

	control, _, _ := makeControlAssoc(t, nm, remoteAddr, 99999)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := nm.EnsureOrdinarySibling(ctx, control); err != nil {
		t.Fatalf("EnsureOrdinarySibling: %v", err)
	}

	// Sibling must be linked.
	control.mu.RLock()
	sib := control.ordinarySibling
	control.mu.RUnlock()
	if sib == nil {
		t.Fatalf("ordinarySibling not set on control assoc")
	}
	if got := len(sib.lanes); got != 3 {
		t.Fatalf("sibling lanes len = %d, want 3", got)
	}
	if sib.streamId != 2 {
		t.Fatalf("sibling streamId = %d, want 2", sib.streamId)
	}

	// Each lane reaches ASSOCIATED with handshakeDone closed (writes are
	// posted synchronously by EnsureOrdinarySibling; small wait for the
	// retry goroutine to observe the state transition).
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for i, lane := range sib.lanes {
			if lane.state != ASSOCIATED {
				ok = false
				break
			}
			select {
			case <-lane.handshakeDone:
			default:
				ok = false
				_ = i
			}
		}
		if ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	for i, lane := range sib.lanes {
		if lane.state != ASSOCIATED {
			t.Errorf("lane[%d].state = %v, want ASSOCIATED", i, lane.state)
		}
		select {
		case <-lane.handshakeDone:
		default:
			t.Errorf("lane[%d].handshakeDone not closed", i)
		}
	}

	// Sibling registered under (host:port, uid, streamId=2).
	if reg, ok := nm.GetAssociation(&gproto_remote.UniqueAddress{
		Address: remoteAddr,
		Uid:     proto.Uint64(99999),
	}, 2); !ok || reg != sib {
		t.Errorf("sibling not registered at streamId=2, got=%v ok=%v", reg, ok)
	}

	// Reverse sibling pointer.
	sib.mu.RLock()
	rev := sib.ordinarySibling
	sib.mu.RUnlock()
	if rev != control {
		t.Errorf("sibling.ordinarySibling = %p, want control %p", rev, control)
	}

	// Idempotent: a second call must be a no-op.
	if err := nm.EnsureOrdinarySibling(ctx, control); err != nil {
		t.Fatalf("EnsureOrdinarySibling (2nd call): %v", err)
	}
	control.mu.RLock()
	sib2 := control.ordinarySibling
	control.mu.RUnlock()
	if sib2 != sib {
		t.Errorf("idempotency violated: sibling changed from %p to %p", sib, sib2)
	}
}

// TestOutboundLanes_DispatchHashesByRecipient asserts that outboxFor on a
// control assoc with an ordinary sibling routes user-message frames onto
// distinct lane outboxes hashed by recipient path, while cluster /
// artery-internal traffic stays on the control outbox.
func TestOutboundLanes_DispatchHashesByRecipient(t *testing.T) {
	const N = 4
	// Pin two recipients that hash to different lanes via laneIndex/N.
	var rA, rB string
	for i := 0; i < 1000; i++ {
		a := "/user/recipientA-" + dispatchHashRecipientItoa(i)
		b := "/user/recipientB-" + dispatchHashRecipientItoa(i)
		if laneIndex(a, N) != laneIndex(b, N) {
			rA, rB = a, b
			break
		}
	}
	if rA == "" || rB == "" {
		t.Fatalf("could not find recipient strings hashing to distinct lanes")
	}

	nm, _ := newOutboundLanesNodeManager(t, N)
	ctrlS, ctrlC := net.Pipe()
	t.Cleanup(func() { _ = ctrlS.Close(); _ = ctrlC.Close() })
	remote := &gproto_remote.UniqueAddress{
		Address: &gproto_remote.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("Peer"),
			Hostname: proto.String("127.0.0.1"),
			Port:     proto.Uint32(2552),
		},
		Uid: proto.Uint64(7777),
	}
	control := &GekkaAssociation{
		state:     ASSOCIATED,
		role:      OUTBOUND,
		conn:      ctrlC,
		nodeMgr:   nm,
		Handshake: make(chan struct{}),
		localUid:  nm.localUid,
		outbox:    make(chan []byte, 16),
		remote:    remote,
		streamId:  1,
	}
	close(control.Handshake)

	// Build sibling with N lanes (no real conns — direct-mode unit test).
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
	control.ordinarySibling = sib
	sib.ordinarySibling = control

	// User serializer (anything not Cluster/ArteryInternal) routes onto
	// the lanes via outboxFor.
	const userSerializer = int32(4) // ByteArraySerializer
	chA := control.outboxFor(rA, userSerializer)
	chB := control.outboxFor(rB, userSerializer)
	expA := lanes[laneIndex(rA, N)].outbox
	expB := lanes[laneIndex(rB, N)].outbox
	if chA != expA {
		t.Errorf("outboxFor(rA) routed to wrong lane")
	}
	if chB != expB {
		t.Errorf("outboxFor(rB) routed to wrong lane")
	}
	if chA == chB {
		t.Errorf("rA and rB hashed to same lane (test pin failed)")
	}

	// Cluster + ArteryInternal stay on the control outbox.
	if got := control.outboxFor(rA, ClusterSerializerID); got != control.outbox {
		t.Errorf("ClusterSerializerID did not stay on control outbox")
	}
	if got := control.outboxFor(rA, ArteryInternalSerializerID); got != control.outbox {
		t.Errorf("ArteryInternalSerializerID did not stay on control outbox")
	}
}

// TestOutboundLanes_QuarantineTearsDownAllLanes verifies that closing a
// streamId=2 sibling's lane conns (the production teardown path called by
// the existing quarantine logic) propagates to every lane writer
// goroutine, draining all N+1 TCP conns (control + N ordinary) on the
// peer side.
func TestOutboundLanes_QuarantineTearsDownAllLanes(t *testing.T) {
	const N = 3
	pl := newPreambleAcceptingListener(t)
	nm, _ := newOutboundLanesNodeManager(t, N)

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

	control, _, _ := makeControlAssoc(t, nm, remoteAddr, 555)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := nm.EnsureOrdinarySibling(ctx, control); err != nil {
		t.Fatalf("EnsureOrdinarySibling: %v", err)
	}
	control.mu.RLock()
	sib := control.ordinarySibling
	control.mu.RUnlock()

	if !pl.waitFor(N, 1*time.Second) {
		t.Fatalf("peer did not see N=%d accepts", N)
	}

	// Tear down: close all lane conns (the production quarantine path
	// closes assoc.conn and lane.conn, which causes lane writer goroutines
	// to exit on WriteFrame error and the read loop on EOF).
	for _, lane := range sib.lanes {
		_ = lane.conn.Close()
	}

	// All peer-side conns should observe close (read returns EOF).
	peerConns, _ := pl.snapshot()
	for i, c := range peerConns {
		_ = c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		buf := make([]byte, 1)
		_, err := c.Read(buf)
		if err == nil {
			t.Errorf("peer conn[%d] did not observe close", i)
		}
	}
}
