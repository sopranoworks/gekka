/*
 * artery_heartbeat_sender_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"net"
	"strings"
	"testing"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// TestArteryHeartbeat_SetsLocalRemoteWatcherSender pins the regression for
// the deadletter flood observed in the full multi-jvm gate after f69bed6.
// When gekka sends an ArteryHeartbeat without a sender path, Akka's
// RemoteWatcher resolves the sender to akka://<sys>/deadLetters and its
// `sender ! ArteryHeartbeatRsp` send routes to /deadLetters on the Akka
// side, generating a continuous WARN log:
//
//   received dead letter from Actor[akka://.../system/remote-watcher]:
//   ArteryHeartbeatRsp(<uid>)
//
// At ~30 warnings/second the Akka logger thread starves the ProcessLogger
// callback that the multi-jvm GekkaCompatSpec relies on to append to its
// goLogs buffer, causing the test's `awaitAssert(goLogs.exists("STEP_2"))`
// to time out even when the marker was written by the Go binary.
//
// Fix: set the senderPath argument on the outgoing ArteryHeartbeat frame to
// gekka's own /system/remote-watcher path so Akka's reply finds a real
// recipient (gekka's own remote-watcher) and travels back on the wire
// rather than being dead-lettered locally.
func TestArteryHeartbeat_SetsLocalRemoteWatcherSender(t *testing.T) {
	nm, _ := newOutboundLanesNodeManager(t, 1)
	nm.LocalAddr = &gproto_remote.Address{
		Protocol: proto.String("akka"),
		System:   proto.String("GekkaSystem"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}

	remoteAddr := &gproto_remote.Address{
		Protocol: proto.String("akka"),
		System:   proto.String("GekkaSystem"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2551),
	}

	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	outbound := &GekkaAssociation{
		state:     ASSOCIATED,
		role:      OUTBOUND,
		conn:      client,
		nodeMgr:   nm,
		Handshake: make(chan struct{}),
		localUid:  nm.localUid,
		outbox:    make(chan []byte, 16),
		streamId:  1,
	}
	outbound.remote.Store(&gproto_remote.UniqueAddress{
		Address: remoteAddr,
		Uid:     proto.Uint64(0xDEAD),
	})
	close(outbound.Handshake)

	if err := SendArteryHeartbeat(outbound); err != nil {
		t.Fatalf("SendArteryHeartbeat: %v", err)
	}

	select {
	case frame := <-outbound.outbox:
		meta, err := ParseArteryFrame(frame, nil, 0)
		if err != nil {
			t.Fatalf("ParseArteryFrame: %v", err)
		}
		senderPath := ""
		if meta.Sender != nil {
			senderPath = meta.Sender.GetPath()
		}
		// Sender path must reference gekka's own /system/remote-watcher so
		// Akka's reply lands on a real recipient instead of /deadLetters.
		wantSubstr := "/system/remote-watcher"
		if !strings.Contains(senderPath, wantSubstr) {
			t.Errorf("ArteryHeartbeat sender path = %q, want substring %q — empty sender causes Akka deadletter flood",
				senderPath, wantSubstr)
		}
		// Must reference gekka's own address (LocalAddr), not the peer's.
		if !strings.Contains(senderPath, "127.0.0.1:2552") {
			t.Errorf("ArteryHeartbeat sender path = %q, want gekka's own host:port (127.0.0.1:2552)", senderPath)
		}
	default:
		t.Fatal("ArteryHeartbeat frame not queued on outbox")
	}
}
