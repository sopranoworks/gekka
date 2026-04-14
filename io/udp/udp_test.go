// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

package udp_test

import (
	"net"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/io/udp"
)

func TestSimpleSender(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPacket: %v", err)
	}
	defer pc.Close()

	sender, err := udp.NewSimpleSender()
	if err != nil {
		t.Fatalf("NewSimpleSender: %v", err)
	}
	defer sender.Close()

	dst := pc.LocalAddr().(*net.UDPAddr)
	if err := sender.Send(dst, []byte("hello-udp")); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Read with timeout.
	if err := pc.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	buf := make([]byte, 128)
	n, _, err := pc.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if got := string(buf[:n]); got != "hello-udp" {
		t.Errorf("got %q, want %q", got, "hello-udp")
	}
}

func TestBind(t *testing.T) {
	// Bind a UDP server that echoes every packet back to its sender.
	binding, err := udp.Bind("127.0.0.1:0", func(msg udp.Received) {
		_ = msg.Conn.Send(msg.Sender, msg.Data)
	})
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	defer binding.Unbind()

	// Client socket used to send and receive the echo.
	clientConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero})
	if err != nil {
		t.Fatalf("ListenUDP client: %v", err)
	}
	defer clientConn.Close()

	// Send "ping" to the binding.
	if _, err := clientConn.WriteToUDP([]byte("ping"), binding.LocalAddr()); err != nil {
		t.Fatalf("WriteToUDP: %v", err)
	}

	// Receive the echo with a 2s timeout.
	if err := clientConn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	buf := make([]byte, 128)
	n, _, err := clientConn.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("ReadFromUDP echo: %v", err)
	}
	if got := string(buf[:n]); got != "ping" {
		t.Errorf("got %q, want %q", got, "ping")
	}
}
