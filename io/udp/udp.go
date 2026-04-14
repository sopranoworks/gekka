// Copyright (c) 2026 Sopranoworks, Osamu Takahashi
// SPDX-License-Identifier: MIT

// Package udp provides a user-facing UDP I/O API mirroring Pekko's Udp.Bind
// and Udp.SimpleSender actors.
package udp

import (
	"net"
	"sync"
)

// Received is delivered to the Bind handler for each incoming UDP packet.
type Received struct {
	Data   []byte
	Sender *net.UDPAddr
	Conn   *Binding
}

// Binding represents an active UDP listener.
type Binding struct {
	conn    *net.UDPConn
	handler func(Received)
	once    sync.Once
}

// Bind opens a UDP socket at addr and calls handler for each received datagram.
// Returns the Binding on success.
func Bind(addr string, handler func(Received)) (*Binding, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	b := &Binding{conn: conn, handler: handler}
	go b.readLoop()
	return b, nil
}

func (b *Binding) readLoop() {
	buf := make([]byte, 65535)
	for {
		n, addr, err := b.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		data := make([]byte, n)
		copy(data, buf[:n])
		b.handler(Received{Data: data, Sender: addr, Conn: b})
	}
}

// Send writes data to dst.
func (b *Binding) Send(dst *net.UDPAddr, data []byte) error {
	_, err := b.conn.WriteToUDP(data, dst)
	return err
}

// LocalAddr returns the resolved local address of the bound socket.
func (b *Binding) LocalAddr() *net.UDPAddr {
	return b.conn.LocalAddr().(*net.UDPAddr)
}

// Unbind closes the socket and stops the read loop. Safe to call multiple times.
func (b *Binding) Unbind() {
	b.once.Do(func() {
		b.conn.Close()
	})
}

// SimpleSender is a fire-and-forget UDP sender (no listening socket).
type SimpleSender struct {
	conn *net.UDPConn
}

// NewSimpleSender creates a SimpleSender bound to an ephemeral local port.
func NewSimpleSender() (*SimpleSender, error) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		return nil, err
	}
	return &SimpleSender{conn: conn}, nil
}

// Send transmits data to dst.
func (s *SimpleSender) Send(dst *net.UDPAddr, data []byte) error {
	_, err := s.conn.WriteToUDP(data, dst)
	return err
}

// Close releases the underlying socket.
func (s *SimpleSender) Close() error {
	return s.conn.Close()
}
