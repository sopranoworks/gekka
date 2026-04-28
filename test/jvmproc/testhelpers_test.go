/*
 * testhelpers_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package jvmproc

import (
	"net"
	"testing"
	"time"
)

// pickFreePort asks the kernel for an ephemeral TCP port and returns it
// after closing the listener.  There is an unavoidable race window — by
// the time the caller binds, the port could be taken — but the window is
// small enough that this pattern is the standard Go idiom for tests.
func pickFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pickFreePort: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// dial is a tiny wrapper around net.Dial with a short timeout so the
// port-release test isn't held up by a hung listener.
func dial(addr string) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, 200*time.Millisecond)
}
