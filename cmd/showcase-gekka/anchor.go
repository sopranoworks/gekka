// cmd/showcase-gekka/anchor.go — steady-state anchor for the showcase
// traffic actors.
//
// Spec §4 scopes the strict Gate-2 log window from Gate 1 PASS: WARN/ERROR
// during setup are "written to per-node artifact logs but not counted".
// Spec §5.4.2 anchors SingletonWarmupGrace at the same instant ("measured
// from Gate 1 PASS"). A child process cannot observe the runner's Gate-1
// poll directly; the per-node equivalent of that instant is the FIRST time
// the local gossip view reports the full expected membership Up — the same
// condition Gate 1 polls over the management endpoint, observed locally.
//
// Traffic itself is NOT gated on the anchor (spec revision-2 note: "DO NOT
// delay or constrain FT1/2/3 ... keeping T0 anchor unchanged") — only the
// ERROR accounting is: a timeout whose SEND predates the anchor belongs to
// the setup phase and is not counted.
//
// SPDX-License-Identifier: MIT
package main

import (
	"sync/atomic"
	"time"

	gekka "github.com/sopranoworks/gekka"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	"github.com/sopranoworks/gekka/logger"
)

// steadyAnchor latches the first instant the local node observed the full
// expected cluster membership Up. Zero until then.
type steadyAnchor struct {
	atNanos atomic.Int64
}

// mark latches the anchor at now; only the first call wins.
func (a *steadyAnchor) mark() {
	a.atNanos.CompareAndSwap(0, time.Now().UnixNano())
}

// at returns the anchor instant and whether it has been latched.
func (a *steadyAnchor) at() (time.Time, bool) {
	ns := a.atNanos.Load()
	if ns == 0 {
		return time.Time{}, false
	}
	return time.Unix(0, ns), true
}

// countsForStrictWindow reports whether an outbound request sent at sentAt
// belongs to the strict Gate-2 accounting window: the anchor is latched and
// the send did not precede it. Pre-anchor sends are setup-phase traffic
// (spec §4) and their timeouts must not ERROR.
func (a *steadyAnchor) countsForStrictWindow(sentAt time.Time) bool {
	t, ok := a.at()
	return ok && !sentAt.Before(t)
}

// watchAnchor polls upCount until it first reaches expected, then latches
// the anchor and returns. Runs on its own goroutine.
func watchAnchor(a *steadyAnchor, upCount func() int, expected int, stop <-chan struct{}) {
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			if upCount() >= expected {
				a.mark()
				at, _ := a.at()
				logger.Default().Info("showcase: steady-state anchor latched",
					"membersUp", expected, "at", at.Format(time.RFC3339Nano))
				return
			}
		}
	}
}

// clusterUpCount returns a closure counting Up members in the local gossip
// view.
func clusterUpCount(c *gekka.Cluster) func() int {
	cm := c.ClusterManager()
	return func() int {
		cm.Mu.RLock()
		defer cm.Mu.RUnlock()
		n := 0
		for _, m := range cm.State.Members {
			if m.GetStatus() == gproto_cluster.MemberStatus_Up {
				n++
			}
		}
		return n
	}
}

// classifyPingTimeout decides whether a failed/unanswered singleton ping is
// ERROR-worthy under spec §4 + §5.4.1 + §5.4.2:
//
//   - sends predating the anchor are setup-phase → silent (§4);
//   - a ping sent AFTER its role established is a regression of a working
//     singleton → ERROR, even inside the grace window (§5.4.2: "the grace
//     window does not soften failures of already-working roles");
//   - a ping sent before the role established (including the in-flight-at-
//     establishment race) is "unresolved" → silent inside the grace window
//     (§5.4.1);
//   - after the grace window every miss is ERROR (§5.4.2: the distinction
//     "is not a permanent ERROR-mute").
func classifyPingTimeout(sentAt time.Time, anchor *steadyAnchor, warmupOver bool, established bool, establishedAt time.Time) bool {
	if !anchor.countsForStrictWindow(sentAt) {
		return false
	}
	if established && sentAt.After(establishedAt) {
		return true
	}
	return warmupOver
}
