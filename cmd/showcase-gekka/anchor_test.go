// cmd/showcase-gekka/anchor_test.go — spec §4/§5.4.1/§5.4.2 classification.
//
// SPDX-License-Identifier: MIT
package main

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestSteadyAnchor_StrictWindow: nothing counts before the anchor latches;
// after it latches, only sends at-or-after the anchor count.
func TestSteadyAnchor_StrictWindow(t *testing.T) {
	a := &steadyAnchor{}

	if a.countsForStrictWindow(time.Now()) {
		t.Error("nothing may count while the anchor is unlatched (cluster still forming)")
	}

	before := time.Now().Add(-time.Second)
	a.mark()
	after := time.Now().Add(time.Second)

	if a.countsForStrictWindow(before) {
		t.Error("a send predating the anchor is setup-phase traffic (§4) and must not count")
	}
	if !a.countsForStrictWindow(after) {
		t.Error("a send after the anchor must count")
	}
}

// TestSteadyAnchor_MarkLatchesOnce: only the first mark wins.
func TestSteadyAnchor_MarkLatchesOnce(t *testing.T) {
	a := &steadyAnchor{}
	a.mark()
	first, ok := a.at()
	if !ok {
		t.Fatal("anchor must be latched after mark")
	}
	time.Sleep(2 * time.Millisecond)
	a.mark()
	second, _ := a.at()
	if !second.Equal(first) {
		t.Errorf("anchor re-latched: first=%v second=%v", first, second)
	}
}

// TestClassifyPingTimeout pins the §5.4 warm-up state machine with the
// anchor semantics.
func TestClassifyPingTimeout(t *testing.T) {
	latched := &steadyAnchor{}
	latched.mark()
	anchorAt, _ := latched.at()
	unlatched := &steadyAnchor{}

	preAnchor := anchorAt.Add(-10 * time.Second)
	postAnchor := anchorAt.Add(2 * time.Second)
	estAt := anchorAt.Add(5 * time.Second)

	cases := []struct {
		name        string
		sentAt      time.Time
		anchor      *steadyAnchor
		warmupOver  bool
		established bool
		estAt       time.Time
		want        bool
	}{
		{"cluster still forming: silent", time.Now(), unlatched, false, false, time.Time{}, false},
		{"setup-phase send: silent even post-warmup", preAnchor, latched, true, true, preAnchor.Add(-time.Minute), false},
		{"unresolved role inside grace: silent (§5.4.2)", postAnchor, latched, false, false, time.Time{}, false},
		{"regression of established role inside grace: ERROR (§5.4.2)", estAt.Add(time.Second), latched, false, true, estAt, true},
		{"in-flight ping raced establishment, inside grace: silent (§5.4.1 unresolved)", postAnchor, latched, false, true, estAt, false},
		{"post-warmup unresolved: ERROR (§5.4.2)", postAnchor, latched, true, false, time.Time{}, true},
		{"post-warmup, sent pre-establishment: ERROR (§5.4.2 not a permanent mute)", postAnchor, latched, true, true, estAt, true},
	}
	for _, c := range cases {
		if got := classifyPingTimeout(c.sentAt, c.anchor, c.warmupOver, c.established, c.estAt); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

// TestWatchAnchor_LatchesAtExpectedCount: the poll loop latches exactly when
// the up-count first reaches the expected membership.
func TestWatchAnchor_LatchesAtExpectedCount(t *testing.T) {
	a := &steadyAnchor{}
	var n atomic.Int32
	count := func() int { return int(n.Load()) }
	stop := make(chan struct{})
	defer close(stop)
	go watchAnchor(a, count, 3, stop)

	time.Sleep(450 * time.Millisecond)
	if _, ok := a.at(); ok {
		t.Fatal("anchor latched before membership completed")
	}
	n.Store(3)
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, ok := a.at(); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("anchor never latched after membership completed")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
