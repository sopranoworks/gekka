/*
 * actor/coordinated_shutdown.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package actor — CoordinatedShutdown runs registered tasks in a fixed phase
// order that mirrors Pekko's pekko.coordinated-shutdown configuration.
package actor

import (
	"context"
	"log"
	"sync"
	"time"
)

// DefaultPhases lists every standard shutdown phase in execution order.
// The names are intentionally identical to Pekko's coordinated-shutdown phase
// keys so that HOCON configuration snippets transfer directly.
var DefaultPhases = []string{
	"before-service-unbind",
	"service-unbind",
	"service-requests-done",
	"service-stop",
	"before-cluster-shutdown",
	"cluster-sharding-shutdown-region",
	"cluster-leave",
	"cluster-exiting",
	"cluster-exiting-done",
	"cluster-shutdown",
	"before-actor-system-terminate",
	"actor-system-terminate",
}

// DefaultPhaseTimeout is the per-phase timeout applied when no HOCON
// override is configured.
const DefaultPhaseTimeout = 10 * time.Second

// ShutdownTask pairs a human-readable name with its implementation.
type ShutdownTask struct {
	Name string
	Fn   func(context.Context) error
}

// CoordinatedShutdown executes registered tasks phase-by-phase in the order
// defined by DefaultPhases.  All tasks within a phase run concurrently; the
// next phase begins only after all tasks in the current phase complete or the
// per-phase timeout expires.
//
// This design is intentionally compatible with Pekko's CoordinatedShutdown so
// that existing run-book documentation transfers without modification.
//
// Usage:
//
//	cs := actor.NewCoordinatedShutdown()
//	cs.AddTask("cluster-leave", "send-leave", func(ctx context.Context) error {
//	    return node.cm.LeaveCluster()
//	})
//	cs.Run(context.Background())
type CoordinatedShutdown struct {
	phases   []string
	tasks    map[string][]ShutdownTask
	timeouts map[string]time.Duration

	mu      sync.Mutex
	runOnce sync.Once
}

// NewCoordinatedShutdown returns a CoordinatedShutdown pre-populated with the
// standard phase list and no registered tasks.
func NewCoordinatedShutdown() *CoordinatedShutdown {
	phases := make([]string, len(DefaultPhases))
	copy(phases, DefaultPhases)
	return &CoordinatedShutdown{
		phases:   phases,
		tasks:    make(map[string][]ShutdownTask),
		timeouts: make(map[string]time.Duration),
	}
}

// SetPhaseTimeout overrides the timeout for a specific phase.
// Must be called before Run; has no effect after Run has been invoked.
func (cs *CoordinatedShutdown) SetPhaseTimeout(phase string, d time.Duration) {
	cs.mu.Lock()
	cs.timeouts[phase] = d
	cs.mu.Unlock()
}

// AddTask registers fn under the given phase and descriptive name.
// Tasks within a phase execute concurrently when that phase runs.
// If phase is not in the standard phase list it is added before
// "actor-system-terminate".
func (cs *CoordinatedShutdown) AddTask(phase, name string, fn func(context.Context) error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for _, p := range cs.phases {
		if p == phase {
			cs.tasks[phase] = append(cs.tasks[phase], ShutdownTask{Name: name, Fn: fn})
			return
		}
	}

	// Phase not found, insert it before actor-system-terminate if possible
	idx := -1
	for i, p := range cs.phases {
		if p == "actor-system-terminate" {
			idx = i
			break
		}
	}

	if idx != -1 {
		cs.phases = append(cs.phases[:idx], append([]string{phase}, cs.phases[idx:]...)...)
	} else {
		cs.phases = append(cs.phases, phase)
	}

	cs.tasks[phase] = append(cs.tasks[phase], ShutdownTask{Name: name, Fn: fn})
}

// Run executes every registered task in phase order and returns after all
// phases complete.  The provided ctx acts as a hard deadline — if it is
// cancelled mid-run, the current phase task contexts are cancelled and Run
// returns.
//
// Run is idempotent: only the first call executes; subsequent calls return
// immediately with nil.
func (cs *CoordinatedShutdown) Run(ctx context.Context) error {
	var runErr error
	cs.runOnce.Do(func() {
		runErr = cs.runAllPhases(ctx)
	})
	return runErr
}

// runAllPhases iterates over every phase sequentially.
func (cs *CoordinatedShutdown) runAllPhases(ctx context.Context) error {
	// Snapshot state under lock so callers cannot race with AddTask.
	cs.mu.Lock()
	phases := make([]string, len(cs.phases))
	copy(phases, cs.phases)
	tasks := make(map[string][]ShutdownTask, len(cs.tasks))
	for k, v := range cs.tasks {
		cp := make([]ShutdownTask, len(v))
		copy(cp, v)
		tasks[k] = cp
	}
	timeouts := make(map[string]time.Duration, len(cs.timeouts))
	for k, v := range cs.timeouts {
		timeouts[k] = v
	}
	cs.mu.Unlock()

	for _, phase := range phases {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		phaseTasks := tasks[phase]
		if len(phaseTasks) == 0 {
			continue
		}
		timeout := timeouts[phase]
		if timeout <= 0 {
			timeout = DefaultPhaseTimeout
		}
		if err := runPhase(ctx, phase, phaseTasks, timeout); err != nil {
			// Log but continue — mirrors Pekko's default recover=on behaviour.
			log.Printf("CoordinatedShutdown: phase %q did not complete cleanly: %v", phase, err)
		}
	}
	return nil
}

// runPhase executes all tasks for a single phase concurrently and waits for
// all of them to finish or the phase timeout to expire.
func runPhase(ctx context.Context, phase string, tasks []ShutdownTask, timeout time.Duration) error {
	log.Printf("CoordinatedShutdown: [%s] starting (%d task(s))", phase, len(tasks))

	phaseCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var wg sync.WaitGroup
	for _, t := range tasks {
		t := t
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := t.Fn(phaseCtx); err != nil {
				log.Printf("CoordinatedShutdown: [%s] task %q: %v", phase, t.Name, err)
			}
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		log.Printf("CoordinatedShutdown: [%s] done", phase)
		return nil
	case <-phaseCtx.Done():
		log.Printf("CoordinatedShutdown: [%s] timeout", phase)
		return phaseCtx.Err()
	}
}
