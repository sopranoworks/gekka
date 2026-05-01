/*
 * work_pulling.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package delivery

import (
	"log"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
)

// ── WorkPulling message types ─────────────────────────────────────────────────

// RegisterWorker is sent by a worker actor to the WorkPullingProducerController
// to register itself as available to receive work.
type RegisterWorker struct {
	// Worker is the actor reference of the registering worker.
	Worker actor.Ref
}

// DeregisterWorker is sent by a worker (or its death-watcher) to remove it
// from the pool.
type DeregisterWorker struct {
	Worker actor.Ref
}

// RequestNext is sent by the WorkPullingProducerController to the producer
// actor when a worker is ready and the controller needs the next work item.
// The producer should reply by calling SendNextTo.Tell(msg).
type RequestNext struct {
	// SendNextTo is the actor reference the producer should send its next
	// work item to. The controller routes it to the waiting worker.
	SendNextTo actor.Ref
}

// Work is delivered to a worker by the WorkPullingProducerController.
// The worker must send WorkConfirmed to ConfirmTo when it finishes processing
// so the controller can request the next item for that worker.
type Work struct {
	// Msg is the work item sent by the producer.
	Msg any
	// ConfirmTo is the WorkPullingProducerController reference. The worker
	// must tell WorkConfirmed{Worker: self} to this ref when done.
	ConfirmTo actor.Ref
}

// WorkConfirmed is sent by a worker to the WorkPullingProducerController to
// signal that it has finished processing its current work item and is ready
// for the next one.
type WorkConfirmed struct {
	// Worker is the actor reference of the confirming worker.
	Worker actor.Ref
}

// ── WorkPullingProducerController ────────────────────────────────────────────

// workPullingState holds the mutable state of the WorkPullingProducerController.
type workPullingState struct {
	selfRef  actor.Ref
	producer actor.Ref // set lazily on first Start message or via SetProducer

	// idleWorkers is the queue of workers that are ready for work.
	idleWorkers []actor.Ref
	// busyWorkers tracks which workers are currently processing a work item.
	busyWorkers map[string]actor.Ref // key = worker.Path()

	// pendingWork holds work items that arrived before any worker was ready.
	pendingWork []any
	// bufferSize mirrors
	// pekko.reliable-delivery.work-pulling.producer-controller.buffer-size.
	// 0 means unlimited (legacy behaviour).
	bufferSize int
	// droppedCount counts items dropped because the buffer was full. Read
	// by tests via DroppedCount(); never reset.
	droppedCount int
}

// NewWorkPullingProducerController returns a typed.Behavior[any] that
// implements the WorkPulling producer side of Pekko's Reliable Delivery
// work-pulling pattern.
//
// The controller mediates between a single producer actor and a pool of
// worker (consumer) actors:
//
//  1. Workers register by sending RegisterWorker{Worker: self} to the controller.
//  2. When a worker is idle and work is available, the controller delivers a
//     Work{Msg: item, ConfirmTo: controllerRef} message to the worker.
//  3. When no idle workers exist, the controller requests the next item from
//     the producer by sending RequestNext{SendNextTo: controllerRef}. The
//     producer replies by sending the item directly to SendNextTo.
//  4. After processing, the worker sends WorkConfirmed{Worker: self} back to
//     the controller, making itself idle again.
//
// Set the producer reference by sending it as a plain actor.Ref message after
// spawning, or wire it manually after construction.
//
// Example:
//
//	ctrl := delivery.NewWorkPullingProducerController()
//	ctrlRef, _ := sys.ActorOf(actor.Props{New: func() actor.Actor {
//	    return typed.NewTypedActor[any](ctrl)
//	}}, "work-controller")
//
//	// Register a worker.
//	ctrlRef.Tell(delivery.RegisterWorker{Worker: workerRef})
//
//	// Set the producer.
//	ctrlRef.Tell(producerRef)
func NewWorkPullingProducerController() typed.Behavior[any] {
	state := &workPullingState{
		busyWorkers: make(map[string]actor.Ref),
	}
	return state.handle
}

// NewWorkPullingProducerControllerFromConfig is the HOCON-driven counterpart
// of NewWorkPullingProducerController. It honours
// pekko.reliable-delivery.work-pulling.producer-controller.* values:
// buffer-size caps pendingWork length (excess items are dropped and
// counted, observable via the returned tuple's state pointer for tests).
//
// The state pointer is exposed so tests can observe runtime decisions
// (e.g., DroppedCount after overflow). Production callers can ignore it.
func NewWorkPullingProducerControllerFromConfig(cfg WorkPullingProducerControllerConfig) (typed.Behavior[any], *WorkPullingState) {
	state := &workPullingState{
		busyWorkers: make(map[string]actor.Ref),
		bufferSize:  cfg.BufferSize,
	}
	return state.handle, (*WorkPullingState)(state)
}

// WorkPullingState is an opaque handle returned from
// NewWorkPullingProducerControllerFromConfig that lets tests observe
// runtime counters (e.g., DroppedCount). Methods on this type are safe
// to call only after the controller has stopped processing, since the
// underlying state is updated from the actor goroutine.
type WorkPullingState workPullingState

// DroppedCount returns the number of work items dropped because the
// configured buffer-size was exceeded.
func (s *WorkPullingState) DroppedCount() int { return s.droppedCount }

func (s *workPullingState) handle(ctx typed.TypedContext[any], msg any) typed.Behavior[any] {
	// Capture self on first message.
	if s.selfRef == nil {
		s.selfRef = ctx.Self().Untyped()
	}

	switch m := msg.(type) {

	// ── Worker lifecycle ──────────────────────────────────────────────────

	case RegisterWorker:
		if m.Worker == nil {
			return nil
		}
		// Watch the worker so we can clean up if it stops.
		ctx.Watch(m.Worker)
		log.Printf("WorkPulling: worker registered %s", m.Worker.Path())

		// If we have pending work, dispatch immediately.
		if len(s.pendingWork) > 0 {
			item := s.pendingWork[0]
			s.pendingWork = s.pendingWork[1:]
			s.busyWorkers[m.Worker.Path()] = m.Worker
			m.Worker.Tell(Work{Msg: item, ConfirmTo: s.selfRef})
		} else {
			s.idleWorkers = append(s.idleWorkers, m.Worker)
			// Request next work item from the producer if one is configured.
			s.maybeRequestNext()
		}

	case DeregisterWorker:
		s.removeWorker(m.Worker)
		log.Printf("WorkPulling: worker deregistered %s", m.Worker.Path())

	// ── Work confirmation (worker finished) ───────────────────────────────

	case WorkConfirmed:
		if m.Worker == nil {
			return nil
		}
		delete(s.busyWorkers, m.Worker.Path())

		// If there is pending work, dispatch it immediately.
		if len(s.pendingWork) > 0 {
			item := s.pendingWork[0]
			s.pendingWork = s.pendingWork[1:]
			s.busyWorkers[m.Worker.Path()] = m.Worker
			m.Worker.Tell(Work{Msg: item, ConfirmTo: s.selfRef})
			return nil
		}
		// Worker is now idle.
		s.idleWorkers = append(s.idleWorkers, m.Worker)
		s.maybeRequestNext()

	// ── Work items arriving (sent to SendNextTo by the producer) ─────────

	default:
		// Check for actor.Ref (producer registration).
		if ref, ok := msg.(actor.Ref); ok && ref != nil {
			s.producer = ref
			log.Printf("WorkPulling: producer set to %s", ref.Path())
			s.maybeRequestNext()
			return nil
		}

		// Any other message is treated as a work item from the producer.
		if len(s.idleWorkers) > 0 {
			worker := s.idleWorkers[0]
			s.idleWorkers = s.idleWorkers[1:]
			s.busyWorkers[worker.Path()] = worker
			worker.Tell(Work{Msg: msg, ConfirmTo: s.selfRef})
			// Request another item proactively if more workers are idle.
			s.maybeRequestNext()
		} else {
			// No idle worker — buffer the item, respecting the
			// configured buffer-size cap.
			s.appendPending(msg)
		}
	}
	return nil
}

// appendPending appends an item to pendingWork, respecting the configured
// buffer-size cap. Items beyond the cap are dropped and counted in
// droppedCount. bufferSize == 0 means unlimited (legacy behaviour).
// Called by handle when a work item arrives with no idle workers.
func (s *workPullingState) appendPending(item any) {
	if s.bufferSize > 0 && len(s.pendingWork) >= s.bufferSize {
		s.droppedCount++
		log.Printf("WorkPulling: buffer full (size=%d), dropping work item", s.bufferSize)
		return
	}
	s.pendingWork = append(s.pendingWork, item)
}

// maybeRequestNext asks the producer for the next work item if there is an
// idle worker and a configured producer.
func (s *workPullingState) maybeRequestNext() {
	if s.producer == nil || len(s.idleWorkers) == 0 {
		return
	}
	s.producer.Tell(RequestNext{SendNextTo: s.selfRef})
}

// removeWorker removes a worker from both the idle and busy sets.
func (s *workPullingState) removeWorker(worker actor.Ref) {
	if worker == nil {
		return
	}
	path := worker.Path()
	delete(s.busyWorkers, path)
	filtered := s.idleWorkers[:0]
	for _, w := range s.idleWorkers {
		if w.Path() != path {
			filtered = append(filtered, w)
		}
	}
	s.idleWorkers = filtered
}
