/*
 * work_pulling_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package delivery

import (
	"sync"
	"testing"

	"github.com/sopranoworks/gekka/actor"
)

// ── stubRef ───────────────────────────────────────────────────────────────────

// stubRef is a minimal actor.Ref that records messages sent to it.
type stubRef struct {
	path string
	mu   sync.Mutex
	msgs []any
}

func (r *stubRef) Tell(msg any, _ ...actor.Ref) {
	r.mu.Lock()
	r.msgs = append(r.msgs, msg)
	r.mu.Unlock()
}

func (r *stubRef) Path() string { return r.path }

func (r *stubRef) received() []any {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]any, len(r.msgs))
	copy(out, r.msgs)
	return out
}

func (r *stubRef) last() any {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.msgs) == 0 {
		return nil
	}
	return r.msgs[len(r.msgs)-1]
}

func (r *stubRef) clear() {
	r.mu.Lock()
	r.msgs = nil
	r.mu.Unlock()
}

func newStub(path string) *stubRef { return &stubRef{path: path} }

// ── direct dispatch helper ────────────────────────────────────────────────────

// dispatch exercises workPullingState's logic directly without a real actor
// context. selfRef must be pre-injected before calling dispatch.
func (s *workPullingState) dispatch(msg any) {
	switch m := msg.(type) {
	case RegisterWorker:
		if m.Worker == nil {
			return
		}
		if len(s.pendingWork) > 0 {
			item := s.pendingWork[0]
			s.pendingWork = s.pendingWork[1:]
			s.busyWorkers[m.Worker.Path()] = m.Worker
			m.Worker.Tell(Work{Msg: item, ConfirmTo: s.selfRef})
		} else {
			s.idleWorkers = append(s.idleWorkers, m.Worker)
			s.maybeRequestNext()
		}
	case DeregisterWorker:
		s.removeWorker(m.Worker)
	case WorkConfirmed:
		if m.Worker == nil {
			return
		}
		delete(s.busyWorkers, m.Worker.Path())
		if len(s.pendingWork) > 0 {
			item := s.pendingWork[0]
			s.pendingWork = s.pendingWork[1:]
			s.busyWorkers[m.Worker.Path()] = m.Worker
			m.Worker.Tell(Work{Msg: item, ConfirmTo: s.selfRef})
			return
		}
		s.idleWorkers = append(s.idleWorkers, m.Worker)
		s.maybeRequestNext()
	default:
		// actor.Ref → set as producer
		if ref, ok := msg.(actor.Ref); ok && ref != nil {
			s.producer = ref
			s.maybeRequestNext()
			return
		}
		// Work item from producer
		if len(s.idleWorkers) > 0 {
			worker := s.idleWorkers[0]
			s.idleWorkers = s.idleWorkers[1:]
			s.busyWorkers[worker.Path()] = worker
			worker.Tell(Work{Msg: msg, ConfirmTo: s.selfRef})
			s.maybeRequestNext()
		} else {
			s.pendingWork = append(s.pendingWork, msg)
		}
	}
}

func newState() (*workPullingState, *stubRef) {
	self := newStub("/user/ctrl")
	s := &workPullingState{
		selfRef:     self,
		busyWorkers: make(map[string]actor.Ref),
	}
	return s, self
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestWorkPulling_WorkerReceivesWork(t *testing.T) {
	s, _ := newState()
	producer := newStub("/user/producer")
	worker := newStub("/user/worker1")

	s.dispatch(producer)                       // register producer
	s.dispatch(RegisterWorker{Worker: worker}) // idle worker registered

	// Controller should have asked producer for the next item.
	rn, ok := producer.last().(RequestNext)
	if !ok {
		t.Fatalf("expected RequestNext sent to producer, got %T", producer.last())
	}
	if rn.SendNextTo == nil {
		t.Fatal("RequestNext.SendNextTo should not be nil")
	}

	// Producer sends the work item to SendNextTo (the controller).
	s.dispatch("task-1")

	msgs := worker.received()
	if len(msgs) == 0 {
		t.Fatal("worker should have received a Work message")
	}
	w, ok := msgs[0].(Work)
	if !ok {
		t.Fatalf("expected Work, got %T", msgs[0])
	}
	if w.Msg != "task-1" {
		t.Errorf("Work.Msg = %v, want %q", w.Msg, "task-1")
	}
	if w.ConfirmTo == nil {
		t.Error("Work.ConfirmTo must not be nil")
	}
}

func TestWorkPulling_ConfirmMakesWorkerIdleAgain(t *testing.T) {
	s, _ := newState()
	producer := newStub("/user/producer")
	worker := newStub("/user/worker1")

	s.dispatch(producer)
	s.dispatch(RegisterWorker{Worker: worker})
	s.dispatch("task-1")
	producer.clear()

	// Worker confirms processing is done.
	s.dispatch(WorkConfirmed{Worker: worker})

	// Controller should request the next item from the producer.
	if _, ok := producer.last().(RequestNext); !ok {
		t.Fatalf("expected RequestNext after WorkConfirmed, got %T", producer.last())
	}
}

func TestWorkPulling_BuffersWorkWhenNoWorker(t *testing.T) {
	s, _ := newState()
	producer := newStub("/user/producer")
	s.dispatch(producer)

	// Send work before any worker registers.
	s.dispatch("buffered-task")
	if len(s.pendingWork) != 1 {
		t.Fatalf("expected 1 pending item, got %d", len(s.pendingWork))
	}

	// Registering a worker should drain the pending queue immediately.
	worker := newStub("/user/worker1")
	s.dispatch(RegisterWorker{Worker: worker})

	if len(s.pendingWork) != 0 {
		t.Errorf("pending queue should be empty, got %d items", len(s.pendingWork))
	}
	msgs := worker.received()
	if len(msgs) == 0 {
		t.Fatal("worker should receive the buffered task")
	}
	if w, ok := msgs[0].(Work); !ok || w.Msg != "buffered-task" {
		t.Errorf("expected Work{Msg: buffered-task}, got %v", msgs[0])
	}
}

func TestWorkPulling_MultipleWorkersRoundRobin(t *testing.T) {
	s, _ := newState()
	producer := newStub("/user/producer")
	w1 := newStub("/user/worker1")
	w2 := newStub("/user/worker2")

	s.dispatch(producer)
	s.dispatch(RegisterWorker{Worker: w1})
	s.dispatch(RegisterWorker{Worker: w2})

	// Send two items — each worker gets one.
	s.dispatch("item-A")
	s.dispatch("item-B")

	if len(w1.received()) == 0 {
		t.Error("worker1 should have received item-A")
	}
	if len(w2.received()) == 0 {
		t.Error("worker2 should have received item-B")
	}
}

func TestWorkPulling_DeregisterRemovesFromIdlePool(t *testing.T) {
	s, _ := newState()
	producer := newStub("/user/producer")
	w1 := newStub("/user/worker1")
	w2 := newStub("/user/worker2")

	s.dispatch(producer)
	s.dispatch(RegisterWorker{Worker: w1})
	s.dispatch(RegisterWorker{Worker: w2})

	// Deregister w1.
	s.dispatch(DeregisterWorker{Worker: w1})

	if len(s.idleWorkers) != 1 || s.idleWorkers[0].Path() != w2.Path() {
		t.Errorf("only w2 should remain idle; got %v", s.idleWorkers)
	}

	// Work should now go only to w2.
	w1.clear()
	w2.clear()
	s.dispatch("task-X")

	if len(w1.received()) != 0 {
		t.Error("deregistered w1 should not receive work")
	}
	if len(w2.received()) == 0 {
		t.Error("w2 should receive the work item")
	}
}

func TestWorkPulling_MessageTypes(t *testing.T) {
	w := newStub("/user/w")
	ctrl := newStub("/user/ctrl")

	rw := RegisterWorker{Worker: w}
	if rw.Worker.Path() != w.Path() {
		t.Error("RegisterWorker field")
	}
	dw := DeregisterWorker{Worker: w}
	if dw.Worker.Path() != w.Path() {
		t.Error("DeregisterWorker field")
	}
	rn := RequestNext{SendNextTo: ctrl}
	if rn.SendNextTo.Path() != ctrl.Path() {
		t.Error("RequestNext field")
	}
	wk := Work{Msg: 42, ConfirmTo: ctrl}
	if wk.Msg != 42 || wk.ConfirmTo.Path() != ctrl.Path() {
		t.Error("Work fields")
	}
	wc := WorkConfirmed{Worker: w}
	if wc.Worker.Path() != w.Path() {
		t.Error("WorkConfirmed field")
	}
}

func TestWorkPulling_NoRaceMultiGoroutine(t *testing.T) {
	s, _ := newState()
	producer := newStub("/user/producer")

	var mu sync.Mutex
	dispatch := func(msg any) {
		mu.Lock()
		s.dispatch(msg)
		mu.Unlock()
	}

	dispatch(producer)

	var wg sync.WaitGroup
	for i := range 4 {
		w := newStub("/user/worker" + string(rune('0'+i)))
		wg.Add(1)
		go func(w *stubRef) {
			defer wg.Done()
			dispatch(RegisterWorker{Worker: w})
		}(w)
	}
	wg.Wait()

	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dispatch("work-item")
		}()
	}
	wg.Wait()
}
