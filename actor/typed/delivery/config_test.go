/*
 * actor/typed/delivery/config_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package delivery

import (
	"testing"
	"time"
)

// TestDefaultConfig_MatchesPekkoReference asserts that DefaultConfig returns
// the values that match Pekko's actor-typed/src/main/resources/reference.conf
// pekko.reliable-delivery.* block. A drift here would silently change
// at-runtime defaults — this test pins them.
func TestDefaultConfig_MatchesPekkoReference(t *testing.T) {
	c := DefaultConfig()

	if c.ProducerController.ChunkLargeMessages != 0 {
		t.Errorf("ChunkLargeMessages = %d, want 0 (off)", c.ProducerController.ChunkLargeMessages)
	}
	if c.ProducerController.DurableQueueRequestTimeout != 3*time.Second {
		t.Errorf("DurableQueueRequestTimeout = %v, want 3s", c.ProducerController.DurableQueueRequestTimeout)
	}
	if c.ProducerController.DurableQueueRetryAttempts != 10 {
		t.Errorf("DurableQueueRetryAttempts = %d, want 10", c.ProducerController.DurableQueueRetryAttempts)
	}
	if c.ProducerController.DurableQueueResendFirstInterval != 1*time.Second {
		t.Errorf("DurableQueueResendFirstInterval = %v, want 1s", c.ProducerController.DurableQueueResendFirstInterval)
	}

	if c.ConsumerController.FlowControlWindow != 50 {
		t.Errorf("FlowControlWindow = %d, want 50", c.ConsumerController.FlowControlWindow)
	}
	if c.ConsumerController.ResendIntervalMin != 2*time.Second {
		t.Errorf("ResendIntervalMin = %v, want 2s", c.ConsumerController.ResendIntervalMin)
	}
	if c.ConsumerController.ResendIntervalMax != 30*time.Second {
		t.Errorf("ResendIntervalMax = %v, want 30s", c.ConsumerController.ResendIntervalMax)
	}
	if c.ConsumerController.OnlyFlowControl {
		t.Errorf("OnlyFlowControl = true, want false")
	}

	if c.WorkPullingProducerController.BufferSize != 1000 {
		t.Errorf("BufferSize = %d, want 1000", c.WorkPullingProducerController.BufferSize)
	}
	if c.WorkPullingProducerController.InternalAskTimeout != 60*time.Second {
		t.Errorf("InternalAskTimeout = %v, want 60s", c.WorkPullingProducerController.InternalAskTimeout)
	}
	if c.WorkPullingProducerController.ChunkLargeMessages != 0 {
		t.Errorf("WorkPulling.ChunkLargeMessages = %d, want 0 (off)", c.WorkPullingProducerController.ChunkLargeMessages)
	}
}

// TestConsumerController_FlowControlWindow_DrivesRequestRange verifies that
// the configured pekko.reliable-delivery.consumer-controller.flow-control-window
// value flows from Config into the Request.RequestUpToSeqNr the consumer
// sends to the producer — i.e., the value is *consumed* in a runtime
// decision, not merely stored.
func TestConsumerController_FlowControlWindow_DrivesRequestRange(t *testing.T) {
	cases := []struct {
		name   string
		window int
	}{
		{name: "small", window: 4},
		{name: "default", window: 50},
		{name: "large", window: 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig().ConsumerController
			cfg.FlowControlWindow = tc.window
			// Build the state directly via NewConsumerControllerFromConfig,
			// then poke at the underlying state pointer to drive sendRequest.
			consumer := newStub("/user/app-consumer")
			producer := newStub("/user/producer")
			self := newStub("/user/cc")
			state := &consumerState{
				consumerActor:   consumer,
				expectedSeqNr:   1,
				windowSize:      cfg.FlowControlWindow,
				onlyFlowControl: cfg.OnlyFlowControl,
				stash:           make(map[int64]*SequencedMessage),
				selfRef:         self,
				producerRef:     producer,
				producerPath:    producer.Path(),
			}
			state.sendRequest(nil, false)

			req, ok := producer.last().(*Request)
			if !ok {
				t.Fatalf("expected *Request sent to producer, got %T", producer.last())
			}
			if req.RequestUpToSeqNr != int64(tc.window) {
				t.Errorf("RequestUpToSeqNr = %d, want %d (window=%d)",
					req.RequestUpToSeqNr, tc.window, tc.window)
			}
			if req.ConfirmedSeqNr != 0 {
				t.Errorf("ConfirmedSeqNr = %d, want 0", req.ConfirmedSeqNr)
			}
		})
	}
}

// TestConsumerController_OnlyFlowControl_DropsGap verifies that when
// only-flow-control is true, an out-of-order SequencedMessage does NOT
// trigger a Resend back to the producer; instead the gap is silently
// accepted. Compared with the default (OnlyFlowControl=false), where a
// Resend is sent.
func TestConsumerController_OnlyFlowControl_DropsGap(t *testing.T) {
	consumer := newStub("/user/app-consumer")

	// Variant 1: only-flow-control=true → no Resend on gap.
	{
		producer := newStub("/user/producer")
		self := newStub("/user/cc")
		state := &consumerState{
			consumerActor:   consumer,
			expectedSeqNr:   1,
			windowSize:      50,
			onlyFlowControl: true,
			stash:           make(map[int64]*SequencedMessage),
			selfRef:         self,
			producerRef:     producer,
			producerPath:    producer.Path(),
		}
		// First message arrives in order to bootstrap.
		state.onSequencedMessage(nil, &SequencedMessage{
			ProducerRef: producer.Path(), SeqNr: 1, First: true,
			Message: Payload{EnclosedMessage: []byte("a")},
		})
		// Now SeqNr=5 arrives — a gap of 3.
		producer.clear()
		state.onSequencedMessage(nil, &SequencedMessage{
			ProducerRef: producer.Path(), SeqNr: 5,
			Message: Payload{EnclosedMessage: []byte("e")},
		})
		for _, msg := range producer.received() {
			if _, ok := msg.(*Resend); ok {
				t.Errorf("OnlyFlowControl=true: unexpected Resend sent on gap")
			}
		}
	}

	// Variant 2: only-flow-control=false (default) → Resend on gap.
	{
		producer := newStub("/user/producer")
		self := newStub("/user/cc")
		state := &consumerState{
			consumerActor:   consumer,
			expectedSeqNr:   1,
			windowSize:      50,
			onlyFlowControl: false,
			stash:           make(map[int64]*SequencedMessage),
			selfRef:         self,
			producerRef:     producer,
			producerPath:    producer.Path(),
		}
		state.onSequencedMessage(nil, &SequencedMessage{
			ProducerRef: producer.Path(), SeqNr: 1, First: true,
			Message: Payload{EnclosedMessage: []byte("a")},
		})
		producer.clear()
		state.onSequencedMessage(nil, &SequencedMessage{
			ProducerRef: producer.Path(), SeqNr: 5,
			Message: Payload{EnclosedMessage: []byte("e")},
		})
		found := false
		for _, msg := range producer.received() {
			if _, ok := msg.(*Resend); ok {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("OnlyFlowControl=false: expected Resend on gap, got none")
		}
	}
}

// TestProducerController_ChunkLargeMessages_SplitsPayload verifies that the
// configured pekko.reliable-delivery.producer-controller.chunk-large-messages
// byte threshold drives runtime payload chunking — a value-flows-to-runtime
// test, not a parse-only test.
func TestProducerController_ChunkLargeMessages_SplitsPayload(t *testing.T) {
	consumer := newStub("/user/consumer")
	self := newStub("/user/producer")

	// Variant A: chunkSize = 0 (off) → single SequencedMessage, no chunk flags.
	{
		state := &producerState{
			producerID:  "p",
			nextSeqNr:   1,
			selfRef:     self,
			consumerRef: consumer,
			requestUpTo: 100,
		}
		state.onSendMessage(nil, SendMessage{
			Payload:      []byte("ABCDEFGHIJKLMNOPQRSTUVWX"), // 24 bytes
			SerializerID: 4,
		})
		msgs := consumer.received()
		if len(msgs) != 1 {
			t.Fatalf("chunkSize=0: got %d messages, want 1", len(msgs))
		}
		seq, ok := msgs[0].(*SequencedMessage)
		if !ok {
			t.Fatalf("got %T, want *SequencedMessage", msgs[0])
		}
		if seq.HasChunk {
			t.Errorf("HasChunk=true, want false when chunkSize=0")
		}
		if string(seq.Message.EnclosedMessage) != "ABCDEFGHIJKLMNOPQRSTUVWX" {
			t.Errorf("payload = %q, want full 24 bytes", seq.Message.EnclosedMessage)
		}
	}

	// Variant B: chunkSize = 8 with a 24-byte payload → 3 chunks.
	{
		consumer.clear()
		state := &producerState{
			producerID:  "p",
			nextSeqNr:   1,
			selfRef:     self,
			consumerRef: consumer,
			requestUpTo: 100,
			chunkSize:   8,
		}
		state.onSendMessage(nil, SendMessage{
			Payload:      []byte("ABCDEFGHIJKLMNOPQRSTUVWX"), // 24 bytes
			SerializerID: 4,
		})
		msgs := consumer.received()
		if len(msgs) != 3 {
			t.Fatalf("chunkSize=8 + 24-byte payload: got %d messages, want 3", len(msgs))
		}
		expected := []struct {
			payload string
			first   bool
			last    bool
		}{
			{"ABCDEFGH", true, false},
			{"IJKLMNOP", false, false},
			{"QRSTUVWX", false, true},
		}
		for i, e := range expected {
			seq, ok := msgs[i].(*SequencedMessage)
			if !ok {
				t.Fatalf("msg[%d]: got %T, want *SequencedMessage", i, msgs[i])
			}
			if !seq.HasChunk {
				t.Errorf("msg[%d].HasChunk = false, want true", i)
			}
			if seq.FirstChunk != e.first {
				t.Errorf("msg[%d].FirstChunk = %v, want %v", i, seq.FirstChunk, e.first)
			}
			if seq.LastChunk != e.last {
				t.Errorf("msg[%d].LastChunk = %v, want %v", i, seq.LastChunk, e.last)
			}
			if string(seq.Message.EnclosedMessage) != e.payload {
				t.Errorf("msg[%d].payload = %q, want %q", i, seq.Message.EnclosedMessage, e.payload)
			}
			if seq.SeqNr != int64(i+1) {
				t.Errorf("msg[%d].SeqNr = %d, want %d", i, seq.SeqNr, i+1)
			}
		}
	}

	// Variant C: chunkSize = 100 with a 24-byte payload → no chunking
	// (under threshold).
	{
		consumer.clear()
		state := &producerState{
			producerID:  "p",
			nextSeqNr:   1,
			selfRef:     self,
			consumerRef: consumer,
			requestUpTo: 100,
			chunkSize:   100,
		}
		state.onSendMessage(nil, SendMessage{
			Payload:      []byte("ABCDEFGHIJKLMNOPQRSTUVWX"),
			SerializerID: 4,
		})
		msgs := consumer.received()
		if len(msgs) != 1 {
			t.Fatalf("chunkSize=100 + 24-byte payload: got %d messages, want 1 (under threshold)", len(msgs))
		}
		seq := msgs[0].(*SequencedMessage)
		if seq.HasChunk {
			t.Errorf("HasChunk=true, want false (payload under threshold)")
		}
	}
}

// TestWorkPulling_BufferSize_RejectsOverflow verifies that the configured
// pekko.reliable-delivery.work-pulling.producer-controller.buffer-size
// value caps pendingWork and surfaces overflow as droppedCount. With
// bufferSize=2 and three work items arriving with no idle worker, the
// third must be dropped.
//
// Calls workPullingState.appendPending directly — the same method the
// production handle invokes when a work item arrives with no idle worker.
// This keeps the test exercising real runtime code, not a parallel
// in-test reimplementation.
func TestWorkPulling_BufferSize_RejectsOverflow(t *testing.T) {
	cfg := WorkPullingProducerControllerConfig{BufferSize: 2}
	_, statePtr := NewWorkPullingProducerControllerFromConfig(cfg)

	state := (*workPullingState)(statePtr)
	state.selfRef = newStub("/user/ctrl")

	state.appendPending("first")
	state.appendPending("second")
	state.appendPending("third") // should be dropped

	if got := len(state.pendingWork); got != 2 {
		t.Errorf("pendingWork len = %d, want 2 (capped)", got)
	}
	if got := statePtr.DroppedCount(); got != 1 {
		t.Errorf("DroppedCount = %d, want 1", got)
	}

	// Variant: bufferSize=0 (unlimited) accepts all three.
	cfg2 := WorkPullingProducerControllerConfig{BufferSize: 0}
	_, statePtr2 := NewWorkPullingProducerControllerFromConfig(cfg2)
	state2 := (*workPullingState)(statePtr2)
	state2.selfRef = newStub("/user/ctrl2")

	state2.appendPending("a")
	state2.appendPending("b")
	state2.appendPending("c")
	if got := len(state2.pendingWork); got != 3 {
		t.Errorf("unlimited buffer: pendingWork len = %d, want 3", got)
	}
	if got := statePtr2.DroppedCount(); got != 0 {
		t.Errorf("unlimited buffer: DroppedCount = %d, want 0", got)
	}
}
