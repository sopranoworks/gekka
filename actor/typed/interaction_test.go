/*
 * interaction_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
)

// ── Helpers ────────────────────────────────────────────────────────────────

// mailboxRef is a Ref whose Tell delivers into a real mailbox channel.
type mailboxRef struct {
	path string
	mb   chan any
}

func (r *mailboxRef) Tell(msg any, sender ...actor.Ref) {
	select {
	case r.mb <- msg:
	default:
	}
}
func (r *mailboxRef) Path() string { return r.path }

// watchCapturingContext captures Watch calls without panicking.
type watchCapturingContext struct {
	actor.ActorContext
	watched []actor.Ref
}

func (c *watchCapturingContext) ActorOf(props actor.Props, name string) (actor.Ref, error) {
	return &typedMockRef{path: "/user/" + name}, nil
}
func (c *watchCapturingContext) Watch(watcher actor.Ref, target actor.Ref) {
	c.watched = append(c.watched, target)
}
func (c *watchCapturingContext) Stop(target actor.Ref) {}

// setupTestActor creates a minimal TypedActor[T] with a mailbox-backed self
// ref so that Tell actually delivers messages into the actor's mailbox.
func setupTestActor[T any](behavior Behavior[T]) (*TypedActor[T], TypedContext[T]) {
	ta := NewTypedActorInternal(behavior)
	selfRef := &mailboxRef{path: "/user/test", mb: ta.Mailbox()}
	ta.SetSelf(selfRef)
	ta.SetSystem(&watchCapturingContext{})
	ta.PreStart()
	return ta, ta.ctx
}

// ── P14-A: PipeToSelf Tests ───────────────────────────────────────────────

type pipeMsg struct {
	Value string
	Err   error
}

func TestPipeToSelf_Success(t *testing.T) {
	received := make(chan pipeMsg, 1)

	behavior := func(ctx TypedContext[pipeMsg], msg pipeMsg) Behavior[pipeMsg] {
		received <- msg
		return Same[pipeMsg]()
	}

	ta, ctx := setupTestActor(behavior)
	actor.Start(ta)
	defer ta.CloseMailbox()

	PipeToSelf(ctx, func() (string, error) {
		return "hello", nil
	}, func(val string, err error) pipeMsg {
		return pipeMsg{Value: val, Err: err}
	})

	select {
	case msg := <-received:
		if msg.Value != "hello" || msg.Err != nil {
			t.Errorf("expected {hello, nil}, got {%s, %v}", msg.Value, msg.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for PipeToSelf result")
	}
}

func TestPipeToSelf_Error(t *testing.T) {
	received := make(chan pipeMsg, 1)

	behavior := func(ctx TypedContext[pipeMsg], msg pipeMsg) Behavior[pipeMsg] {
		received <- msg
		return Same[pipeMsg]()
	}

	ta, ctx := setupTestActor(behavior)
	actor.Start(ta)
	defer ta.CloseMailbox()

	PipeToSelf(ctx, func() (string, error) {
		return "", errors.New("something failed")
	}, func(val string, err error) pipeMsg {
		return pipeMsg{Value: val, Err: err}
	})

	select {
	case msg := <-received:
		if msg.Err == nil || msg.Err.Error() != "something failed" {
			t.Errorf("expected error 'something failed', got %v", msg.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for PipeToSelf error result")
	}
}

func TestPipeToSelf_Panic(t *testing.T) {
	received := make(chan pipeMsg, 1)

	behavior := func(ctx TypedContext[pipeMsg], msg pipeMsg) Behavior[pipeMsg] {
		received <- msg
		return Same[pipeMsg]()
	}

	ta, ctx := setupTestActor(behavior)
	actor.Start(ta)
	defer ta.CloseMailbox()

	PipeToSelf(ctx, func() (string, error) {
		panic("boom")
	}, func(val string, err error) pipeMsg {
		return pipeMsg{Value: val, Err: err}
	})

	select {
	case msg := <-received:
		if msg.Err == nil {
			t.Fatal("expected error from panic, got nil")
		}
		if msg.Err.Error() != "pipeToSelf panic: boom" {
			t.Errorf("unexpected error message: %s", msg.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for PipeToSelf panic recovery")
	}
}

// ── P14-B: MessageAdapter Tests ───────────────────────────────────────────

type myMsg struct {
	Text string
}

type externalEvent struct {
	Data int
}

func TestMessageAdapter(t *testing.T) {
	received := make(chan myMsg, 1)

	behavior := func(ctx TypedContext[myMsg], msg myMsg) Behavior[myMsg] {
		received <- msg
		return Same[myMsg]()
	}

	ta, ctx := setupTestActor(behavior)
	actor.Start(ta)
	defer ta.CloseMailbox()

	adapter := MessageAdapter[myMsg, externalEvent](ctx, func(e externalEvent) myMsg {
		return myMsg{Text: fmt.Sprintf("adapted-%d", e.Data)}
	})

	adapter.Tell(externalEvent{Data: 42})

	select {
	case msg := <-received:
		if msg.Text != "adapted-42" {
			t.Errorf("expected 'adapted-42', got %q", msg.Text)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for adapted message")
	}
}

func TestMessageAdapter_Path(t *testing.T) {
	behavior := func(ctx TypedContext[myMsg], msg myMsg) Behavior[myMsg] {
		return Same[myMsg]()
	}
	_, ctx := setupTestActor(behavior)

	adapter := MessageAdapter[myMsg, externalEvent](ctx, func(e externalEvent) myMsg {
		return myMsg{}
	})

	if path := adapter.Path(); path != "/user/test/$messageAdapter" {
		t.Errorf("expected adapter path '/user/test/$messageAdapter', got %q", path)
	}
}

// ── P14-C: AskWithStatus Tests ────────────────────────────────────────────

type askReq struct {
	ReplyTo actor.Ref
}

func setupTargetActor[T any](behavior Behavior[T], path string) *TypedActor[T] {
	ta := NewTypedActorInternal(behavior)
	selfRef := &mailboxRef{path: path, mb: ta.Mailbox()}
	ta.SetSelf(selfRef)
	ta.SetSystem(&watchCapturingContext{})
	ta.PreStart()
	return ta
}

func TestAskWithStatus_Success(t *testing.T) {
	targetActor := setupTargetActor(func(ctx TypedContext[askReq], msg askReq) Behavior[askReq] {
		msg.ReplyTo.Tell(NewStatusReplySuccess[string]("result-ok"))
		return Same[askReq]()
	}, "/user/target")
	actor.Start(targetActor)
	defer targetActor.CloseMailbox()

	_, reqCtx := setupTestActor(func(ctx TypedContext[string], msg string) Behavior[string] {
		return Same[string]()
	})

	result, err := AskWithStatus[string, string](reqCtx, targetActor.Self(), func(replyTo actor.Ref) any {
		return askReq{ReplyTo: replyTo}
	}, 2*time.Second)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "result-ok" {
		t.Errorf("expected 'result-ok', got %q", result)
	}
}

func TestAskWithStatus_Error(t *testing.T) {
	targetActor := setupTargetActor(func(ctx TypedContext[askReq], msg askReq) Behavior[askReq] {
		msg.ReplyTo.Tell(NewStatusReplyError[string](errors.New("not found")))
		return Same[askReq]()
	}, "/user/target")
	actor.Start(targetActor)
	defer targetActor.CloseMailbox()

	_, reqCtx := setupTestActor(func(ctx TypedContext[string], msg string) Behavior[string] {
		return Same[string]()
	})

	_, err := AskWithStatus[string, string](reqCtx, targetActor.Self(), func(replyTo actor.Ref) any {
		return askReq{ReplyTo: replyTo}
	}, 2*time.Second)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "not found" {
		t.Errorf("expected 'not found', got %q", err.Error())
	}
}

func TestAskWithStatus_Timeout(t *testing.T) {
	targetActor := setupTargetActor(func(ctx TypedContext[askReq], msg askReq) Behavior[askReq] {
		return Same[askReq]() // never replies
	}, "/user/target")
	actor.Start(targetActor)
	defer targetActor.CloseMailbox()

	_, reqCtx := setupTestActor(func(ctx TypedContext[string], msg string) Behavior[string] {
		return Same[string]()
	})

	_, err := AskWithStatus[string, string](reqCtx, targetActor.Self(), func(replyTo actor.Ref) any {
		return askReq{ReplyTo: replyTo}
	}, 100*time.Millisecond)

	if !errors.Is(err, actor.ErrAskTimeout) {
		t.Errorf("expected ErrAskTimeout, got %v", err)
	}
}

// ── P14-D: WatchWith Tests ────────────────────────────────────────────────

type watchMsg struct {
	Kind string
}

func TestWatchWith_CustomTerminationMessage(t *testing.T) {
	received := make(chan watchMsg, 1)

	behavior := func(ctx TypedContext[watchMsg], msg watchMsg) Behavior[watchMsg] {
		received <- msg
		return Same[watchMsg]()
	}

	ta, ctx := setupTestActor(behavior)
	actor.Start(ta)
	defer ta.CloseMailbox()

	target := &typedMockRef{path: "/user/target"}
	customMsg := watchMsg{Kind: "target-died"}

	WatchWith(ctx, target, customMsg)

	// Simulate termination signal
	ta.Receive(mockTerminated{actor: target})

	select {
	case msg := <-received:
		if msg.Kind != "target-died" {
			t.Errorf("expected 'target-died', got %q", msg.Kind)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for watchWith message")
	}
}

// ── P14-E: ReceiveTimeout Tests ───────────────────────────────────────────

type timeoutMsg struct {
	Kind string
}

func TestReceiveTimeout_IdleActorReceivesTimeout(t *testing.T) {
	var count atomic.Int32
	var mu sync.Mutex
	var msgs []timeoutMsg

	behavior := func(ctx TypedContext[timeoutMsg], msg timeoutMsg) Behavior[timeoutMsg] {
		count.Add(1)
		mu.Lock()
		msgs = append(msgs, msg)
		mu.Unlock()
		return Same[timeoutMsg]()
	}

	ta, ctx := setupTestActor(behavior)
	actor.Start(ta)
	defer ta.CloseMailbox()

	SetReceiveTimeout(ctx, 100*time.Millisecond, timeoutMsg{Kind: "idle-timeout"})

	// Wait for timeout to fire
	time.Sleep(300 * time.Millisecond)

	if c := count.Load(); c < 1 {
		t.Fatalf("expected at least 1 timeout message, got %d", c)
	}

	mu.Lock()
	if msgs[0].Kind != "idle-timeout" {
		t.Errorf("expected 'idle-timeout', got %q", msgs[0].Kind)
	}
	mu.Unlock()
}

func TestReceiveTimeout_ActiveActorResetsTimer(t *testing.T) {
	var timeoutCount atomic.Int32

	behavior := func(ctx TypedContext[timeoutMsg], msg timeoutMsg) Behavior[timeoutMsg] {
		if msg.Kind == "idle-timeout" {
			timeoutCount.Add(1)
		}
		return Same[timeoutMsg]()
	}

	ta, ctx := setupTestActor(behavior)
	actor.Start(ta)
	defer ta.CloseMailbox()

	SetReceiveTimeout(ctx, 200*time.Millisecond, timeoutMsg{Kind: "idle-timeout"})

	// Keep sending messages to prevent timeout
	for i := 0; i < 5; i++ {
		time.Sleep(100 * time.Millisecond)
		ta.Self().Tell(timeoutMsg{Kind: "keep-alive"})
	}

	// No timeout should have fired during the active period
	if c := timeoutCount.Load(); c > 0 {
		t.Errorf("expected 0 timeout messages during active period, got %d", c)
	}
}

func TestCancelReceiveTimeout(t *testing.T) {
	var timeoutCount atomic.Int32

	behavior := func(ctx TypedContext[timeoutMsg], msg timeoutMsg) Behavior[timeoutMsg] {
		if msg.Kind == "idle-timeout" {
			timeoutCount.Add(1)
		}
		return Same[timeoutMsg]()
	}

	ta, ctx := setupTestActor(behavior)
	actor.Start(ta)
	defer ta.CloseMailbox()

	SetReceiveTimeout(ctx, 100*time.Millisecond, timeoutMsg{Kind: "idle-timeout"})

	// Cancel immediately
	CancelReceiveTimeout(ctx)

	time.Sleep(300 * time.Millisecond)

	if c := timeoutCount.Load(); c != 0 {
		t.Errorf("expected 0 timeout messages after cancel, got %d", c)
	}
}
