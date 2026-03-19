/*
 * pubsub.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package pubsub

import (
	"context"

	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/cluster/pubsub"
)

// Topic represents a type-safe pub-sub topic for messages of type M.
type Topic[M any] struct{}

// ─── Topic Protocol ──────────────────────────────────────────────────────

// Command is the base interface for topic commands.
type Command[M any] interface {
	handle(name string, mediator pubsub.Mediator, ctx typed.TypedContext[any])
}

// Publish publishes a message to all subscribers of the topic.
type Publish[M any] struct {
	Message M
}

func (p Publish[M]) handle(name string, mediator pubsub.Mediator, ctx typed.TypedContext[any]) {
	err := mediator.Publish(context.Background(), name, p.Message)
	if err != nil {
		ctx.Log().Error("PubSub: failed to publish message", "topic", name, "error", err)
	}
}

// Subscribe registers a subscriber for the topic.
type Subscribe[M any] struct {
	Subscriber typed.TypedActorRef[M]
}

func (s Subscribe[M]) handle(name string, mediator pubsub.Mediator, ctx typed.TypedContext[any]) {
	err := mediator.Subscribe(context.Background(), name, "", s.Subscriber.Path())
	if err != nil {
		ctx.Log().Error("PubSub: failed to subscribe", "topic", name, "path", s.Subscriber.Path(), "error", err)
	}
}

// Unsubscribe removes a subscriber from the topic.
type Unsubscribe[M any] struct {
	Subscriber typed.TypedActorRef[M]
}

func (u Unsubscribe[M]) handle(name string, mediator pubsub.Mediator, ctx typed.TypedContext[any]) {
	err := mediator.Unsubscribe(context.Background(), name, "", u.Subscriber.Path())
	if err != nil {
		ctx.Log().Error("PubSub: failed to unsubscribe", "topic", name, "path", u.Subscriber.Path(), "error", err)
	}
}

// ─── Topic Behavior ──────────────────────────────────────────────────────

// Behavior returns a typed behavior for the topic.
// It acts as a local bridge to the distributed cluster pub-sub mediator.
func (Topic[M]) Behavior(name string, mediator pubsub.Mediator) typed.Behavior[any] {
	return func(ctx typed.TypedContext[any], msg any) typed.Behavior[any] {
		switch m := msg.(type) {
		case Command[M]:
			m.handle(name, mediator, ctx)
		}
		return typed.Same[any]()
	}
}
