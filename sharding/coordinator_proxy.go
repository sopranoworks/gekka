/*
 * coordinator_proxy.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"context"
	"fmt"
	"time"
	"github.com/sopranoworks/gekka/actor"
)

// CoordinatorSender is an interface for sending messages to the coordinator.
type CoordinatorSender interface {
	Send(ctx context.Context, msg any) error
	SendWithSender(ctx context.Context, path string, senderPath string, msg any) error
}

// ShardCoordinatorProxy is an actor that routes messages to the ShardCoordinator singleton.
type ShardCoordinatorProxy struct {
	actor.BaseActor
	sender CoordinatorSender
	stash  []actor.Envelope
	timer  *time.Timer
}

func NewShardCoordinatorProxy(sender CoordinatorSender) *ShardCoordinatorProxy {
	return &ShardCoordinatorProxy{
		BaseActor: actor.NewBaseActor(),
		sender:    sender,
	}
}

func (p *ShardCoordinatorProxy) Receive(msg any) {
	switch msg.(type) {
	case retryFlush:
		p.flushStash()
		return
	}

	sender := p.Sender()
	p.Log().Debug("Proxy received message", "type", fmt.Sprintf("%T", msg), "sender", sender.Path())

	// Try to send
	var err error
	if sender != nil {
		err = p.sender.SendWithSender(context.Background(), "", sender.Path(), msg)
	} else {
		err = p.sender.Send(context.Background(), msg)
	}

	if err == nil {
		p.Log().Debug("Forwarded message to ShardCoordinator")
		p.flushStash()
		return
	}

	p.Log().Warn("Failed to forward message, stashing", "error", err)
	// Stash if failed (likely coordinator not yet available)
	p.stash = append(p.stash, actor.Envelope{Payload: msg, Sender: sender})
	if len(p.stash) > 100 {
		p.stash = p.stash[1:] // Limit stash size
	}
	
	p.scheduleRetry()
}

func (p *ShardCoordinatorProxy) flushStash() {
	if len(p.stash) == 0 {
		return
	}
	
	p.Log().Debug("Attempting to flush stashed messages", "count", len(p.stash))
	var remaining []actor.Envelope
	for _, e := range p.stash {
		var err error
		if e.Sender != nil {
			err = p.sender.SendWithSender(context.Background(), "", e.Sender.Path(), e.Payload)
		} else {
			err = p.sender.Send(context.Background(), e.Payload)
		}
		if err == nil {
			p.Log().Debug("Successfully flushed stashed message", "type", fmt.Sprintf("%T", e.Payload))
		} else {
			remaining = append(remaining, e)
		}
	}
	p.stash = remaining
	if len(p.stash) > 0 {
		p.scheduleRetry()
	}
}

func (p *ShardCoordinatorProxy) scheduleRetry() {
	if p.timer != nil {
		return
	}
	p.timer = time.AfterFunc(1*time.Second, func() {
		p.Self().Tell(retryFlush{}, p.Self())
	})
}

type retryFlush struct{}
