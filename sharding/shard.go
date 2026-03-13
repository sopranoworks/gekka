/*
 * shard.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"encoding/json"
	"fmt"
	"github.com/sopranoworks/gekka/actor"
)

// Shard manages entities within a shard.
type Shard struct {
	actor.BaseActor
	typeName           string
	entityCreator      func(ctx actor.ActorContext, entityId EntityId) (actor.Ref, error)
	messageUnmarshaler func(manifest string, data json.RawMessage) (any, error)
	entities           map[EntityId]actor.Ref
}

func NewShard(typeName string, creator func(ctx actor.ActorContext, entityId EntityId) (actor.Ref, error), unmarshaler func(string, json.RawMessage) (any, error)) *Shard {
	return &Shard{
		BaseActor:          actor.NewBaseActor(),
		typeName:           typeName,
		entityCreator:      creator,
		messageUnmarshaler: unmarshaler,
		entities:           make(map[EntityId]actor.Ref),
	}
}

func (s *Shard) Receive(msg any) {
	switch m := msg.(type) {
	case ShardingEnvelope:
		// Unmarshal the message if it's RawMessage
		var userMsg any = m.Message
		if s.messageUnmarshaler != nil {
			var err error
			userMsg, err = s.messageUnmarshaler(m.MessageManifest, m.Message)
			if err != nil {
				s.Log().Error("Failed to unmarshal user message", "manifest", m.MessageManifest, "error", err)
				return
			}
			s.Log().Debug("Unmarshaled user message", "type", fmt.Sprintf("%T", userMsg))
		}

		entity, ok := s.entities[m.EntityId]
		if !ok {
			// Spawn entity
			var err error
			entity, err = s.entityCreator(s.System(), m.EntityId)
			if err != nil {
				s.Log().Error("Failed to spawn entity", "entityId", m.EntityId, "error", err)
				return
			}
			s.entities[m.EntityId] = entity
		}
		entity.Tell(userMsg, s.Sender())

	case actor.Passivate:
		// Handle passivation request from entity
		entity := m.Entity
		// Find entityId by path
		for id, ref := range s.entities {
			if ref.Path() == entity.Path() {
				if stopper, ok := s.System().(interface{ Stop(actor.Ref) }); ok {
					stopper.Stop(entity)
				}
				delete(s.entities, id)
				s.Log().Info("Entity passivated", "entityId", id)
				break
			}
		}
	}
}
