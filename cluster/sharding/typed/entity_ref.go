/*
 * entity_ref.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster/sharding"
)

// EntityRef is a type-safe handle to a specific sharded entity.
type EntityRef[M any] struct {
	typeName string
	entityID string
	region   actor.Ref
}

// NewEntityRef creates a new type-safe EntityRef.
func NewEntityRef[M any](typeName string, entityID string, region actor.Ref) *EntityRef[M] {
	return &EntityRef[M]{
		typeName: typeName,
		entityID: entityID,
		region:   region,
	}
}

// Tell sends a message to the sharded entity.
func (r *EntityRef[M]) Tell(msg M) {
	data, _ := json.Marshal(msg)
	r.region.Tell(sharding.ShardingEnvelope{
		EntityId:        r.entityID,
		Message:         data,
		MessageManifest: reflect.TypeOf(msg).String(),
	})
}

// Ask sends a message to the sharded entity and waits for a reply.
// It follows the Akka Typed 'Ask' pattern.
func (r *EntityRef[M]) Ask(ctx context.Context, timeout time.Duration, msg any) (any, error) {
	return nil, fmt.Errorf("EntityRef.Ask: not fully implemented (requires system-wide Ask integration)")
}

// EntityId returns the entity identifier.
func (r *EntityRef[M]) EntityId() string {
	return r.entityID
}
