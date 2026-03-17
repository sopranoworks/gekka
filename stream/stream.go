/*
 * stream.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package stream implements a reactive-streams-inspired processing pipeline DSL
// for the Gekka actor framework.
//
// # Core Abstractions
//
//   - [Source][T, Mat]   — a stage with one output port, emitting elements of type T.
//   - [Flow][In, Out, Mat] — a stage with one input and one output port.
//   - [Sink][In, Mat]    — a stage with one input port that consumes elements.
//   - [RunnableGraph][Mat] — a fully connected topology, ready to be executed.
//   - [Materializer]     — the engine that executes a RunnableGraph.
//
// # DSL
//
// Stages are connected with the package-level [Via] function and the [Source.To]
// method:
//
//	graph := stream.Via(source, flow).To(sink)
//	graph.Run(materializer)
//
// # Back-pressure
//
// The internal signaling model is pull-based: each downstream stage pulls the
// next element from its upstream by calling iterator.next().  This gives natural
// back-pressure — an upstream stage never produces more elements than the
// downstream has requested.  The current synchronous [SyncMaterializer] runs the
// entire pipeline in the calling goroutine, providing a simple baseline before
// the async actor-backed materializer is introduced.
package stream

// NotUsed is the materialized value for stages that do not produce a meaningful
// result, analogous to akka.NotUsed / Unit.
type NotUsed = struct{}

// Materializer is the engine that executes [RunnableGraph] instances.
//
// The default implementation is [SyncMaterializer], which runs graphs
// synchronously in the calling goroutine.  [ActorMaterializer] provides
// actor-dispatcher-backed execution.
//
// Third-party implementations may embed [BaseMaterializer] to satisfy the
// interface without implementing all future hook methods.
type Materializer interface {
	isMaterializer()
}

// BaseMaterializer can be embedded by custom [Materializer] implementations to
// forward-compatibly satisfy the interface as new methods are added.
type BaseMaterializer struct{}

func (BaseMaterializer) isMaterializer() {}

// SyncMaterializer executes stream graphs synchronously in the calling
// goroutine.  All stages run inline — there is no concurrency between them.
// Suitable for testing and simple single-threaded use-cases.
type SyncMaterializer struct{ BaseMaterializer }

// ActorMaterializer uses the actor system's goroutine pool to execute stream
// graphs.  The initial implementation delegates to synchronous execution;
// fully async dispatch will be added in a later release.
type ActorMaterializer struct{ BaseMaterializer }

// ─── internal pull model ───────────────────────────────────────────────────

// iterator is the internal pull-based signaling interface between stages.
//
// A downstream stage requests the next element by calling next().  The call
// blocks (synchronously) until the element is available or the stream
// completes / errors.  This models the Reactive Streams "Request(1)" demand
// signal in a simple single-goroutine form.
//
//   - hasMore == false with err == nil  → stream completed normally.
//   - hasMore == false with err != nil  → stream failed with err.
type iterator[T any] interface {
	next() (elem T, hasMore bool, err error)
}

// sinkConnector is implemented by any Sink[T, *] and allows [Source.To] to
// accept a sink of any materialized-value type without losing element-type
// safety.
type sinkConnector[T any] interface {
	connect(upstream iterator[T]) error
}
