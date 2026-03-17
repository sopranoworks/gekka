/*
 * runnable.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

// RunnableGraph[Mat] is a fully connected stream topology — every input port
// is connected to an output port.  Call [RunnableGraph.Run] to materialize and
// execute it.
//
// A RunnableGraph is typically produced by [Source.To]:
//
//	graph := stream.Via(src, flow).To(sink)
//	mat, err := graph.Run(stream.SyncMaterializer{})
type RunnableGraph[Mat any] struct {
	run func(m Materializer) (Mat, error)
}

// Run materializes and executes the graph using m.  It returns the
// materialized value and any stream-level error.
//
// For [SyncMaterializer], Run blocks until the stream completes.
func (g RunnableGraph[Mat]) Run(m Materializer) (Mat, error) {
	return g.run(m)
}

// MustRun is like [Run] but panics if the stream returns an error.
// Useful in tests and simple scripts where error handling is not needed.
func (g RunnableGraph[Mat]) MustRun(m Materializer) Mat {
	v, err := g.run(m)
	if err != nil {
		panic("stream: MustRun: " + err.Error())
	}
	return v
}
