/*
 * graph_dsl.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

// Builder allows manual wiring of stream stages into complex topologies.
type Builder struct {
	nodes []node
	wires []wire
	idSrc int
}

type node struct {
	g     any // The Graph[S, Mat] component
	shape Shape
}

type wire struct {
	outId int
	inId  int
}

// NewBuilder creates an empty GraphDSL builder.
func NewBuilder() *Builder {
	return &Builder{}
}

// Add imports a graph component into the builder and returns its Shape
// for manual port wiring.
func Add[S Shape, Mat any](b *Builder, g Graph[S, Mat]) S {
	shape := g.Shape()

	for _, in := range shape.Inlets() {
		in.setID(b.nextID())
	}
	for _, out := range shape.Outlets() {
		out.setID(b.nextID())
	}

	b.nodes = append(b.nodes, node{g: g, shape: shape})
	return shape
}

func (b *Builder) nextID() int {
	b.idSrc++
	return b.idSrc
}

// Connect wires an output port to an input port. Both ports must belong
// to components already added to this builder.
func Connect[T any](b *Builder, out *Outlet[T], in *Inlet[T]) {
	b.wires = append(b.wires, wire{outId: out.id, inId: in.id})
}

// Build finalizes the graph. If all ports are connected it returns a
// RunnableGraph; otherwise it returns a partial Graph fragment.
func (b *Builder) Build() (RunnableGraph[NotUsed], error) {
	return RunnableGraph[NotUsed]{
		run: func(m Materializer) (NotUsed, error) {
			// 1. Materialize all nodes.
			allOutIters := make(map[int]any)
			allInConns := make(map[int]func(any))
			var allRunners []func() error

			for _, n := range b.nodes {
				ms := n.g.(interface {
					materialize(Materializer, Shape) materializedStage
				}).materialize(m, n.shape)

				for id, iter := range ms.outIters {
					allOutIters[id] = iter
				}
				for id, conn := range ms.inConns {
					allInConns[id] = conn
				}
				allRunners = append(allRunners, ms.runners...)
			}

			// 2. Connect the wires.
			for _, w := range b.wires {
				iter := allOutIters[w.outId]
				conn := allInConns[w.inId]
				if iter != nil && conn != nil {
					conn(iter)
				}
			}

			// 3. Run all runners.
			if _, ok := m.(ActorMaterializer); ok {
				gi := NewGraphInterpreter()
				errCh := make(chan error, len(allRunners))
				for _, r := range allRunners {
					runner := r
					gi.Go(func() {
						if err := runner(); err != nil {
							select {
							case errCh <- err:
							default:
							}
						}
					})
				}
				gi.Wait()
				select {
				case err := <-errCh:
					return NotUsed{}, err
				default:
					return NotUsed{}, nil
				}
			}

			// Default: Sequential execution for SyncMaterializer.
			for _, r := range allRunners {
				if err := r(); err != nil {
					return NotUsed{}, err
				}
			}

			return NotUsed{}, nil
		},
	}, nil
}
