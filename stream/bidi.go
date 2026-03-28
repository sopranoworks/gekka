/*
 * bidi.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

// BidiFlow[I1, O1, I2, O2] is a bidirectional flow stage with two independent
// transformation pipelines sharing a single graph node.
//
//	I1 → [forward:  I1→O1] → O1
//	O2 ← [backward: I2→O2] ← I2
//
// Use [BidiFlowFromFlows] to construct a BidiFlow from two existing Flows, then
// wire it into a GraphDSL topology via [Add], or close it into a single Flow
// with [BidiFlow.Join].
type BidiFlow[I1, O1, I2, O2 any] struct {
	forward  func(iterator[I1]) iterator[O1]
	backward func(iterator[I2]) iterator[O2]
}

// BidiFlowFromFlows constructs a [BidiFlow] from two independent Flows.
//
//   - top processes the forward direction: I1 → O1 (e.g. encode / frame).
//   - bottom processes the backward direction: I2 → O2 (e.g. decode / unframe).
func BidiFlowFromFlows[I1, O1, I2, O2 any](
	top Flow[I1, O1, NotUsed],
	bottom Flow[I2, O2, NotUsed],
) BidiFlow[I1, O1, I2, O2] {
	return BidiFlow[I1, O1, I2, O2]{
		forward: func(up iterator[I1]) iterator[O1] {
			out, _ := top.attach(up)
			return out
		},
		backward: func(up iterator[I2]) iterator[O2] {
			out, _ := bottom.attach(up)
			return out
		},
	}
}

// Shape returns the [BidiShape] of this BidiFlow.
func (b BidiFlow[I1, O1, I2, O2]) Shape() BidiShape[I1, O1, I2, O2] {
	return BidiShape[I1, O1, I2, O2]{
		In1:  &Inlet[I1]{},
		Out1: &Outlet[O1]{},
		In2:  &Inlet[I2]{},
		Out2: &Outlet[O2]{},
	}
}

// materialize implements the internal Graph interface so that BidiFlow can be
// added to a GraphDSL [Builder] via [Add].
//
// The two pipelines are wired independently:
//
//	In1 → forward  → Out1
//	In2 → backward → Out2
func (b BidiFlow[I1, O1, I2, O2]) materialize(_ Materializer, shape Shape) materializedStage {
	s := shape.(BidiShape[I1, O1, I2, O2])

	lazyIn1 := &lazyIterator[I1]{}
	lazyOut1 := &lazyIterator[O1]{}
	lazyIn2 := &lazyIterator[I2]{}
	lazyOut2 := &lazyIterator[O2]{}

	return materializedStage{
		outIters: map[int]any{
			s.Out1.id: lazyOut1,
			s.Out2.id: lazyOut2,
		},
		inConns: map[int]func(any){
			s.In1.id: func(up any) {
				lazyIn1.inner = up.(iterator[I1])
				lazyOut1.inner = b.forward(lazyIn1)
			},
			s.In2.id: func(up any) {
				lazyIn2.inner = up.(iterator[I2])
				lazyOut2.inner = b.backward(lazyIn2)
			},
		},
	}
}

// Join closes this BidiFlow by wiring its Out1 through flow into In2, returning
// a simple [Flow][I1, O2] that can be used in a linear pipeline.
//
// The resulting topology is:
//
//	I1 → [forward: I1→O1] → O1 → [flow: O1→I2] → I2 → [backward: I2→O2] → O2
//
// Example — build a framing codec around a transport flow:
//
//	codec := stream.BidiFlowFromFlows(frameEncoder, frameDecoder)
//	pipeline := codec.Join(transportFlow)
//	// pipeline : Flow[RawMessage, DecodedMessage, NotUsed]
func (b BidiFlow[I1, O1, I2, O2]) Join(flow Flow[O1, I2, NotUsed]) Flow[I1, O2, NotUsed] {
	return Flow[I1, O2, NotUsed]{
		attach: func(upstream iterator[I1]) (iterator[O2], NotUsed) {
			// I1 → forward → O1
			mid1 := b.forward(upstream)
			// O1 → flow → I2
			mid2, _ := flow.attach(mid1)
			// I2 → backward → O2
			out := b.backward(mid2)
			return out, NotUsed{}
		},
	}
}
