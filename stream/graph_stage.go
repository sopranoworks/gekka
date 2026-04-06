/*
 * graph_stage.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"errors"
	"sync"
)

// ─── GraphStage API ──────────────────────────────────────────────────────────
//
// GraphStage allows users to implement custom push/pull stages with
// InHandler/OutHandler callbacks, following the Pekko GraphStage pattern.

// Attributes holds metadata that flows through the stream graph.
type Attributes struct {
	Name string
}

// InHandler defines the callback interface for an input port.
type InHandler interface {
	// OnPush is called when an element is available from the upstream.
	OnPush()
}

// OutHandler defines the callback interface for an output port.
type OutHandler interface {
	// OnPull is called when the downstream is ready for an element.
	OnPull()
}

// GraphStageLogic holds the runtime state for a custom GraphStage.
// It provides methods to push/pull/complete/cancel on ports, and to
// register InHandler/OutHandler callbacks.
type GraphStageLogic struct {
	mu          sync.Mutex
	inHandlers  map[int]InHandler
	outHandlers map[int]OutHandler

	// pulled tracks which inlets have been pulled (demand signal).
	pulled map[int]bool
	// pushed tracks the element pushed to each outlet.
	pushed map[int]any
	// completed tracks which outlets have been completed.
	completed map[int]bool
	// cancelled tracks which inlets have been cancelled.
	cancelled map[int]bool
	// failed stores an error if the stage has failed.
	failed error

	// stageCompleteCallbacks are called when the stage completes.
	stageCompleteCallbacks []func()
}

// NewGraphStageLogic creates a new GraphStageLogic.
func NewGraphStageLogic() *GraphStageLogic {
	return &GraphStageLogic{
		inHandlers:  make(map[int]InHandler),
		outHandlers: make(map[int]OutHandler),
		pulled:      make(map[int]bool),
		pushed:      make(map[int]any),
		completed:   make(map[int]bool),
		cancelled:   make(map[int]bool),
	}
}

// SetHandler registers an InHandler for the given inlet.
func (l *GraphStageLogic) SetInHandler(in Port, handler InHandler) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inHandlers[in.getID()] = handler
}

// SetOutHandler registers an OutHandler for the given outlet.
func (l *GraphStageLogic) SetOutHandler(out Port, handler OutHandler) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.outHandlers[out.getID()] = handler
}

// Push sends an element downstream through the outlet.
func (l *GraphStageLogic) Push(out Port, elem any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pushed[out.getID()] = elem
}

// Pull signals demand to the upstream through the inlet.
func (l *GraphStageLogic) Pull(in Port) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pulled[in.getID()] = true
}

// Complete signals that no more elements will be emitted from this outlet.
func (l *GraphStageLogic) Complete(out Port) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.completed[out.getID()] = true
}

// Cancel signals that no more elements are needed from this inlet.
func (l *GraphStageLogic) Cancel(in Port) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cancelled[in.getID()] = true
}

// Fail signals a stage failure with the given error.
func (l *GraphStageLogic) Fail(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.failed = err
}

// CompleteStage signals that the entire stage is done.
func (l *GraphStageLogic) CompleteStage() {
	l.mu.Lock()
	cbs := l.stageCompleteCallbacks
	l.mu.Unlock()
	for _, cb := range cbs {
		cb()
	}
}

// Grab retrieves the element that was pushed to this inlet.
// Must be called after OnPush.
func (l *GraphStageLogic) Grab(in Port) any {
	l.mu.Lock()
	defer l.mu.Unlock()
	v := l.pushed[in.getID()]
	delete(l.pushed, in.getID())
	return v
}

// ─── GraphStage interface ────────────────────────────────────────────────────

// GraphStageWithMaterializedValue is the interface for user-defined custom
// push/pull stages. Users implement CreateLogic to return a GraphStageLogic
// that handles push/pull callbacks.
type GraphStageWithMaterializedValue[S Shape, Mat any] interface {
	Shape() S
	CreateLogic(attrs Attributes) (*GraphStageLogic, Mat)
}

// ─── FlowShape GraphStage convenience ────────────────────────────────────────

// GraphStageFlow wraps a user-defined GraphStage with FlowShape into a
// standard Flow that can be composed with Via/ViaFlow.
func GraphStageFlow[In, Out any](stage GraphStageWithMaterializedValue[FlowShape[In, Out], NotUsed]) Flow[In, Out, NotUsed] {
	return Flow[In, Out, NotUsed]{
		attach: func(upstream iterator[In]) (iterator[Out], NotUsed) {
			return newGraphStageIterator[In, Out](stage, upstream), NotUsed{}
		},
	}
}

// GraphStageSource wraps a user-defined GraphStage with SourceShape into a Source.
func GraphStageSource[T any](stage GraphStageWithMaterializedValue[SourceShape[T], NotUsed]) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			return newGraphStageSourceIterator[T](stage), NotUsed{}
		},
	}
}

// GraphStageSink wraps a user-defined GraphStage with SinkShape into a Sink.
func GraphStageSink[T any](stage GraphStageWithMaterializedValue[SinkShape[T], NotUsed]) Sink[T, NotUsed] {
	return Sink[T, NotUsed]{
		runWith: func(upstream iterator[T]) (NotUsed, error) {
			return NotUsed{}, runGraphStageSink[T](stage, upstream)
		},
	}
}

// ─── FlowShape stage iterator (pull-based execution engine) ──────────────────

// graphStageFlowIterator drives a FlowShape GraphStage by translating the
// pull-based iterator model into push/pull handler callbacks.
type graphStageFlowIterator[In, Out any] struct {
	upstream iterator[In]
	logic    *GraphStageLogic
	shape    FlowShape[In, Out]
	done     bool
	err      error
}

func newGraphStageIterator[In, Out any](
	stage GraphStageWithMaterializedValue[FlowShape[In, Out], NotUsed],
	upstream iterator[In],
) *graphStageFlowIterator[In, Out] {
	shape := stage.Shape()
	logic, _ := stage.CreateLogic(Attributes{})
	return &graphStageFlowIterator[In, Out]{
		upstream: upstream,
		logic:    logic,
		shape:    shape,
	}
}

func (g *graphStageFlowIterator[In, Out]) next() (Out, bool, error) {
	var zero Out
	if g.done {
		return zero, false, g.err
	}

	// Signal downstream demand by calling OnPull on the outlet handler.
	g.logic.mu.Lock()
	outHandler := g.logic.outHandlers[g.shape.Out.getID()]
	g.logic.mu.Unlock()

	if outHandler == nil {
		g.done = true
		return zero, false, errors.New("stream: GraphStage has no OutHandler set")
	}

	// Reset state for this pull cycle.
	g.logic.mu.Lock()
	delete(g.logic.pushed, g.shape.Out.getID())
	delete(g.logic.pulled, g.shape.In.getID())
	g.logic.mu.Unlock()

	// OnPull typically calls Pull(in), which we detect below.
	outHandler.OnPull()

	// Check if the stage completed or failed.
	g.logic.mu.Lock()
	if g.logic.failed != nil {
		g.done = true
		g.err = g.logic.failed
		g.logic.mu.Unlock()
		return zero, false, g.err
	}
	if g.logic.completed[g.shape.Out.getID()] {
		g.done = true
		g.logic.mu.Unlock()
		return zero, false, nil
	}
	// Check if an element was pushed directly in OnPull (e.g. from internal buffer).
	if elem, ok := g.logic.pushed[g.shape.Out.getID()]; ok {
		delete(g.logic.pushed, g.shape.Out.getID())
		g.logic.mu.Unlock()
		return elem.(Out), true, nil
	}
	wantsPull := g.logic.pulled[g.shape.In.getID()]
	g.logic.mu.Unlock()

	if !wantsPull {
		// Stage didn't pull or push — treat as completion.
		g.done = true
		return zero, false, nil
	}

	// The stage wants an upstream element — pull from upstream.
	elem, hasMore, err := g.upstream.next()
	if err != nil {
		g.done = true
		g.err = err
		return zero, false, err
	}
	if !hasMore {
		g.done = true
		return zero, false, nil
	}

	// Deliver the element to the InHandler via OnPush.
	g.logic.mu.Lock()
	g.logic.pushed[g.shape.In.getID()] = elem
	inHandler := g.logic.inHandlers[g.shape.In.getID()]
	g.logic.mu.Unlock()

	if inHandler == nil {
		g.done = true
		return zero, false, errors.New("stream: GraphStage has no InHandler set")
	}

	inHandler.OnPush()

	// After OnPush, check what the handler did.
	g.logic.mu.Lock()
	defer g.logic.mu.Unlock()

	if g.logic.failed != nil {
		g.done = true
		g.err = g.logic.failed
		return zero, false, g.err
	}
	if g.logic.completed[g.shape.Out.getID()] {
		g.done = true
		return zero, false, nil
	}

	if outElem, ok := g.logic.pushed[g.shape.Out.getID()]; ok {
		delete(g.logic.pushed, g.shape.Out.getID())
		return outElem.(Out), true, nil
	}

	// OnPush didn't push anything — this element was filtered out.
	// Recurse to pull the next element.
	g.logic.mu.Unlock()
	result, ok, err2 := g.next()
	g.logic.mu.Lock() // re-lock for the deferred unlock
	return result, ok, err2
}

// ─── SourceShape stage iterator ──────────────────────────────────────────────

type graphStageSourceIterator[T any] struct {
	logic *GraphStageLogic
	shape SourceShape[T]
	done  bool
	err   error
}

func newGraphStageSourceIterator[T any](
	stage GraphStageWithMaterializedValue[SourceShape[T], NotUsed],
) *graphStageSourceIterator[T] {
	shape := stage.Shape()
	logic, _ := stage.CreateLogic(Attributes{})
	return &graphStageSourceIterator[T]{
		logic: logic,
		shape: shape,
	}
}

func (g *graphStageSourceIterator[T]) next() (T, bool, error) {
	var zero T
	if g.done {
		return zero, false, g.err
	}

	g.logic.mu.Lock()
	outHandler := g.logic.outHandlers[g.shape.Out.getID()]
	delete(g.logic.pushed, g.shape.Out.getID())
	g.logic.mu.Unlock()

	if outHandler == nil {
		g.done = true
		return zero, false, errors.New("stream: GraphStage source has no OutHandler set")
	}

	outHandler.OnPull()

	g.logic.mu.Lock()
	defer g.logic.mu.Unlock()

	if g.logic.failed != nil {
		g.done = true
		g.err = g.logic.failed
		return zero, false, g.err
	}
	if g.logic.completed[g.shape.Out.getID()] {
		g.done = true
		return zero, false, nil
	}

	if elem, ok := g.logic.pushed[g.shape.Out.getID()]; ok {
		delete(g.logic.pushed, g.shape.Out.getID())
		return elem.(T), true, nil
	}

	g.done = true
	return zero, false, nil
}

// ─── SinkShape stage runner ──────────────────────────────────────────────────

func runGraphStageSink[T any](
	stage GraphStageWithMaterializedValue[SinkShape[T], NotUsed],
	upstream iterator[T],
) error {
	shape := stage.Shape()
	logic, _ := stage.CreateLogic(Attributes{})

	inHandler := func() InHandler {
		logic.mu.Lock()
		defer logic.mu.Unlock()
		return logic.inHandlers[shape.In.getID()]
	}()

	if inHandler == nil {
		return errors.New("stream: GraphStage sink has no InHandler set")
	}

	for {
		elem, hasMore, err := upstream.next()
		if err != nil {
			return err
		}
		if !hasMore {
			return nil
		}

		logic.mu.Lock()
		logic.pushed[shape.In.getID()] = elem
		logic.mu.Unlock()

		inHandler.OnPush()

		logic.mu.Lock()
		if logic.failed != nil {
			err := logic.failed
			logic.mu.Unlock()
			return err
		}
		if logic.cancelled[shape.In.getID()] {
			logic.mu.Unlock()
			return nil
		}
		logic.mu.Unlock()
	}
}

// ─── Convenience handler types ───────────────────────────────────────────────

// InHandlerFunc is a convenience adapter that allows using a plain function
// as an InHandler.
type InHandlerFunc func()

func (f InHandlerFunc) OnPush() { f() }

// OutHandlerFunc is a convenience adapter that allows using a plain function
// as an OutHandler.
type OutHandlerFunc func()

func (f OutHandlerFunc) OnPull() { f() }
