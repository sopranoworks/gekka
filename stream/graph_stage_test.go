package stream_test

import (
	"testing"

	"github.com/sopranoworks/gekka/stream"
)

// ─── Custom filter stage via GraphStage ──────────────────────────────────────

// evenFilterStage is a custom GraphStage that filters out odd numbers.
type evenFilterStage struct{}

func (s *evenFilterStage) Shape() stream.FlowShape[int, int] {
	return stream.FlowShape[int, int]{
		In:  &stream.Inlet[int]{},
		Out: &stream.Outlet[int]{},
	}
}

func (s *evenFilterStage) CreateLogic(attrs stream.Attributes) (*stream.GraphStageLogic, stream.NotUsed) {
	logic := stream.NewGraphStageLogic()
	shape := s.Shape()

	logic.SetOutHandler(shape.Out, stream.OutHandlerFunc(func() {
		logic.Pull(shape.In)
	}))

	logic.SetInHandler(shape.In, stream.InHandlerFunc(func() {
		elem := logic.Grab(shape.In).(int)
		if elem%2 == 0 {
			logic.Push(shape.Out, elem)
		}
		// odd elements: don't push → filtered out (iterator recurses)
	}))

	return logic, stream.NotUsed{}
}

func TestGraphStage_CustomFilter(t *testing.T) {
	src := stream.FromSlice([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	flow := stream.GraphStageFlow[int, int](&evenFilterStage{})
	result, err := stream.RunWith(stream.Via(src, flow), stream.Collect[int](), stream.SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []int{2, 4, 6, 8, 10}
	if len(result) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, result)
	}
	for i, v := range expected {
		if result[i] != v {
			t.Errorf("result[%d] = %d, want %d", i, result[i], v)
		}
	}
}

// ─── Custom stateful counting stage ──────────────────────────────────────────

// countingStage is a custom GraphStage that emits (element, count) pairs.
type countingStage struct{}

type CountedItem struct {
	Value any
	Count int
}

func (s *countingStage) Shape() stream.FlowShape[string, CountedItem] {
	return stream.FlowShape[string, CountedItem]{
		In:  &stream.Inlet[string]{},
		Out: &stream.Outlet[CountedItem]{},
	}
}

func (s *countingStage) CreateLogic(attrs stream.Attributes) (*stream.GraphStageLogic, stream.NotUsed) {
	logic := stream.NewGraphStageLogic()
	shape := s.Shape()
	count := 0

	logic.SetOutHandler(shape.Out, stream.OutHandlerFunc(func() {
		logic.Pull(shape.In)
	}))

	logic.SetInHandler(shape.In, stream.InHandlerFunc(func() {
		elem := logic.Grab(shape.In).(string)
		count++
		logic.Push(shape.Out, CountedItem{Value: elem, Count: count})
	}))

	return logic, stream.NotUsed{}
}

func TestGraphStage_StatefulCounting(t *testing.T) {
	src := stream.FromSlice([]string{"a", "b", "c"})
	flow := stream.GraphStageFlow[string, CountedItem](&countingStage{})
	result, err := stream.RunWith(stream.Via(src, flow), stream.Collect[CountedItem](), stream.SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 items, got %d", len(result))
	}
	for i, item := range result {
		if item.Count != i+1 {
			t.Errorf("item[%d].Count = %d, want %d", i, item.Count, i+1)
		}
	}
	if result[0].Value != "a" || result[1].Value != "b" || result[2].Value != "c" {
		t.Errorf("unexpected values: %v", result)
	}
}

// ─── Custom source stage ─────────────────────────────────────────────────────

type rangeSourceStage struct {
	from, to int
}

func (s *rangeSourceStage) Shape() stream.SourceShape[int] {
	return stream.SourceShape[int]{Out: &stream.Outlet[int]{}}
}

func (s *rangeSourceStage) CreateLogic(attrs stream.Attributes) (*stream.GraphStageLogic, stream.NotUsed) {
	logic := stream.NewGraphStageLogic()
	shape := s.Shape()
	current := s.from

	logic.SetOutHandler(shape.Out, stream.OutHandlerFunc(func() {
		if current > s.to {
			logic.Complete(shape.Out)
			return
		}
		logic.Push(shape.Out, current)
		current++
	}))

	return logic, stream.NotUsed{}
}

func TestGraphStage_CustomSource(t *testing.T) {
	src := stream.GraphStageSource[int](&rangeSourceStage{from: 5, to: 8})
	result, err := stream.RunWith(src, stream.Collect[int](), stream.SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []int{5, 6, 7, 8}
	if len(result) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, result)
	}
	for i, v := range expected {
		if result[i] != v {
			t.Errorf("result[%d] = %d, want %d", i, result[i], v)
		}
	}
}

// ─── Custom sink stage ───────────────────────────────────────────────────────

type collectSinkStage struct {
	collected *[]int
}

func (s *collectSinkStage) Shape() stream.SinkShape[int] {
	return stream.SinkShape[int]{In: &stream.Inlet[int]{}}
}

func (s *collectSinkStage) CreateLogic(attrs stream.Attributes) (*stream.GraphStageLogic, stream.NotUsed) {
	logic := stream.NewGraphStageLogic()
	shape := s.Shape()

	logic.SetInHandler(shape.In, stream.InHandlerFunc(func() {
		elem := logic.Grab(shape.In).(int)
		*s.collected = append(*s.collected, elem)
	}))

	return logic, stream.NotUsed{}
}

func TestGraphStage_CustomSink(t *testing.T) {
	src := stream.FromSlice([]int{10, 20, 30})
	var collected []int
	sink := stream.GraphStageSink[int](&collectSinkStage{collected: &collected})
	_, err := stream.RunWith(src, sink, stream.SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(collected) != 3 {
		t.Fatalf("expected 3 items, got %d", len(collected))
	}
	expected := []int{10, 20, 30}
	for i, v := range expected {
		if collected[i] != v {
			t.Errorf("collected[%d] = %d, want %d", i, collected[i], v)
		}
	}
}

// ─── Stage failure propagation ───────────────────────────────────────────────

type failingStage struct{}

func (s *failingStage) Shape() stream.FlowShape[int, int] {
	return stream.FlowShape[int, int]{
		In:  &stream.Inlet[int]{},
		Out: &stream.Outlet[int]{},
	}
}

func (s *failingStage) CreateLogic(attrs stream.Attributes) (*stream.GraphStageLogic, stream.NotUsed) {
	logic := stream.NewGraphStageLogic()
	shape := s.Shape()

	logic.SetOutHandler(shape.Out, stream.OutHandlerFunc(func() {
		logic.Pull(shape.In)
	}))

	logic.SetInHandler(shape.In, stream.InHandlerFunc(func() {
		elem := logic.Grab(shape.In).(int)
		if elem == 3 {
			logic.Fail(stream.ErrTooManySubstreams) // reuse an existing error for testing
			return
		}
		logic.Push(shape.Out, elem)
	}))

	return logic, stream.NotUsed{}
}

func TestGraphStage_FailurePropagation(t *testing.T) {
	src := stream.FromSlice([]int{1, 2, 3, 4, 5})
	flow := stream.GraphStageFlow[int, int](&failingStage{})
	result, err := stream.RunWith(stream.Via(src, flow), stream.Collect[int](), stream.SyncMaterializer{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// Should have collected 1 and 2 before the error on 3.
	_ = result
}

// ─── Map-like stage (push every element) ─────────────────────────────────────

type doubleStage struct{}

func (s *doubleStage) Shape() stream.FlowShape[int, int] {
	return stream.FlowShape[int, int]{
		In:  &stream.Inlet[int]{},
		Out: &stream.Outlet[int]{},
	}
}

func (s *doubleStage) CreateLogic(attrs stream.Attributes) (*stream.GraphStageLogic, stream.NotUsed) {
	logic := stream.NewGraphStageLogic()
	shape := s.Shape()

	logic.SetOutHandler(shape.Out, stream.OutHandlerFunc(func() {
		logic.Pull(shape.In)
	}))

	logic.SetInHandler(shape.In, stream.InHandlerFunc(func() {
		elem := logic.Grab(shape.In).(int)
		logic.Push(shape.Out, elem*2)
	}))

	return logic, stream.NotUsed{}
}

func TestGraphStage_MapDouble(t *testing.T) {
	src := stream.FromSlice([]int{1, 2, 3})
	flow := stream.GraphStageFlow[int, int](&doubleStage{})
	result, err := stream.RunWith(stream.Via(src, flow), stream.Collect[int](), stream.SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []int{2, 4, 6}
	if len(result) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, result)
	}
	for i, v := range expected {
		if result[i] != v {
			t.Errorf("result[%d] = %d, want %d", i, result[i], v)
		}
	}
}
