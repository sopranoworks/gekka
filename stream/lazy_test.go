package stream_test

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/sopranoworks/gekka/stream"
)

// ─── LazySource ──────────────────────────────────────────────────────────────

func TestLazySource_MaterializesOnPull(t *testing.T) {
	var materialized atomic.Bool

	src := stream.LazySource(func() (stream.Source[int, stream.NotUsed], error) {
		materialized.Store(true)
		return stream.FromSlice([]int{1, 2, 3}), nil
	})

	if materialized.Load() {
		t.Fatal("factory should not be called before pull")
	}

	result, err := stream.RunWith(src, stream.Collect[int](), stream.SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !materialized.Load() {
		t.Fatal("factory should have been called after pull")
	}

	expected := []int{1, 2, 3}
	if len(result) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, result)
	}
	for i, v := range expected {
		if result[i] != v {
			t.Errorf("result[%d] = %d, want %d", i, result[i], v)
		}
	}
}

func TestLazySource_FactoryError(t *testing.T) {
	testErr := errors.New("lazy source factory error")
	src := stream.LazySource(func() (stream.Source[int, stream.NotUsed], error) {
		return stream.Source[int, stream.NotUsed]{}, testErr
	})

	_, err := stream.RunWith(src, stream.Collect[int](), stream.SyncMaterializer{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, testErr) {
		t.Fatalf("expected %v, got %v", testErr, err)
	}
}

func TestLazySource_EmptySource(t *testing.T) {
	src := stream.LazySource(func() (stream.Source[int, stream.NotUsed], error) {
		return stream.FromSlice([]int{}), nil
	})

	result, err := stream.RunWith(src, stream.Collect[int](), stream.SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty result, got %v", result)
	}
}

// ─── LazySink ────────────────────────────────────────────────────────────────

func TestLazySink_MaterializesOnPush(t *testing.T) {
	var materialized atomic.Bool
	var collected []int

	sink := stream.LazySink(func() (stream.Sink[int, stream.NotUsed], error) {
		materialized.Store(true)
		return stream.Foreach(func(v int) {
			collected = append(collected, v)
		}), nil
	})

	if materialized.Load() {
		t.Fatal("factory should not be called before push")
	}

	src := stream.FromSlice([]int{10, 20, 30})
	_, err := stream.RunWith(src, sink, stream.SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !materialized.Load() {
		t.Fatal("factory should have been called after push")
	}

	expected := []int{10, 20, 30}
	if len(collected) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, collected)
	}
	for i, v := range expected {
		if collected[i] != v {
			t.Errorf("collected[%d] = %d, want %d", i, collected[i], v)
		}
	}
}

func TestLazySink_EmptyStream(t *testing.T) {
	var materialized atomic.Bool

	sink := stream.LazySink(func() (stream.Sink[int, stream.NotUsed], error) {
		materialized.Store(true)
		return stream.Ignore[int](), nil
	})

	src := stream.FromSlice([]int{})
	_, err := stream.RunWith(src, sink, stream.SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if materialized.Load() {
		t.Fatal("factory should NOT be called for empty stream")
	}
}

func TestLazySink_FactoryError(t *testing.T) {
	testErr := errors.New("lazy sink factory error")
	sink := stream.LazySink(func() (stream.Sink[int, stream.NotUsed], error) {
		return stream.Sink[int, stream.NotUsed]{}, testErr
	})

	src := stream.FromSlice([]int{1})
	_, err := stream.RunWith(src, sink, stream.SyncMaterializer{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, testErr) {
		t.Fatalf("expected %v, got %v", testErr, err)
	}
}

// ─── LazyFlow ────────────────────────────────────────────────────────────────

func TestLazyFlow_MaterializesOnFirstElement(t *testing.T) {
	var materialized atomic.Bool

	flow := stream.LazyFlow(func() (stream.Flow[int, int, stream.NotUsed], error) {
		materialized.Store(true)
		return stream.Map(func(x int) int { return x * 10 }), nil
	})

	if materialized.Load() {
		t.Fatal("factory should not be called before first element")
	}

	src := stream.FromSlice([]int{1, 2, 3})
	result, err := stream.RunWith(stream.Via(src, flow), stream.Collect[int](), stream.SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !materialized.Load() {
		t.Fatal("factory should have been called")
	}

	expected := []int{10, 20, 30}
	if len(result) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, result)
	}
	for i, v := range expected {
		if result[i] != v {
			t.Errorf("result[%d] = %d, want %d", i, result[i], v)
		}
	}
}

func TestLazyFlow_FactoryError(t *testing.T) {
	testErr := errors.New("lazy flow factory error")
	flow := stream.LazyFlow(func() (stream.Flow[int, int, stream.NotUsed], error) {
		return stream.Flow[int, int, stream.NotUsed]{}, testErr
	})

	src := stream.FromSlice([]int{1})
	_, err := stream.RunWith(stream.Via(src, flow), stream.Collect[int](), stream.SyncMaterializer{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, testErr) {
		t.Fatalf("expected %v, got %v", testErr, err)
	}
}

func TestLazyFlow_EmptyUpstream(t *testing.T) {
	var materialized atomic.Bool

	flow := stream.LazyFlow(func() (stream.Flow[int, int, stream.NotUsed], error) {
		materialized.Store(true)
		return stream.Map(func(x int) int { return x * 10 }), nil
	})

	src := stream.FromSlice([]int{})
	result, err := stream.RunWith(stream.Via(src, flow), stream.Collect[int](), stream.SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty result, got %v", result)
	}
}
