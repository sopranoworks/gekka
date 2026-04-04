/*
 * context_flow_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"strings"
	"testing"

	"github.com/sopranoworks/gekka/stream"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func collectPairs[T, Ctx any](
	t *testing.T,
	src stream.SourceWithContext[T, Ctx, stream.NotUsed],
) []stream.Pair[T, Ctx] {
	t.Helper()
	var out []stream.Pair[T, Ctx]
	graph := src.To(stream.Foreach(func(p stream.Pair[T, Ctx]) {
		out = append(out, p)
	}))
	if _, err := graph.Run(stream.SyncMaterializer{}); err != nil {
		t.Fatalf("graph error: %v", err)
	}
	return out
}

// ── SourceWithContext ──────────────────────────────────────────────────────────

func TestSourceWithContext_IndexContext(t *testing.T) {
	src := stream.AsSourceWithContext(
		stream.FromSlice([]string{"a", "b", "c"}),
		func(i int, _ string) int { return i },
	)
	pairs := collectPairs(t, src)
	if len(pairs) != 3 {
		t.Fatalf("got %d pairs, want 3", len(pairs))
	}
	for i, p := range pairs {
		if p.Second != i {
			t.Errorf("pair[%d].ctx = %d, want %d", i, p.Second, i)
		}
	}
}

func TestSourceWithContext_ElementPreserved(t *testing.T) {
	src := stream.AsSourceWithContext(
		stream.FromSlice([]string{"hello", "world"}),
		func(_ int, s string) int { return len(s) },
	)
	pairs := collectPairs(t, src)
	if len(pairs) != 2 {
		t.Fatalf("got %d pairs, want 2", len(pairs))
	}
	if pairs[0].First != "hello" || pairs[0].Second != 5 {
		t.Errorf("pair[0] = %v, want {hello 5}", pairs[0])
	}
	if pairs[1].First != "world" || pairs[1].Second != 5 {
		t.Errorf("pair[1] = %v, want {world 5}", pairs[1])
	}
}

func TestSourceWithContext_Map_PreservesContext(t *testing.T) {
	src := stream.AsSourceWithContext(
		stream.FromSlice([]string{"hello", "world"}),
		func(i int, _ string) int { return i },
	).Map(strings.ToUpper)

	pairs := collectPairs(t, src)
	if len(pairs) != 2 {
		t.Fatalf("got %d pairs, want 2", len(pairs))
	}
	if pairs[0].First != "HELLO" || pairs[0].Second != 0 {
		t.Errorf("pair[0] = %v, want {HELLO 0}", pairs[0])
	}
	if pairs[1].First != "WORLD" || pairs[1].Second != 1 {
		t.Errorf("pair[1] = %v, want {WORLD 1}", pairs[1])
	}
}

func TestSourceWithContext_Filter_DropsWithContext(t *testing.T) {
	src := stream.AsSourceWithContext(
		stream.FromSlice([]string{"", "hello", "", "world"}),
		func(i int, _ string) int { return i },
	).Filter(func(s string) bool { return s != "" })

	pairs := collectPairs(t, src)
	if len(pairs) != 2 {
		t.Fatalf("got %d pairs, want 2; got %v", len(pairs), pairs)
	}
	// Original indices 1 and 3.
	if pairs[0].First != "hello" || pairs[0].Second != 1 {
		t.Errorf("pair[0] = %v, want {hello 1}", pairs[0])
	}
	if pairs[1].First != "world" || pairs[1].Second != 3 {
		t.Errorf("pair[1] = %v, want {world 3}", pairs[1])
	}
}

func TestSourceWithContext_AsSource_ComposesWithStandardFlow(t *testing.T) {
	// AsSource() lets SourceWithContext plug into standard stream.Via.
	inner := stream.AsSourceWithContext(
		stream.FromSlice([]int{1, 2, 3}),
		func(i int, _ int) string { return "ctx" },
	).AsSource()

	// Map the Pair[int,string] to just the element.
	src := stream.Via(inner,
		stream.Map(func(p stream.Pair[int, string]) int { return p.First * 10 }))

	var got []int
	graph := src.To(stream.Foreach(func(n int) { got = append(got, n) }))
	if _, err := graph.Run(stream.SyncMaterializer{}); err != nil {
		t.Fatalf("graph error: %v", err)
	}
	want := []int{10, 20, 30}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("AsSource[%d] = %d, want %d", i, got[i], w)
		}
	}
}

// ── FlowWithContext ────────────────────────────────────────────────────────────

func TestFlowWithContext_Map(t *testing.T) {
	flow := stream.NewFlowWithContext[string, int]().
		Map(strings.ToUpper)

	src := stream.AsSourceWithContext(
		stream.FromSlice([]string{"a", "b"}),
		func(i int, _ string) int { return i * 100 },
	)
	result := stream.ViaWithContext(src, flow)
	pairs := collectPairs(t, result)

	if len(pairs) != 2 {
		t.Fatalf("FlowWithContext Map: got %d pairs, want 2", len(pairs))
	}
	if pairs[0].First != "A" || pairs[0].Second != 0 {
		t.Errorf("pair[0] = %v, want {A 0}", pairs[0])
	}
	if pairs[1].First != "B" || pairs[1].Second != 100 {
		t.Errorf("pair[1] = %v, want {B 100}", pairs[1])
	}
}

func TestFlowWithContext_Filter(t *testing.T) {
	flow := stream.NewFlowWithContext[int, string]().
		Filter(func(n int) bool { return n > 2 })

	src := stream.AsSourceWithContext(
		stream.FromSlice([]int{1, 2, 3, 4}),
		func(_ int, n int) string { return "x" },
	)
	result := stream.ViaWithContext(src, flow)
	pairs := collectPairs(t, result)

	if len(pairs) != 2 {
		t.Fatalf("FlowWithContext Filter: got %d pairs, want 2; got %v", len(pairs), pairs)
	}
	if pairs[0].First != 3 || pairs[1].First != 4 {
		t.Errorf("FlowWithContext Filter values: got %v", pairs)
	}
}

func TestFlowWithContext_ChainedOperators(t *testing.T) {
	flow := stream.NewFlowWithContext[string, int]().
		Filter(func(s string) bool { return len(s) > 0 }).
		Map(strings.ToUpper)

	src := stream.AsSourceWithContext(
		stream.FromSlice([]string{"hello", "", "world"}),
		func(i int, _ string) int { return i },
	)
	result := stream.ViaWithContext(src, flow)
	pairs := collectPairs(t, result)

	if len(pairs) != 2 {
		t.Fatalf("chained: got %d pairs, want 2", len(pairs))
	}
	if pairs[0].First != "HELLO" || pairs[0].Second != 0 {
		t.Errorf("chained pair[0] = %v, want {HELLO 0}", pairs[0])
	}
	if pairs[1].First != "WORLD" || pairs[1].Second != 2 {
		t.Errorf("chained pair[1] = %v, want {WORLD 2}", pairs[1])
	}
}

func TestFlowWithContext_ToFlow(t *testing.T) {
	// ToFlow converts to a plain Flow that works with standard Via.
	flow := stream.NewFlowWithContext[int, string]().
		Map(func(n int) int { return n * 2 }).
		ToFlow()

	src := stream.AsSourceWithContext(
		stream.FromSlice([]int{1, 2, 3}),
		func(_ int, _ int) string { return "tag" },
	).AsSource()

	result := stream.Via(src, flow)
	var got []stream.Pair[int, string]
	graph := result.To(stream.Foreach(func(p stream.Pair[int, string]) { got = append(got, p) }))
	if _, err := graph.Run(stream.SyncMaterializer{}); err != nil {
		t.Fatalf("ToFlow graph error: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("ToFlow: got %d elements, want 3", len(got))
	}
	for i, p := range got {
		if p.First != (i+1)*2 {
			t.Errorf("ToFlow[%d].First = %d, want %d", i, p.First, (i+1)*2)
		}
		if p.Second != "tag" {
			t.Errorf("ToFlow[%d].Second = %q, want %q", i, p.Second, "tag")
		}
	}
}

func TestSourceWithContextFrom_Roundtrip(t *testing.T) {
	// SourceWithContextFrom accepts an existing Source[Pair[T,Ctx]].
	pairs := stream.FromSlice([]stream.Pair[string, int]{
		{First: "a", Second: 1},
		{First: "b", Second: 2},
	})
	src := stream.SourceWithContextFrom[string, int, stream.NotUsed](pairs)
	got := collectPairs(t, src)
	if len(got) != 2 {
		t.Fatalf("SourceWithContextFrom: got %d pairs, want 2", len(got))
	}
	if got[0].First != "a" || got[0].Second != 1 {
		t.Errorf("pair[0] = %v", got[0])
	}
	if got[1].First != "b" || got[1].Second != 2 {
		t.Errorf("pair[1] = %v", got[1])
	}
}
