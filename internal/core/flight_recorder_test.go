package core

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"
)

func makeEvent(msg string) FlightEvent {
	return FlightEvent{
		Timestamp: time.Now(),
		Severity:  SeverityInfo,
		Category:  CatHandshake,
		Message:   msg,
	}
}

func TestRingBuffer_AppendAndSnapshot(t *testing.T) {
	rb := &RingBuffer{}
	rb.Append(makeEvent("e1"))
	rb.Append(makeEvent("e2"))
	rb.Append(makeEvent("e3"))

	snap := rb.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("expected 3 events, got %d", len(snap))
	}
	if snap[0].Message != "e1" || snap[1].Message != "e2" || snap[2].Message != "e3" {
		t.Fatalf("wrong order: %v", []string{snap[0].Message, snap[1].Message, snap[2].Message})
	}
}

func TestRingBuffer_Wraparound(t *testing.T) {
	rb := &RingBuffer{}
	for i := 0; i < flightRecorderBufSize+100; i++ {
		rb.Append(makeEvent(fmt.Sprintf("e%d", i)))
	}

	snap := rb.Snapshot()
	if len(snap) != flightRecorderBufSize {
		t.Fatalf("expected %d events, got %d", flightRecorderBufSize, len(snap))
	}
	if snap[0].Message != fmt.Sprintf("e%d", 100) {
		t.Fatalf("expected oldest e100, got %s", snap[0].Message)
	}
	if snap[len(snap)-1].Message != fmt.Sprintf("e%d", flightRecorderBufSize+99) {
		t.Fatalf("expected newest e%d, got %s", flightRecorderBufSize+99, snap[len(snap)-1].Message)
	}
}

func TestRingBuffer_Clear(t *testing.T) {
	rb := &RingBuffer{}
	rb.Append(makeEvent("e1"))
	rb.Clear()
	snap := rb.Snapshot()
	if len(snap) != 0 {
		t.Fatalf("expected 0 events after clear, got %d", len(snap))
	}
}

func TestFlightRecorder_Disabled(t *testing.T) {
	fr := NewFlightRecorder(false, LevelLifecycle)
	fr.Emit("10.0.0.1:2551", FlightEvent{
		Timestamp: time.Now(),
		Severity:  SeverityInfo,
		Category:  CatHandshake,
		Message:   "INITIATED",
	})
	snap := fr.Snapshot("10.0.0.1:2551")
	if len(snap) != 0 {
		t.Fatalf("disabled recorder should not store events, got %d", len(snap))
	}
}

func TestFlightRecorder_EmitLifecycle(t *testing.T) {
	fr := NewFlightRecorder(true, LevelLifecycle)
	fr.Emit("10.0.0.1:2551", FlightEvent{
		Timestamp: time.Now(),
		Severity:  SeverityInfo,
		Category:  CatHandshake,
		Message:   "ASSOCIATED",
	})
	snap := fr.Snapshot("10.0.0.1:2551")
	if len(snap) != 1 {
		t.Fatalf("expected 1 event, got %d", len(snap))
	}
	if snap[0].Message != "ASSOCIATED" {
		t.Fatalf("expected ASSOCIATED, got %s", snap[0].Message)
	}
}

func TestFlightRecorder_EmitFrame_LifecycleLevel_Skipped(t *testing.T) {
	fr := NewFlightRecorder(true, LevelLifecycle)
	for i := 0; i < 1000; i++ {
		fr.EmitFrame("10.0.0.1:2551", "send", 256, 4)
	}
	snap := fr.Snapshot("10.0.0.1:2551")
	if len(snap) != 0 {
		t.Fatalf("lifecycle level should not record frames, got %d", len(snap))
	}
}

func TestFlightRecorder_EmitFrame_SampledLevel(t *testing.T) {
	fr := NewFlightRecorder(true, LevelSampled)
	for i := 0; i < 10000; i++ {
		fr.EmitFrame("10.0.0.1:2551", "send", 256, 4)
	}
	snap := fr.Snapshot("10.0.0.1:2551")
	// 10000 frames at 1:100 sampling -> expect ~100 events (tolerance: 50-200)
	if len(snap) < 50 || len(snap) > 200 {
		t.Fatalf("sampled level: expected ~100 events, got %d", len(snap))
	}
}

func TestFlightRecorder_EmitFrame_FullLevel(t *testing.T) {
	fr := NewFlightRecorder(true, LevelFull)
	for i := 0; i < 500; i++ {
		fr.EmitFrame("10.0.0.1:2551", "send", 256, 4)
	}
	snap := fr.Snapshot("10.0.0.1:2551")
	if len(snap) != 500 {
		t.Fatalf("full level: expected 500 events, got %d", len(snap))
	}
}

func TestFlightRecorder_SnapshotAll(t *testing.T) {
	fr := NewFlightRecorder(true, LevelLifecycle)
	fr.Emit("10.0.0.1:2551", FlightEvent{
		Timestamp: time.Now(), Severity: SeverityInfo, Category: CatHandshake, Message: "A",
	})
	fr.Emit("10.0.0.2:2551", FlightEvent{
		Timestamp: time.Now(), Severity: SeverityInfo, Category: CatHandshake, Message: "B",
	})
	all := fr.SnapshotAll()
	if len(all) != 2 {
		t.Fatalf("expected 2 associations, got %d", len(all))
	}
	if len(all["10.0.0.1:2551"]) != 1 || len(all["10.0.0.2:2551"]) != 1 {
		t.Fatal("expected 1 event per association")
	}
}

func TestFlightEvent_FormatText(t *testing.T) {
	ts := time.Date(2026, 4, 11, 10, 23, 45, 1_000_000, time.UTC)
	e := FlightEvent{
		Timestamp: ts,
		Severity:  SeverityWarn,
		Category:  CatHeartbeat,
		Message:   "MISS",
		Fields:    map[string]any{"phi": 12.3},
	}
	text := e.FormatText()
	if !strings.Contains(text, "2026-04-11T10:23:45") {
		t.Fatalf("missing timestamp: %s", text)
	}
	if !strings.Contains(text, "WARN") {
		t.Fatalf("missing severity: %s", text)
	}
	if !strings.Contains(text, "heartbeat") {
		t.Fatalf("missing category: %s", text)
	}
	if !strings.Contains(text, "MISS") {
		t.Fatalf("missing message: %s", text)
	}
	if !strings.Contains(text, "phi=12.3") {
		t.Fatalf("missing phi field: %s", text)
	}
}

func TestFlightRecorder_DumpAll(t *testing.T) {
	fr := NewFlightRecorder(true, LevelLifecycle)
	fr.Emit("10.0.0.1:2551", FlightEvent{
		Timestamp: time.Now(), Severity: SeverityInfo, Category: CatHandshake, Message: "ASSOCIATED",
	})
	fr.Emit("10.0.0.2:2551", FlightEvent{
		Timestamp: time.Now(), Severity: SeverityError, Category: CatQuarantine, Message: "QUARANTINED",
	})

	var buf bytes.Buffer
	fr.DumpAll(&buf)
	output := buf.String()

	if !strings.Contains(output, "10.0.0.1:2551") {
		t.Fatalf("missing association 1 in dump: %s", output)
	}
	if !strings.Contains(output, "10.0.0.2:2551") {
		t.Fatalf("missing association 2 in dump: %s", output)
	}
	if !strings.Contains(output, "ASSOCIATED") {
		t.Fatalf("missing ASSOCIATED event: %s", output)
	}
	if !strings.Contains(output, "QUARANTINED") {
		t.Fatalf("missing QUARANTINED event: %s", output)
	}
}
