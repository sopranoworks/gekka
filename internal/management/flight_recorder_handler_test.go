/*
 * flight_recorder_handler_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/internal/core"
)

func TestFlightRecorderHandler_JSON(t *testing.T) {
	fr := core.NewFlightRecorder(true, core.LevelLifecycle)
	fr.Emit("10.0.0.1:2551", core.FlightEvent{
		Timestamp: time.Now(),
		Severity:  core.SeverityInfo,
		Category:  core.CatHandshake,
		Message:   "ASSOCIATED",
	})

	handler := flightRecorderHandler(fr)
	req := httptest.NewRequest("GET", "/flight-recorder/10.0.0.1%3A2551", nil)
	req.URL.Path = "/flight-recorder/10.0.0.1:2551"
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var events []core.FlightEvent
	if err := json.Unmarshal(w.Body.Bytes(), &events); err != nil {
		t.Fatalf("invalid JSON: %v\nbody: %s", err, w.Body.String())
	}
	if len(events) != 1 || events[0].Message != "ASSOCIATED" {
		t.Fatalf("unexpected events: %v", events)
	}
}

func TestFlightRecorderHandler_Text(t *testing.T) {
	fr := core.NewFlightRecorder(true, core.LevelLifecycle)
	fr.Emit("10.0.0.1:2551", core.FlightEvent{
		Timestamp: time.Now(),
		Severity:  core.SeverityWarn,
		Category:  core.CatHeartbeat,
		Message:   "MISS",
		Fields:    map[string]any{"phi": 12.3},
	})

	handler := flightRecorderHandler(fr)
	req := httptest.NewRequest("GET", "/flight-recorder/10.0.0.1:2551?format=text", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "MISS") {
		t.Fatalf("missing MISS in text output: %s", body)
	}
}

func TestFlightRecorderHandler_AllAssociations(t *testing.T) {
	fr := core.NewFlightRecorder(true, core.LevelLifecycle)
	fr.Emit("10.0.0.1:2551", core.FlightEvent{
		Timestamp: time.Now(), Severity: core.SeverityInfo, Category: core.CatHandshake, Message: "A",
	})
	fr.Emit("10.0.0.2:2551", core.FlightEvent{
		Timestamp: time.Now(), Severity: core.SeverityInfo, Category: core.CatHandshake, Message: "B",
	})

	handler := flightRecorderHandler(fr)
	req := httptest.NewRequest("GET", "/flight-recorder", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var result map[string][]core.FlightEvent
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON: %v\nbody: %s", err, w.Body.String())
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 associations, got %d", len(result))
	}
}

func TestFlightRecorderHandler_UnknownAssociation(t *testing.T) {
	fr := core.NewFlightRecorder(true, core.LevelLifecycle)
	handler := flightRecorderHandler(fr)
	req := httptest.NewRequest("GET", "/flight-recorder/unknown:9999", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var events []core.FlightEvent
	if err := json.Unmarshal(w.Body.Bytes(), &events); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected empty array, got %d", len(events))
	}
}
