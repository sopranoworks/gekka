// cmd/showcase-runner/summary.go
package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

type runSummary struct {
	RunID      string            `json:"run_id"`
	StartedAt  time.Time         `json:"started_at"`
	FinishedAt time.Time         `json:"finished_at"`
	Verdict    string            `json:"verdict"`        // "ShowcasePassed" or one of EXIT names
	ExitCode   int               `json:"exit_code"`
	Gate1At    time.Time         `json:"gate1_pass_at"`
	Gate2T0    time.Time         `json:"gate2_t0"`
	OffendingLine string         `json:"offending_line,omitempty"`
	OffendingSource string       `json:"offending_source,omitempty"`
	SingletonWarmupMisses map[string]int64 `json:"singleton_warmup_misses,omitempty"`
	// per-node sent/received counters (driver-observed via log scraping)
	FT1Counts map[string]int64 `json:"ft1_counts,omitempty"`
	FT2Counts map[string]int64 `json:"ft2_counts,omitempty"`
}

func writeSummary(artifactDir string, s runSummary) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil { return err }
	return os.WriteFile(filepath.Join(artifactDir, "summary.json"), b, 0o644)
}
