// cmd/showcase-runner/allowlist_test.go
package main

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTmp(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "allowlist.md")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestAllowlist_LoadAbsentIsEmpty(t *testing.T) {
	list, err := loadAllowlist("/nonexistent/path.md")
	if err != nil {
		t.Fatalf("err=%v want nil", err)
	}
	if len(list) != 0 {
		t.Fatalf("want empty, got %d", len(list))
	}
}

func TestAllowlist_RejectsLevelField(t *testing.T) {
	path := writeTmp(t, `
- pattern: "foo"
  scope: "any"
  level: "WARN"
  rationale: "x"
  evidence: "docs/x.md"
`)
	_, err := loadAllowlist(path)
	if err == nil {
		t.Fatal("expected error rejecting level field")
	}
}

func TestAllowlist_RejectsMissingEvidence(t *testing.T) {
	path := writeTmp(t, `
- pattern: "foo"
  scope: "any"
  rationale: "x"
  evidence: "/nonexistent/evidence.md"
`)
	_, err := loadAllowlist(path)
	if err == nil {
		t.Fatal("expected error rejecting missing evidence")
	}
}
