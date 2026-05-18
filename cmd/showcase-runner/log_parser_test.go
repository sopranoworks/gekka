// cmd/showcase-runner/log_parser_test.go
package main

import "testing"

func TestClassifyLine_ScalaWarn(t *testing.T) {
	c := classifyLine("[12:34:56.789] WARN  org.apache.pekko.X - foo")
	if c.level != "WARN" {
		t.Fatalf("got level=%q want WARN", c.level)
	}
}

func TestClassifyLine_ScalaError(t *testing.T) {
	c := classifyLine("[12:34:56.789] ERROR org.apache.pekko.X - bar")
	if c.level != "ERROR" {
		t.Fatalf("got level=%q want ERROR", c.level)
	}
}

func TestClassifyLine_GekkaWarn(t *testing.T) {
	c := classifyLine(`{"time":"2026-05-18T01:00:00Z","level":"WARN","msg":"x"}`)
	if c.level != "WARN" {
		t.Fatalf("got level=%q want WARN", c.level)
	}
}

func TestClassifyLine_GekkaError(t *testing.T) {
	c := classifyLine(`{"time":"2026-05-18T01:00:00Z","level":"ERROR","msg":"x"}`)
	if c.level != "ERROR" {
		t.Fatalf("got level=%q want ERROR", c.level)
	}
}

func TestClassifyLine_Info_IsNotCounted(t *testing.T) {
	c := classifyLine("[12:34:56.789] INFO  org.apache.pekko.X - hello")
	if c.level != "" {
		t.Fatalf("got level=%q want empty (INFO not counted)", c.level)
	}
}

func TestErrorBypassesAllowlist(t *testing.T) {
	allow := []allowEntry{{pattern: regexpMust("^.*ERROR.*$"), scope: "any"}}
	c := classifyLine("[12:34:56.789] ERROR org.apache.pekko.X - whatever")
	if isAllowed(c, allow) {
		t.Fatalf("ERROR must never be allowlisted, even with matching regex")
	}
}
