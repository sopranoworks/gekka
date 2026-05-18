// cmd/showcase-runner/log_parser.go
package main

import (
	"encoding/json"
	"regexp"
)

type classification struct {
	level string // "WARN", "ERROR", or ""
	text  string
}

var scalaLevelRE = regexp.MustCompile(`^\[.*?]\s+(WARN|ERROR)\s+`)

func classifyLine(line string) classification {
	if m := scalaLevelRE.FindStringSubmatch(line); m != nil {
		return classification{level: m[1], text: line}
	}
	// JSON / gekka path
	if len(line) > 0 && line[0] == '{' {
		var p struct {
			Level string `json:"level"`
		}
		if err := json.Unmarshal([]byte(line), &p); err == nil {
			switch p.Level {
			case "WARN", "ERROR":
				return classification{level: p.Level, text: line}
			}
		}
	}
	return classification{level: "", text: line}
}

type allowEntry struct {
	pattern *regexp.Regexp
	scope   string // node-name or "any"
}

func regexpMust(s string) *regexp.Regexp { return regexp.MustCompile(s) }

// isAllowed returns true ONLY for WARN classifications that match an entry.
// ERROR is NEVER allowlisted — this satisfies spec Revision 1 §6.2 and §6.4.
func isAllowed(c classification, list []allowEntry) bool {
	if c.level != "WARN" {
		return false // ERROR bypasses; empty-level lines are not counted upstream anyway
	}
	for _, e := range list {
		if e.pattern.MatchString(c.text) {
			return true
		}
	}
	return false
}
