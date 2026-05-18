// cmd/showcase-runner/allowlist.go
package main

import (
	"fmt"
	"os"
	"regexp"
	"strings"
)

// loadAllowlist parses a Markdown file like docs/showcase-allowed-warnings.md.
// File absence is NOT an error; returns an empty list.
// Returns error if any entry has a "level:" field or an unreadable evidence path.
func loadAllowlist(path string) ([]allowEntry, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// Very simple block-parser: each entry starts with "- pattern:" and
	// continues until the next "- pattern:" or EOF.
	lines := strings.Split(string(data), "\n")
	var entries []allowEntry
	var cur map[string]string
	flush := func() error {
		if cur == nil {
			return nil
		}
		if _, has := cur["level"]; has {
			return fmt.Errorf("allowlist entry has forbidden 'level:' field (pattern=%q)", cur["pattern"])
		}
		ev := cur["evidence"]
		if ev == "" {
			return fmt.Errorf("allowlist entry missing 'evidence:' field (pattern=%q)", cur["pattern"])
		}
		if _, err := os.Stat(ev); err != nil {
			return fmt.Errorf("allowlist entry evidence not found (%s): %w", ev, err)
		}
		pat, err := regexp.Compile(cur["pattern"])
		if err != nil {
			return fmt.Errorf("bad regex %q: %w", cur["pattern"], err)
		}
		scope := cur["scope"]
		if scope == "" {
			scope = "any"
		}
		entries = append(entries, allowEntry{pattern: pat, scope: scope})
		cur = nil
		return nil
	}
	for _, line := range lines {
		t := strings.TrimSpace(line)
		if strings.HasPrefix(t, "- pattern:") {
			if err := flush(); err != nil {
				return nil, err
			}
			cur = map[string]string{}
			cur["pattern"] = strings.Trim(strings.TrimSpace(strings.TrimPrefix(t, "- pattern:")), `"`)
		} else if cur != nil {
			for _, k := range []string{"scope:", "rationale:", "evidence:", "level:"} {
				if strings.HasPrefix(t, k) {
					key := strings.TrimSuffix(k, ":")
					val := strings.Trim(strings.TrimSpace(strings.TrimPrefix(t, k)), `"`)
					cur[key] = val
				}
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return entries, nil
}
