/*
 * stream_router.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"strings"
)

// LargeMessageRouter decides whether an outbound recipient path should be
// routed onto the dedicated large-message stream (streamId=3).
//
// Patterns are matched against either the path component of an actor URI
// ("pekko://Sys@host:port/user/large-1") or a bare actor path
// ("/user/large-1"). Supported pattern forms:
//
//   - Exact path:        "/user/large-1"   matches only "/user/large-1"
//   - Trailing wildcard: "/user/large-*"   matches "/user/large-1", "/user/large-99"
//   - Single segment "*": "/user/*/big"    matches "/user/foo/big" but not "/user/foo/bar/big"
//   - Double-star tail:  "/user/large/**"  matches "/user/large/anything/below"
//
// Empty pattern list → IsLarge always returns false. Patterns are compiled
// into segment slices once at construction time; matching is allocation-free.
type LargeMessageRouter struct {
	patterns [][]string // pre-split pattern segments; empty when no destinations configured
}

// NewLargeMessageRouter compiles the supplied path patterns. Whitespace-only
// or empty patterns are skipped. Patterns missing a leading "/" are accepted
// as relative ("user/large-*") and matched against the recipient's path tail.
func NewLargeMessageRouter(patterns []string) *LargeMessageRouter {
	r := &LargeMessageRouter{}
	for _, p := range patterns {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		segs := splitPathSegments(p)
		if len(segs) == 0 {
			continue
		}
		r.patterns = append(r.patterns, segs)
	}
	return r
}

// IsLarge reports whether recipient matches any configured pattern. It
// accepts both URI form ("pekko://Sys@host:1234/user/large-1") and bare
// path form ("/user/large-1").
func (r *LargeMessageRouter) IsLarge(recipient string) bool {
	if r == nil || len(r.patterns) == 0 {
		return false
	}
	path := pathOnly(recipient)
	if path == "" {
		return false
	}
	segs := splitPathSegments(path)
	if len(segs) == 0 {
		return false
	}
	for _, pat := range r.patterns {
		if matchSegments(pat, segs) {
			return true
		}
	}
	return false
}

// pathOnly extracts the path portion ("/user/...") from an actor URI or
// returns the input unchanged when it is already a bare path.
func pathOnly(recipient string) string {
	if i := strings.Index(recipient, "://"); i >= 0 {
		// scheme://authority/path — find the first '/' after the authority.
		rest := recipient[i+3:]
		if j := strings.Index(rest, "/"); j >= 0 {
			return rest[j:]
		}
		return ""
	}
	return recipient
}

// splitPathSegments splits "/user/large-1" into ["user", "large-1"].
func splitPathSegments(path string) []string {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	parts := strings.Split(path, "/")
	out := parts[:0]
	for _, p := range parts {
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

// matchSegments walks pattern and path segments side by side. "**" at the
// end of pattern matches one or more trailing segments — i.e. "/foo/**"
// matches "/foo/x" and "/foo/x/y/z" but NOT "/foo" itself, mirroring the
// "destinations under /foo" intuition for actor-path routing.
// "*" inside a segment behaves as a glob within that single segment.
func matchSegments(pattern, path []string) bool {
	for i, ps := range pattern {
		if ps == "**" && i == len(pattern)-1 {
			return len(path) > i // need at least one trailing segment
		}
		if i >= len(path) {
			return false
		}
		if !matchSegment(ps, path[i]) {
			return false
		}
	}
	return len(pattern) == len(path)
}

// matchSegment matches a single pattern segment against a single path
// segment. The pattern may contain "*" which globs within that segment.
func matchSegment(pattern, seg string) bool {
	if pattern == "*" {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return pattern == seg
	}
	// Glob within a single segment: split on '*' and walk left-to-right.
	parts := strings.Split(pattern, "*")
	rem := seg
	for i, part := range parts {
		switch {
		case i == 0:
			if !strings.HasPrefix(rem, part) {
				return false
			}
			rem = rem[len(part):]
		case i == len(parts)-1:
			return strings.HasSuffix(rem, part)
		default:
			idx := strings.Index(rem, part)
			if idx < 0 {
				return false
			}
			rem = rem[idx+len(part):]
		}
	}
	return true
}

