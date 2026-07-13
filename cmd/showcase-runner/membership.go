// cmd/showcase-runner/membership.go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type memberStatus struct {
	upCount     int
	nonUp       []memberRow
	unreachable []string
}

type memberRow struct {
	node   string
	status string
}

// membersAPI is Pekko Cluster HTTP Management's /cluster/members envelope.
// Unreachable entries are objects ({"node": ..., "observedBy": [...]}), per
// pekko-management-cluster-http's ClusterUnreachableMembership JSON.
type membersAPI struct {
	Members []struct {
		Node   string `json:"node"`
		Status string `json:"status"`
	} `json:"members"`
	Unreachable []struct {
		Node       string   `json:"node"`
		ObservedBy []string `json:"observedBy"`
	} `json:"unreachable"`
}

// gekkaMemberAPI is one element of gekka's /cluster/members response
// (internal/management/server.go MemberInfo) — a bare JSON array, with
// reachability carried per-member instead of in a separate list.
type gekkaMemberAPI struct {
	Address   string `json:"address"`
	Status    string `json:"status"`
	Reachable bool   `json:"reachable"`
}

// parseMembersJSON understands both membership response shapes present in
// the showcase topology: Pekko's object envelope and gekka's bare array.
// Both are reduced to the same memberStatus so pollAll applies one
// invariant — every member Up, nothing unreachable — to every node.
func parseMembersJSON(b []byte) (memberStatus, error) {
	trimmed := bytes.TrimLeft(b, " \t\r\n")
	var st memberStatus

	if len(trimmed) > 0 && trimmed[0] == '[' {
		var rows []gekkaMemberAPI
		if err := json.Unmarshal(b, &rows); err != nil {
			return memberStatus{}, err
		}
		for _, row := range rows {
			if row.Status == "Up" {
				st.upCount++
			} else {
				st.nonUp = append(st.nonUp, memberRow{node: row.Address, status: row.Status})
			}
			if !row.Reachable {
				st.unreachable = append(st.unreachable, row.Address)
			}
		}
		return st, nil
	}

	var m membersAPI
	if err := json.Unmarshal(b, &m); err != nil {
		return memberStatus{}, err
	}
	for _, row := range m.Members {
		if row.Status == "Up" {
			st.upCount++
		} else {
			st.nonUp = append(st.nonUp, memberRow{node: row.Node, status: row.Status})
		}
	}
	for _, u := range m.Unreachable {
		st.unreachable = append(st.unreachable, u.Node)
	}
	return st, nil
}

// pollOnce queries one node's mgmt endpoint at `host:port` and returns its view.
// Times out at 2 seconds (per spec §6.3).
func pollOnce(ctx context.Context, host string, port int) (memberStatus, error) {
	url := fmt.Sprintf("http://%s:%d/cluster/members", host, port)
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(cctx, "GET", url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return memberStatus{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return memberStatus{}, fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
	}
	// io.ReadAll, not a single Read: one Read call may legally return a
	// partial body, which then fails JSON parsing intermittently.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return memberStatus{}, fmt.Errorf("read body from %s: %w", url, err)
	}
	return parseMembersJSON(body)
}

// pollAll polls every node in `nodes`. Returns the first failure encountered
// or nil if all 8 nodes agree on 8-Up with no unreachable.
func pollAll(ctx context.Context, nodes []nodeEndpoint, requiredUp int) error {
	for _, n := range nodes {
		st, err := pollOnce(ctx, n.host, n.mgmtPort)
		if err != nil {
			return fmt.Errorf("%s mgmt poll: %w", n.label, err)
		}
		if st.upCount != requiredUp {
			return fmt.Errorf("%s reports %d Up (want %d)", n.label, st.upCount, requiredUp)
		}
		if len(st.nonUp) != 0 {
			return fmt.Errorf("%s sees non-Up peer: %+v", n.label, st.nonUp)
		}
		if len(st.unreachable) != 0 {
			return fmt.Errorf("%s reports unreachable: %v", n.label, st.unreachable)
		}
	}
	return nil
}

type nodeEndpoint struct {
	label    string
	host     string
	mgmtPort int
}
