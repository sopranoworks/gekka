// cmd/showcase-runner/membership.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type memberStatus struct {
	state struct {
		body []byte
	}
	upCount     int
	nonUp       []memberRow
	unreachable []string
}

type memberRow struct {
	node   string
	status string
}

type membersAPI struct {
	Members []struct {
		Node   string `json:"node"`
		Status string `json:"status"`
	} `json:"members"`
	Unreachable []string `json:"unreachable"`
}

func parseMembersJSON(b []byte) (memberStatus, error) {
	var m membersAPI
	if err := json.Unmarshal(b, &m); err != nil {
		return memberStatus{}, err
	}
	var st memberStatus
	for _, row := range m.Members {
		if row.Status == "Up" {
			st.upCount++
		} else {
			st.nonUp = append(st.nonUp, memberRow{node: row.Node, status: row.Status})
		}
	}
	st.unreachable = append(st.unreachable, m.Unreachable...)
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
	var buf [16 * 1024]byte
	n, _ := resp.Body.Read(buf[:])
	return parseMembersJSON(buf[:n])
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
