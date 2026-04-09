/*
 * client.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package client provides a lightweight HTTP client for the Gekka Cluster
// HTTP Management API.  It is shared between gekka-cli and gekka-metrics so
// that serialisation details live in a single place.
package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// MemberInfo is the JSON representation of a single cluster member as returned
// by GET /cluster/members and GET /cluster/members/{address}.
type MemberInfo struct {
	Address    string   `json:"address"`
	Status     string   `json:"status"`
	Roles      []string `json:"roles"`
	DataCenter string   `json:"dc"`
	UpNumber   int32    `json:"upNumber"`
	Reachable  bool     `json:"reachable"`
	LatencyMs  int64    `json:"latency_ms"`
	Self       bool     `json:"self"`
}

// Client is a thin HTTP client scoped to a single management API base URL.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// New returns a Client for the management server at baseURL
// (e.g. "http://127.0.0.1:8558").  A default 10-second timeout is applied.
func New(baseURL string) *Client {
	return &Client{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// Members calls GET /cluster/members and returns the parsed member list.
func (c *Client) Members() ([]MemberInfo, error) {
	endpoint := c.baseURL + "/cluster/members"

	resp, err := c.httpClient.Get(endpoint)
	if err != nil {
		return nil, fmt.Errorf("client: GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("client: read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("client: server returned %s: %s", resp.Status, body)
	}

	var members []MemberInfo
	if err := json.Unmarshal(body, &members); err != nil {
		return nil, fmt.Errorf("client: parse response: %w", err)
	}
	return members, nil
}

// ShardDistribution calls GET /cluster/sharding/{typeName} and returns the
// shard→region allocation map.  Returns an error when the server responds 404
// (no coordinator registered) or any other non-200 status.
func (c *Client) ShardDistribution(typeName string) (map[string]string, error) {
	endpoint := c.baseURL + "/cluster/sharding/" + typeName
	resp, err := c.httpClient.Get(endpoint)
	if err != nil {
		return nil, fmt.Errorf("client: GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("client: read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("client: server returned %s: %s", resp.Status, body)
	}
	var dist map[string]string
	if err := json.Unmarshal(body, &dist); err != nil {
		return nil, fmt.Errorf("client: parse response: %w", err)
	}
	return dist, nil
}

// DurableStateResponse is the JSON structure returned by GET /durable-state/{persistenceId}.
type DurableStateResponse struct {
	PersistenceID string `json:"persistence_id"`
	Revision      uint64 `json:"revision"`
	State         any    `json:"state"`
}

// DurableState calls GET /durable-state/{persistenceId} and returns the current state.
func (c *Client) DurableState(persistenceID string) (*DurableStateResponse, error) {
	endpoint := c.baseURL + "/durable-state/" + persistenceID
	resp, err := c.httpClient.Get(endpoint)
	if err != nil {
		return nil, fmt.Errorf("client: GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("client: read response: %w", err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil // not found
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("client: server returned %s: %s", resp.Status, body)
	}

	var res DurableStateResponse
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, fmt.Errorf("client: parse response: %w", err)
	}
	return &res, nil
}

// DownMember calls POST /cluster/members/{address}/down.
func (c *Client) DownMember(address string) error {
	endpoint := c.baseURL + "/cluster/members/" + url.PathEscape(address) + "/down"
	resp, err := c.httpClient.Post(endpoint, "application/json", nil)
	if err != nil {
		return fmt.Errorf("client: POST %s: %w", endpoint, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("client: server returned %s: %s", resp.Status, body)
	}
	return nil
}

// LeaveMember calls POST /cluster/members/{address}/leave.
func (c *Client) LeaveMember(address string) error {
	endpoint := c.baseURL + "/cluster/members/" + url.PathEscape(address) + "/leave"
	resp, err := c.httpClient.Post(endpoint, "application/json", nil)
	if err != nil {
		return fmt.Errorf("client: POST %s: %w", endpoint, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("client: server returned %s: %s", resp.Status, body)
	}
	return nil
}

// RebalanceShard calls POST /cluster/sharding/{typeName}/rebalance and
// requests that shardID be moved to targetRegion.
func (c *Client) RebalanceShard(typeName, shardID, targetRegion string) error {
	payload, _ := json.Marshal(map[string]string{
		"shard_id":      shardID,
		"target_region": targetRegion,
	})
	endpoint := c.baseURL + "/cluster/sharding/" + typeName + "/rebalance"
	resp, err := c.httpClient.Post(endpoint, "application/json", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("client: POST %s: %w", endpoint, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("client: read response: %w", err)
	}
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("client: server returned %s: %s", resp.Status, body)
	}
	return nil
}

// Services calls GET /cluster/services and returns the service→addresses map.
func (c *Client) Services() (map[string][]string, error) {
	endpoint := c.baseURL + "/cluster/services"
	resp, err := c.httpClient.Get(endpoint)
	if err != nil {
		return nil, fmt.Errorf("client: GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("client: read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("client: server returned %s: %s", resp.Status, body)
	}
	var out map[string][]string
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("client: parse response: %w", err)
	}
	return out, nil
}

// ConfigEntries calls GET /cluster/config and returns the flat key→value map.
// Keys are dotted (e.g. "pekko.cluster.roles"); values are arbitrary JSON.
func (c *Client) ConfigEntries() (map[string]any, error) {
	endpoint := c.baseURL + "/cluster/config"
	resp, err := c.httpClient.Get(endpoint)
	if err != nil {
		return nil, fmt.Errorf("client: GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("client: read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("client: server returned %s: %s", resp.Status, body)
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("client: parse response: %w", err)
	}
	return out, nil
}

// healthResponse matches the JSON shape returned by /health/alive and
// /health/ready: {"status":"...","reason":"..."}.
type healthResponse struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
}

// Alive calls GET /health/alive and returns (ok, status, err).
// ok=true iff HTTP 200; message is the "status" field.
func (c *Client) Alive() (bool, string, error) {
	return c.healthProbe("/health/alive")
}

// Ready calls GET /health/ready and returns (ok, message, err).
// ok=true iff HTTP 200; message is the "status" field on success or
// the "reason" field on 503.
func (c *Client) Ready() (bool, string, error) {
	return c.healthProbe("/health/ready")
}

func (c *Client) healthProbe(path string) (bool, string, error) {
	endpoint := c.baseURL + path
	resp, err := c.httpClient.Get(endpoint)
	if err != nil {
		return false, "", fmt.Errorf("client: GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, "", fmt.Errorf("client: read response: %w", err)
	}
	var hr healthResponse
	_ = json.Unmarshal(body, &hr) // tolerate non-JSON bodies
	if resp.StatusCode == http.StatusOK {
		return true, hr.Status, nil
	}
	msg := hr.Reason
	if msg == "" {
		msg = hr.Status
	}
	if msg == "" {
		msg = resp.Status
	}
	return false, msg, nil
}
