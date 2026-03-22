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
