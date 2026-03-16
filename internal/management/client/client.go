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
