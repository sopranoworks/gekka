/*
 * debug.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package client

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// CRDTEntry mirrors internal/management.CRDTEntry for client consumers.
type CRDTEntry struct {
	Key  string `json:"key"`
	Type string `json:"type"`
}

// CRDTValue mirrors internal/management.CRDTValue for client consumers.
type CRDTValue struct {
	Key   string      `json:"key"`
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

// ActorEntry mirrors internal/management.ActorEntry for client consumers.
type ActorEntry struct {
	Path string `json:"path"`
	Kind string `json:"kind"`
}

// crdtListEnvelope is the shape returned by GET /cluster/debug/crdt.
type crdtListEnvelope struct {
	Kind     string      `json:"kind"`
	Data     []CRDTEntry `json:"data"`
	Warnings []string    `json:"warnings,omitempty"`
}

// crdtEnvelope is the shape returned by GET /cluster/debug/crdt/{key}.
type crdtEnvelope struct {
	Kind     string     `json:"kind"`
	Data     *CRDTValue `json:"data"`
	Warnings []string   `json:"warnings,omitempty"`
}

// actorsEnvelope is the shape returned by GET /cluster/debug/actors.
type actorsEnvelope struct {
	Kind     string       `json:"kind"`
	Data     []ActorEntry `json:"data"`
	Warnings []string     `json:"warnings,omitempty"`
}

// DebugCRDTList calls GET /cluster/debug/crdt.
func (c *Client) DebugCRDTList() ([]CRDTEntry, error) {
	body, err := c.debugGET("/cluster/debug/crdt")
	if err != nil {
		return nil, err
	}
	var env crdtListEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("client: parse response: %w", err)
	}
	return env.Data, nil
}

// DebugCRDT calls GET /cluster/debug/crdt/{key}.  Returns (nil, nil) on 404.
func (c *Client) DebugCRDT(key string) (*CRDTValue, error) {
	endpoint := c.baseURL + "/cluster/debug/crdt/" + key
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
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("client: server returned %s: %s", resp.Status, body)
	}
	var env crdtEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("client: parse response: %w", err)
	}
	return env.Data, nil
}

// DebugActors calls GET /cluster/debug/actors[?system=true].
func (c *Client) DebugActors(includeSystem bool) ([]ActorEntry, error) {
	path := "/cluster/debug/actors"
	if includeSystem {
		path += "?system=true"
	}
	body, err := c.debugGET(path)
	if err != nil {
		return nil, err
	}
	var env actorsEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("client: parse response: %w", err)
	}
	return env.Data, nil
}

// debugGET is a shared helper for simple GETs that return the envelope.
func (c *Client) debugGET(path string) ([]byte, error) {
	endpoint := c.baseURL + path
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
	return body, nil
}
