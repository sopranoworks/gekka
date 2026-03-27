/*
 * external_strategy.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/sopranoworks/gekka/actor"
)

type ExternalShardAllocationStrategy struct {
	url      string
	timeout  time.Duration
	client   *http.Client
	fallback ShardAllocationStrategy
}

func NewExternalShardAllocationStrategy(url string, timeout time.Duration, fallback ShardAllocationStrategy) *ExternalShardAllocationStrategy {
	return &ExternalShardAllocationStrategy{
		url:      url,
		timeout:  timeout,
		client:   &http.Client{Timeout: timeout},
		fallback: fallback,
	}
}

type allocateShardRequest struct {
	Action                  string              `json:"action"`
	Requester               string              `json:"requester"`
	ShardId                 ShardId             `json:"shardId"`
	CurrentShardAllocations map[string][]string `json:"currentShardAllocations"`
}

type allocateShardResponse struct {
	Region string `json:"region"`
}

type rebalanceRequest struct {
	Action                  string              `json:"action"`
	CurrentShardAllocations map[string][]string `json:"currentShardAllocations"`
	RebalanceInProgress     []ShardId           `json:"rebalanceInProgress"`
}

type rebalanceResponse struct {
	Shards []ShardId `json:"shards"`
}

func formatAllocations(allocs map[actor.Ref][]ShardId) map[string][]string {
	res := make(map[string][]string, len(allocs))
	for k, v := range allocs {
		strV := make([]string, len(v))
		for i, id := range v {
			strV[i] = string(id)
		}
		if k != nil {
			res[k.Path()] = strV
		} else {
			res["nil"] = strV
		}
	}
	return res
}

func (s *ExternalShardAllocationStrategy) AllocateShard(requester actor.Ref, shardId ShardId, currentShardAllocations map[actor.Ref][]ShardId) actor.Ref {
	if s.url == "" {
		return s.fallback.AllocateShard(requester, shardId, currentShardAllocations)
	}

	reqBody := allocateShardRequest{
		Action:                  "allocate",
		Requester:               requester.Path(),
		ShardId:                 shardId,
		CurrentShardAllocations: formatAllocations(currentShardAllocations),
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return s.fallback.AllocateShard(requester, shardId, currentShardAllocations)
	}

	resp, err := s.client.Post(s.url, "application/json", bytes.NewReader(data))
	if err != nil {
		return s.fallback.AllocateShard(requester, shardId, currentShardAllocations)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return s.fallback.AllocateShard(requester, shardId, currentShardAllocations)
	}

	var result allocateShardResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return s.fallback.AllocateShard(requester, shardId, currentShardAllocations)
	}

	for k := range currentShardAllocations {
		if k != nil && k.Path() == result.Region {
			return k
		}
	}

	return s.fallback.AllocateShard(requester, shardId, currentShardAllocations)
}

func (s *ExternalShardAllocationStrategy) Rebalance(currentShardAllocations map[actor.Ref][]ShardId, rebalanceInProgress []ShardId) []ShardId {
	if s.url == "" {
		return s.fallback.Rebalance(currentShardAllocations, rebalanceInProgress)
	}

	reqBody := rebalanceRequest{
		Action:                  "rebalance",
		CurrentShardAllocations: formatAllocations(currentShardAllocations),
		RebalanceInProgress:     rebalanceInProgress,
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return s.fallback.Rebalance(currentShardAllocations, rebalanceInProgress)
	}

	resp, err := s.client.Post(s.url, "application/json", bytes.NewReader(data))
	if err != nil {
		return s.fallback.Rebalance(currentShardAllocations, rebalanceInProgress)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return s.fallback.Rebalance(currentShardAllocations, rebalanceInProgress)
	}

	var result rebalanceResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return s.fallback.Rebalance(currentShardAllocations, rebalanceInProgress)
	}

	return result.Shards
}
