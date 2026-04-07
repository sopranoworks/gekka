/*
 * remote_deploy.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/sopranoworks/gekka/actor"
)

// RemoteScope specifies that an actor should be deployed on a remote node.
type RemoteScope struct {
	Address actor.Address
}

// RemoteDeploy extends Props with remote deployment information.
// When RemoteDeploy is set, ActorOf sends a deployment request to the target
// node instead of spawning locally.
type RemoteDeploy struct {
	// Scope identifies the remote node where the actor should be deployed.
	Scope RemoteScope

	// FactoryID is a registered factory identifier that the remote node can
	// resolve to create the actor. This replaces the non-serializable Props.New
	// closure.
	FactoryID string
}

// ── Actor factory registry ───────────────────────────────────────────────────

// actorFactoryRegistry maps string IDs to actor factory functions. Remote
// deployment uses this to resolve a factory on the target node.
var actorFactoryRegistry = make(map[string]func() actor.Actor)

// RegisterActorFactory registers a named factory so that remote nodes can
// spawn the same actor type by referencing factoryID.
//
//	gekka.RegisterActorFactory("my-echo", func() actor.Actor {
//	    return &EchoActor{BaseActor: actor.NewBaseActor()}
//	})
func RegisterActorFactory(factoryID string, factory func() actor.Actor) {
	actorFactoryRegistry[factoryID] = factory
}

// getActorFactory returns the factory registered under factoryID, or an error.
func getActorFactory(factoryID string) (func() actor.Actor, error) {
	f, ok := actorFactoryRegistry[factoryID]
	if !ok {
		return nil, fmt.Errorf("remote deploy: no factory registered for %q", factoryID)
	}
	return f, nil
}

// ── Protocol messages ────────────────────────────────────────────────────────

// RemoteActorDeployRequest is sent from the deploying node to the target node
// to request creation of an actor.
type RemoteActorDeployRequest struct {
	FactoryID string `json:"factoryId"`
	ActorName string `json:"actorName"`
	// RequestID allows the deployer to correlate the response.
	RequestID string `json:"requestId"`
}

// RemoteActorDeployResponse is sent back from the target node to confirm
// successful deployment or report an error.
type RemoteActorDeployResponse struct {
	RequestID string `json:"requestId"`
	ActorPath string `json:"actorPath,omitempty"`
	Error     string `json:"error,omitempty"`
}

// handleRemoteDeployRequest is called on the target node when it receives a
// deployment request. It spawns the actor locally and returns the response.
func handleRemoteDeployRequest(sys ActorSystem, req *RemoteActorDeployRequest) *RemoteActorDeployResponse {
	factory, err := getActorFactory(req.FactoryID)
	if err != nil {
		return &RemoteActorDeployResponse{
			RequestID: req.RequestID,
			Error:     err.Error(),
		}
	}

	ref, err := sys.ActorOf(Props{New: factory}, req.ActorName)
	if err != nil {
		return &RemoteActorDeployResponse{
			RequestID: req.RequestID,
			Error:     err.Error(),
		}
	}

	return &RemoteActorDeployResponse{
		RequestID: req.RequestID,
		ActorPath: ref.Path(),
	}
}

// DeployRemote sends a deployment request to the remote node identified by
// deploy.Scope.Address. It returns an ActorRef pointing to the remotely
// spawned actor.
//
// The target node must have the same factoryID registered via
// RegisterActorFactory. The function blocks until the remote node responds
// or ctx is cancelled.
func DeployRemote(ctx context.Context, sys ActorSystem, name string, deploy RemoteDeploy) (ActorRef, error) {
	if deploy.FactoryID == "" {
		return ActorRef{}, fmt.Errorf("remote deploy: FactoryID must not be empty")
	}

	addr := deploy.Scope.Address
	reqID := fmt.Sprintf("deploy-%s-%s", addr.Host, name)

	req := &RemoteActorDeployRequest{
		FactoryID: deploy.FactoryID,
		ActorName: name,
		RequestID: reqID,
	}

	data, err := json.Marshal(req)
	if err != nil {
		return ActorRef{}, fmt.Errorf("remote deploy: marshal request: %w", err)
	}

	// Send the deploy request to the remote node's system actor.
	deployPath := fmt.Sprintf("pekko://%s@%s:%d/system/remote-deployer",
		addr.System, addr.Host, addr.Port)

	reply, err := sys.Ask(ctx, deployPath, data)
	if err != nil {
		return ActorRef{}, fmt.Errorf("remote deploy: ask failed: %w", err)
	}

	var resp RemoteActorDeployResponse
	if err := json.Unmarshal(reply.Payload, &resp); err != nil {
		return ActorRef{}, fmt.Errorf("remote deploy: unmarshal response: %w", err)
	}

	if resp.Error != "" {
		return ActorRef{}, fmt.Errorf("remote deploy: remote error: %s", resp.Error)
	}

	// Create a remote ActorRef pointing to the deployed actor.
	return sys.RemoteActorOf(addr, resp.ActorPath), nil
}

// RemoteDeployerActor handles incoming RemoteActorDeployRequest messages on
// the target node. Register it as a system actor at "/system/remote-deployer":
//
//	sys.SystemActorOf(typed.Receive(func(ctx typed.TypedContext[any], msg any) typed.Behavior[any] {
//	    gekka.HandleDeployMessage(sys, msg)
//	    return typed.Same[any]()
//	}), "remote-deployer")
func HandleDeployMessage(sys ActorSystem, senderRef actor.Ref, msg any) {
	data, ok := msg.([]byte)
	if !ok {
		return
	}

	var req RemoteActorDeployRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return
	}

	// Only handle deploy requests.
	if req.FactoryID == "" {
		return
	}

	resp := handleRemoteDeployRequest(sys, &req)

	respData, err := json.Marshal(resp)
	if err != nil {
		return
	}

	if senderRef != nil {
		senderRef.Tell(respData)
	}
}
