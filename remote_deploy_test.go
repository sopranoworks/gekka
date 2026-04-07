/*
 * remote_deploy_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"encoding/json"
	"testing"

	"github.com/sopranoworks/gekka/actor"
)

type remoteDeployTestActor struct {
	actor.BaseActor
}

func (a *remoteDeployTestActor) Receive(msg any) {}

func TestRegisterActorFactory(t *testing.T) {
	RegisterActorFactory("test-deploy-actor", func() actor.Actor {
		return &remoteDeployTestActor{BaseActor: actor.NewBaseActor()}
	})

	f, err := getActorFactory("test-deploy-actor")
	if err != nil {
		t.Fatalf("expected factory, got error: %v", err)
	}
	a := f()
	if _, ok := a.(*remoteDeployTestActor); !ok {
		t.Fatalf("expected *remoteDeployTestActor, got %T", a)
	}
}

func TestGetActorFactory_NotRegistered(t *testing.T) {
	_, err := getActorFactory("nonexistent")
	if err == nil {
		t.Fatal("expected error for unregistered factory")
	}
}

func TestRemoteActorDeployRequest_JSON(t *testing.T) {
	req := &RemoteActorDeployRequest{
		FactoryID: "echo",
		ActorName: "echo-1",
		RequestID: "deploy-127.0.0.1-echo-1",
	}

	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded RemoteActorDeployRequest
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.FactoryID != req.FactoryID || decoded.ActorName != req.ActorName || decoded.RequestID != req.RequestID {
		t.Errorf("round-trip mismatch: got %+v, want %+v", decoded, req)
	}
}

func TestRemoteActorDeployResponse_JSON(t *testing.T) {
	resp := &RemoteActorDeployResponse{
		RequestID: "deploy-1",
		ActorPath: "pekko://System@127.0.0.1:2551/user/echo-1",
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded RemoteActorDeployResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.ActorPath != resp.ActorPath {
		t.Errorf("path mismatch: got %q, want %q", decoded.ActorPath, resp.ActorPath)
	}
}

func TestRemoteActorDeployResponse_Error(t *testing.T) {
	resp := &RemoteActorDeployResponse{
		RequestID: "deploy-2",
		Error:     "factory not found",
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded RemoteActorDeployResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Error != resp.Error {
		t.Errorf("error mismatch: got %q, want %q", decoded.Error, resp.Error)
	}
}

func TestHandleRemoteDeployRequest_Success(t *testing.T) {
	// Register a factory.
	RegisterActorFactory("test-handle-deploy", func() actor.Actor {
		return &remoteDeployTestActor{BaseActor: actor.NewBaseActor()}
	})

	sys, err := NewActorSystem("TestDeploy")
	if err != nil {
		t.Fatalf("NewActorSystem: %v", err)
	}
	defer sys.Terminate()

	req := &RemoteActorDeployRequest{
		FactoryID: "test-handle-deploy",
		ActorName: "deployed-actor",
		RequestID: "req-1",
	}

	resp := handleRemoteDeployRequest(sys, req)
	if resp.Error != "" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
	if resp.ActorPath == "" {
		t.Fatal("expected non-empty actor path")
	}
	if resp.RequestID != "req-1" {
		t.Errorf("requestID mismatch: got %q", resp.RequestID)
	}
}

func TestHandleRemoteDeployRequest_UnknownFactory(t *testing.T) {
	sys, err := NewActorSystem("TestDeploy2")
	if err != nil {
		t.Fatalf("NewActorSystem: %v", err)
	}
	defer sys.Terminate()

	req := &RemoteActorDeployRequest{
		FactoryID: "no-such-factory",
		ActorName: "x",
		RequestID: "req-2",
	}

	resp := handleRemoteDeployRequest(sys, req)
	if resp.Error == "" {
		t.Fatal("expected error for unknown factory")
	}
}

func TestHandleRemoteDeployRequest_DuplicateName(t *testing.T) {
	RegisterActorFactory("test-dup-deploy", func() actor.Actor {
		return &remoteDeployTestActor{BaseActor: actor.NewBaseActor()}
	})

	sys, err := NewActorSystem("TestDeploy3")
	if err != nil {
		t.Fatalf("NewActorSystem: %v", err)
	}
	defer sys.Terminate()

	req := &RemoteActorDeployRequest{
		FactoryID: "test-dup-deploy",
		ActorName: "unique-actor",
		RequestID: "req-3",
	}

	// First deploy succeeds.
	resp := handleRemoteDeployRequest(sys, req)
	if resp.Error != "" {
		t.Fatalf("first deploy error: %s", resp.Error)
	}

	// Second deploy with same name should fail.
	resp = handleRemoteDeployRequest(sys, req)
	if resp.Error == "" {
		t.Fatal("expected error for duplicate actor name")
	}
}

func TestRemoteScope(t *testing.T) {
	scope := RemoteScope{
		Address: actor.Address{
			Protocol: "pekko",
			System:   "ClusterSystem",
			Host:     "192.168.1.1",
			Port:     2551,
		},
	}

	deploy := RemoteDeploy{
		Scope:     scope,
		FactoryID: "echo",
	}

	if deploy.Scope.Address.Host != "192.168.1.1" {
		t.Errorf("address mismatch: got %v", deploy.Scope.Address)
	}
	if deploy.FactoryID != "echo" {
		t.Errorf("factoryID mismatch: got %q", deploy.FactoryID)
	}
}

func TestDeployRemote_EmptyFactoryID(t *testing.T) {
	sys, err := NewActorSystem("TestEmptyFactory")
	if err != nil {
		t.Fatalf("NewActorSystem: %v", err)
	}
	defer sys.Terminate()

	_, err = DeployRemote(nil, sys, "test", RemoteDeploy{
		Scope: RemoteScope{
			Address: actor.Address{Host: "127.0.0.1", Port: 2551},
		},
		FactoryID: "",
	})
	if err == nil {
		t.Fatal("expected error for empty factoryID")
	}
}

func TestHandleDeployMessage_NonBytes(t *testing.T) {
	sys, err := NewActorSystem("TestNonBytes")
	if err != nil {
		t.Fatalf("NewActorSystem: %v", err)
	}
	defer sys.Terminate()

	// Should not panic on non-byte messages.
	HandleDeployMessage(sys, nil, "not bytes")
	HandleDeployMessage(sys, nil, 42)
}

func TestHandleDeployMessage_InvalidJSON(t *testing.T) {
	sys, err := NewActorSystem("TestInvalidJSON")
	if err != nil {
		t.Fatalf("NewActorSystem: %v", err)
	}
	defer sys.Terminate()

	// Should not panic on invalid JSON.
	HandleDeployMessage(sys, nil, []byte("not json"))
}
