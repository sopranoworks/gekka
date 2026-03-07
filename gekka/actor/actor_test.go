/*
 * actor_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import "testing"

func TestAddress_String(t *testing.T) {
	a := Address{Protocol: "pekko", System: "ClusterSystem", Host: "127.0.0.1", Port: 2552}
	want := "pekko://ClusterSystem@127.0.0.1:2552"
	if got := a.String(); got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestParseAddress(t *testing.T) {
	tests := []struct {
		uri     string
		want    Address
		wantErr bool
	}{
		{
			uri:  "pekko://ClusterSystem@127.0.0.1:2552",
			want: Address{Protocol: "pekko", System: "ClusterSystem", Host: "127.0.0.1", Port: 2552},
		},
		{
			uri:  "akka://MySystem@10.0.0.1:2553",
			want: Address{Protocol: "akka", System: "MySystem", Host: "10.0.0.1", Port: 2553},
		},
		{uri: "notauri%%", wantErr: true},
		{uri: "pekko://127.0.0.1:2552", wantErr: true}, // no system name
	}
	for _, tt := range tests {
		got, err := ParseAddress(tt.uri)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseAddress(%q): expected error, got nil", tt.uri)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseAddress(%q) unexpected error: %v", tt.uri, err)
			continue
		}
		if got != tt.want {
			t.Errorf("ParseAddress(%q) = %+v, want %+v", tt.uri, got, tt.want)
		}
	}
}

func TestParseAddress_RoundTrip(t *testing.T) {
	orig := Address{Protocol: "akka", System: "MySys", Host: "192.168.1.1", Port: 9000}
	parsed, err := ParseAddress(orig.String())
	if err != nil {
		t.Fatalf("ParseAddress(%q): %v", orig.String(), err)
	}
	if parsed != orig {
		t.Errorf("round-trip: got %+v, want %+v", parsed, orig)
	}
}

func TestActorPath_String(t *testing.T) {
	addr := Address{Protocol: "pekko", System: "Sys", Host: "127.0.0.1", Port: 2552}
	tests := []struct {
		path ActorPath
		want string
	}{
		{addr.Root(), "pekko://Sys@127.0.0.1:2552/"},
		{addr.WithRoot("user"), "pekko://Sys@127.0.0.1:2552/user"},
		{addr.WithRoot("user").Child("myActor"), "pekko://Sys@127.0.0.1:2552/user/myActor"},
		{addr.WithRoot("system").Child("cluster").Child("core").Child("daemon"),
			"pekko://Sys@127.0.0.1:2552/system/cluster/core/daemon"},
	}
	for _, tt := range tests {
		if got := tt.path.String(); got != tt.want {
			t.Errorf("String() = %q, want %q", got, tt.want)
		}
	}
}

func TestActorPath_Child_Immutable(t *testing.T) {
	addr := Address{Protocol: "pekko", System: "Sys", Host: "h", Port: 1}
	parent := addr.WithRoot("user")
	child := parent.Child("a")
	_ = child.Child("b") // must not modify child

	if parent.String() != "pekko://Sys@h:1/user" {
		t.Errorf("parent mutated: %q", parent.String())
	}
	if child.String() != "pekko://Sys@h:1/user/a" {
		t.Errorf("child mutated: %q", child.String())
	}
}

func TestActorPath_Parent(t *testing.T) {
	addr := Address{Protocol: "pekko", System: "S", Host: "h", Port: 1}
	p := addr.WithRoot("user").Child("foo").Child("bar")
	if p.Parent().String() != "pekko://S@h:1/user/foo" {
		t.Errorf("Parent() = %q", p.Parent().String())
	}
	if p.Parent().Parent().String() != "pekko://S@h:1/user" {
		t.Errorf("Parent().Parent() = %q", p.Parent().Parent().String())
	}
}

func TestActorPath_Name(t *testing.T) {
	addr := Address{Protocol: "pekko", System: "S", Host: "h", Port: 1}
	if got := addr.Root().Name(); got != "" {
		t.Errorf("Root.Name() = %q, want empty", got)
	}
	p := addr.WithRoot("user").Child("myActor")
	if got := p.Name(); got != "myActor" {
		t.Errorf("Name() = %q, want %q", got, "myActor")
	}
}

func TestActorPath_Depth(t *testing.T) {
	addr := Address{Protocol: "pekko", System: "S", Host: "h", Port: 1}
	if addr.Root().Depth() != 0 {
		t.Error("Root depth must be 0")
	}
	if addr.WithRoot("user").Depth() != 1 {
		t.Error("WithRoot depth must be 1")
	}
	if addr.WithRoot("user").Child("a").Child("b").Depth() != 3 {
		t.Error("depth must be 3")
	}
}

func TestParseActorPath(t *testing.T) {
	uri := "pekko://ClusterSystem@127.0.0.1:2552/user/myActor"
	p, err := ParseActorPath(uri)
	if err != nil {
		t.Fatalf("ParseActorPath: %v", err)
	}
	if p.String() != uri {
		t.Errorf("round-trip: got %q, want %q", p.String(), uri)
	}
	if p.Address.System != "ClusterSystem" {
		t.Errorf("System = %q", p.Address.System)
	}
	if p.Name() != "myActor" {
		t.Errorf("Name = %q", p.Name())
	}
}

func TestParseActorPath_RootPath(t *testing.T) {
	uri := "pekko://Sys@127.0.0.1:2552/"
	p, err := ParseActorPath(uri)
	if err != nil {
		t.Fatalf("ParseActorPath: %v", err)
	}
	if p.Depth() != 0 {
		t.Errorf("root path depth = %d, want 0", p.Depth())
	}
}

func TestAddress_WithRoot_Akka(t *testing.T) {
	addr := Address{Protocol: "akka", System: "MySystem", Host: "10.0.0.1", Port: 2552}
	path := addr.WithRoot("user").Child("worker")
	want := "akka://MySystem@10.0.0.1:2552/user/worker"
	if got := path.String(); got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}
