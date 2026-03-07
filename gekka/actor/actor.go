/*
 * actor.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package actor provides type-safe representations of Pekko/Akka actor addresses
// and paths, equivalent to org.apache.pekko.actor.{Address,ActorPath}.
//
// Usage:
//
//	addr := actor.Address{Protocol: "pekko", System: "ClusterSystem", Host: "127.0.0.1", Port: 2552}
//	path := addr.WithRoot("user").Child("myActor")
//	fmt.Println(path) // → pekko://ClusterSystem@127.0.0.1:2552/user/myActor
package actor

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// Address identifies an actor system on the network.
// It is the Go equivalent of org.apache.pekko.actor.Address.
type Address struct {
	Protocol string // "pekko" or "akka"
	System   string // actor system name, e.g. "ClusterSystem"
	Host     string // hostname or IP, e.g. "127.0.0.1"
	Port     int    // TCP port, e.g. 2552
}

// String returns the address in URI form: "protocol://system@host:port".
func (a Address) String() string {
	return fmt.Sprintf("%s://%s@%s:%d", a.Protocol, a.System, a.Host, a.Port)
}

// Root returns an ActorPath at the root guardian of this address.
func (a Address) Root() ActorPath {
	return ActorPath{Address: a}
}

// WithRoot returns an ActorPath rooted at the named top-level guardian.
// Pekko has two top-level guardians: "user" (for user-created actors) and
// "system" (for system actors such as cluster daemons).
//
//	addr.WithRoot("user").Child("myActor")
//	// → "pekko://ClusterSystem@127.0.0.1:2552/user/myActor"
func (a Address) WithRoot(name string) ActorPath {
	return ActorPath{Address: a, elements: []string{name}}
}

// ParseAddress parses a URI of the form "protocol://system@host:port" into an Address.
//
//	addr, err := actor.ParseAddress("pekko://ClusterSystem@127.0.0.1:2552")
func ParseAddress(uri string) (Address, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return Address{}, fmt.Errorf("actor: parse address %q: %w", uri, err)
	}
	if u.User == nil || u.User.Username() == "" {
		return Address{}, fmt.Errorf("actor: parse address %q: missing actor system name", uri)
	}
	host, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		return Address{}, fmt.Errorf("actor: parse address %q: %w", uri, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return Address{}, fmt.Errorf("actor: parse address %q: invalid port: %w", uri, err)
	}
	return Address{
		Protocol: u.Scheme,
		System:   u.User.Username(),
		Host:     host,
		Port:     port,
	}, nil
}

// ActorPath identifies a specific actor within a remote actor system.
// It is the Go equivalent of org.apache.pekko.actor.ActorPath.
//
// ActorPath is immutable: Child and Parent return new values.
type ActorPath struct {
	// Address is the actor system this path belongs to.
	Address Address

	elements []string // ordered path segments, e.g. ["user", "myActor"]
}

// ParseActorPath parses a full actor path URI into an ActorPath.
//
//	path, err := actor.ParseActorPath("pekko://ClusterSystem@127.0.0.1:2552/user/myActor")
func ParseActorPath(uri string) (ActorPath, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return ActorPath{}, fmt.Errorf("actor: parse path %q: %w", uri, err)
	}
	addrURI := u.Scheme + "://" + u.Host
	if u.User != nil {
		addrURI = u.Scheme + "://" + u.User.String() + "@" + u.Host
	}
	addr, err := ParseAddress(addrURI)
	if err != nil {
		return ActorPath{}, err
	}
	elems := strings.FieldsFunc(u.Path, func(r rune) bool { return r == '/' })
	return ActorPath{Address: addr, elements: elems}, nil
}

// String returns the full actor path URI:
// "protocol://system@host:port/elem1/elem2/…"
func (p ActorPath) String() string {
	if len(p.elements) == 0 {
		return p.Address.String() + "/"
	}
	return p.Address.String() + "/" + strings.Join(p.elements, "/")
}

// Child appends name as a child element and returns the new ActorPath.
// The receiver is not modified.
//
//	path.Child("myActor")           // → …/user/myActor
//	path.Child("myActor").Child("a") // → …/user/myActor/a
func (p ActorPath) Child(name string) ActorPath {
	elems := make([]string, len(p.elements)+1)
	copy(elems, p.elements)
	elems[len(p.elements)] = name
	return ActorPath{Address: p.Address, elements: elems}
}

// Parent returns the parent path, removing the last element.
// Calling Parent on a root path (no elements) returns the same path.
func (p ActorPath) Parent() ActorPath {
	if len(p.elements) == 0 {
		return p
	}
	elems := make([]string, len(p.elements)-1)
	copy(elems, p.elements[:len(p.elements)-1])
	return ActorPath{Address: p.Address, elements: elems}
}

// Name returns the last path element (the actor's local name).
// Returns an empty string for a root path.
func (p ActorPath) Name() string {
	if len(p.elements) == 0 {
		return ""
	}
	return p.elements[len(p.elements)-1]
}

// Elements returns a copy of the ordered path segments.
func (p ActorPath) Elements() []string {
	return append([]string(nil), p.elements...)
}

// Depth returns the number of path elements.
func (p ActorPath) Depth() int {
	return len(p.elements)
}
