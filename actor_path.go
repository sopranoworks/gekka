/*
 * actor_path.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"fmt"
	"net"
	"net/url"
	"strconv"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// ActorPath represents a parsed Artery actor path.
type ActorPath struct {
	Protocol string
	System   string
	Host     string
	Port     uint32
	Path     string
}

// ParseActorPath parses a string like "pekko://system@host:port/user/actor".
func ParseActorPath(raw string) (*ActorPath, error) {
	// Pekko paths are similar to URLs: protocol://system@host:port/path
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("failed to parse actor path: %w", err)
	}

	res := &ActorPath{
		Protocol: u.Scheme,
		Path:     u.Path,
	}

	if u.User != nil {
		res.System = u.User.Username()
	}

	host, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		// Might not have a port
		res.Host = u.Host
	} else {
		res.Host = host
		p, _ := strconv.ParseUint(portStr, 10, 32)
		res.Port = uint32(p)
	}

	return res, nil
}

// ToAddress converts the parsed path into a Protobuf Address.
func (ap *ActorPath) ToAddress() *gproto_remote.Address {
	return &gproto_remote.Address{
		Protocol: proto.String(ap.Protocol),
		System:   proto.String(ap.System),
		Hostname: proto.String(ap.Host),
		Port:     proto.Uint32(ap.Port),
	}
}

// ToUniqueAddress converts the parsed path into a Protobuf UniqueAddress (UID unknown).
func (ap *ActorPath) ToUniqueAddress(uid uint64) *gproto_remote.UniqueAddress {
	return &gproto_remote.UniqueAddress{
		Address: ap.ToAddress(),
		Uid:     proto.Uint64(uid),
	}
}
