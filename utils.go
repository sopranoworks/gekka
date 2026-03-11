/*
 * utils.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"bytes"
	"compress/gzip"
	"io"

	"github.com/sopranoworks/gekka/cluster"
	"google.golang.org/protobuf/proto"
)

// gzipCompress compresses bytes with GZIP.
func gzipCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// gzipDecompress decompresses GZIP-compressed bytes.
func gzipDecompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func toClusterAddress(a *Address) *cluster.Address {
	if a == nil {
		return nil
	}
	return &cluster.Address{
		Protocol: a.Protocol,
		System:   a.System,
		Hostname: a.Hostname,
		Port:     a.Port,
	}
}

func toClusterUniqueAddress(ua *UniqueAddress) *cluster.UniqueAddress {
	if ua == nil {
		return nil
	}
	uid64 := *ua.Uid
	return &cluster.UniqueAddress{
		Address: toClusterAddress(ua.Address),
		Uid:     proto.Uint32(uint32(uid64 & 0xFFFFFFFF)),
		Uid2:    proto.Uint32(uint32(uid64 >> 32)),
	}
}
