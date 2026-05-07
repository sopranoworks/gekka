/*
 * proxy_transport.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ProxyTransport carries persistence-proxy RPC bytes to a remote node
// and waits for the reply.  It is the abstraction used by off-mode
// ProxyJournal / ProxySnapshotStore so the persistence package never
// imports the cluster layer; production wiring registers a
// Cluster.Ask-backed implementation at startup, and the unit tests
// register an in-process channel-based implementation.
//
// The contract is intentionally byte-in / byte-out — the request and
// response payloads are gob-encoded internally and the transport must
// not interpret them.  An implementation MUST honour the supplied
// timeout; a request that exceeds it is expected to surface as a
// non-nil error so the persistent actor's failure handler runs instead
// of the call hanging indefinitely.
type ProxyTransport interface {
	Ask(ctx context.Context, targetAddress string, payload []byte, timeout time.Duration) ([]byte, error)
}

// Well-known recipient suffixes appended by the proxy when computing
// the full Pekko URI of the remote target.  These must agree with the
// server-side registration done by ServePersistenceProxy* in the
// gekka root package.  Keeping them as exported constants lets test
// fixtures and production wiring use the same path without re-typing
// the string.
const (
	RemoteJournalPathSuffix       = "/system/persistence/journal-target"
	RemoteSnapshotPathSuffix      = "/system/persistence/snapshot-target"
	defaultProxyRequestTimeout    = 30 * time.Second
	defaultProxyServerErrorPrefix = "remote: "
)

var (
	transportMu       sync.RWMutex
	registeredProxyTx ProxyTransport
)

// RegisterProxyTransport installs the runtime transport used by
// off-mode ProxyJournal / ProxySnapshotStore instances created via
// HOCON.  Pass nil to clear the registration.  The cluster wires its
// own Cluster.Ask-backed transport on startup so HOCON-driven proxies
// pick it up automatically.
func RegisterProxyTransport(t ProxyTransport) {
	transportMu.Lock()
	registeredProxyTx = t
	transportMu.Unlock()
}

// CurrentProxyTransport returns the active transport (or nil if none
// has been registered yet).
func CurrentProxyTransport() ProxyTransport {
	transportMu.RLock()
	defer transportMu.RUnlock()
	return registeredProxyTx
}

// ── Wire format ───────────────────────────────────────────────────────────────
//
// All operations share the same envelope: a one-byte Kind followed by a
// gob-encoded rpcRequest body.  The reply mirrors that with rpcResponse.
// Using gob keeps user payloads (PersistentRepr.Payload, the snapshot
// `any`) opaque to the wire format — callers who use custom event types
// must register them with gob.Register, the same discipline Pekko users
// face with Java serialization.

type rpcKind uint8

const (
	rpcKindJournalWrite rpcKind = iota + 1
	rpcKindJournalDelete
	rpcKindJournalReplay
	rpcKindJournalHighest
	rpcKindSnapshotLoad
	rpcKindSnapshotSave
	rpcKindSnapshotDelete
	rpcKindSnapshotDeleteByCriteria
)

// rpcRequest is the unified request envelope for every operation.  Each
// field is read only by the kinds that need it.
type rpcRequest struct {
	Kind          rpcKind
	PersistenceID string
	From          uint64
	To            uint64
	Max           uint64
	Messages      []rpcPersistentRepr
	Criteria      SnapshotSelectionCriteria
	Metadata      SnapshotMetadata
	Snapshot      []byte // gob-encoded snapshot payload
}

// rpcPersistentRepr mirrors PersistentRepr but stores the user payload
// as a gob-encoded blob so the wire format does not depend on
// payload-specific reflection at the boundaries.
type rpcPersistentRepr struct {
	PersistenceID string
	SequenceNr    uint64
	Payload       []byte // gob-encoded payload
	Manifest      string
	Deleted       bool
	SenderPath    string
	Tags          []string
	TraceContext  map[string]string
	WriterUuid    string
}

// rpcResponse carries the reply.  Empty Err means success; the rest of
// the fields are populated based on the request Kind.
type rpcResponse struct {
	Err      string
	Highest  uint64
	Replay   []rpcPersistentRepr
	Snapshot *rpcSelectedSnapshot
}

type rpcSelectedSnapshot struct {
	Metadata SnapshotMetadata
	Payload  []byte // gob-encoded snapshot payload
}

func encodeRPC(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, fmt.Errorf("persistence: encode rpc: %w", err)
	}
	return buf.Bytes(), nil
}

func decodeRPC(b []byte, v any) error {
	if len(b) == 0 {
		return errors.New("persistence: decode rpc: empty payload")
	}
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(v); err != nil {
		return fmt.Errorf("persistence: decode rpc: %w", err)
	}
	return nil
}

// encodeAny gob-encodes an arbitrary user payload so the wire format
// can carry whatever PersistentRepr.Payload / SnapshotStore snapshot
// types the application uses.  A nil payload encodes to an empty
// slice; the matching decodeAny reverses that.
func encodeAny(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&v); err != nil {
		return nil, fmt.Errorf("persistence: encode payload: %w", err)
	}
	return buf.Bytes(), nil
}

func decodeAny(b []byte) (any, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var v any
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&v); err != nil {
		return nil, fmt.Errorf("persistence: decode payload: %w", err)
	}
	return v, nil
}

func toRPCRepr(m PersistentRepr) (rpcPersistentRepr, error) {
	payload, err := encodeAny(m.Payload)
	if err != nil {
		return rpcPersistentRepr{}, err
	}
	return rpcPersistentRepr{
		PersistenceID: m.PersistenceID,
		SequenceNr:    m.SequenceNr,
		Payload:       payload,
		Manifest:      m.Manifest,
		Deleted:       m.Deleted,
		SenderPath:    m.SenderPath,
		Tags:          m.Tags,
		TraceContext:  m.TraceContext,
		WriterUuid:    m.WriterUuid,
	}, nil
}

func fromRPCRepr(r rpcPersistentRepr) (PersistentRepr, error) {
	payload, err := decodeAny(r.Payload)
	if err != nil {
		return PersistentRepr{}, err
	}
	return PersistentRepr{
		PersistenceID: r.PersistenceID,
		SequenceNr:    r.SequenceNr,
		Payload:       payload,
		Manifest:      r.Manifest,
		Deleted:       r.Deleted,
		SenderPath:    r.SenderPath,
		Tags:          r.Tags,
		TraceContext:  r.TraceContext,
		WriterUuid:    r.WriterUuid,
	}, nil
}

// ── Client side ───────────────────────────────────────────────────────────────

// RemoteJournal forwards Journal operations across a ProxyTransport
// to a target plugin running on a remote node.  Each call serializes
// the request with gob, hands it to the transport, and decodes the
// reply.  Transport-level failures (timeout, remote process death)
// surface as plain errors that the persistent actor's failure handler
// can react to.
type RemoteJournal struct {
	transport      ProxyTransport
	address        string
	requestTimeout time.Duration
}

// NewRemoteJournal returns a Journal that forwards every call to the
// remote target reachable through transport at address.  A
// non-positive requestTimeout collapses to the default 30 s.
func NewRemoteJournal(transport ProxyTransport, address string, requestTimeout time.Duration) *RemoteJournal {
	if requestTimeout <= 0 {
		requestTimeout = defaultProxyRequestTimeout
	}
	return &RemoteJournal{
		transport:      transport,
		address:        address,
		requestTimeout: requestTimeout,
	}
}

func (r *RemoteJournal) ask(ctx context.Context, req *rpcRequest) (*rpcResponse, error) {
	body, err := encodeRPC(req)
	if err != nil {
		return nil, err
	}
	replyBytes, err := r.transport.Ask(ctx, r.address, body, r.requestTimeout)
	if err != nil {
		return nil, err
	}
	var resp rpcResponse
	if err := decodeRPC(replyBytes, &resp); err != nil {
		return nil, err
	}
	if resp.Err != "" {
		return nil, errors.New(defaultProxyServerErrorPrefix + resp.Err)
	}
	return &resp, nil
}

// AsyncWriteMessages forwards the batch to the remote journal.
func (r *RemoteJournal) AsyncWriteMessages(ctx context.Context, messages []PersistentRepr) error {
	rpcMsgs := make([]rpcPersistentRepr, len(messages))
	for i := range messages {
		rm, err := toRPCRepr(messages[i])
		if err != nil {
			return err
		}
		rpcMsgs[i] = rm
	}
	_, err := r.ask(ctx, &rpcRequest{Kind: rpcKindJournalWrite, Messages: rpcMsgs})
	return err
}

// AsyncDeleteMessagesTo forwards to the remote journal.
func (r *RemoteJournal) AsyncDeleteMessagesTo(ctx context.Context, persistenceId string, toSequenceNr uint64) error {
	_, err := r.ask(ctx, &rpcRequest{
		Kind:          rpcKindJournalDelete,
		PersistenceID: persistenceId,
		To:            toSequenceNr,
	})
	return err
}

// ReplayMessages forwards to the remote journal and invokes callback
// for every replayed message in the order returned by the target.
func (r *RemoteJournal) ReplayMessages(ctx context.Context, persistenceId string, fromSequenceNr, toSequenceNr, max uint64, callback func(PersistentRepr)) error {
	resp, err := r.ask(ctx, &rpcRequest{
		Kind:          rpcKindJournalReplay,
		PersistenceID: persistenceId,
		From:          fromSequenceNr,
		To:            toSequenceNr,
		Max:           max,
	})
	if err != nil {
		return err
	}
	for _, rm := range resp.Replay {
		m, err := fromRPCRepr(rm)
		if err != nil {
			return err
		}
		callback(m)
	}
	return nil
}

// ReadHighestSequenceNr forwards to the remote journal.
func (r *RemoteJournal) ReadHighestSequenceNr(ctx context.Context, persistenceId string, fromSequenceNr uint64) (uint64, error) {
	resp, err := r.ask(ctx, &rpcRequest{
		Kind:          rpcKindJournalHighest,
		PersistenceID: persistenceId,
		From:          fromSequenceNr,
	})
	if err != nil {
		return 0, err
	}
	return resp.Highest, nil
}

var _ Journal = (*RemoteJournal)(nil)

// RemoteSnapshotStore forwards SnapshotStore operations across a
// ProxyTransport to a target running on a remote node.
type RemoteSnapshotStore struct {
	transport      ProxyTransport
	address        string
	requestTimeout time.Duration
}

// NewRemoteSnapshotStore returns a SnapshotStore that forwards every
// call to the remote target reachable through transport at address.
func NewRemoteSnapshotStore(transport ProxyTransport, address string, requestTimeout time.Duration) *RemoteSnapshotStore {
	if requestTimeout <= 0 {
		requestTimeout = defaultProxyRequestTimeout
	}
	return &RemoteSnapshotStore{
		transport:      transport,
		address:        address,
		requestTimeout: requestTimeout,
	}
}

func (r *RemoteSnapshotStore) ask(ctx context.Context, req *rpcRequest) (*rpcResponse, error) {
	body, err := encodeRPC(req)
	if err != nil {
		return nil, err
	}
	replyBytes, err := r.transport.Ask(ctx, r.address, body, r.requestTimeout)
	if err != nil {
		return nil, err
	}
	var resp rpcResponse
	if err := decodeRPC(replyBytes, &resp); err != nil {
		return nil, err
	}
	if resp.Err != "" {
		return nil, errors.New(defaultProxyServerErrorPrefix + resp.Err)
	}
	return &resp, nil
}

// LoadSnapshot forwards to the remote snapshot store.  A nil
// SelectedSnapshot reply (target had nothing) is returned as (nil,
// nil), matching the in-process semantics.
func (r *RemoteSnapshotStore) LoadSnapshot(ctx context.Context, persistenceId string, criteria SnapshotSelectionCriteria) (*SelectedSnapshot, error) {
	resp, err := r.ask(ctx, &rpcRequest{
		Kind:          rpcKindSnapshotLoad,
		PersistenceID: persistenceId,
		Criteria:      criteria,
	})
	if err != nil {
		return nil, err
	}
	if resp.Snapshot == nil {
		return nil, nil
	}
	payload, err := decodeAny(resp.Snapshot.Payload)
	if err != nil {
		return nil, err
	}
	return &SelectedSnapshot{Metadata: resp.Snapshot.Metadata, Snapshot: payload}, nil
}

// SaveSnapshot forwards to the remote snapshot store.
func (r *RemoteSnapshotStore) SaveSnapshot(ctx context.Context, metadata SnapshotMetadata, snapshot any) error {
	payload, err := encodeAny(snapshot)
	if err != nil {
		return err
	}
	_, err = r.ask(ctx, &rpcRequest{
		Kind:     rpcKindSnapshotSave,
		Metadata: metadata,
		Snapshot: payload,
	})
	return err
}

// DeleteSnapshot forwards to the remote snapshot store.
func (r *RemoteSnapshotStore) DeleteSnapshot(ctx context.Context, metadata SnapshotMetadata) error {
	_, err := r.ask(ctx, &rpcRequest{
		Kind:     rpcKindSnapshotDelete,
		Metadata: metadata,
	})
	return err
}

// DeleteSnapshots forwards to the remote snapshot store.
func (r *RemoteSnapshotStore) DeleteSnapshots(ctx context.Context, persistenceId string, criteria SnapshotSelectionCriteria) error {
	_, err := r.ask(ctx, &rpcRequest{
		Kind:          rpcKindSnapshotDeleteByCriteria,
		PersistenceID: persistenceId,
		Criteria:      criteria,
	})
	return err
}

var _ SnapshotStore = (*RemoteSnapshotStore)(nil)

// ── Server side ───────────────────────────────────────────────────────────────

// ServeJournalRequest decodes a single RPC request, dispatches it to
// the supplied Journal, and returns the encoded reply.  The function
// is pure and re-entrant; it is the dispatch primitive used by the
// cluster-side server actor and by the in-memory test transport.
func ServeJournalRequest(ctx context.Context, j Journal, req []byte) ([]byte, error) {
	var r rpcRequest
	if err := decodeRPC(req, &r); err != nil {
		return encodeRPC(&rpcResponse{Err: err.Error()})
	}
	resp := dispatchJournal(ctx, j, &r)
	return encodeRPC(resp)
}

func dispatchJournal(ctx context.Context, j Journal, r *rpcRequest) *rpcResponse {
	switch r.Kind {
	case rpcKindJournalWrite:
		msgs := make([]PersistentRepr, len(r.Messages))
		for i := range r.Messages {
			m, err := fromRPCRepr(r.Messages[i])
			if err != nil {
				return &rpcResponse{Err: err.Error()}
			}
			msgs[i] = m
		}
		if err := j.AsyncWriteMessages(ctx, msgs); err != nil {
			return &rpcResponse{Err: err.Error()}
		}
		return &rpcResponse{}
	case rpcKindJournalDelete:
		if err := j.AsyncDeleteMessagesTo(ctx, r.PersistenceID, r.To); err != nil {
			return &rpcResponse{Err: err.Error()}
		}
		return &rpcResponse{}
	case rpcKindJournalReplay:
		var replayed []rpcPersistentRepr
		err := j.ReplayMessages(ctx, r.PersistenceID, r.From, r.To, r.Max, func(m PersistentRepr) {
			rm, encErr := toRPCRepr(m)
			if encErr != nil {
				replayed = append(replayed, rpcPersistentRepr{PersistenceID: m.PersistenceID, SequenceNr: m.SequenceNr, Manifest: m.Manifest})
				return
			}
			replayed = append(replayed, rm)
		})
		if err != nil {
			return &rpcResponse{Err: err.Error()}
		}
		return &rpcResponse{Replay: replayed}
	case rpcKindJournalHighest:
		hi, err := j.ReadHighestSequenceNr(ctx, r.PersistenceID, r.From)
		if err != nil {
			return &rpcResponse{Err: err.Error()}
		}
		return &rpcResponse{Highest: hi}
	default:
		return &rpcResponse{Err: fmt.Sprintf("persistence: unknown journal rpc kind %d", r.Kind)}
	}
}

// ServeSnapshotRequest decodes a single RPC request, dispatches it to
// the supplied SnapshotStore, and returns the encoded reply.
func ServeSnapshotRequest(ctx context.Context, s SnapshotStore, req []byte) ([]byte, error) {
	var r rpcRequest
	if err := decodeRPC(req, &r); err != nil {
		return encodeRPC(&rpcResponse{Err: err.Error()})
	}
	resp := dispatchSnapshot(ctx, s, &r)
	return encodeRPC(resp)
}

func dispatchSnapshot(ctx context.Context, s SnapshotStore, r *rpcRequest) *rpcResponse {
	switch r.Kind {
	case rpcKindSnapshotLoad:
		sel, err := s.LoadSnapshot(ctx, r.PersistenceID, r.Criteria)
		if err != nil {
			return &rpcResponse{Err: err.Error()}
		}
		if sel == nil {
			return &rpcResponse{}
		}
		payload, err := encodeAny(sel.Snapshot)
		if err != nil {
			return &rpcResponse{Err: err.Error()}
		}
		return &rpcResponse{Snapshot: &rpcSelectedSnapshot{Metadata: sel.Metadata, Payload: payload}}
	case rpcKindSnapshotSave:
		payload, err := decodeAny(r.Snapshot)
		if err != nil {
			return &rpcResponse{Err: err.Error()}
		}
		if err := s.SaveSnapshot(ctx, r.Metadata, payload); err != nil {
			return &rpcResponse{Err: err.Error()}
		}
		return &rpcResponse{}
	case rpcKindSnapshotDelete:
		if err := s.DeleteSnapshot(ctx, r.Metadata); err != nil {
			return &rpcResponse{Err: err.Error()}
		}
		return &rpcResponse{}
	case rpcKindSnapshotDeleteByCriteria:
		if err := s.DeleteSnapshots(ctx, r.PersistenceID, r.Criteria); err != nil {
			return &rpcResponse{Err: err.Error()}
		}
		return &rpcResponse{}
	default:
		return &rpcResponse{Err: fmt.Sprintf("persistence: unknown snapshot rpc kind %d", r.Kind)}
	}
}
