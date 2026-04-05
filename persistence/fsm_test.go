/*
 * fsm_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Order FSM domain ──────────────────────────────────────────────────────────

type OrderState int

const (
	OrderCreated OrderState = iota
	OrderPaid
	OrderShipped
)

func (s OrderState) String() string {
	switch s {
	case OrderCreated:
		return "Created"
	case OrderPaid:
		return "Paid"
	case OrderShipped:
		return "Shipped"
	default:
		return "Unknown"
	}
}

type OrderData struct {
	OrderID string
	Total   float64
}

// Domain commands (messages)
type PayOrder struct{ Amount float64 }
type ShipOrder struct{ TrackingID string }
type GetOrderStatus struct{}

// Persisted events
type OrderEvent interface{ isOrderEvent() }
type OrderPaidEvent struct{ Amount float64 }
type OrderShippedEvent struct{ TrackingID string }

func (OrderPaidEvent) isOrderEvent()    {}
func (OrderShippedEvent) isOrderEvent() {}

// ── Test actor embedding PersistentFSM ────────────────────────────────────────

type orderFSMActor struct {
	*PersistentFSM[OrderState, OrderData, OrderEvent]
	// captured state for assertions
	lastStatus OrderState
}

func newOrderFSMActor(journal Journal, orderID string) *orderFSMActor {
	fsm := NewPersistentFSM[OrderState, OrderData, OrderEvent](orderID, journal)
	a := &orderFSMActor{PersistentFSM: fsm}

	fsm.StartWith(OrderCreated, OrderData{OrderID: orderID})

	// Apply events during recovery
	fsm.ApplyEvent(func(state OrderState, data OrderData, event OrderEvent) (OrderState, OrderData) {
		switch e := event.(type) {
		case OrderPaidEvent:
			data.Total = e.Amount
			return OrderPaid, data
		case OrderShippedEvent:
			_ = e.TrackingID
			return OrderShipped, data
		}
		return state, data
	})

	// Created → Paid on PayOrder
	fsm.When(OrderCreated, func(e FSMEvent[OrderData]) FSMStateResult[OrderState, OrderData, OrderEvent] {
		switch cmd := e.Msg.(type) {
		case PayOrder:
			newData := e.Data
			newData.Total = cmd.Amount
			return fsm.Goto(OrderPaid).Using(newData).Persisting(OrderPaidEvent(cmd)).Build()
		}
		return fsm.Unhandled()
	})

	// Paid → Shipped on ShipOrder
	fsm.When(OrderPaid, func(e FSMEvent[OrderData]) FSMStateResult[OrderState, OrderData, OrderEvent] {
		switch cmd := e.Msg.(type) {
		case ShipOrder:
			return fsm.Goto(OrderShipped).Persisting(OrderShippedEvent(cmd)).Build()
		}
		return fsm.Unhandled()
	})

	// Shipped state — nothing to do
	fsm.When(OrderShipped, func(e FSMEvent[OrderData]) FSMStateResult[OrderState, OrderData, OrderEvent] {
		return fsm.Stay().Build()
	})

	return a
}

// Receive delegates to the embedded FSM but also captures status transitions for
// the test helpers.
func (a *orderFSMActor) Receive(msg any) {
	a.PersistentFSM.Receive(msg)
	a.lastStatus = a.State()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// simulateRecover creates a fresh FSM actor sharing the same journal, simulating
// an actor restart and full recovery from the event log.
func simulateRecover(journal Journal, orderID string) *orderFSMActor {
	a := newOrderFSMActor(journal, orderID)
	// Manually trigger recovery the same way PreStart would.
	a.PersistentFSMPreStart()
	return a
}

func TestPersistentFSM_BasicTransitions(t *testing.T) {
	journal := NewInMemoryJournal()
	a := newOrderFSMActor(journal, "order-1")
	a.PersistentFSMPreStart()

	assert.Equal(t, OrderCreated, a.State())

	// Pay the order
	a.Receive(PayOrder{Amount: 99.99})
	assert.Equal(t, OrderPaid, a.State())
	assert.Equal(t, 99.99, a.Data().Total)

	// Ship the order
	a.Receive(ShipOrder{TrackingID: "TRK-123"})
	assert.Equal(t, OrderShipped, a.State())
}

func TestPersistentFSM_RecoveryRestoresState(t *testing.T) {
	journal := NewInMemoryJournal()

	// First actor instance: drive through Created → Paid → Shipped.
	a1 := newOrderFSMActor(journal, "order-2")
	a1.PersistentFSMPreStart()

	a1.Receive(PayOrder{Amount: 50.0})
	require.Equal(t, OrderPaid, a1.State())
	a1.Receive(ShipOrder{TrackingID: "TRK-456"})
	require.Equal(t, OrderShipped, a1.State())

	// Verify events were persisted.
	seqNr, err := journal.ReadHighestSequenceNr(nil, "order-2", 0) //nolint:staticcheck
	require.NoError(t, err)
	assert.Equal(t, uint64(2), seqNr, "expected 2 events persisted")

	// Second actor instance: recover from journal, no commands sent.
	a2 := simulateRecover(journal, "order-2")
	assert.Equal(t, OrderShipped, a2.State(), "recovered state should be Shipped")
	assert.Equal(t, 50.0, a2.Data().Total, "recovered total should be 50.0")
}

func TestPersistentFSM_UnhandledMessage(t *testing.T) {
	journal := NewInMemoryJournal()
	a := newOrderFSMActor(journal, "order-3")
	a.PersistentFSMPreStart()

	// ShipOrder is not handled in Created state — should be ignored gracefully.
	a.Receive(ShipOrder{TrackingID: "TRK-789"})
	assert.Equal(t, OrderCreated, a.State(), "state should not change on unhandled message")
}

func TestPersistentFSM_PartialRecovery(t *testing.T) {
	journal := NewInMemoryJournal()

	// First instance: only pay (no ship).
	a1 := newOrderFSMActor(journal, "order-4")
	a1.PersistentFSMPreStart()
	a1.Receive(PayOrder{Amount: 200.0})
	require.Equal(t, OrderPaid, a1.State())

	// Recover and continue: should resume from Paid state.
	a2 := simulateRecover(journal, "order-4")
	assert.Equal(t, OrderPaid, a2.State())
	assert.Equal(t, 200.0, a2.Data().Total)

	// Can now ship from the recovered state.
	a2.Receive(ShipOrder{TrackingID: "TRK-000"})
	assert.Equal(t, OrderShipped, a2.State())
}
