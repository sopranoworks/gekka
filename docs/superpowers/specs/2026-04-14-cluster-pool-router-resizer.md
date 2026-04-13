# Design: ClusterPoolRouter Resizer Integration (Phase A)

**Date:** 2026-04-14
**Status:** Approved
**Companion plan:** `.plan/20260414.md` Phase A

---

## Problem

`ClusterPoolRouter.Receive` overrides `PoolRouter.Receive` for the default (user message) case to build a combined local+remote routee list. This override never calls `evaluateResizeIfNeeded`, so `DefaultResizer` never fires for cluster-routed messages. The Phase 2.4 verification gate is blocked as a result.

---

## Approach: Export and Call (Option A)

Rename `PoolRouter.evaluateResizeIfNeeded` to `EvaluateResizeIfNeeded` (exported). `ClusterPoolRouter.Receive` calls it after routing each message.

### Why not the alternatives

- **`MaybeResize()` shim:** Same result with an extra indirection layer and a less descriptive name.
- **Delegate to `r.PoolRouter.Receive(msg)`:** Unsafe — `PoolRouter.Receive` routes through local `Routees` only, bypassing the combined local+remote routing that `ClusterPoolRouter` must maintain.

---

## Changes

### `actor/router.go`

- Rename `evaluateResizeIfNeeded` → `EvaluateResizeIfNeeded`.
- Update the one internal callsite in `PoolRouter.Receive` default case.
- No behaviour change; only visibility changes.

### `cluster/cluster_router.go`

- Add `NewClusterPoolRouterWithResizer(cm, logic, totalInstances, allowLocalRoutees, useRole, props, resizer)` constructor — sets `PoolRouter.Resizer` before returning, so callers don't need to set the field manually.
- Add `PoolSize() int` on `ClusterPoolRouter` delegating to `r.PoolRouter.NrOfInstances()` — used by tests to assert pool growth without accessing internal fields.
- In `ClusterPoolRouter.Receive` default case, append `r.PoolRouter.EvaluateResizeIfNeeded()` as the last statement.

### `cluster/cluster_router_test.go`

- Add `TestClusterPoolRouter_AutoResize`: constructs a `ClusterPoolRouter` with `DefaultResizer{LowerBound:2, UpperBound:8, MessagesPerResize:5}`, sends 20 messages, asserts `router.PoolSize() > 2`.

---

## Data Flow

```
User message → ClusterPoolRouter.Receive (default)
  → build combined [local routees] + [remote routees]
  → Logic.Select → target.Tell(msg)
  → EvaluateResizeIfNeeded()
      if Resizer == nil → no-op (non-resizing pools unaffected)
      else: increment counter → at threshold → sample mailbox depths
          → DefaultResizer.Capacity() → delta
          → delta > 0: self.Tell(AdjustPoolSize{Delta})
          → delta < 0: stop tail routees
```

---

## Error Handling

`EvaluateResizeIfNeeded` guards on `r.Resizer == nil` at entry — safe for any `ClusterPoolRouter` created without a resizer. No new error paths introduced.

---

## Test Gate

```bash
go build ./...
go test ./...
~/go/bin/golangci-lint run ./...
go test -tags integration ./...
cd test/compatibility/akka-multi-node && sbt multi-jvm:test
```

All must pass before committing. This resolves the Phase 2.4 BLOCKED state.
