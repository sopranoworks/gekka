# Akka Multi-Node Artery Compatibility Test

Verifies that a Go/Gekka node can establish an Artery TCP connection with an
Akka 2.6 cluster seed node and appear as a `MemberUp` cluster member.

## Architecture

```
┌──────────────────────────────────────────┐   Artery TCP (2551 ← 2552)
│  Node A (JVM / Akka 2.6.21)             │◄─────────────────────────────┐
│  ActorSystem: GekkaSystem                │                              │
│  Port: 2551  (seed)                      │  Cluster gossip              │
│  Management: n/a                         │                              │
└──────────────────────────────────────────┘                              │
                                                                          │
┌──────────────────────────────────────────┐                              │
│  Node B (Go / Gekka)                    │──────────────────────────────┘
│  SystemName: GekkaSystem                 │
│  Port: 2552                              │
│  Management HTTP: 127.0.0.1:8558         │
└──────────────────────────────────────────┘
```

Node A is the JVM process started by `sbt multi-jvm:test` (role `GekkaSystem`).
Node B is the `gekka-compat-test` binary spawned as a subprocess from within
the test.

## Prerequisites

| Tool    | Version  |
|---------|----------|
| sbt     | 1.9.x    |
| Java    | 11 or 17 |
| Go      | 1.23+    |

## Build the Go binary

From the repository root:

```bash
go build -o bin/gekka-compat-test ./cmd/gekka-compat-test
```

## Run the test

```bash
cd test/compatibility/akka-multi-node
sbt multi-jvm:test
```

The `GEKKA_COMPAT_TEST_BIN` environment variable can override the binary path:

```bash
GEKKA_COMPAT_TEST_BIN=/path/to/gekka-compat-test sbt multi-jvm:test
```

## Expected output

A passing run prints the following sentinel lines in order:

```
AKKA_SEED_UP: akka://GekkaSystem@127.0.0.1:2551
GEKKA_ARTERY_ASSOCIATED
GEKKA_MEMBER_UP:akka://GekkaSystem@127.0.0.1:2552
CLUSTER_MEMBERS:{...}
COMPAT_TEST_PASSED
```

## What the test checks

1. Akka seed node starts and transitions to `MemberUp`.
2. Go node completes the Artery TCP handshake (`GEKKA_ARTERY_ASSOCIATED`).
3. Akka cluster gossip delivers a `MemberUp` event for the Go node.
4. The Go node's Management HTTP API (`GET /cluster/members`) returns a
   response containing `"Up"` — confirming the management server binds
   successfully and the cluster state is visible from the Go side.

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `binary not found` | Go binary not built | `go build -o bin/gekka-compat-test ./cmd/gekka-compat-test` |
| `WaitForHandshake timeout` | Artery protocol mismatch | Check logs for `OptionVal.None.get` on the Akka side — ensure manifest codes are correct |
| `GET /cluster/members: connection refused` | Management server not started | Verify `management.enabled = true` in gekka config |
| `timeout waiting for MemberUp` | SBR not down-ing unreachable node | Increase `acceptable-heartbeat-pause` or reduce `stable-after` |
