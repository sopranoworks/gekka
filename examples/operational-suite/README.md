# Operational Suite Example

This example demonstrates the synergy between the three operational tools:

| Component | Role |
|---|---|
| **seed node** | Single-node cluster that exposes the HTTP Management API |
| **gekka-metrics** | Monitoring node — joins the cluster with the `metrics-exporter` role and reads gossip state directly |
| **gekka-cli** | CLI that queries the Management API to list members and their roles |

## Quick Start

```bash
cd examples/operational-suite
./run.sh
```

`run.sh` builds the three binaries, writes temporary HOCON configs, starts
the seed and metrics nodes in the background, waits for both to reach **Up**
status, prints the member table from `gekka-cli`, and then shuts everything
down cleanly.

## Expected Output

```
==> gekka-cli members --url http://127.0.0.1:8558

ADDRESS                               STATUS  DC       ROLES             REACHABLE
-------                               ------  --       -----             ---------
pekko://ClusterSystem@127.0.0.1:2552  Up      default  -                 yes
pekko://ClusterSystem@127.0.0.1:2560  Up      default  metrics-exporter  yes
```

## Ports Used

| Service | Port |
|---|---|
| Seed node Artery TCP | 2552 |
| Metrics node Artery TCP | 2560 |
| Seed HTTP Management API | 8558 |

## Automated Test

The programmatic equivalent of this example lives in
[`test/integration/operational_test.go`](../../test/integration/operational_test.go).
It uses dynamic ports and runs fully in-process:

```bash
go test -tags integration -v -run TestMetricsNodeAndCLISynergy ./test/integration/
```
