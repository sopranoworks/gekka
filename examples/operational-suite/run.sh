#!/usr/bin/env bash
# run.sh — Reproduces the multi-node operational-suite:
#   - a seed node with the HTTP Management API enabled
#   - a gekka-metrics monitoring node that joins the cluster
#   - a gekka-cli invocation that lists members
#
# Usage:
#   cd examples/operational-suite
#   ./run.sh
#
# Requirements:
#   - Go toolchain (go build)
#   - Ports 2552, 2560, and 8558 must be free on localhost.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TMP="$(mktemp -d)"
trap 'echo "Cleaning up..."; kill "$SEED_PID" "$METRICS_PID" 2>/dev/null; rm -rf "$TMP"' EXIT

echo "==> Building binaries..."
go build -o "$TMP/seed-node"    "$REPO_ROOT/examples/operational-suite/seed"
go build -o "$TMP/gekka-metrics" "$REPO_ROOT/cmd/gekka-metrics"
go build -o "$TMP/gekka-cli"    "$REPO_ROOT/cmd/gekka-cli"

echo "==> Writing configs..."
cat > "$TMP/seed.conf" <<'CONF'
pekko {
  actor {
    provider = cluster
  }
  remote {
    artery {
      transport = tcp
      canonical {
        hostname = "127.0.0.1"
        port = 2552
      }
    }
  }
  cluster {
    seed-nodes = ["pekko://ClusterSystem@127.0.0.1:2552"]
  }
}
gekka {
  management {
    http {
      enabled = true
      hostname = "127.0.0.1"
      port = 8558
      health-checks {
        enabled = true
      }
    }
    debug {
      enabled = true
    }
  }
}
CONF

cat > "$TMP/metrics.conf" <<'CONF'
pekko {
  actor {
    provider = cluster
  }
  remote {
    artery {
      transport = tcp
      canonical {
        hostname = "127.0.0.1"
        port = 2560
      }
    }
  }
  cluster {
    seed-nodes = ["pekko://ClusterSystem@127.0.0.1:2552"]
  }
}
gekka {
  management {
    http {
      enabled = true
      hostname = "127.0.0.1"
      port = 8559
    }
    debug {
      enabled = true
    }
  }
}
CONF

echo "==> Starting seed node..."
"$TMP/seed-node" --config "$TMP/seed.conf" >"$TMP/seed.log" 2>&1 &
SEED_PID=$!

echo "   Waiting for management API at http://127.0.0.1:8558 ..."
for i in $(seq 1 20); do
  sleep 0.5
  if curl -sf http://127.0.0.1:8558/health/alive >/dev/null 2>&1; then
    echo "   Ready after $((i * 500)) ms."
    break
  fi
  if [ "$i" -eq 20 ]; then
    echo "ERROR: management API did not start. Seed log:"; cat "$TMP/seed.log"; exit 1
  fi
done

echo "==> Starting gekka-metrics..."
"$TMP/gekka-metrics" --config "$TMP/metrics.conf" >"$TMP/metrics.log" 2>&1 &
METRICS_PID=$!

echo "   Waiting for metrics node to join and reach Up status..."
for i in $(seq 1 30); do
  sleep 1
  UP_COUNT=$(curl -sf http://127.0.0.1:8558/cluster/members 2>/dev/null \
    | grep -o '"status":"Up"' | wc -l | tr -d ' ')
  if [ "$UP_COUNT" -ge 2 ] 2>/dev/null; then
    echo "   Both nodes Up after ${i}s."
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERROR: cluster did not converge. Logs:"; cat "$TMP/seed.log"; cat "$TMP/metrics.log"; exit 1
  fi
done

echo "   Waiting for metrics management API at http://127.0.0.1:8559 ..."
for i in $(seq 1 20); do
  sleep 0.5
  if curl -sf http://127.0.0.1:8559/health/alive >/dev/null 2>&1; then
    echo "   Ready after $((i * 500)) ms."
    break
  fi
  if [ "$i" -eq 20 ]; then
    echo "ERROR: metrics management API did not start."; cat "$TMP/metrics.log"; exit 1
  fi
done

echo ""
echo "==> gekka-cli members --url http://127.0.0.1:8558"
"$TMP/gekka-cli" members --url http://127.0.0.1:8558
echo ""

echo "==> gekka-metrics cluster_state log (last 3 lines):"
grep "cluster_state\|joined cluster" "$TMP/metrics.log" | tail -3 || \
  echo "   (no cluster_state log yet — first tick is at 30s)"

echo ""
echo "==> Demonstrating debug endpoints on seed (:8558)..."
"$TMP/gekka-cli" --url http://127.0.0.1:8558 debug crdt
"$TMP/gekka-cli" --url http://127.0.0.1:8558 debug actors --json

echo ""
echo "==> Demonstrating debug endpoints on metrics node (:8559)..."
"$TMP/gekka-cli" --url http://127.0.0.1:8559 debug crdt
"$TMP/gekka-cli" --url http://127.0.0.1:8559 debug actors --json

echo ""
echo "SUCCESS: operational-suite completed."
