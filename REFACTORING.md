# v0.14.0-dev Refactoring — Ultra Thin Core Audit

> **Local working document — DO NOT COMMIT.**
> This file tracks the extraction of heavy SDKs from the core module.

---

## Guiding Principle

Every entry in `go.mod` must be justified against the "Thin Core" rule:
> The core module (`github.com/sopranoworks/gekka`) must compile using only the
> Go standard library plus minimal, stable, low-dependency packages.
> Heavy cloud and observability SDKs belong in independent extension modules.

---

## Heavy SDK Inventory (direct dependencies)

### 1. `cloud.google.com/go/spanner` — **EXTRACT**

| Item | Detail |
|------|--------|
| Source files | `persistence/spanner/journal.go`, `persistence/spanner/snapshot.go`, `persistence/spanner/codec.go`, `persistence/spanner/schema.go` |
| Target | `extensions/persistence/spanner` (own `go.mod`) |
| Interface | `persistence.Journal` + `persistence.SnapshotStore` (already defined; no changes needed) |
| Transitive cost | pulls in `google.golang.org/grpc`, `google.golang.org/api`, `cel.dev/expr`, `cloud.google.com/go/iam`, `cloud.google.com/go/longrunning`, dozens of googleapis genproto packages |

### 2. `go.opentelemetry.io/otel` (and sub-modules) — **EXTRACT / ISOLATE**

| Item | Detail |
|------|--------|
| Source files (core — must fix) | `persistence/journal.go` (TracingJournal), `cluster/sharding/region.go` (injectTraceContext), `persistence/projection/projection.go` (runManual) |
| Source files (intentionally external) | `telemetry/otel/provider.go`, `cmd/gekka-metrics/main.go` |
| Fix strategy | Replace `otel.Tracer()` / `otel.GetTextMapPropagator()` with `telemetry.GetTracer()` from the existing `telemetry` abstraction package — no OTel import required. |
| Target for extension | `extensions/telemetry/otel` (own `go.mod`) — houses `telemetry/otel/provider.go` once migration is complete |
| Transitive cost | `go.opentelemetry.io/otel/sdk`, `otel/sdk/metric`, `otel/exporters/otlp/...`, `grpc-ecosystem/grpc-gateway`, `go.opentelemetry.io/proto/otlp` |

### 3. `k8s.io/client-go` + `k8s.io/api` — **EXTRACT**

| Item | Detail |
|------|--------|
| Source files | `cluster/sbr/k8s_provider.go`, `discovery/kubernetes/api_provider.go` |
| Target | `extensions/cluster/k8s` (own `go.mod`) |
| Interface | `cluster/sbr.DowningProvider` + `discovery.Provider` (standard interfaces; no changes needed) |
| Transitive cost | `k8s.io/apimachinery`, `k8s.io/klog`, `k8s.io/kube-openapi`, `sigs.k8s.io/structured-merge-diff`, `github.com/emicklei/go-restful`, dozens of k8s ecosystem packages |

### 4. `github.com/jackc/pgx/v5` — **EXTRACT**

| Item | Detail |
|------|--------|
| Source files | `persistence/sql/journal.go`, `persistence/sql/snapshot.go`, `persistence/sql/codec.go` |
| Note | The `persistence/sql` package uses `database/sql` (stdlib) internally; `pgx` is only needed by the driver registration. Move driver + integration to `extensions/persistence/sql`. |
| Target | `extensions/persistence/sql` (own `go.mod`) |

### 5. `github.com/testcontainers/testcontainers-go` — **TEST-ONLY (low priority)**

| Item | Detail |
|------|--------|
| Source files | `persistence/sql/sqltest/postgres_test.go`, `test/bench/persistence_test.go` |
| Status | Test-only; does not affect production binary. Extract to a dev-tools module or accept as test dependency. |

---

## Packages Already Clean (no action needed)

| Package | Reason |
|---------|--------|
| `telemetry/telemetry.go` | Pure interfaces + no-op defaults; zero OTel imports |
| `telemetry/noop.go` | Stdlib-only |
| `persistence/snapshot.go` | Stdlib-only |
| `persistence/in_memory.go` | Stdlib-only |
| `persistence/sql/journal.go` | Uses `database/sql` (stdlib) |
| `actor/`, `internal/core/` | No heavy SDK imports |

---

## Extraction Order (recommended)

1. **v0.14.0-dev** (current): Fix OTel imports in core packages (journal, region, projection).  Create extension skeletons.
2. **v0.13.1-dev**: Move `persistence/spanner/` → `extensions/persistence/spanner/` with own `go.mod`. Remove `cloud.google.com/go/spanner` from root `go.mod`.
3. **v0.13.2-dev**: Move `telemetry/otel/` → `extensions/telemetry/otel/` with own `go.mod`. Remove `go.opentelemetry.io/otel*` from root `go.mod`.
4. **v0.13.3-dev**: Move `cluster/sbr/k8s_provider.go` + `discovery/kubernetes/` → `extensions/cluster/k8s/` with own `go.mod`. Remove `k8s.io/*` from root `go.mod`.
5. **v0.13.4-dev**: Move `persistence/sql/` → `extensions/persistence/sql/`. Remove `github.com/jackc/pgx/v5` from root `go.mod`.
6. **v0.14.0-dev** (release): Root `go.mod` contains only: `google.golang.org/protobuf`, `gopkg.in/yaml.v3`, `github.com/sopranoworks/gekka-config`, `github.com/stretchr/testify`, `github.com/spf13/cobra`, Charmbracelet UI libs.

---

## Definition of Done for v0.14.0-dev

- [x] `version.go` → `"0.14.0-dev"`
- [x] `persistence/journal.go` — no `go.opentelemetry.io` imports
- [x] `cluster/sharding/region.go` — no `go.opentelemetry.io` imports
- [x] `persistence/projection/projection.go` — no `go.opentelemetry.io` imports
- [x] `extensions/persistence/spanner/` skeleton created
- [x] `extensions/telemetry/otel/` skeleton created
- [ ] `go build ./...` passes on core module
- [ ] `persistence/spanner/` physically relocated (v0.13.1-dev)
- [ ] `telemetry/otel/` physically relocated (v0.13.2-dev)
- [ ] `k8s.io/*` relocated (v0.13.3-dev)
