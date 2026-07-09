# Orbit

**A distributed IaaS control plane, written from scratch in Go on a self-implemented Raft
consensus engine, with Postgres-backed durability, running as a 5-node quorum on
Kubernetes.**

No consensus library is used (`hashicorp/raft`, `etcd/raft`, etc.) — the election,
replication, and commit logic in [`raft.go`](./raft.go) is implemented directly
against the original paper: Ongaro & Ousterhout,
*["In Search of an Understandable Consensus Algorithm"](https://raft.github.io/raft.pdf)* (2014).

---

## What this is

Every real IaaS control plane (AWS EC2's control plane, GCP Compute Engine, and yes,
Kubernetes' own API server + etcd) is fundamentally the same shape: a **replicated
state machine tracking resource lifecycle**, coordinated by **consensus** so multiple
control-plane replicas never disagree about whether a VM is running, provisioning,
or torn down — with **durable storage** so a crashed control-plane node doesn't lose
track of what it was managing.

Orbit implements exactly that core, at small scale, with nothing hidden behind a
library:

- **A resource lifecycle state machine** — `PENDING → PROVISIONING → RUNNING →
  TERMINATING → TERMINATED` (with `FAILED` reachable from any non-terminal state) —
  for `compute`, `network`, and `storage` resources.
- **A reconciler loop** — the same "observe, diff, act" pattern every Kubernetes
  controller uses — that runs continuously on whichever node is currently leader and
  drives resources forward automatically. You `POST` a resource once; you don't
  manually walk it through its states.
- **Raft consensus** underneath both of the above, so exactly one node is ever
  "in charge" of accepting writes, and a resource's state is never ambiguous across
  replicas even during a leader failure.
- **Postgres-backed durability** for Raft's log, term, vote, and commit index, so a
  killed-and-restarted control-plane node recovers its exact prior state — including
  in-flight resource provisioning — without waiting on the rest of the cluster.

## Why this maps to a real infra/platform engineering role

| What a control-plane team actually does | Where Orbit demonstrates it |
|---|---|
| Build microservices managing virtualized resource lifecycles | `store.go` — explicit state machine, not implicit/ad-hoc state |
| High-throughput, low-latency API responses for provisioning | `api.go` — leader-local reads, quorum-committed writes, sub-second reconcile ticks |
| Resource state transitions & API orchestration | `resource.go` — `ResourceManager` (API-driven) + `Reconciler` (autonomous, controller-style) |
| Design for reliability, focus on edge cases & failure modes | Documented split-vote avoidance, log-conflict truncation, deterministic replay (see below) |
| Observability, testing coverage, robust CI/CD | 32 tests (unit + integration), race detector, GitHub Actions with a live Postgres service container |
| Working knowledge of Kubernetes, Docker, Postgres | `k8s.yaml` (StatefulSets, PDB, headless service), multi-stage `Dockerfile`, `persistence.go` |
| Production support / root-cause analysis mindset | "Known limitations" section below is written the way a real postmortem or design-review doc would be — gaps named explicitly, not hidden |

---

## Architecture

```
                            ┌───────────────────────────────┐
                            │   Client (curl / provisioning  │
                            │        API caller)             │
                            └────────────────┬────────────────┘
                                             │  Service: orbitcp (ClusterIP)
              ┌──────────────┬───────────────┼───────────────┬──────────────┐
              │              │               │               │              │
        ┌─────▼────┐   ┌─────▼────┐   ┌──────▼─────┐  ┌──────▼────┐  ┌──────▼────┐
        │  raft-0  │   │  raft-1  │   │   raft-2   │  │  raft-3   │  │  raft-4   │
        │(follower)│◄──┤ (LEADER) ├──►│ (follower) │  │(follower) │  │(follower) │
        │          │   │  + active│   │            │  │           │  │           │
        │ Raft +   │   │ reconciler│  │  Raft +    │  │ Raft +    │  │ Raft +    │
        │ Resource │   │  loop    │   │  Resource  │  │ Resource  │  │ Resource  │
        │ registry │   │          │   │  registry  │  │ registry  │  │ registry  │
        └─────┬────┘   └─────┬────┘   └──────┬─────┘  └─────┬─────┘  └─────┬─────┘
              │              │               │              │              │
              └──────────────┴───RequestVote/AppendEntries───┴──────────────┘
                              RPCs (HTTP+JSON) over headless Service
                                          │
                                  ┌───────▼────────┐
                                  │    Postgres     │   each node's WAL:
                                  │ (raft_meta,     │   currentTerm, votedFor,
                                  │  raft_log)      │   full log, commitIndex
                                  └─────────────────┘

        Cluster size: 5    Quorum (majority): 3    Tolerates: 2 node failures
```

* **Writes** (create/terminate a resource) are only accepted by the **leader**. A
  follower answers `HTTP 421` with the current `leader_id`.
* A write only returns success once **applied** — meaning it reached a **quorum** of
  nodes (`⌊N/2⌋ + 1` = 3 of 5) *and* was applied to the local state machine — not
  merely appended to the leader's own log. This distinction mattered: an earlier
  version of this code returned as soon as the entry was committed but before the
  apply loop had actually run, which meant a client could get a "success" response
  and then immediately read stale state on the very node that told them it
  succeeded. Fixed and covered by `TestPropose_*` / the reconciler tests.
* Every state mutation — `CREATE_RESOURCE`, `TRANSITION_STATE` — carries its own
  timestamp, computed **once by the leader** at propose time, not derived
  independently by `time.Now()` on each replica as it applies the entry. This is the
  same category of bug as clock-skew-sensitive TTLs: if every node computed its own
  timestamp at apply time, replicas could disagree by milliseconds about
  `updated_at`, and worse, two racing reconcile decisions on different nodes (before
  a leadership change settles) could diverge. Baking the timestamp into the
  replicated command itself makes state converge identically everywhere.
* The **reconciler** only acts while its node believes it's the leader (checked via
  `raft.GetStatus().State`), and every action still goes through `Propose()`, which
  independently re-validates leadership. A stale/late reconciler tick from a node
  that *just* lost leadership fails harmlessly with `NotLeaderError` rather than
  corrupting state — there's no separate "trust me, I'm the leader" fast path.

## Project layout

| File | Responsibility |
|---|---|
| `main.go` | Config from env vars; wires Raft + Postgres + resource store + reconciler + HTTP |
| `raft.go` | The consensus engine: elections, replication, quorum commit, persistence hooks |
| `transport.go` | HTTP+JSON RPC transport between Raft peers |
| `persistence.go` | Postgres-backed durable storage for Raft's term/vote/log/commit index |
| `store.go` | The replicated state machine: resource lifecycle registry + transition table |
| `resource.go` | `ResourceManager` (API-driven ops) + `Reconciler` (autonomous orchestration loop) |
| `api.go` | Client-facing HTTP API |
| `raft_test.go` | Unit tests for consensus safety properties |
| `store_test.go` | Unit tests for the resource state machine and transition table |
| `resource_test.go` | Unit tests for `ResourceManager` and the reconciler |
| `persistence_test.go` | Unit tests for Postgres storage + full crash-recovery scenario |
| `test/integration_test.sh` | Real 5-node + real-Postgres network integration + chaos test |
| `third_party/pq/` | Vendored copy of `github.com/lib/pq` (see note below) |
| `Dockerfile` | Multi-stage build → minimal Alpine runtime image (pure Go, no cgo) |
| `k8s.yaml` | Namespace, Postgres StatefulSet, headless/client Services, PDB, 5-node StatefulSet |
| `setup.sh` | One-command pipeline: test → build → deploy → smoke test |
| `Makefile` | `make test`, `make deploy`, etc. |
| `.github/workflows/ci.yml` | CI with a live Postgres service container, every push |
| `go.mod` | One dependency: `github.com/lib/pq` (vendored) |

**A note on the vendored dependency:** this project's dev environment had no access
to the Go module proxy, so `github.com/lib/pq` (a pure-Go Postgres driver, no cgo) is
vendored directly under `third_party/pq/` with a `replace` directive in `go.mod`
pointing at it. In a normal environment this would just be a plain `require`
resolved from `proxy.golang.org` — functionally identical either way; it only
affects where the source comes from. `go build` works fully offline as shipped.

---

## Quickstart

Requires: `docker`, [`kind`](https://kind.sigs.k8s.io/), `kubectl`, `go` 1.22+.

```bash
./setup.sh
```

This runs the **entire pipeline**: gofmt/vet checks → unit tests (spins up a
throwaway Postgres container for the persistence tests) → a real 5-node integration
test (leader election, reconciler-driven provisioning, chaos/failover,
crash-recovery) → creates a local `kind` cluster → builds and loads both the app
image and `postgres:16-alpine` → applies the manifests → waits for Postgres and all
5 control-plane pods `Ready` → port-forwards and provisions a live resource.

Tear down with `./setup.sh teardown`.

### Resource footprint (fits any free tier)

5 control-plane pods × `25m CPU / 24Mi memory` requests (`100m` / `64Mi` limits) +
one small Postgres instance (`50m CPU / 64Mi memory` requests, 1Gi storage) =
comfortably inside a local `kind`/`minikube` cluster, a single free-tier VM
(`t3.micro`, `e2-micro`, Oracle's Always-Free ARM tier), or most managed-Kubernetes
free trials.

---

## API reference

```bash
# Provision a resource. Starts PENDING; the reconciler drives it to RUNNING
# automatically over the next few seconds (standing in for a real
# hypervisor/cloud-API call).
curl -X POST http://localhost:8080/resources \
  -d '{"kind":"compute","spec":"{\"cpu\":4,\"memory_gb\":16}"}'
# -> {"id":"compute-a1b2c3d4","kind":"compute","state":"PENDING",...}

curl http://localhost:8080/resources/compute-a1b2c3d4
# ... a few seconds later ...
# -> {"id":"compute-a1b2c3d4","kind":"compute","state":"RUNNING",
#     "reason":"reconciler: provisioning complete",...}

curl http://localhost:8080/resources                      # list everything
curl -X POST http://localhost:8080/resources/compute-a1b2c3d4/terminate

curl http://localhost:8080/status
# -> {"id":"raft-1","state":"LEADER","term":3,"leader_id":"raft-1",
#     "commit_index":42,"log_length":43,"peers":4,"quorum_size":3,
#     "persistent_storage":true}
```

`kind` must be one of `compute`, `network`, `storage`. Writes against a non-leader
return `HTTP 421` with `{"error":"not leader","leader_id":"..."}`.

---

## Testing

**1. Unit tests** — `go test ./... -race -cover` (32 tests): Raft safety properties
(quorum arithmetic, vote-granting rules, log-conflict truncation), the resource state
machine's transition table (every legal/illegal transition explicitly tested), the
reconciler's timing behavior, and — with `TEST_PG_DSN` set — the Postgres persistence
layer including a **full simulated crash-and-restart** that asserts a fresh `Raft` +
empty in-memory store recovers exact prior resource state from Postgres alone.

```bash
go test ./... -race -cover                       # skips Postgres tests if no TEST_PG_DSN
TEST_PG_DSN=postgres://... go test ./... -race    # runs everything
```

**2. Integration/chaos test (`test/integration_test.sh`)** — a **real 5-node cluster
against a real Postgres instance** (not mocked), verifying end-to-end:
- exactly one leader elected
- a created resource is autonomously driven `PENDING → PROVISIONING → RUNNING` by
  the reconciler with zero manual intervention
- state replicates to all 5 nodes
- a write against a follower is correctly rejected with `421`
- the state machine allows cancelling a `PENDING` resource but rejects a redundant
  double-terminate
- **kills the leader process mid-flight** → confirms new leader election and zero
  data loss
- **kills a follower entirely, restarts it fresh** → confirms it recovers its exact
  resource state from Postgres in well under a second, before any plausible cluster
  resync — this is the core capability the persistence layer exists to provide

```bash
bash test/integration_test.sh
```

**3. CI (`.github/workflows/ci.yml`)** — `gofmt`/`vet`/race-detector unit tests
against a **live Postgres service container**, the full integration/chaos test, a
Docker build, and Kubernetes manifest validation via `kubeconform`, on every push.

---

## Design notes: edge cases and failure modes considered

Written the way a real design-review doc would be, since that's explicitly part of
what this kind of role asks for:

- **Concurrent schema migration race.** Starting all 5 nodes simultaneously against a
  shared Postgres instance hit a real, reproducible bug: concurrent `CREATE TABLE IF
  NOT EXISTS` from multiple connections raced on Postgres's own system catalog
  (`pg_type`) and failed with a spurious duplicate-key error — a known Postgres
  gotcha, not specific to this schema. Fixed with a `pg_advisory_lock` around
  migration (see `persistence.go`). Caught by actually running a 5-node cluster
  concurrently during testing, not by code review.
- **What's safe to replay on restart.** `RestoreFromStorage()` re-applies persisted
  log entries up to the persisted `commitIndex` — and *only* up to that index —
  directly to the state machine on startup, without waiting to hear from a leader.
  This is safe specifically because Raft never truncates a majority-committed
  prefix; anything at or below a previously-persisted `commitIndex` is permanently
  settled. Entries *above* that index are left for normal `AppendEntries` traffic to
  resolve (including truncating them, if a new leader's log diverges) rather than
  being blindly replayed.
- **Split-vote avoidance.** Election timeout is randomized across 1.5–3s specifically
  because it needs to comfortably exceed real pod-to-pod network latency inside
  Kubernetes; a naive Raft implementation tuned for a single-machine demo will
  produce split-vote storms in a containerized network.
- **Deterministic timestamps.** Every replicated command carries `TimestampUnix`,
  computed once at proposal time — not `time.Now()` independently at apply time on
  each node — so all replicas converge on identical `created_at`/`updated_at` values.
- **Idempotent replay.** A `CREATE_RESOURCE` for an ID that already exists is a
  no-op (not an overwrite or an error), and an invalid `TRANSITION_STATE` is a
  deterministic no-op on every replica — both matter because Raft can, in edge
  cases, redeliver log entries during leader changes, and the state machine has to
  handle that safely without special-casing it at the RPC layer.
- **Reconciler statelessness.** The reconciler holds no state of its own between
  ticks — it derives everything from `store.ListByState()` each cycle — so a
  leadership change mid-reconciliation just means the new leader's reconciler picks
  up exactly where the state machine says work is, with no handoff protocol needed.

## Known limitations & roadmap

- **Single shared Postgres instance**, not one-per-node. Each node's rows are
  correctly isolated by `node_id`, so there's no correctness issue, but that shared
  Postgres instance is itself a single point of failure for durability (not for
  availability — the Raft cluster keeps serving reads/writes on quorum even if
  Postgres is briefly down, since only the *durability* guarantee degrades, not
  liveness). A production deployment would give each node its own database/volume,
  or run Postgres itself as an HA cluster.
- **No log compaction/snapshotting** — the log (and the Postgres table backing it)
  grows unbounded. Fine for a demo, not for a long-lived cluster.
- **HTTP+JSON transport**, not gRPC — simpler and dependency-free, but higher RPC
  overhead than a real deployment would want at scale.
- **Simulated provisioning** — the reconciler's `PROVISIONING → RUNNING` transition
  is time-based (a fixed delay), standing in for what would be a real call to a
  hypervisor/cloud-provider/Terraform-style API in production.
- **No metrics endpoint** — `/status` is enough for a demo; a real deployment would
  want Prometheus counters for term changes, election frequency, reconcile latency,
  and provisioning success/failure rates.
- **No multi-region topology** — this is a single Raft group; a real multi-region
  control plane would likely shard by region with per-region quorums, and use a
  higher-level coordination layer across regions rather than one global Raft group
  (global consensus across regions means every write pays cross-region latency).

## Why "from scratch" instead of `hashicorp/raft`/etcd

In a real production system, you should absolutely use a battle-tested consensus
implementation — this project isn't an argument against that. It exists to
demonstrate the mechanism itself: what a quorum actually is, why a `StatefulSet`
(not a `Deployment`) is required for this class of system, what a durable WAL
actually has to guarantee to make crash-recovery safe, and what genuinely breaks
(and what doesn't) when you kill a live node — leader or follower — of a running
cluster.
