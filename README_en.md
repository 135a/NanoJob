# NanoJob

A distributed job scheduling engine written in **Go**, coordinated by **etcd**, with core protocol compatibility with **XXL-Job** executors.

<p align="center">
  <img src="https://img.shields.io/badge/Language-Go-00ADD8.svg" alt="Language">
  <img src="https://img.shields.io/badge/Coordination-etcd-blue.svg" alt="etcd">
  <img src="https://img.shields.io/badge/Compatibility-XXL--Job-important.svg" alt="XXL-Job">
</p>

> **Status: learning project.** Built to study distributed scheduling, etcd coordination, and fault-tolerance design. Not production-grade; several limitations are listed below.

## Overview

| Layer | Implementation | Notes |
| :--- | :--- | :--- |
| Storage & coordination | etcd | Job persistence, leader election, executor heartbeat registry (Lease, 90s TTL auto-eviction) |
| Scheduling core | Single-level in-memory time wheel + circle counting | O(1) insertion (mutex-protected); engine runs at **1s tick × 60 slots** |
| Trigger protocol | XXL-Job HTTP subset | Executor `/run` trigger + `/registry` heartbeat registration |
| Job ID | Snowflake | Worker ID atomically claimed via etcd Txn (1–1023) |

## Core mechanisms (three distributed-systems bugs fixed)

### 1. Unified Watch consumption — fixes "orphan jobs"

**Problem**: previously a job was hot-loaded for scheduling on whichever engine received the write. A request that hit a Standby node (never elected, time wheel never started) left the job persisted but never scheduled — an orphan.

**Fix**: scheduling is centralized on the Leader. Any engine (including Standby) only writes the job to etcd; the Leader consumes all incremental writes through etcd Watch. To close the race window between `ListJobs` and `Watch`, it uses **read-then-watch**:

1. `ListJobs(ctx)` returns existing jobs **and** the etcd global `revision`;
2. `WatchJobs(rev+1)` starts watching from the next revision;
3. each Watch event is mounted via `scheduleJob`, deduplicated by `(jobID, trigger point)` to avoid re-mounting the Leader's own `NextTriggerTime` write-back (spin loop).

### 2. Fail-fast leadership loss — fixes "split-brain"

**Problem**: the old code spun on `select {}` after winning the election and never checked the lease. A disconnected old Leader kept dispatching like a zombie alongside the new Leader = split-brain (duplicate triggers).

**Fix**: watch two complementary signals and stop scheduling the moment leadership is confirmed lost:

- `session.Done()` — local signal (etcd connection lost / lease revoked), still fires when etcd is unreachable;
- `election.Observe()` — remote signal (leader key deleted / replaced by a new Leader).

On either signal: `tw.Stop()` (stop the wheel) + `watcherCancel()` (stop the watcher) + return; `defer session.Close()` revokes the lease so a Standby can take over cleanly.

⚠️ Implementation detail: `Observe` may first push "I'm still the leader" — exiting on that would create a "win-then-immediately-yield" live-lock, so a loop exits only when the key is gone or holds a different node ID. On disconnect the library closes the channel internally and a closed-channel receive yields `nil` — guard against nil before touching `resp.Kvs` to avoid a panic.

### 3. Deterministic execution ID + executor idempotency — fixes "handover duplicate"

**Problem**: the delivery contract is at-least-once. The old leader legitimately dispatched slot N, then lost connectivity; the new leader can't know whether it was dispatched, so it compensates as a missed fire → slot N dispatched twice. This is structural in distributed systems and cannot be fully removed at the scheduler layer.

**Fix**: the engine generates a **deterministic execution ID** = `jobID:triggerTimestamp` (e.g. `1834567890123456789:1723456789`), passed via `executorParams`; the Java executor atomically claims it before running:

- Go `fireOnce`: `execID = jobID + ":" + slot`, marshaled as `{"executionId": execID}` into `RunReq.ExecutorParams`;
- Java `ExecutionDedup.tryClaim()`: parses the ID from `XxlJobHelper.getJobParam()` and claims it with `ConcurrentHashMap.putIfAbsent`; a failed claim means a duplicate dispatch → skip.

Key points:

- the exec ID must be **deterministically derived** (never a random UUID), otherwise two dispatches yield different IDs and the executor can't recognize the duplicate;
- the `slot` must be **snapshotted synchronously** before async dispatch (especially in misfire compensation), otherwise the async goroutine reads `job.NextTriggerTime` after it has been advanced to the next period;
- the demo dedup table is in-process only (single JVM); multi-instance deployments need shared storage (MySQL unique index / Redis SETNX).

## Benchmarks (measured on this machine)

Numbers below come from `core/timewheel` microbenchmarks (**1ms tick**):

| Metric | Measured | Scenario |
| :--- | :--- | :--- |
| Concurrent insert | ~115 ns/op (~8.7M ops/s, 2 allocs/op) | 1ms tick × 3600 slots |
| Scheduling precision | avg 4.1ms / p95 7.4ms / max 7.8ms deviation | 2000 tasks, uniform 2–4s delays |
| Memory footprint | 1,000,000 tasks +53.5MB (56B/task) | 1ms tick × 3600 slots |
| Trigger throughput | 50,000 tasks all fired in 3.5s, zero loss | uniform 0.5–3.5s delays |
| Statement coverage | 100% | full Go test suite green |

⚠️ **Important**: the millisecond precision and sub-microsecond inserts come from a **1ms-tick microbenchmark**. The engine runs at a **1s tick × 60 slots**, so real trigger granularity is ~1 second. Don't present the microbenchmark numbers as production accuracy.

Reproduce:

```bash
go test ./core/timewheel/ -run 'TestSchedulingPrecision|TestTriggerThroughput|TestMemoryFootprint' -v
go test ./core/timewheel/ -bench BenchmarkTimeWheelAdd -benchtime=1s -run '^$'
```

## Quick start

### Prerequisites

- Go 1.20+
- etcd (a single node is enough)

### 1. Docker Compose (etcd + engine)

```bash
docker-compose up -d
```

etcd on `2379`, engine on `8080`. **Note**: the compose file only includes etcd and the engine, not a Java executor — bring up the sample executor to run an end-to-end job.

### 2. Run from source

```bash
go run ./cmd/nanojob/main.go                          # etcd 127.0.0.1:2379, listen :8080
go run ./cmd/nanojob/main.go -etcd="host:2379" -port="9090"
```

### 3. Web dashboard

The engine does **not** serve static files. Open `ui/index.html` directly in a browser (it talks to `http://localhost:8080/api` over CORS), or serve the `ui/` directory with any static server.

### 4. Kubernetes (sample manifests)

```bash
docker build -t nanojob/engine:v1.0 .
kubectl apply -f deploy/k8s/nanojob-deployment.yaml
# the same directory also has etcd-deployment.yaml / java-executor-deployment.yaml
```

### 5. Java executor integration

The engine implements the core subset of the XXL-Job executor protocol, so xxl-job-core clients can plug in:

```yaml
xxl:
  job:
    admin:
      addresses: http://<nanojob-engine-ip>:8080   # replace the old xxl-job-admin address
```

See `examples/java-executor` (Spring Boot + xxl-job-core, includes the idempotency demo). The executor must report heartbeats to the engine's `/registry`.

## Directory layout

```
cmd/nanojob/             engine entrypoint (election, Watch, fail-fast, wheel mounting)
core/timewheel/          single-level time wheel (tick + circle counting)
core/store/              etcd persistence (ListJobs returns global revision)
core/registry/           executor heartbeat registry (etcd Lease, 90s TTL)
core/router/             sharding broadcast / single-node routing
core/parser/             Spring 6-field cron parsing (robfig/cron)
adapter/xxljob/          XXL-Job trigger protocol (/run)
pkg/uid/                 Snowflake ID generator
examples/java-executor/  sample Java executor (with ExecutionDedup demo)
ui/                      web dashboard (open index.html directly)
```

## Known limitations

- **No execution callback**: fire-and-forget dispatch; no result/status feedback, no scheduling logs (`/api/callback` not implemented).
- **No auth** on `/api/*` or `/api/registry`.
- **Slightly late fires are skipped**: a trigger within 0–5s late is skipped for that cycle and rescheduled; misfire compensation only covers >5s and fires once.
- **~1s trigger granularity**: engine wheel ticks at 1s.
- **ROUND_ROBIN is a misnomer**: it currently always picks the first alive node.
- **Worker ID lease loss only logs a warning** (does not exit the process); ID collision is possible under extreme disconnects.
- **Java dedup is in-process only**: multi-instance needs MySQL unique index / Redis SETNX.
- **No job-delete API**: `DeleteJob` exists in the store layer but has no HTTP route.
- **No cluster stress-testing**: assumes a single etcd; etcd cluster / multi-engine scaling unverified.
