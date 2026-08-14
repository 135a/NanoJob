# NanoJob

A distributed job scheduling engine written in **Go**, backed by **MySQL + Redis**, with core protocol compatibility with **XXL-Job** executors.

<p align="center">
  <img src="https://img.shields.io/badge/Language-Go-00ADD8.svg" alt="Language">
  <img src="https://img.shields.io/badge/Storage-MySQL-4479A1.svg" alt="MySQL">
  <img src="https://img.shields.io/badge/Coordination-Redis-DC382D.svg" alt="Redis">
  <img src="https://img.shields.io/badge/Compatibility-XXL--Job-important.svg" alt="XXL-Job">
</p>

> **Status: learning project.** Built to study distributed scheduling, Redis leader election, write convergence, and fault tolerance. Storage (MySQL/Redis) is single-instance; app-layer engines are HA replicas.

## Overview

| Layer | Implementation | Notes |
| :--- | :--- | :--- |
| Storage | MySQL | Job config, next-fire-time, execution logs (`nanojob_job` / `nanojob_log`) |
| Election & registry | Redis | SETNX + TTL custom lock for leader election; executor heartbeat `SET key EX 90` auto-eviction |
| Scheduling core | Single-level in-memory time wheel + circle counting | 1s tick × 60 slots, runs **on the Leader only** |
| Trigger protocol | XXL-Job HTTP subset | `/run` trigger, `/api/callback` result callback, `/api/registry` heartbeat |
| Job ID | MySQL auto-increment | Globally unique by construction (replaces Snowflake + WorkerID pool) |

## Project mind map

```mermaid
mindmap
  root((NanoJob scheduling engine))
    What it is
      Distributed job scheduling
      XXL-Job protocol compatible
      Learning project
    Tech stack
      Go 1.26+
      MySQL storage
      Redis coordination
      xxl-job-core executors
    Architecture
      Storage layer
        MySQL jobs triggers logs
        Redis election lock + registry
      Scheduling core
        1s tick 60 slots
        Cron parser
        Single-target router
      Election and registry
        SETNX + TTL election
        Heartbeat TTL registry
      Protocol layer
        /run trigger
        /api/callback callback
        /api/registry heartbeat
      API and UI
        Admin API
        Dashboard
    Core mechanisms
      Redis leader election
        Atomic SETNX lock
        Lua value-check renewal
        No split-brain
      Write convergence
        307 redirect
        VerifyLeadership before write
      Callback loop
        Insert log first, get logId
        Idempotent backfill by logId
      Deterministic exec ID
        jobID and slot
        Atomic dedup in Java
      Failover
        New leader reloads
        Skip missed, reschedule now
    How to start
      Docker Compose
      Run from source
      Seed job
      Dashboard UI
```

## Architecture

```
              Frontend (ui/index.html, any engine address)
                       │ writes
                       ▼
        ┌───────────────────────────────┐
        │  Go engine cluster (HA)       │
        │  Leader: time wheel + writes  │
        │  Standby: 307-redirect writes │
        │          to the Leader        │
        └──────┬───────────────┬─────────┘
               │               │
       ┌───────▼─────┐   ┌─────▼──────────┐
       │ MySQL       │   │ Redis          │
       │ jobs+logs   │   │ lock+registry  │
       └─────────────┘   └────────────────┘
               ▲
               │ /run trigger + executionId idempotency
               │ POST /api/callback with the result
        ┌──────┴──────────┐
        │ Java executors  │
        │ (xxl-job-core)  │
        └─────────────────┘
```

### Core mechanisms

1. **Redis leader election** — `SET key <nodeID> NX EX 5` is atomic acquisition; the Leader renews the TTL through a Lua script that **re-validates the current value** (only renew if the key still holds its own ID), so a disconnected old leader can never renew alongside the new one (split-brain). The lock value doubles as the Leader's advertised address for redirects.

2. **Write convergence (etcd Watch and the 3-layer dedup removed)** — the Leader persists and mounts new jobs directly; a Standby receiving a write replies **307 to the Leader**, and the browser `fetch` follows with the body intact. Right before writing, the Leader calls `VerifyLeadership()` (one more Redis GET) to cover the gap between the check and the write.

3. **Callback loop (`/api/callback` + `nanojob_log`)** — before triggering, the engine inserts a "running" log row and passes `LogID`/`LogDateTime` in `RunReq`; after the job finishes, `xxl-job-core` auto-POSTs to `/api/callback`, which idempotently backfills `handleCode` (200/500) and `handleMsg`. The endpoint is registered on **every** engine (logs go to shared MySQL keyed by auto-increment logId, so no Leader funneling is needed). Note the field spelling `logDateTim` (not `logDateTime`).

4. **Deterministic execution ID** — `execID = jobID:slot` sent via `executorParams`; the Java executor atomically claims it to skip duplicate dispatches across leader handover (at-least-once delivery).

5. **Failover** — the new Leader loads all jobs from MySQL and remounts the wheel. A missed fire (previous trigger point already in the past) is **skipped and rescheduled from now** — deterministic, no misfire compensation.

## Startup

### Prerequisites

- Go 1.26+
- MySQL 8+ and Redis (single instance is enough)

### Environment variables (optional, needed for multi-engine setups)

Config is driven by `conf.json`; these variables **override** key fields (docker-compose gives each engine a distinct identity this way):

| Env var | Overrides | Notes |
| :--- | :--- | :--- |
| `NANOJOB_DSN` | `mysql.dsn` | MySQL DSN (which database to connect to) |
| `NANOJOB_REDIS_ADDR` | `redis.addr` | Redis address (shared by election lock + registry) |
| `NANOJOB_ADVERTISE_ADDR` | `api_server.http.advertise_addr` | This node's public address = election-lock value + redirect target; **must differ across engines** |

### Option A: Docker Compose (MySQL + Redis + 3 engines)

```bash
docker-compose up -d
```

Brings up MySQL (3306), Redis (6379), and three Go engines (8081/8082/8083). Stop any engine and the others re-elect a Leader — watch the failover logs.

> Engines talk to each other via container service names (`nanojob1:8080`). To exercise the browser redirect, set `NANOJOB_ADVERTISE_ADDR` to a `localhost:<mapped-port>` address.

### Option B: Run from source

```bash
# Single engine (database and tables are created automatically)
go run ./cmd/nanojob/main.go -c conf.json

# Three engines locally: one terminal each, vary the port + advertise_addr
NANOJOB_ADVERTISE_ADDR=http://127.0.0.1:9090 go run ./cmd/nanojob/main.go -c conf.json
NANOJOB_ADVERTISE_ADDR=http://127.0.0.1:9091 go run ./cmd/nanojob/main.go -c conf.json
```

On startup the engine (1) creates the `nanojob` database if missing (name taken from `mysql.dsn`), and (2) ensures the `nanojob_job` / `nanojob_log` tables exist (`CREATE TABLE IF NOT EXISTS`, idempotent). The DDL lives as standalone `.sql` files under `core/store/sql/`, embedded into the binary via `go:embed`.

### Seed a demo job

```bash
go run ./cmd/seed/main.go     # inserts a job that fires every 10 seconds
```

### Web dashboard

Open `ui/index.html` directly in a browser (talks to the engines over CORS). Shows each job's next-fire-time and execution logs.

The frontend lists **all engine addresses** in the `API_ENGINES` array at the top of `ui/index.html` and tries them in order: if the current engine dies it fails over to the next one automatically (ordered retry, not random). Writes landing on a Standby are 307-redirected to the Leader and `fetch` follows. The header status bar shows which engine is currently connected — kill an engine and refresh to watch the failover.

### Verify the core loop (curl)

```bash
# 1. Simulate an executor heartbeat → registers in Redis (auto-evicted after TTL 90s)
curl -X POST http://127.0.0.1:8080/api/registry \
  -d '{"registryGroup":"EXECUTOR","registryKey":"loan-service","registryValue":"192.168.1.100:9999"}'

# 2. Add a job (writes converge to the Leader; returns the auto-increment id)
curl -X POST http://127.0.0.1:8080/api/job/add \
  -H 'Content-Type: application/json' \
  -d '{"cron":"0/10 * * * * ?","executorHandler":"loanInterestJobHandler","appName":"loan-service"}'

# 3. List jobs (with next-fire-time)
curl http://127.0.0.1:8080/api/job/list

# 4. Simulate the Java callback → backfills that execution's log result (grab logId from the log list)
curl -X POST http://127.0.0.1:8080/api/callback \
  -d '[{"logId":1,"logDateTim":1700000000000,"handleCode":200,"handleMsg":"ok"}]'

# 5. List execution logs (handleCode: 0=running / 200=ok / 500=failed)
curl 'http://127.0.0.1:8080/api/job/logs?id=1'
```

### Java executor integration

```yaml
xxl:
  job:
    admin:
      addresses: http://<nanojob-engine-ip>:8080
    executor:
      appname: loan-service
```

See `examples/java-executor` (Spring Boot + xxl-job-core, includes the `ExecutionDedup` demo). The executor must report heartbeats to `/registry`.

### Troubleshooting

| Symptom | Where to look |
| :--- | :--- |
| Startup fails to connect MySQL | MySQL not running, or `mysql.dsn` user/password/port wrong in `conf.json` |
| Startup fails to connect Redis | Redis not running, or `redis.addr` wrong |
| No leader is ever elected | Redis down, or all engines share the same `advertise_addr` (lock-value conflict) |
| Jobs not dispatched / logs stuck at "running" | No live executor under that AppName (no heartbeat to `/api/registry`) |
| Adding a job from the browser fails | Write landed on a Standby and the redirect target is unreachable (container service name — see Option A note) |
| Port `8080` taken | Change `port` in `conf.json` and the port inside `advertise_addr` accordingly |

## Directory layout

```
cmd/nanojob/               engine entrypoint (config, election, API wiring)
cmd/seed/                  MySQL seed job injector
core/timewheel/            single-level time wheel (tick + circle counting)
core/store/                MySQL persistence (jobs + logs; sql/ holds the DDL files, go:embed)
core/registry/             executor heartbeat registry (Redis TTL, 90s)
core/election/             Redis SETNX+TTL election (Lua value-check renewal)
core/scheduler/            scheduling core (wheel, dispatch, logs, callbacks)
core/router/               single-target routing (round-robin)
core/parser/               Spring 6-field cron parsing (robfig/cron)
adapter/xxljob/            XXL-Job trigger protocol (/run)
api/                       admin API + /api/callback + /api/registry
pkg/config/                JSON config loader (env overrides)
examples/java-executor/    sample Java executor (with ExecutionDedup demo)
ui/                        web dashboard (open index.html directly)
conf.json                  default config
```

## Differences from the old (etcd) architecture

| Aspect | Old (etcd) | New (MySQL + Redis) |
| :--- | :--- | :--- |
| Storage | etcd: jobs + fire time | MySQL: jobs + fire time + **execution logs** |
| Election | etcd concurrency Election | Redis SETNX + TTL custom lock (Lua value-check renewal) |
| Incremental consumption | etcd Watch + read-then-watch | **write convergence** (307 redirect + pre-write lock check) |
| Dedup | 3-layer dedup | removed (no more watch loop) |
| Misfire | compensate once (>5s) | removed (skip and reschedule from now) |
| Routing | SHARDING broadcast | removed, single-target round-robin |
| Job ID | Snowflake + WorkerID pool | MySQL auto-increment |
| Result feedback | fire-and-forget | **/api/callback loop + log persistence** |
| Deploy | docker-compose(etcd) + K8s | docker-compose(MySQL+Redis+3 engines); K8s dropped |

## Known limitations

- **No auth** on `/api/*`, `/api/callback`, `/api/registry`.
- **Single-instance storage**: MySQL/Redis are single nodes; app-layer HA only.
- **~1s trigger granularity**: the engine wheel ticks at 1s.
- **Redirect-based write convergence**: Standby redirects rely on the client following 307; container service names aren't resolvable from a browser (see Compose note above).
- **Java dedup is in-process only** (single JVM demo).
- **No job-delete HTTP route** (`DeleteJob` exists in the store layer).
- **No cluster stress-testing**; verified on single MySQL/Redis.
