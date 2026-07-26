# NanoJob: Cloud-Native Distributed Scheduling Engine

<p align="center">
  <img src="https://img.shields.io/badge/Language-Go-00ADD8.svg" alt="Language">
  <img src="https://img.shields.io/badge/Architecture-Cloud%20Native-blueviolet.svg" alt="Architecture">
  <img src="https://img.shields.io/badge/Store-etcd-red.svg" alt="Store">
  <img src="https://img.shields.io/badge/Ecosystem-Kubernetes-326ce5.svg" alt="Kubernetes">
</p>

## 📖 Introduction

**NanoJob** is a high-performance, distributed job scheduling control plane rewritten from the ground up in Go. 
It maintains **100% backward compatibility** with the underlying RPC protocol of traditional enterprise schedulers (such as XXL-Job), but completely redesigns the core architecture to solve historical pain points like heavy database polling, high latency, and deployment bloat.

NanoJob is built for the Cloud-Native era, serving as the sharpest, most lightweight scheduling solution for massive microservice architectures.

---

## 🎯 Why NanoJob? (Solving Industry Pain Points)

In massive data processing scenarios (e.g., millions of overnight interest calculations, timeout scanning for e-commerce orders), traditional Java-based scheduling frameworks face fatal bottlenecks. NanoJob crushes them one by one:

1. **Eradicate Database Polling Avalanches**
   Traditional frameworks rely on infinite loop polling against MySQL to find triggered jobs, easily causing table locks and database crashes under heavy loads. NanoJob completely abandons DB polling, utilizing a **Hierarchical TimeWheel** algorithm for lock-free, O(1) complexity, millisecond-precision triggering.
2. **Eliminate Deployment Bloat**
   Traditional schedulers are heavy Spring Boot processes, slow to start and consuming hundreds of megabytes of RAM per node. NanoJob compiles into a minimalist Linux static binary (< 20MB) with an extremely low memory footprint. Its millisecond-level startup speed makes it a perfect fit for Kubernetes (K8s) instantaneous scaling.
3. **Prevent Split-Brain with Strong Consistency**
   Traditional schedulers use relational databases for registration and distributed locks, which are prone to "split-brain" under severe network jitter, leading to disastrous duplicate executions. NanoJob embraces **etcd**—the de facto Cloud-Native standard—leveraging its Raft algorithm to guarantee absolute strong consistency.

---

## ⚔️ Architecture Comparison

| Feature | Traditional Java Scheduler (e.g., XXL-Job) | NanoJob Engine |
| :--- | :--- | :--- |
| **Core Engine** | Java ThreadPool + Relational DB infinite polling | Golang Goroutines + Hierarchical TimeWheel |
| **Storage Medium** | MySQL (Write-heavy, concurrent bottlenecks) | etcd (Strong consistency KV, supports Watch) |
| **Heartbeat Registry** | Heavy `UPDATE` queries on DB | In-memory routines with 90s TTL auto-eviction |
| **Cloud-Native Fit**| Poor (Stateful, heavy JVM/Tomcat dependencies) | Excellent (Stateless, 12-Factor app, K8s friendly) |
| **High Availability** | Pessimistic DB locks | Distributed Lease & Mutex via etcd |
| **Compatibility** | - | **100% Zero-Code Migration** (Drop-in replacement for Java clients) |

---

## 🚀 Core Architectural Highlights

1. **Dynamic Sharding (MapReduce-like)**
   When a job is triggered, the engine does not blindly dump millions of records onto a single machine. It dynamically senses the total number of alive Java nodes in the cluster. Using a **Broadcast + Modulo algorithm**, it dispatches unique `Index` parameters to each node. Compute power scales infinitely as you add more machines!
2. **K8s Decoupling & Injection**
   Fully embraces Infrastructure as Code (IaC). NanoJob avoids hardcoded configs, exposing parameters via CLI flags. In K8s, CoreDNS domains are injected via YAML args, allowing operators to hot-switch etcd IPs or listening ports without rebuilding code.
3. **High Availability & Split-Brain Defense**
   Powered by etcd's `concurrency.NewElection`, NanoJob guarantees strict Leader-Follower consistency. Even if deployed with 10 K8s replicas, only 1 Leader commands the TimeWheel. Severe network partitions will automatically trigger safe failovers, strictly preventing duplicate job execution (Split-Brain).
4. **Stateless Dynamic Registry**
   Completely eliminated legacy in-memory `sync.Map` islands. Executor heartbeats are directly bound to etcd **Leases**. If a node dies, etcd auto-expires its lease globally. The Leader accesses a real-time, globally consistent view before every dispatch, guaranteeing 100% accurate sharding routing.
5. **Misfire Compensation Strategy**
   Zero tolerance for business data loss during power outages. When the NanoJob cluster recovers from total downtime, the newly elected Leader automatically audits the database. Any severely delayed critical tasks are instantly salvaged via a forced `FIRE_ONCE_NOW` compensation before resuming normal TimeWheel cadence.

---

## 🛠️ Quick Start

### Prerequisites
- Go 1.20+
- etcd server (Local single-node or Cloud cluster)

### 1. Minimal Local Startup
Fire up NanoJob easily via CLI flags, dynamically binding your etcd node and port:

```bash
# Default (Connects to 127.0.0.1:2379, listens on 8080)
go run ./cmd/nanojob/main.go

# Production-grade startup with custom DNS and port
go run ./cmd/nanojob/main.go -etcd="etcd-service.local:2379" -port="9090"
```
Once started, visit `http://127.0.0.1:8080` in your browser to experience the sleek, dark-mode visual dashboard!

### 2. Kubernetes Deployment (Recommended)
NanoJob provides a native `Dockerfile` and standard `deployment.yaml` for a seamless Cloud-Native deployment:

```bash
# 1. Build the lightweight Alpine image
docker build -t nanojob/engine:v1.0 .

# 2. Deploy to K8s cluster
kubectl apply -f deploy/k8s/nanojob-deployment.yaml
```

### 3. Java Client Integration (Zero Intrusion)
We achieved stunning backward compatibility! For your downstream Java/Spring Boot applications, **you do not need to modify a single line of core business code.**
Simply update your `application.yml` to replace the old admin IP with the new NanoJob engine IP:

```yaml
xxl:
  job:
    admin:
      # Old Address: addresses: http://192.168.1.1:8080/xxl-job-admin
      # New NanoJob Address:
      addresses: http://nanojob-engine-ip:8080
```
Restart your Java app, and your Java legion will obediently report to NanoJob, ready to receive TimeWheel dispatches!

---
*Built with ❤️ for the Cloud-Native Community.*
