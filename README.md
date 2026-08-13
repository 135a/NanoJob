# NanoJob

基于 **Go + etcd** 的分布式任务调度引擎（学习型项目），核心接口兼容 **XXL-Job** 执行器触发协议。

<p align="center">
  <img src="https://img.shields.io/badge/Language-Go-00ADD8.svg" alt="Language">
  <img src="https://img.shields.io/badge/Coordination-etcd-blue.svg" alt="etcd">
  <img src="https://img.shields.io/badge/Compatibility-XXL--Job-important.svg" alt="XXL-Job">
</p>

## 简介

NanoJob 用 Go 从零实现了分布式任务调度的核心链路：

| 层 | 实现 | 说明 |
| :--- | :--- | :--- |
| 存储与协调 | etcd | 任务定义持久化、Leader 选举、执行器心跳注册（Lease，90s TTL 自动剔除） |
| 调度内核 | 单层内存时间轮 + 圈数计数 | 插入 O(1)（受互斥锁保护）；引擎以 1s 滴答 × 60 槽运行 |
| 触发协议 | XXL-Job HTTP 协议 | 兼容执行器 `/run` 触发与 `/registry` 心跳注册的核心子集 |
| 任务 ID | Snowflake | Worker ID 由 etcd Txn 原子抢占（1~1023） |

> 定位：本仓库用于学习分布式调度、etcd 协调与容错设计，非生产级产品。当前存在若干已知限制（见下文），未在大流量下做生产验证。

## 核心机制（三个已修复的分布式问题）

### 1. Watch 统一消费 —— 修复"孤儿任务"

**问题**：旧实现里任务"写到哪台引擎就在哪台热加载调度"。请求打到 Standby（从未当选 Leader、时间轮未启动）时，任务入库后无人调度，变成孤儿。

**方案**：调度权收拢到 Leader。任意引擎（含 Standby）都只负责把任务写进 etcd；Leader 通过 etcd Watch 统一消费增量。为封死 "ListJobs 与 Watch 之间新写入被漏掉" 的竞态窗口，采用 read-then-watch：

1. `ListJobs(ctx)` 捞出存量任务，同时拿到 etcd 全局 `revision`；
2. `WatchJobs(rev+1)` 从该 revision 的下一笔增量开始监听；
3. Watch 事件到达后 `scheduleJob` 挂载进时间轮，并按 `(jobID, 触发点)` 去重，防住"Leader 写回 NextTriggerTime 被自己 Watch 到 → 自旋重复挂载"。

### 2. fail-fast 夺权 —— 修复"脑裂"

**问题**：旧代码当选后 `select {}` 空转，永不检查租约。旧 Leader 与 etcd 失联后仍僵尸化派发，与接管的新 Leader 同时下发 = 脑裂（重复触发）。

**方案**：监听两个互补信号，确认失去领导权立即停止调度：

- `session.Done()`：本地信号（etcd 连接断开 / 租约被撤销），etcd 不可达时依然有效；
- `election.Observe()`：远端信号（leader key 被删 / 被新 Leader 顶替）。

任一信号触发 → `tw.Stop()` 停时间轮 + `watcherCancel()` 停 Watch + return，`defer session.Close()` 撤销租约，让 Standby 干净接管。

⚠️ 实现细节：Observe 可能先推一条"当前主还是我自己"，此时绝不能退出（否则"当选即让位"活锁），需用 for 循环只在"key 被删 / 值不是本节点"时退出；断连时库内部 Get 失败会 close channel，从已关闭 channel 收到的是 nil，必须判空，否则 `resp.Kvs` 空指针 panic。

### 3. 确定性执行 ID + 执行器幂等 —— 修复"交接期重复"

**问题**：本系统对外契约是 at-least-once（至少一次）投递。旧 Leader 合法在位时派发过 slot N，随后失联；新 Leader 接管时无法确认"旧主派没派"，把它当漏发再补一次 → 同一触发被派发两次。这是分布式系统的结构性问题，调度层无法根除。

**方案**：调度端为每次触发生成**确定性执行 ID** = `任务ID:触发时间戳`（如 `1834567890123456789:1723456789`），通过 `executorParams` 透传给执行器；执行端（Java demo）按执行 ID 做**原子占位**去重：

- Go 端 `fireOnce`：`execID = jobID + ":" + slot`，`json.Marshal({"executionId": execID})` 写入 `RunReq.ExecutorParams`；
- Java 端 `ExecutionDedup.tryClaim()`：`XxlJobHelper.getJobParam()` 解析出 executionId，用 `ConcurrentHashMap.putIfAbsent` 原子占位，占位失败即重复派发，直接跳过。

关键点：

- execID 必须**确定性派生**（不能用随机 UUID），否则两次派发 ID 不同、执行器认不出重复；
- 执行 ID 的 `slot` 必须在派发前**同步快照**（misfire 补偿时尤其如此），异步派发内读 `job.NextTriggerTime` 会读到已被改写的下一周期值；
- demo 用进程内内存表去重，只覆盖单 JVM；多实例部署需换成共享存储（MySQL 唯一索引 / Redis SETNX）。

## 性能数据（本机实测）

以下数字来自 `core/timewheel` 微基准测试（**1ms 滴答**）：

| 指标 | 实测 | 场景 |
| :--- | :--- | :--- |
| 并发插入 | 约 115 ns/op（约 870 万次/秒，2 allocs/op） | 1ms 滴答 × 3600 槽 |
| 调度精度 | 平均偏差 4.1ms / P95 7.4ms / 最大 7.8ms | 2000 任务，2~4s 均匀延迟 |
| 内存占用 | 100 万任务 +53.5MB（单任务 56B） | 1ms 滴答 × 3600 槽 |
| 触发吞吐 | 5 万任务 3.5s 全部触发，零丢失 | 0.5~3.5s 均匀延迟 |
| 语句覆盖率 | 100% | 全套 Go 测试通过 |

⚠️ **重要**：毫秒级精度、纳秒级插入均来自 **1ms 滴答的微基准**；**引擎运行配置为 1s 滴答 × 60 槽，实际触发粒度约 1 秒**。不要把这些数字当成生产环境精度。

复现命令：

```bash
go test ./core/timewheel/ -run 'TestSchedulingPrecision|TestTriggerThroughput|TestMemoryFootprint' -v
go test ./core/timewheel/ -bench BenchmarkTimeWheelAdd -benchtime=1s -run '^$'
```

## 快速启动

### 准备

- Go 1.20+
- etcd（单节点即可）

### 1. Docker Compose（etcd + 引擎）

```bash
docker-compose up -d
```

启动后 etcd 在 `2379`、引擎在 `8080`。**注意**：Compose 只包含 etcd 和调度引擎，不含 Java 执行器；要端到端触发一个任务，还需自行启动示例执行器。

### 2. 源码启动

```bash
go run ./cmd/nanojob/main.go                          # 默认连接 127.0.0.1:2379，监听 :8080
go run ./cmd/nanojob/main.go -etcd="host:2379" -port="9090"
```

### 3. 可视化大盘

引擎**不托管静态页面**。直接双击打开 `ui/index.html`（它通过 `http://localhost:8080/api` + CORS 读取/新增任务），或用任意静态服务器托管 `ui/` 目录。

### 4. K8s 部署（示例清单）

```bash
docker build -t nanojob/engine:v1.0 .
kubectl apply -f deploy/k8s/nanojob-deployment.yaml
# 同一目录还有 etcd-deployment.yaml / java-executor-deployment.yaml 示例
```

### 5. Java 执行器接入

实现的是 XXL-Job 执行器协议核心子集，因此基于 xxl-job-core 的客户端可以接入：

```yaml
xxl:
  job:
    admin:
      addresses: http://<nanojob-engine-ip>:8080   # 替换原 xxl-job-admin 地址
```

示例执行器在 `examples/java-executor`（Spring Boot + xxl-job-core，含幂等去重 demo），执行器需向引擎 `/registry` 上报心跳。

## 目录结构

```
cmd/nanojob/             引擎入口（选主、Watch、fail-fast、时间轮挂载）
core/timewheel/          单层时间轮（tick + 圈数计数）
core/store/              etcd 持久化（ListJobs 返回全局 revision）
core/registry/           执行器心跳注册（etcd Lease，90s TTL）
core/router/             分片广播 / 单机路由
core/parser/             Spring 6 位 Cron 解析（robfig/cron）
adapter/xxljob/          XXL-Job 触发协议封装（/run）
pkg/uid/                 Snowflake 发号器
examples/java-executor/  示例 Java 执行器（含 ExecutionDedup 幂等 demo）
ui/                      可视化大盘（直接打开 index.html 使用）
```

## 已知限制（未实现）

- **触发无回调**：fire-and-forget，派发后不感知执行结果，无调度日志（`/api/callback` 未实现）。
- **接口无鉴权**：`/api/*` 与 `/api/registry` 未做认证。
- **轻微迟到会被跳过**：0~5s 内的迟到触发当次直接跳过、排到下周期；misfire 只补偿 >5s 的漏发，且每次只补一次。
- **触发粒度约 1s**：引擎时间轮 1s 滴答。
- **ROUND_ROBIN 名不副实**：当前实现固定取第一个存活节点。
- **Worker ID 租约丢失仅告警**：不退出进程，极端断连下存在 ID 冲突风险（代码注释已标注）。
- **Java 去重仅进程内**：多实例需换成 MySQL 唯一索引 / Redis SETNX 等共享存储。
- **无任务删除接口**：store 层有 `DeleteJob`，但未暴露 HTTP 路由。
- **未做集群压测**：假设单 etcd；etcd 集群、多引擎横向扩展均未验证。
