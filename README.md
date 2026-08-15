# NanoJob

基于 **Go + MySQL + Redis** 的分布式任务调度引擎（学习型项目），核心接口兼容 **XXL-Job** 执行器触发协议。

<p align="center">
  <img src="https://img.shields.io/badge/Language-Go-00ADD8.svg" alt="Language">
  <img src="https://img.shields.io/badge/Storage-MySQL-4479A1.svg" alt="MySQL">
  <img src="https://img.shields.io/badge/Coordination-Redis-DC382D.svg" alt="Redis">
  <img src="https://img.shields.io/badge/Compatibility-XXL--Job-important.svg" alt="XXL-Job">
</p>

## 简介

NanoJob 用 Go 从零实现了分布式任务调度的核心链路，砍掉了边缘功能，保留时间轮 + XXL-JOB 适配器这两个差异化卖点：

| 层 | 实现 | 说明 |
| :--- | :--- | :--- |
| 存储 | MySQL | 任务配置、触发时间、执行日志（`nanojob_job` / `nanojob_log` 两张表） |
| 选主与注册表 | Redis | 选主用 SETNX + TTL 自研锁；执行器心跳 `SET key EX 90` 过期自动摘除 |
| 调度内核 | 单层内存时间轮 + 圈数计数 | 1s 滴答 × 60 槽，**仅 Leader 运行** |
| 触发协议 | XXL-Job HTTP 协议 | `/run` 触发、`/api/callback` 结果回调、`/api/registry` 心跳 |
| 任务 ID | MySQL 自增 | 天然全局唯一，替代雪花 + WorkerID 池 |

> 定位：学习分布式调度、Redis 选主、写收敛与容错设计。存储层（MySQL/Redis）暂为单实例，应用层多副本 HA。

## 项目思维导图

```mermaid
flowchart TD
    root((NanoJob 分布式调度引擎))

    root --> loc
    loc["定位"] --> loc1["分布式任务调度"]
    loc --> loc2["XXL-Job 协议兼容"]
    loc --> loc3["学习型项目"]

    root --> tech
    tech["技术栈"] --> tech1["Go 1.26+"]
    tech --> tech2["MySQL 存储"]
    tech --> tech3["Redis 协调"]
    tech --> tech4["xxl-job-core 执行器"]

    root --> layer
    layer["架构分层"] --> l1["存储层"]
    l1 --> l1a["MySQL：任务 / 触发 / 日志"]
    l1 --> l1b["Redis：选主锁 / 注册表"]
    layer --> l2["调度内核"]
    l2 --> l2a["时间轮 1s 滴答 × 60 槽"]
    l2 --> l2b["Cron 解析器"]
    l2 --> l2c["单目标路由"]
    layer --> l3["选举与注册"]
    l3 --> l3a["SETNX + TTL 选主"]
    l3 --> l3b["心跳 TTL 注册表"]
    layer --> l4["协议层"]
    l4 --> l4a["/run 触发"]
    l4 --> l4b["/api/callback 回调"]
    l4 --> l4c["/api/registry 心跳"]
    layer --> l5["API 与 UI"]
    l5 --> l5a["管理 API"]
    l5 --> l5b["控制台大盘"]

    root --> core
    core["核心机制"] --> c1["Redis 选主"]
    c1 --> c1a["SETNX 原子抢锁"]
    c1 --> c1b["Lua 值校验续期"]
    c1 --> c1c["防双主脑裂"]
    core --> c2["写收敛 Leader"]
    c2 --> c2a["307 重定向"]
    c2 --> c2b["写前校验锁"]
    core --> c3["回调闭环"]
    c3 --> c3a["先落库拿 logId"]
    c3 --> c3b["按 logId 幂等回填"]
    core --> c4["确定性执行 ID"]
    c4 --> c4a["jobID 与 slot"]
    c4 --> c4b["执行日志关联"]
    core --> c5["故障转移"]
    c5 --> c5a["新 Leader 重载"]
    c5 --> c5b["错过即重排"]

    root --> start
    start["启动方式"] --> st1["Docker Compose"]
    start --> st2["源码启动"]
    start --> st3["种子任务"]
    start --> st4["控制台 UI"]
```

## 架构

```
            前端 UI (ui/index.html, 任意节点地址)
                    │ 写请求
                    ▼
        ┌───────────────────────────────┐
        │  Go 引擎集群 (应用层多副本)      │
        │  Leader: 时间轮调度 + 写路径    │
        │  Standby: 只收 HTTP, 写请求     │
        │          307 重定向到 Leader    │
        └──────┬───────────────┬─────────┘
               │               │
       ┌───────▼─────┐   ┌─────▼──────────┐
       │ MySQL       │   │ Redis          │
       │ 任务+触发+日志│   │ 选主锁 + 注册表 │
       └─────────────┘   └────────────────┘
               ▲
               │ /run 触发 + executionId 幂等
               │ 跑完 POST /api/callback 回填结果
        ┌──────┴──────────┐
        │ Java 执行器集群   │
        │ (xxl-job-core)  │
        └─────────────────┘
```

## 启动方法

### 准备

- Go 1.26+
- MySQL 8+ 与 Redis（单实例即可）

### 环境变量（可选，多引擎差异化时用）

配置以 `conf.json` 为基准，以下环境变量可**覆盖**关键字段（docker-compose 三引擎就是靠它让每台引擎互不相同）：

| 环境变量 | 覆盖的配置字段 | 说明 |
| :--- | :--- | :--- |
| `NANOJOB_DSN` | `mysql.dsn` | MySQL 连接串（连哪台库） |
| `NANOJOB_REDIS_ADDR` | `redis.addr` | Redis 地址（选主锁 + 注册表共用） |
| `NANOJOB_ADVERTISE_ADDR` | `api_server.http.advertise_addr` | 本节点对外地址 = 选主锁持有值 + 重定向目标，**多引擎必须各不相同** |

### 方式一：Docker Compose（MySQL + Redis + 3 引擎）

```bash
docker-compose up -d
```

一条命令拉起：MySQL（3306）、Redis（6379）、三个 Go 引擎（8081/8082/8083）。停止中间某一台引擎，另两台会自动重新选主接管——可直接观察故障转移日志。

**注意**：引擎之间用容器服务名互通（如 `nanojob1:8080`）。浏览器访问 `localhost:8081~8083` 时，若写请求落在 Standby，重定向 Location 是容器内地址、浏览器解析不了 —— 想从浏览器验证"重定向"效果，把对应引擎的 `NANOJOB_ADVERTISE_ADDR` 改成 `http://localhost:<映射端口>` 即可。

### 方式二：源码启动

```bash
# 单引擎（库和表都不用手动建 —— 引擎启动时自举建库 + 建表）
go run ./cmd/nanojob/main.go -c conf.json

# 三引擎本地演示：各开一个终端，改端口 + advertise_addr
NANOJOB_ADVERTISE_ADDR=http://127.0.0.1:9090 go run ./cmd/nanojob/main.go -c conf.json
NANOJOB_ADVERTISE_ADDR=http://127.0.0.1:9091 go run ./cmd/nanojob/main.go -c conf.json
```

引擎启动时自动做两件事：① 若 `nanojob` 库不存在则创建（库名取自 `mysql.dsn`）；② 确保 `nanojob_job` / `nanojob_log` 两表存在（`CREATE TABLE IF NOT EXISTS`，幂等）。建库建表 SQL 以独立 `.sql` 文件管理在 `core/store/sql/` 下，随二进制 `go:embed` 嵌入。

### 注入种子任务

```bash
go run ./cmd/seed/main.go    # 向 MySQL 插入一条每 10 秒触发的演示任务
```

### 可视化大盘

引擎不托管静态页面。双击打开 `ui/index.html`（通过 API + CORS 读写），可新增任务、查看每行任务的**下次触发时间**与**执行日志**。

前端在 `ui/index.html` 顶部 `API_ENGINES` 数组里配置**全部引擎地址**，请求按顺序尝试：当前引擎挂了自动切下一台（**失败重试，非随机**），写请求落在 Standby 时引擎 307 重定向到 Leader、`fetch` 自动跟随。头部状态栏会实时显示当前连的是哪台引擎——杀掉一台引擎再刷新页面，就能看到故障转移。