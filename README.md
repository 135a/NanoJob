# NanoJob 🚀 

<p align="center">
  <img src="https://img.shields.io/badge/Language-Go-00ADD8.svg" alt="Language">
  <img src="https://img.shields.io/badge/Architecture-Cloud%20Native-blueviolet.svg" alt="Architecture">
  <img src="https://img.shields.io/badge/Store-etcd-red.svg" alt="Store">
</p>

## 📖 项目简介

NanoJob 是一款完全基于 Go 语言重写底层引擎的**高性能、云原生分布式调度控制面**。
它不仅完美兼容传统大厂调度中心（如 XXL-Job）的底层 HTTP 通信协议，更彻底重构了控制面的核心架构。通过引入 `etcd` 强一致性协调和 `多层内存时间轮算法`，NanoJob 旨在彻底解决大规模微服务架构下定时任务“调度慢、并发低、数据库易雪崩、脑裂频发”的历史顽疾，是云原生（Cloud-Native）时代最锋利的轻量级调度利器。

---

## 🎯 传统架构的“阿喀琉斯之踵”与 NanoJob 的降维打击

在金融、电商等海量数据处理场景中，传统的 Java 任务调度框架往往面临以下致命痛点，而 NanoJob 凭借创新的架构将其逐一击破：

### 1. 彻底告别数据库轮询引发的“性能雪崩”
- **痛点**：传统框架强依赖一个死循环线程去轮询 MySQL，不断扫表寻找即将触发的任务。数据量稍大极易引发全局锁表和 DB 崩溃。
- **降维打击**：NanoJob 彻底抛弃了 DB 轮询，采用底层优化的**多层内存时间轮 (Hierarchical TimeWheel)** 算法，并结合位掩码 (Bitmask) 时间压缩技术，实现时间复杂度为 `O(1)` 的无锁化、纳秒级精准调度触发。

### 2. CP 控制面 + AP 数据面的“大厂黄金架构”
- **痛点**：传统调度依靠关系型数据库充当注册中心和分布式锁，在网络剧烈抖动时极易发生脑裂，导致多台机器重复触发，引发灾难性后果（如重复扣款）。
- **降维打击**：NanoJob 采用教科书级的分布式设计哲学：
  - **控制面 (Go 引擎 + etcd) = 绝对 CP 架构**：引入云原生事实标准 `etcd`，借助 Raft 算法提供分布式选主与元数据强一致性保证，宁可宕机拒绝服务也绝不容忍脑裂。
  - **数据面 (Java 执行器) = 绝对 AP 架构**：下发的 Java 集群作为纯粹的无状态算力节点，兄弟宕机我顶上，永远保持极端的可用性和无缝弹性伸缩。

### 3. 消灭臃肿包袱，全面拥抱云原生
- **痛点**：传统调度中心是一个厚重的 Spring Boot 进程，启动慢且单节点动辄吃掉数百兆内存。
- **降维打击**：NanoJob 编译后仅为极其精简的静态二进制文件（约 20MB），内存占用仅数十兆。毫秒级的极速启动速度，完美契合 Kubernetes (K8s) 的瞬时弹性扩缩容场景。

---

## ⚔️ 与传统 Java 调度中心的深度对比

| 对比维度 | 传统 Java 调度中心 (如 XXL-Job) | NanoJob 核心引擎 |
| :--- | :--- | :--- |
| **核心动力源** | Java 线程池 + 关系型 DB 扫表 | **Go 协程并发 + O(1) 内存时间轮** |
| **底层元数据** | MySQL (读写极重，极易遇并发瓶颈) | **etcd (强一致性，支持毫秒级 Watch 监听)** |
| **高可用策略** | 依赖数据库悲观锁 (`FOR UPDATE`) 抢占 | **基于 etcd Lease (租约) 实现 Leader-Follower 秒级抢主** |
| **WorkerID分配**| 依赖 MySQL 自增主键或手动配置 | **基于 etcd 事务 (Txn) + 租约保活，动态横向扩容自动抢占机器 ID** |
| **心跳注册机制** | 依赖高频执行 DB UPDATE 语句 | **纯内存协程维持心跳，结合 etcd 租约极速“清道夫”剔除死节点** |
| **老业务兼容性** | 亲儿子 | **100% 协议级无感平替 (Java 业务代码一行不改即可接入)** |

---

## 🚀 核心黑科技

1. **云原生动态发号器 (Dynamic Snowflake Allocator)**
   彻底抛弃手动配置的机器 ID！引擎在 K8s 启动的瞬间，基于 etcd `Txn` 原子事务自动抢占空闲的 Worker ID (支持最高 1024 个引擎节点扩容)，并配合 Lease 心跳安全续期。结合底层 Snowflake 算法，无惧剧烈缩扩容，安全下发大厂级全局唯一数字 JobID。
2. **动态分片路由 (Dynamic Sharding Broadcast)**
   针对海量跑批场景，引擎绝不会愚蠢地把百万数据砸给一台机器。它会动态感知当前 Java 集群存活节点数 (Total)，利用**并发分片广播算法**向所有活着的机器分别投递专属的分片序号 (Index)。算力可随着容器的增加实现真正的无限横向扩展！
3. **闭包生命周期与异步网络隔离 (Closure & Async Isolation)**
   时间轮底层的每一次“滴答”，都会巧妙利用 Go 的闭包(Closure)与内存逃逸特性，裂变出独立的 Goroutine 去发起 HTTP 触发请求。无论远端 Java 集群网络多卡顿，调度引擎的主指针永远不会被阻塞 1 毫秒，完美保障全盘任务的极致时效性。
4. **Misfire 漏发补偿机制**
   即便遭遇机房断电级宕机，核心资金任务也绝不丢失。引擎恢复重启并当选 Leader 后，将自动触发“秋后算账”，精准比对 `NextTriggerTime` 识别宕机错漏任务，执行强力补偿随后平滑回归标准时间轮。

---

## 🛠️ 使用方法与快速启动

### 准备工作
- Go 1.20+
- etcd 服务（本地单机或云端集群均可）

### 1. Docker Compose 极速体验 (强推！30秒一键启动)
这是为了让新用户最快体验到项目魅力准备的极速启动包。你不需要安装任何 Go 环境或数据库。

```bash
# 在项目根目录，只需执行这一行命令：
docker-compose up -d
```
启动成功后，直接打开浏览器访问 `http://127.0.0.1:8080`，即可沉浸式体验极具极客审美的**暗黑模式可视化大盘**！此环境包含了一个完整的 etcd 和 Go 调度引擎，完美运行所有分布式核心代码。

### 2. 源码本地调试启动
通过极简的命令行参数，你可以轻松点火启动 NanoJob，并动态挂载 etcd 节点和监听端口：

```bash
# 默认启动（连接本地 127.0.0.1:2379 的 etcd，监听 8080 端口）
go run ./cmd/nanojob/main.go

# 生产级自定义参数启动
# 演示：注入企业内网 etcd 域名集群，并更换引擎暴露端口至 9090
go run ./cmd/nanojob/main.go -etcd="etcd-service.local:2379" -port="9090"
```
启动成功后，打开浏览器访问 `http://127.0.0.1:8080`，即可沉浸式体验极具极客审美的**暗黑模式可视化大盘**！

### 3. Kubernetes 云原生部署 (大厂生产级推荐)
本项目原生提供 `Dockerfile` 和标准的 `deployment.yaml`，享受一行命令部署大厂级云原生调度的畅快体验：

```bash
# 1. 构建轻量级 Alpine 镜像
docker build -t nanojob/engine:v1.0 .

# 2. 部署到 K8s 集群（体验基于 CoreDNS 劫持的配置平滑热切）
kubectl apply -f deploy/k8s/nanojob-deployment.yaml
```

### 4. Java 兵团接入（零侵入）
我们做到了惊人的向后兼容！对于你下游的 Java/Spring Boot 业务应用，**不需要修改哪怕一行核心业务代码**。
你只需要打开你的 `application.yml`，把原来老版控制台的 IP 替换成咱们 NanoJob 引擎的新 IP 即可：

```yaml
xxl:
  job:
    admin:
      # 旧的地址: addresses: http://192.168.1.1:8080/xxl-job-admin
      # 替换为 NanoJob 地址：
      addresses: http://nanojob-engine-ip:8080
```
保存重启 Java 应用，Java 兵团就会乖乖向你的 NanoJob 报到，开始接收你的时间轮召唤！
