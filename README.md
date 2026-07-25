# NanoJob: 云原生极简分布式调度引擎

<p align="center">
  <img src="https://img.shields.io/badge/Language-Go-00ADD8.svg" alt="Language">
  <img src="https://img.shields.io/badge/Architecture-Cloud%20Native-blueviolet.svg" alt="Architecture">
  <img src="https://img.shields.io/badge/Store-etcd-red.svg" alt="Store">
</p>

## 📖 项目简介

NanoJob 是一个使用 Go 语言重写底层核心逻辑的**高性能分布式调度控制面**。
它完美兼容传统大厂调度中心（如 XXL-Job）的底层 HTTP 通信协议，旨在解决大规模微服务架构下定时任务“调度慢、并发低、重依赖”的历史痛点，是云原生（Cloud-Native）时代最锋利的轻量级调度解决方案。

---

## 🎯 解决了什么痛点？（Why NanoJob？）

在金融、电商等海量数据处理场景中（如：百万级账户的夜间利息并发计算、海量订单的超时扫描作废），传统的 Java 任务调度框架往往面临以下致命痛点，而 NanoJob 将其逐一击破：

1. **彻底告别数据库轮询引发的性能雪崩**
   传统框架依赖死循环轮询 MySQL 来扫表寻找即将触发的任务，数据量稍大极易引发锁表和 DB 崩溃。NanoJob 彻底抛弃了 DB 轮询，采用**多层内存时间轮 (Hierarchical TimeWheel)** 算法，实现时间复杂度为 O(1) 的无锁化、纳秒级精准调度触发。
2. **消灭臃肿的部署包袱**
   传统调度中心是一个厚重的 Spring Boot 进程，启动慢且单节点动辄吃掉数百兆内存。NanoJob 编译后仅为极其精简的 Linux 静态二进制文件（不足 20MB），内存占用仅为数十兆。毫秒级的极速启动速度，使其完美契合 Kubernetes (K8s) 的瞬时弹性扩缩容场景。
3. **消除弱一致性导致的“脑裂”隐患**
   传统调度依靠关系型数据库充当注册中心和分布式锁，在网络剧烈抖动时极易发生脑裂，导致任务在多台机器上重复触发，引发灾难性后果（如重复扣款）。NanoJob 引入云原生事实标准 **etcd**，借助其底层的 Raft 算法，提供绝对的强一致性保证。

---

## ⚔️ 与传统 Java 调度中心的深度对比

| 对比维度 | 传统 Java 调度中心 (如 XXL-Job) | NanoJob 核心引擎 |
| :--- | :--- | :--- |
| **核心动力源** | Java 线程池 + 关系型 DB 死循环查询 | Go 协程 (Goroutine) + 多层内存时间轮算法 |
| **底层存储介质** | MySQL (读写重，易遇并发瓶颈) | etcd (强一致性 KV 存储，支持 Watch 热更新) |
| **心跳注册中心** | 依赖数据库 UPDATE 语句更新心跳时间 | 纯内存协程维持心跳，90秒无响应极速“清道夫”剔除 |
| **云原生友好度**| 较差 (状态耦合严重，依赖外部 Tomcat/JDK) | 极佳 (符合 12-Factor 规范，完全支持 K8s 环境变量热注入) |
| **高可用策略** | 依赖数据库悲观锁抢占执行权 | 依赖 etcd 分布式 Lease 锁实现主备(Leader-Follower) 抢占 |
| **老业务兼容性** | - | **100% 协议级无感平替** (Java 业务端一行代码不改即可接入) |

---

## 🚀 核心架构设计

1. **动态分片路由 (Dynamic Sharding)**：当任务触发时，引擎不会愚蠢地把几百万数据砸给一台机器。它会动态感知当前 Java 集群活着的节点总数 (Total)，利用**广播+取模算法**，向所有活着的机器分别派发独一无二的 `Index` 任务书。算力可随着机器的增加实现真正的无限横向扩展！
2. **K8s 极简解耦注入**：全面拥抱基础设施即代码 (IaC)。NanoJob 不将配置写死在配置文件中，而是暴露在命令行 Flag 参数中。在 K8s 中，可直接通过 YAML 的 args 挂载 CoreDNS 域名，运维随时切换 etcd 数据库 IP 或变更集群监听端口，完全无需重启重编业务代码，拒绝“代码强耦合”。

---

## 🛠️ 使用方法与快速启动

### 准备工作
- Go 1.20+
- etcd 服务（本地单机或云端集群均可）

### 1. 本地极速启动
通过极简的命令行参数，你可以轻松点火启动 NanoJob，并动态挂载 etcd 节点和监听端口：

```bash
# 默认启动（连接本地 127.0.0.1:2379 的 etcd，监听 8080 端口）
go run ./cmd/nanojob/main.go

# 生产级自定义参数启动
# 演示：注入企业内网 etcd 域名集群，并更换引擎暴露端口至 9090
go run ./cmd/nanojob/main.go -etcd="etcd-service.local:2379" -port="9090"
```
启动成功后，打开浏览器访问 `http://127.0.0.1:8080`，即可沉浸式体验极具极客审美的**暗黑模式可视化大盘**！

### 2. Kubernetes 云原生部署 (推荐)
本项目原生提供 `Dockerfile` 和标准的 `deployment.yaml`，享受一行命令部署大厂级云原生调度的畅快体验：

```bash
# 1. 构建轻量级 Alpine 镜像
docker build -t nanojob/engine:v1.0 .

# 2. 部署到 K8s 集群（体验基于 CoreDNS 劫持的配置平滑热切）
kubectl apply -f deploy/k8s/nanojob-deployment.yaml
```

### 3. Java 兵团接入（零侵入）
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
