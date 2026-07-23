# NanoJob (极简分布式调度引擎)

## 1. 项目背景与需求 (Background & Requirements)

在传统的企业架构中，定时任务通常依赖于 XXL-Job 等重量级调度框架。但在中小规模集群或单机容器化部署（如 Docker）的场景下，XXL-Job 暴露出以下痛点：
1. **基建过载**：Admin 调度中心依赖臃肿的 Java JVM 进程，且强依赖 MySQL 数据库。
2. **性能瓶颈**：通过死循环与 MySQL `FOR UPDATE` 行级悲观锁来进行任务调度，存在严重的 I/O 争抢。

**NanoJob** 致力于打造一个**极度轻量、基于内存时间轮、无 MySQL 依赖**的云原生任务调度引擎，并且在协议层完美兼容 XXL-Job 客户端，实现对老旧调度中心的无感平替。

## 2. 核心功能需求 (Core Features)

1. **高性能内存触发 (TimeWheel Engine)**
   - 彻底摒弃数据库轮询，基于多层时间轮（Hierarchical TimeWheel）算法，实现纳秒级纯内存指针触发。
2. **协议无感适配 (Protocol Adapter)**
   - 暴露标准的 HTTP REST API，报文格式 100% 兼容 XXL-Job Executor，无需修改业务端（Java/Go）的一行代码即可接入。
3. **动态路由与分片 (Dynamic Sharding Router)**
   - 支持向存活的执行器节点动态下发 `shardIndex` 和 `shardTotal` 参数，驱动海量业务数据并行处理。
4. **服务注册与发现 (Registry)**
   - 接收业务节点的心跳上报，维护存活执行器列表。
5. **分布式容灾与选主 (HA & Leader Election)**
   - 预留 etcd 接口，在多机部署时，通过 etcd 租约实现 Leader 选举，防止多节点并发触发。

## 3. 目标业务场景 (Target Scenarios)
- 电商大促：为百万用户分片广播下发折扣短信。
- 金融信贷：每日凌晨海量逾期账单的并发计息处理。
- 订单系统：高频次（秒级）的超时未支付订单状态轮询。

## 4. 架构进阶：容灾与漏发补偿 (Misfire Compensation)
为了保证在服务器宕机重启等极端场景下，绝不遗漏核心业务流水（如信贷计息），本系统设计了标准的漏发补偿机制：
1. **核心原理**：在持久层 (etcd) 的 `JobInfo` 中记录任务下一次预期的绝对执行时间戳 (`NextFireTime`)。
2. **重启校验机制**：当 Go 引擎因断电重启时，从 etcd 加载任务后必须比对 `NextFireTime` 与当前时刻。若预期时间在过去，则判定发生漏发 (Misfire)。
3. **策略执行**：一旦触发漏发警报，引擎将根据用户配置的 `MisfireStrategy`，立即进行高并发指令补发（FIRE_ONCE_NOW）来挽回业务损失。
