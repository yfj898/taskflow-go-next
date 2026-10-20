# TaskFlow Go — 中文面试讲解手册

## 1 分钟项目介绍

这个项目最开始是一个 Go 异步任务系统，我后面没有简单继续加 CRUD，而是围绕生产环境里真正会遇到的故障去完善。核心链路是 MySQL 作为 Source of Truth，Task 和 Outbox 在一个事务里提交，再通过 Redis Streams 或 Kafka 投递，由独立 gRPC Worker 执行。为了处理 at-least-once 带来的重复问题，我用了条件 SQL Claim、Lease、enqueue generation 和 Reconciler。

在这个基础上我又加了 AI Provider 路由、预算配额、分布式并发、公平调度；控制面做了 OIDC/JWT、RBAC、审计链和 SIEM/Governance；平台侧补了 KEDA、SLO、Grafana、MySQL GTID/PITR 和 Region A→B 灾备。项目的特点是每个重要能力尽量都用 Smoke Test 或真实基础设施验证，不只是写设计文档。

## 3 分钟架构讲法

### 第一层：任务可靠性

客户端创建任务时，Task 和 Outbox Event 在同一个 MySQL Transaction 中提交。Outbox Dispatcher 后续再发 Redis/Kafka，所以队列短暂不可用不会丢掉已提交任务。

队列不追求“理论上的 exactly-once”，而是假设 at-least-once。重复消息进入后只有匹配当前 generation 且状态合法的任务能完成 SQL Claim，因此重复投递是安全的。

### 第二层：执行与 AI

普通任务通过独立 gRPC Executor Worker 执行。AI Task 复用同一调度底座，但会进入 AI Gateway，做 Provider/Model Routing、Fallback、Token/Cost、Budget/Quota、Distributed Concurrency、Load Shedding 和 Tenant Fairness。

### 第三层：控制面安全

高权限接口支持 Bearer 或 OIDC/JWT，OIDC group 映射为 requester、approver、incident-commander、governance-reviewer 等角色。操作写入持久化审计，再通过 Hash Chain、SIEM Receiver、External Governance Receipt 形成完整证据链。

### 第四层：SRE 与灾备

Prometheus Recording Rules 计算 SLO/Burn Rate，Alertmanager 告警，Grafana 展示。数据库通过 Snapshot + GTID Replica 建立 Region B；故障时 Fence Region A、提升 B、恢复一个 Global Endpoint。

## 高频追问

### Q1：为什么不用 MQ 的 exactly-once？

因为应用正确性不应该完全绑定某个 MQ 的 exactly-once 语义。跨数据库和消息系统仍然存在边界，所以我把核心设计成 at-least-once + application-level idempotency。这样 Redis Streams 和 Kafka 都可以放在同一抽象后面。

### Q2：Outbox 会不会重复发？

会，所以设计本身允许重复发。Outbox 保证的是“发布意图不丢”，而不是“只发一次”。Consumer 端用 task state + generation 做幂等推进。

### Q3：Worker 执行到一半挂了怎么办？

任务有 Lease。Worker 崩溃后 Lease 到期，Reconciler 会把 stale running task 恢复到可再次调度状态；同时 enqueue generation 增加，旧消息不能再合法推进新一代任务状态。

### Q4：为什么 MySQL Replica 必须先做 Snapshot？

因为 Primary 已经有历史 Schema/Data，Fresh Replica 只追当前 Binlog 可能第一条就是对“已经存在表”的 ALTER，而 Replica 根本没有那个表。正确做法是先一致性 Snapshot 建立基线，再用 GTID 追增量。

### Q5：PITR 是怎么验证的？

先做一致性 Backup 并记录 Binary Log File/Position，再在备份之后写入一条目标事务。独立 Recovery MySQL 先恢复 Backup，然后从记录的位置开始复制 Binlog，直到目标事务出现，再停止恢复链路。这证明恢复点确实来自备份后的增量日志。

### Q6：为什么 Region B 启动后还要显式 Promotion？

因为 Replica 默认 `read_only + super_read_only`。Region A 故障时必须先 Fence A，避免双写；确认复制追平后停止复制、关闭只读、再让 Region B TaskFlow 使用新 Writer。

### Q7：RTO 51 秒主要花在哪里？

这个数字包含本地演练中的故障检测、容器停止、数据库 Promotion、Region B TaskFlow 重建和 Global Router 恢复。它不是理论最小值，也不是生产承诺，而是当前自动化演练实测的端到端恢复时间。

### Q8：为什么 Nginx Router 要 runtime DNS resolve？

演练时发现主 Region 容器被停止后，如果 Nginx 在启动阶段静态解析 `taskflow`，主站 DNS 不存在会让 Nginx 整个启动失败，备用站健康也没用。改成 Docker DNS runtime resolve 后，Router 可以记录主站解析失败并继续走 backup。

### Q9：AI 调度有什么不是“普通 API 转发”的地方？

除了 Provider Routing/Fallback，我还把 AI 调用放进统一资源治理：预算、租户 Quota、Per-model 分布式并发、SLO Class、Adaptive Load Shedding、Hedged Request、Fair Scheduling。重点是把模型调用变成可治理的多租户 workload。

### Q10：项目现在最大的真实边界是什么？

它的 HA/DR 验证主要在本地 Docker/Kubernetes 环境，真实生产还需要托管数据库或 Operator、跨 Region 网络、真实云对象存储复制、外部 OIDC/KMS、生产 DNS/Ingress 和 Secret 管理。项目证明的是机制和 Failure Semantics，而不是声称本地环境等同云生产。

## 5 分钟 Demo 顺序

不要按 Phase 1→45 演示。建议只讲四段：

1. **可靠任务**：创建 Task，解释 Task + Outbox Transaction；
2. **AI 治理**：展示 AI Routing / Budget / Concurrency 指标或配置；
3. **可运维**：展示 `/metrics`、Grafana/SLO、一个 DLQ 或 Governance 状态；
4. **灾备证据**：打开 Phase42 PITR 与 Phase45 `rpo_lost_tasks=0 / rto_seconds=51` 的结果。

最后展示 `make gate` 与脚本化 Smoke Tests，强调“这些能力不是 PPT 设计”。

## 最需要记住的数字

- Release 五轮中位数：**5,781.98 RPS**
- P95：**26.816 ms**
- P99：**36.290 ms**
- DR：**RPO = 0 lost durable tasks**
- DR：**RTO = 51 s**

面试时一定同时说明测试环境边界。
