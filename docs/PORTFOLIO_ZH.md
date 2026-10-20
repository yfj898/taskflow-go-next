# TaskFlow Go — 中文简历与作品集版本

## 项目一句话

面向生产场景的 Go 异步任务与 AI 工作负载平台，围绕“任务不丢、重复安全、故障可恢复、权限可审计、跨区域可切换”逐步完善 Transactional Outbox、Redis/Kafka、gRPC Worker、AI 路由与配额、Kubernetes/KEDA、治理控制面、MySQL HA/PITR、SRE 可观测性和多区域灾备。

## 简历版：Go 后端 / 云原生

**TaskFlow Go｜异步任务与 AI 工作负载平台**  
Go / Gin / MySQL / Redis Streams / Kafka / gRPC / Kubernetes / KEDA / Prometheus / Grafana

- 基于 **MySQL + Transactional Outbox + Redis Streams/Kafka + gRPC Worker** 搭建异步任务执行链路，通过条件 SQL Claim、Lease、Reconciler 与 enqueue generation fencing 处理重复投递、Worker 异常和队列故障，避免数据库写入与消息发布的双写不一致。
- 基于 **pprof** 定位创建任务热路径，在保留事务与 Outbox 可靠性语义的前提下优化 SQL 与批处理；最终五轮同机发布基线中位数达到 **5,781.98 RPS，P95 26.816 ms，P99 36.290 ms**，采样集 HTTP 错误数为 0。
- 完成 **Kubernetes/KEDA 队列弹性伸缩、优雅缩容/Drain Fence、DLQ 隔离与归档、Prometheus SLO Burn-rate、Alertmanager、Grafana**，并通过真实 Kafka、MinIO/S3-compatible 与 K8s/KEDA 场景保留可执行验证证据。
- 实现 **MySQL GTID 主从、Snapshot Bootstrap、Writer 启动故障转移、校验备份 + Binlog PITR、Region A→B Writer Promotion**；本地跨区域演练实现 durable task **RPO=0**，Global Endpoint 恢复 **RTO=51s**。

## 简历版：滴滴 Golang / 私有云控制面定向版（STARL 压缩表达）

**TaskFlow Go｜Go 云原生任务控制平台（面向私有云异步生命周期场景）**  
Go / Gin / MySQL / Redis Streams / Kafka / gRPC / Kubernetes / KEDA / Prometheus / Grafana

- **S/T：** 面向企业内部资源申请、异步变更和故障恢复场景，将一次 API 请求拆分为可持久化、可追踪的生命周期执行链；**A：** 采用 MySQL + Transactional Outbox + Redis Streams/Kafka + gRPC Worker，并以条件 SQL Claim、Lease、Reconciler 和 enqueue generation fencing 处理重复投递、Worker 异常和 Broker 暂时不可用；**R：** 保证已落库任务的执行意图可恢复，避免数据库与消息队列双写不一致；**L：** 将“消息成功”与“业务状态正确”解耦，以数据库状态机和幂等收敛替代对 exactly-once 的依赖。
- **S/T：** 可靠写路径引入事务和 Outbox 后需要兼顾吞吐与延迟；**A：** 基于 pprof 定位 GORM callback / allocation 热点，将两处关键写路径改为 prepared `database/sql`，并将 Outbox claim/publish/settle 改为批处理；**R：** 五轮同机发布基线中位数达到 **5,781.98 RPS，P95 26.816 ms，P99 36.290 ms，HTTP error=0**；**L：** 性能优化必须以 profiling 和 A/B 复测为依据，不能通过删除可靠性语义换取表面 QPS。
- **S/T：** 多副本部署下要避免缩容丢任务、重复恢复和队列积压；**A：** 实现 Kubernetes/KEDA 弹性伸缩、Drain Fence、分布式 Reconciler leadership、DLQ/归档、Prometheus SLO Burn-rate 与告警；**R：** 在真实 Kafka、K8s/KEDA、MinIO/S3-compatible 场景完成故障恢复和缩容验证；**L：** 控制面高可用的关键不是“多副本”本身，而是 leader 协调、租约、幂等和最终收敛。
- **S/T：** 单 Region 或 MySQL Writer 故障会中断控制面；**A：** 实现 MySQL GTID 主从、Snapshot Bootstrap、校验备份 + Binlog PITR 与 Region A→B Writer Promotion；**R：** 本地灾备演练实现 durable task **RPO=0、Global Endpoint RTO=51s**；**L：** 真正的容灾需要同时解决数据一致性、旧 Writer fencing、流量切换和恢复验证，而不是只做副本复制。

> 使用原则：STARL 是组织项目经历和面试表达的方法，不建议在正式简历中显式写出 S/T/A/R/L 标签；正式简历保留“场景/目标 → 行动 → 量化结果 → 工程判断”的自然句式即可。

### 正式简历最终版（不显式写 STARL）

**TaskFlow Go｜Go 云原生任务控制平台**  
Go / Gin / MySQL / Redis Streams / Kafka / gRPC / Kubernetes / KEDA / Prometheus / Grafana

- 面向私有云资源全生命周期实现持久化 **Workflow/Saga**：Provision 执行 `Quota → Service → PVC → Deployment → Commit`；Resize 先预占增量配额，失败时恢复上一版本 Bundle；Delete 采用 fail-forward，持续重试删除直到外部资源确认消失后才释放 Quota。协调器和步骤状态全部写入 MySQL，进程重启后从未完成步骤继续执行。
- 在 **Kind Kubernetes v1.35 + MySQL 8.4 + 2 个 TaskFlow Pod** 上完成真实 E2E：16 路并发 reconcile 同一 Workflow 得到 **2×200 / 14×409 Busy**；进一步做 Chaos，在 Network Service 已真实创建但 Step 尚未提交时强杀 owner Pod，另一副本先被 Lease 以 409 fencing，Lease 到期后重试同一步且 **Service UID 不变**；另一路在 Service 创建成功后注入 outcome-unknown 错误，验证 `failed network step` 自身被补偿、Quota 回到 0。双副本空库启动还暴露并修复了 GORM `AutoMigrate` 建表竞争，使用 MySQL `GET_LOCK` 将跨 Pod schema migration 串行化。
- 做高并发私有云订单基准：600 单/点、并发 16/64/128 下，Hot Tenant 在 c128 为 **103.51 orders/s、P95 1487 ms、1125 次 row-lock waits**；分散到 32 个租户后达到 **117.59 orders/s、P95 1342 ms、634 waits**。Controller 从 1→2→4 副本处理 90 个真实 Kubernetes Workflow 时仅 **4.53→5.09→5.40 workflows/s**，4 副本扩展效率 **29.8%**，据此定位瓶颈已转向共享 MySQL/Kubernetes 控制面与逐步状态迁移，而非 Controller CPU。
- 对瓶颈做 A/B：Quota 从显式 `SELECT ... FOR UPDATE` 改为条件原子/versioned update 后，Hot Tenant c128 **109.66→115.35 orders/s、P95 1382→1266 ms**，证明缩短临界区有效但单 quota row 仍是串行化点；Saga 用 `SKIP LOCKED` batch claim 后单 Controller **4.62→5.18 workflows/s**。继续叠加 batch prefetch/fused write 收益很小，而 8-worker 单 Controller 虽到 **5.38 workflows/s**，扩到 4 Controller 时 compute polling 放大到 **201 次/WF**、吞吐仍约 **5.40 workflows/s**，据此放弃“盲目加 worker”作为默认优化。
- 继续做结构性优化：实现租户 Quota **真分桶/sub-ledger**，将实际 `quota_bucket_id` 持久化到 Resource，Resize/Delete/补偿均回原 bucket；真实 MySQL 并发测试发现首版跨 bucket 探测会触发 `1213 deadlock`，改为“单事务只锁一个 bucket + bucketed Admission 使用 READ COMMITTED”后，80 容量/160 并发严格 **80 成功/80 拒绝**且无超卖。Hot c128 row-lock waits **1132→653（-42.3%）**，但吞吐仅 **111.23→115.31 orders/s**，由此定位瓶颈已经转移到其余事务写路径。
- 为解决 Controller polling storm，将指数退避持久化为 MySQL `next_reconcile_at`，Lease 可抢且 deadline 到期才允许推进，Pod 重启/换副本仍遵守同一调度。optimized-w8 四 Controller 使用 40ms base 时，Compute Observe 从 **206.36→12.48 次/WF（-94.0%）**，吞吐 **5.403→5.164 workflows/s（-4.4%）**；因此证明“持久化调度能解除副本数与 polling 压力的乘法关系”，但仍保留保守默认值，未把实验参数直接上线。
- 实现 **Resource + Order + Desired/Actual State** 生命周期模型及租户 **CPU/内存/副本 Quota Admission**，通过 MySQL `SELECT ... FOR UPDATE` 串行化并发预占；补偿删除要求 Kubernetes GET 确认 `404` 后才释放配额，避免异步删除窗口产生资源超卖。
- 支持 Idempotency-Key、Generation Fencing、旧订单 Supersede 与多副本 Reconcile Lease；通过 Kubernetes REST API 对 Deployment 执行 Server-Side Apply / Observe / Delete，并周期性修复集群侧漂移。
- 为异步执行链设计 **MySQL 状态机 + Transactional Outbox + Redis Streams/Kafka + gRPC Worker**，通过条件 SQL Claim、Lease、Reconciler 与 Generation Fencing 处理双写失败、重复投递和 Worker 中断，使已落库操作能够恢复并最终收敛。
- 在事务与 Outbox 可靠性语义不可削弱的约束下，使用 **pprof** 定位 GORM Callback/Allocation 热点，将关键写路径改为 Prepared `database/sql` 并批处理 Outbox Claim/Publish/Settle；五轮同机发布基线中位数达到 **5,781.98 RPS，P95 26.816 ms，P99 36.290 ms，HTTP Error=0**。
- 完成 **Kubernetes/KEDA 弹性伸缩、Drain Fence、DLQ/归档、Prometheus SLO** 与 **MySQL GTID/PITR + Region A→B Writer Promotion**；本地灾备演练实现 durable task **RPO=0、Global Endpoint RTO=51s**。

### 这四条背后的 STARL 逻辑

| 简历条目 | S / T：为什么做 | A：你做了什么 | R：怎么证明 | L：工程沉淀 |
| --- | --- | --- | --- | --- |
| 可靠执行链 | 云平台式异步操作存在双写、重复、宕机恢复问题 | Outbox + Broker + Worker + Claim/Lease/Reconciler/Fencing | Broker/Worker 故障后任务仍可恢复并收敛 | 消息可重复，业务状态必须幂等推进 |
| 性能优化 | 可靠性链路带来额外数据库和消息成本 | pprof + Prepared SQL + Outbox batching | 5,781.98 RPS / P95 26.816 ms / 0 HTTP error | 先 profiling，再优化；正确性是硬约束 |
| 多副本/K8s | 横向扩容会带来重复恢复和缩容在途任务问题 | KEDA + Drain Fence + Reconciler Leader + DLQ + SLO | 真实 Kafka/K8s/KEDA 故障场景验证 | HA 不是多副本数量，而是协调和最终收敛 |
| DR | 单 Writer/单 Region 仍有单点 | GTID + PITR + Writer Promotion + Fencing | RPO=0 / RTO=51s 本地演练 | 数据 HA、应用 HA、流量 HA 必须一起验证 |

> **真实性边界：** 当前代码已经实现 Resource/Order、Desired/Actual State、租户配额账本，以及真实 Kubernetes **Service/PVC/Deployment** 多资源 Provision/Resize/Delete Saga；真实 MySQL 验证故障补偿，真实 Kind v1.35 双副本 E2E 验证 Kubernetes 对象生命周期、Workflow Lease 竞争和最终 Quota 收敛。这里的 Network/Storage 是 Kubernetes Service/PVC，不是云厂商 VPC/块存储；Resize 当前允许 Compute/Service 变更，PVC 配置保持不可变；尚未实现 VM/VPC/云盘、多集群 Provider Registry，因此不包装成完整 IaaS 私有云。

## 简历版：AI 应用工程师

**TaskFlow Go｜AI 任务调度与治理平台**

- 在持久化任务调度框架上构建 AI Provider Adapter，支持 **OpenAI-compatible Provider、模型/供应商路由、Retryable Fallback、Token/Cost 统计及 Trace 透传**，将模型调用与核心调度逻辑解耦。
- 实现 **Budget/Quota Admission、SLO Class、Per-model Distributed Concurrency、Adaptive Load Shedding、Hedged Request、Tenant Fair Scheduling**，对多租户 AI 请求进行资源与成本治理。
- 为 AI/控制面操作补充 **OIDC/JWT + RBAC、Hash-chain Audit、Signed SIEM Delivery、Four-eyes Governance Remediation、Replica Receipt Quorum 与 Key Anti-rollback**，使高权限操作具备可审计、可恢复和 Fail-closed 语义。
- 使用 Prometheus/Grafana 对任务、队列、AI Provider、成本与治理债务建立指标和告警链，并提供生产 Runbook 与可复现 Smoke Test。

## STARL 讲法 1：为什么要做 Transactional Outbox

**S（背景）**：任务创建需要先落 MySQL，再发消息到队列；如果数据库成功但消息发送失败，会出现任务存在但永远没人执行的“双写窗口”。

**T（目标）**：保证只要 API 已经接受并成功落库，后续即使 Redis/Kafka 临时不可用，执行意图仍然可恢复。

**A（行动）**：把 Task 与 Outbox Event 放进同一个 MySQL Transaction；异步 Dispatcher 再发送消息。队列层按 at-least-once 设计，通过 SQL 条件 Claim、Lease 和 enqueue generation 保证重复消息不会产生重复状态推进；Reconciler 负责恢复超时任务。

**R（结果）**：队列故障不再造成已接受任务的执行意图丢失，同时不依赖消息系统提供理想化的 exactly-once。

**L（沉淀）**：分布式系统里真正要保证的是“业务状态最终只推进一次”，而不是要求消息绝不重复。消息层接受 at-least-once，通过持久化状态机、幂等 Claim、Generation Fencing 和 Reconciler 将重复和中断吸收掉，比试图构造端到端 exactly-once 更可落地。

## STARL 讲法 2：性能优化

**S**：可靠写路径包含 MySQL Transaction 和 Outbox，性能不能靠直接删除可靠性逻辑提升。

**T**：在不改变正确性语义的前提下优化创建任务热路径。

**A**：使用 pprof 定位热点，将关键 INSERT 路径优化为 prepared raw `database/sql`，并对 Outbox claim/publish/settle 做批处理；固定硬件、固定并发、Fresh Stack 多轮重复验证。

**R**：最终五轮发布基线中位数 **5,781.98 RPS / P95 26.816 ms / P99 36.290 ms**，测量集 HTTP 错误为 0。该数字是单机开发环境基准，不包装成生产 SLA。

**L**：先用 pprof 找证据，再改最热路径，再做固定硬件 A/B；同时把正确性约束当作不可回退条件。性能优化的目标不是单点极限数字，而是在可靠性语义不变的前提下降低 CPU、分配和网络/数据库往返成本。

## STARL 讲法 3：数据库与多区域灾备

**S**：应用多实例并不能解决 MySQL Writer 或整个 Region 不可用的问题。

**T**：构造一个真正能恢复、能量化 RPO/RTO 的 Writer 灾备路径。

**A**：先做一致性 Snapshot Bootstrap，再启动 MySQL GTID 增量复制；Replica 保持 `read_only + super_read_only`。Region A 故障时先 Fence 旧 Writer，再停止 Replica、提升 Region B、让 Region B TaskFlow 连接新 Writer；Global Router 使用 Docker DNS runtime resolve，避免主站 DNS 消失导致 Router 自身启动失败。

**R**：演练中故障前 durable task 在 Region B 保留，`rpo_lost_tasks=0`；同一个 Global Endpoint 在 **51 秒**内恢复并接受提升后的新任务。

**L**：数据库 HA 不等于业务 HA。真正的 Writer 切换必须同时考虑旧主 fencing、复制位点、应用连接、全局流量入口和恢复后的可写验证，因此用 RPO/RTO 和故障演练比“部署了主从”更能证明系统可恢复。

## STARL 讲法 4：从可靠任务引擎演进到 Kubernetes 资源控制面

**S（背景）**：企业私有云中的资源申请、扩缩容、下线等操作通常是长事务，底层可能跨越 Kubernetes、存储、网络等多个异步系统；同步 RPC 很难覆盖中途失败、重复请求和部分成功。

**T（目标）**：在原有可靠执行底座上实现跨多个外部资源的订单事务：请求一旦被接受，即使 Service、PVC、Deployment 任一步失败或控制器进程重启，也能继续推进或逆序回滚，避免部分成功留下孤儿资源。

**A（行动）**：新增 `cloud_workflows + cloud_workflow_steps` 持久化 Saga，每次 reconcile 只推进一个已落库状态转换；第 0 步通过 MySQL 行锁原子预占 Quota，后续依次 Server-Side Apply Kubernetes Service、PVC、Deployment，最后提交 Resource/Order。Provider 失败后进入补偿态并逆序执行幂等 Delete；即使“请求超时但服务端已成功”也把失败步骤纳入补偿，删除后再次 GET，只有确认 `404` 才完成补偿并最终释放 Quota。Workflow Lease 防止多 API 副本同时推进同一 Saga。

**R（结果）**：真实 MySQL 8.4 smoke 验证重启续跑、正向失败、逆序补偿和补偿失败重试；Kind v1.35 双 Pod E2E 中 16 路跨 Pod reconcile 得到 **2×200 / 14×409 Busy**，真实 Provision→Resize→Delete 最终对象与 Quota 清零。Chaos 中又在 Service 已成功创建但 MySQL Step 尚未提交时直接 kill owner Pod，新副本等 Lease 过期后重试同一步且 **Service UID 不变**；另一路在 Service 创建成功后人为返回 outcome-unknown 错误，Workflow 进入 `compensating`，失败的 Network Step 本身最终 `compensated`、Quota=0。双副本空库还暴露并修复了 `AutoMigrate` 竞争。

**R（规模结果）**：订单 Admission 在 Hot Tenant c128 下为 **103.51 orders/s / P95 1487 ms**，32 租户分片提升到 **117.59 orders/s / P95 1342 ms**，InnoDB row-lock waits 从 **1125 降到 634**；90 个真实 Kubernetes Workflow 的 Controller 1/2/4 副本吞吐为 **4.53 / 5.09 / 5.40 workflows/s**，4 副本只达到 **1.192× speedup、29.8% efficiency**，说明继续横向堆 Controller 的收益已很低。

**R（优化 A/B）**：条件原子/versioned Quota 在 Hot c128 将吞吐 **109.66→115.35 orders/s（+5.2%）**、P95 **1382→1266 ms（-8.4%）**；Saga batch claim 将单 Controller **4.62→5.18 workflows/s（+12.1%）**。但 8-worker + 多 Controller 形成 polling storm，1/2/4 Controller 分别达到 **56.4/119.9/201.0 次 Compute Observe/WF**，有效吞吐仍停留在 **≈5.4 workflows/s**，因此保留低 worker 默认并把下一优化方向转向 observe backoff/event-driven watch。

**R（结构性优化）**：Quota 真分桶在 Hot c128 将 InnoDB row-lock waits **1132→653（-42.3%）**，完整 Saga 补偿验证 Reservation 必须回同一 bucket；持久化 `next_reconcile_at` + 40ms 指数退避在 4 Controller 下将 Compute polling **206.36→12.48/WF（-94.0%）**，代价是吞吐 **5.403→5.164 workflows/s（-4.4%）**，从“堆副本/堆 worker”转向“减少无效控制面请求”。

**L（沉淀）**：跨系统事务不能把数据库 ACID 强行扩展到 Kubernetes；更可落地的是“本地事务 + 幂等外部步骤 + 持久化 Saga + 补偿”。失败响应也不代表副作用没发生，因此补偿范围必须覆盖 outcome-unknown 的失败步骤；同理 DELETE 返回 2xx 也不代表资源已消失，配额释放必须绑定外部资源真正消失的证据。

## 面向滴滴 JD 的面试主线

推荐将项目按下面顺序讲，而不是从技术栈开始罗列：

1. **真实场景**：私有云一次资源申请可能同时涉及配额、网络、存储和计算，最难的不是 CRUD，而是“部分成功之后怎么办”；
2. **订单事务**：使用持久化 Saga 管理 Provision/Resize/Delete；Provision/Resize 有补偿路径，Delete 则 fail-forward 到资源真实消失，避免错误地“删除失败后重建”；
3. **正确性**：用 Idempotency-Key、Workflow Lease、Generation Fencing、幂等 Provider 和 outcome-unknown 补偿处理重复、崩溃与网络不确定性；
4. **底层可靠执行**：MySQL + Transactional Outbox + Redis/Kafka + gRPC Worker + Lease/Reconciler，解决数据库/消息双写和 Worker 中断；
5. **工程证据**：真实 MySQL 验证故障恢复；Kind v1.35 双副本验证 14/16 并发请求被 Lease fencing，并进一步真实 kill owner Pod 验证接管；规模压测定位 Hot Tenant Quota 行锁热点，并验证 Controller 1→2→4 只有 **1.192×** 总加速、4 副本效率 **29.8%**；pprof 基线 **5,781.98 RPS / P95 26.816 ms**；
6. **生产化**：K8s/KEDA、SLO、DLQ、审计、MySQL GTID/PITR 与多区域故障演练，强调“状态收敛和恢复证据”而不是组件罗列。

### 60 秒 STARL 口述版

> TaskFlow-Go 最初解决可靠异步执行，后来我把它演进成了一个可运行的 Kubernetes 私有云控制面。资源申请不是一次 RPC，而是跨 MySQL 与 Kubernetes 的持久化 Saga：Quota 后依次处理 Service、PVC、Deployment，每个步骤和补偿状态都落库；Resize 失败恢复上一 Bundle，Delete 则 fail-forward 到对象真实消失后才释放 Quota。我还在 Kind v1.35 上真实跑了两个 TaskFlow Pod：空库并发启动暴露过一次 AutoMigrate 建表竞争，我用 MySQL `GET_LOCK` 做跨 Pod migration fencing；修复后两副本 restart=0。随后 16 路并发 reconcile 同一 Workflow 得到 2 个 200、14 个 409，证明 Workflow Lease 能挡住多副本重复推进；完整 Provision→Resize→Delete 最终 Kubernetes 对象全部消失、Quota 回到 0。最后我把两个后台 Saga Controller 同时打开，不做任何手工 reconcile，第二个 Resource 仍自动完成 Provision→Delete。底层可靠任务链仍使用 Transactional Outbox、Redis/Kafka、gRPC Worker 和 Reconciler，性能基线为 **5,781.98 RPS、P95 26.816 ms**。

### 90 秒 STARL 深挖版

> 我把 TaskFlow-Go 分成三层。第一层是可靠执行引擎：Task 和 Outbox Event 在同一 MySQL 事务里提交，再经 Redis/Kafka 和 gRPC Worker 执行，Claim、Lease、Reconciler 和 Generation Fencing 吸收重复消息和 Worker 崩溃。第二层是单资源控制器：Resource 保存 Desired/Actual State 和 Generation，Controller 通过 Lease 协调多副本，并用 Kubernetes Server-Side Apply + Observe 做持续收敛。第三层是跨资源订单 Saga：一次 Provision 会先以 MySQL 行锁原子预占 CPU/Memory/Replica Quota，然后依次执行 Service、PVC、Deployment、Commit；`cloud_workflows` 和 `cloud_workflow_steps` 保存每一步、尝试次数和补偿状态，每次 reconcile 只推进一个状态转换，因此进程随时退出都能由新实例从 MySQL 恢复。正向失败后按 Compute、Storage、Network、Quota 逆序补偿；对超时这种结果未知的失败步骤也执行幂等删除，因为外部副作用可能已经发生；DELETE 返回成功还不够，要再次 GET 确认 404 才继续并最终释放 Quota。真实 MySQL smoke 中，我在 Network 完成后模拟 Controller 重启，新实例从 Storage 接着走；另一路注入 Storage 失败和 Network 补偿失败，第二个 Controller 仍能继续回滚到 `workflow=compensated / order=failed / quota=0`。性能侧原任务写路径的五轮单机中位是 **5,781.98 RPS、P95 26.816 ms**，灾备演练是 durable task **RPO=0、入口 RTO=51 秒**。这个项目让我形成的核心判断是：跨系统事务不能依赖一次 RPC 的返回值，必须把业务意图、外部副作用和补偿进度都变成可持久化、可重放、可验证的状态。

### 高频追问：每个问题都按 STARL 回答

**1. 为什么用了 Kafka/Redis Streams，还需要 Transactional Outbox？**  
S：数据库事务和消息 Broker 不共享一个本地事务，先写库或先发消息都会留下失败窗口。T：保证 API 成功后执行意图不会丢。A：Task 与 Outbox 同一 MySQL Transaction，Dispatcher 异步投递；消费端仍使用幂等 Claim。R：Broker 短暂不可用后事件可以继续补发。L：Broker 解决传输，Outbox 解决“业务状态与传输意图原子持久化”，两者职责不同。

**2. 为什么不直接追求 exactly-once？**  
S：网络超时后发送方无法可靠判断对端到底执行没执行。T：让重复消息不会破坏业务。A：采用 at-least-once + 条件状态迁移 + Generation Fencing + Reconciler。R：重复/旧事件不会重复推进任务状态。L：工程上更重要的是业务效果 exactly-once-like，而不是声称底层消息绝不重复。

**3. Reconciler 和消息重试有什么区别？**  
S：消息可能已经 ACK，但 Worker 在业务状态完成前崩溃；单纯 Broker Retry 看不到数据库里的“半完成状态”。T：从 Source of Truth 恢复系统。A：Reconciler 根据数据库状态、Lease 超时和 Generation 判断需要恢复的工作。R：即使消息链路本身没有再次触发，也能把 stale task 拉回可执行状态。L：事件驱动负责快速路径，Reconciliation 负责最终正确性。

**4. 为什么要做 Generation Fencing？**  
S：Outbox、Reconciler、DLQ Replay 可能同时尝试再次发布同一任务，且旧消息可能延迟到达。T：阻止旧一代执行覆盖新状态。A：为 enqueue/执行维护 generation，并在数据库条件更新时校验。R：跨恢复路径的重复发布被约束在同一代内，stale generation 被拒绝。L：分布式锁只能保护某个时间窗口，单调版本号才能表达“谁更新”。

**5. pprof 优化为什么可信？**  
S：可靠写链路慢，但不能凭感觉删事务或换组件。T：找出实际 CPU/Allocation 热点。A：固定硬件、Fresh Stack、相同并发跑 A/B，同时采 CPU/Heap Profile；只替换热点 INSERT 和 Outbox 往返。R：最终五轮中位 5,781.98 RPS / P95 26.816 ms / 0 HTTP error。L：优化必须有 profiling 证据、控制变量和正确性回归。

**6. RPO=0 / RTO=51s 能代表生产吗？**  
S：希望验证 Writer/Region 故障恢复链路，而不是宣称生产级两地三中心。T：量化一次可复现的本地 DR 演练。A：GTID 复制、旧 Writer Fencing、Region B Promotion、Global Endpoint 切换。R：该演练中故障前 durable task 零丢失、入口 51 秒恢复。L：RPO/RTO 必须绑定具体故障模型和测试环境，因此简历明确写“本地演练”，不外推成生产 SLA。

**7. 为什么资源订单要做 Saga，不能放进一个 MySQL Transaction？**  
S：Quota/Order 在 MySQL，但 Service、PVC、Deployment 的状态在 Kubernetes，数据库事务无法把这些外部 API 纳入同一个 ACID 边界。T：解决跨系统部分成功。A：把订单拆成幂等 Forward Step，并将每一步状态持久化；失败后执行逆序 Compensation。R：真实 MySQL smoke 验证 Controller 重启后能从已持久化步骤继续，失败路径最终可以收敛到 compensated。L：跨系统一致性不是把本地事务无限扩大，而是本地原子状态 + 幂等副作用 + 可恢复补偿。

**8. Storage 步骤明明返回失败，为什么还要补偿 Storage？**  
S：HTTP 超时、连接断开时，请求方只能知道“没有拿到成功响应”，不能确定 Kubernetes 有没有实际创建 PVC。T：避免 outcome-unknown 留下孤儿资源。A：失败步骤也进入补偿集合，Delete 设计为幂等，资源不存在时同样视为成功。R：即使是“服务端已创建、响应丢失”的情况，恢复流程也会清理潜在 PVC。L：分布式系统里 error 不等于 side effect=0，必须把结果未知作为独立状态处理。

**9. Kubernetes DELETE 已经返回 2xx，为什么不能马上释放 Quota？**  
S：Kubernetes 删除是异步的，DELETE 2xx 只能说明删除请求被接受，Pod/PVC 等实际资源可能仍存在。T：避免资源仍占用时 Quota 被第二个订单重新消费造成实际超卖。A：补偿执行 DELETE 后再次 GET；只有 404/Gone 才把步骤标记 compensated，Quota 又是最后一个补偿步骤。R：Quota 释放与外部资源真实消失绑定，而不是与 API ACK 绑定。L：控制面的容量账本必须依据可验证的 Actual State，而不是依据命令是否已发送。

## 面试时需要主动说明的边界

- 5,781.98 RPS 是单机发布基线，不是生产 SLA。
- Phase 45 是本地 Docker 模拟 Region A/B 的灾备演练，不等同于真实两地三中心部署。
- GitHub OIDC + Cosign Keyless Signing 的 OIDC 身份只能由 GitHub Actions 提供；本地真实执行的是镜像构建、Syft SBOM 与 Trivy 漏洞门禁。
- Startup DSN Failover 解决的是“进程启动时选择可用 Writer”，不是在线连接池自动完成数据库主从切换。
- 当前 Saga 中 Network/Storage 的真实实现是 Kubernetes **Service/PVC**，不是云厂商 VPC/块存储；Provision/Resize/Delete 已实现，但 Resize 暂不修改 PVC 配置，避免宣称已解决所有存储扩缩容语义。

## 推荐放在一页简历中的最终 4 条

投滴滴这类 **Golang / 私有云控制面 / 订单生命周期** 岗位时，最终只保留下面四条主线，避免 AI Provider 等次要能力冲淡岗位匹配度：

1. **私有云订单 Saga**：实现 Provision/Resize/Delete 持久化工作流；Kind v1.35 双副本验证 Lease 接管与 outcome-unknown 补偿；高并发订单中 32 租户相对 Hot Tenant 在 c128 将吞吐 **103.51→117.59 orders/s**、row-lock waits **1125→634**，而 Controller 1→4 仅 **1.192×** 加速，定位共享控制面瓶颈；
2. **可靠执行底座**：Transactional Outbox + Redis/Kafka + gRPC Worker + Claim/Lease/Reconciler/Generation Fencing，处理双写、重复投递和执行中断；
3. **性能与云原生证据**：pprof 驱动优化，在可靠性语义不变下达到 **5,781.98 RPS / P95 26.816 ms / HTTP Error=0**，并完成 K8s/KEDA、Drain、DLQ、SLO 验证；
4. **数据库与灾备**：MySQL GTID/PITR + Writer Fencing + Region A→B Promotion，本地演练 **durable task RPO=0 / Global Endpoint RTO=51s**。
