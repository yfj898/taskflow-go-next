# TaskFlow-Go 2027 届 Go 后端简历 STAR + 八股证据索引

> 岗位定位：Golang 研发、私有云控制面、云平台后端。项目为独立研发和隔离环境验收，不是企业生产业务的线上贡献。以下每项都对应当前代码与可追溯证据。

## 项目一句话讲解（20 秒）

“我用 Go 实现了私有云资源订单与可靠任务控制平台，让调用方通过 OIDC 授权的 API/Go 客户端申请、变配和下线 Kubernetes 资源。最难的是跨 MySQL、消息队列和 Kubernetes 的部分成功、并发竞争及故障恢复；我用 Outbox、Lease、Fencing、持久化 Saga 与补偿解决，并做了真实 MySQL、Kind 双 Pod 和故障注入验收。”

## 真实产品边界

- 支持：Kubernetes Service/PVC/Deployment 生命周期、订单/工作流查询、Keycloak 机器身份、双租户访问隔离、Go SDK + `taskflowctl` 运维客户端。
- 不支持冒称：生产落地客户、全部公有云产品、正式 GitHub Actions 已运行、完整 72h SLO、生产级 Keycloak HA。
- **API 202 = 持久化接受**，不是资源已就绪；调用方必须查询 Workflow Steps 或使用有 Deadline 的 `wait`。
- CLI 只是业务消费端。它本身不另建状态机、不直接访问 MySQL、不绕过 Kubernetes RBAC；系统真实来源仍是后端持久化数据。

## 五条 STARL 面试核心案例

### 案例 1：跨系统长事务与 Saga

**S**：私有云资源申请要先占 Quota，再创建 Service/PVC/Deployment；Kubernetes 操作返回超时，不代表对象未创建。

**T**：让一次资源申请在 Controller 异常退出、外部失败或重复请求后能够继续推进或者安全回滚。

**A**：将 Resource/Order/Workflow/Steps 持久化；每个 reconcile 只推进一步；K8s 操作使用幂等 Apply/Observe；失败进入补偿，针对 outcome-unknown 的步骤也执行观测/补偿，删除后确认对象确实消失才释放 Quota。

**R**：Kind 真实 Service/PVC/Deployment Provision→Resize→Delete 通过；杀死 Owner Pod 后另一副本租约接管，最终对象及 Quota 归零；Keycloak 真实 JWT→Kind 双副本→资源创建/删除全链路通过。

**L**：跨服务事务不能简单把 MySQL ACID 扩展到 Kubernetes；持久化意图、外部操作幂等、可观测补偿，才能处理不确定的结果。

**代码入口**：`internal/service/cloud_workflow.go`（Create/Resize/Delete、ReconcileOne、compensateOne），`internal/repository/cloud_workflow_repository.go`（Workflow Step 落库与 Claim），`scripts/cloud-control-kind-chaos.sh`，`scripts/cloud-control-kind-oidc-e2e.sh`。

### 案例 2：Outbox + Worker 故障恢复

**S**：MySQL 事务成功而 Broker 发布失败，或消费者 ACK 丢失、Worker 崩溃，会出现任务丢失/重复执行。

**T**：至少保证请求接受后任务意图可恢复、重复消息不破坏业务状态。

**A**：同一 MySQL Transaction 提交 Tasks + Outbox，Dispatcher 批量发布至 Redis Streams/Kafka，Executor 由 gRPC Worker 处理；使用有条件状态更新、Lease、Generation Fencing 和后台 Reconciler 收敛。

**R**：隔离故障和排空压测验证断线可恢复；100 VU 本地突发后排空速率约 101→733 task/s（约 7.3 倍）；Race Detector 全包通过。

**L**：消息系统是 At-least-once，不能无条件宣传端到端 Exactly-once；应当实现业务幂等与幂等状态迁移。

**证据**：`PERFORMANCE_REPORT.md` 的 Outbox backlog/优化章节；`internal/repository/task_repository.go`；`internal/worker/`；`internal/reconciler/`。

### 案例 3：MySQL 热点租户配额

**S**：同一租户的 128+ 路资源请求争夺配额行，行锁竞争增大尾延迟。

**T**：降低锁等待且不能导致 CPU/内存/Replica 超卖。

**A**：事务内校验并预占 Quota；通过分桶账本降低单行热点，补偿时按原 Bucket 归还；保持逻辑容量不变量。

**R**：同机 Hot Tenant 测试 row-lock waits 1132→653（-42.3%）；160 并发容量 80，80 接纳/80 拒绝，无超卖。

**L**：降低锁等待不自动等于提升吞吐；需要把锁、吞吐、P95、超卖正确性分别作为实验指标。

**代码**：`internal/repository/cloud_resource_repository.go`（SetTenantQuota、syncQuotaBuckets、quotaBucketUsage）、`docs/CLOUD_SAGA_STRUCTURAL_OPTIMIZATION_20260923.md`。

### 案例 4：OIDC 多租户授权

**S**：传统内网 API 仅验证 Token 格式，用户可通过猜测资源 ID 查询其他租户的订单。

**T**：让资源、Workflow、配额的访问范围始终以可信身份系统签发的 Claim 为准，而不是由 HTTP 请求头自报租户。

**A**：接入真实 Keycloak 的 Client Credentials、RS256 JWT、Issuer/Audience/JWKS 校验；映射 RBAC roles 与 `tenant_id`；数据库查询拼接已验证租户 scope，并使用 `(tenant_id,idempotency_key)` 复合唯一约束。

**R**：两个实际签名客户端的跨租户访问被拒绝，Read-only 修改返回 403；Kind 双副本在 OIDC-only 下完成实际 K8s 资源创建/删除，跨租户资源 ID 返回 404。

**L**：JWT 签名有效只代表身份凭证真实性，不能代替资源级鉴权；IdP 停机/签名轮换还涉及 JWKS 缓存与授权的可用性边界。

**代码**：`internal/handler/control_plane_oidc.go`、`internal/handler/cloud_tenant_authz.go`、`internal/repository/cloud_tenant_scope_integration_test.go`、`docs/KEYCLOAK_REAL_IDP_INTEGRATION.md`。

### 案例 5：从后端到真实业务调用方

**S**：提供云平台 API 还不等于有业务操作闭环，使用方缺少稳定的客户端和异步订单追踪方式。

**T**：让调用方以可验证的方式完成“申请→查询进度→变配→下线”，并避免操作员绕过认证或误重试导致重复申请。

**A**：在 `pkg/cloudclient` 实现 Go HTTP 客户端；新增 `cmd/taskflowctl` 运维 CLI，支持 Quota、Resource、Order、Workflow 查询、Provision/Resize/Delete、Wait；所有写操作要求稳定幂等键、支持 Context Deadline、私密 Token 文件、非本机 HTTPS 和 JSON 错误输出。

**R**：CLI 单测及 Go SDK 单测通过；独立 `taskflowctl` 使用 Keycloak 真 Token 对真实 TaskFlow HTTP/MySQL 完成读写，跨租户和只读身份的操作正确返回 403，缺失幂等键阻断本地写入；标志性日志 `TASKFLOWCTL_KEYCLOAK_MYSQL_E2E_PASS`。

**L**：业务可用性不只是 API 能返回 JSON；还要考虑身份、提交幂等、异步结果查询、错误可解释性与操作边界。

**代码/命令**：`cmd/taskflowctl/main.go`、`pkg/cloudclient/operations.go`、`scripts/taskflowctl-keycloak-smoke.sh`、`docs/CLOUD_ORDER_OPERATOR_WORKFLOW.md`。

## 15 题 Go 后端高频追问（与项目源码挂钩）

| 问题 | 优先回答的机制 | 建议回到哪个模块 |
| --- | --- | --- |
| 1. GMP 如何调度 Goroutine？为什么不能每个订单无限起 Goroutine？ | G/P/M、阻塞、工作池、Bounded concurrency、背压 | `CloudWorkflowService.SetWorkerConcurrency`, `RunController` |
| 2. Channel 关闭谁负责？如何避免泄漏？ | 生产者关闭、Context 取消、select、WaitGroup 等待、Stop Ticker | `internal/worker`, `internal/service/cloud_workflow.go` |
| 3. Context Timeout 后 K8s 可能创建成功怎么办？ | 超时是观察失败非事务回滚；Observe 外部状态、幂等重放/补偿 | `runProviderForwardStep`, `compensateOne` |
| 4. `sync.Mutex` 与数据库锁区别？ | 进程内互斥不保护多 Pod；MySQL 行锁是共享资源串行化 | `cloud_resource_repository.go` |
| 5. InnoDB RR 下普通 SELECT 与 `SELECT FOR UPDATE` 有何不同？ | 快照读/当前读、锁范围、索引和 next-key/record lock | 配额账本事务 |
| 6. 如何用 EXPLAIN 诊断慢 SQL？ | rows、索引匹配、回表、锁等待和慢查询日志需要分别看 | `PERFORMANCE_REPORT.md` |
| 7. 为什么 MySQL 索引不是越多越好？ | 增加写放大、锁与存储成本；迁移需兼容老版本 | `cmd/schema-migrate`, cloud orders migration |
| 8. Redis Streams ConsumerGroup ACK 丢了怎么办？ | At-least-once、pending entries、超时重领、幂等 Claim | `internal/queue`, `internal/worker` |
| 9. Kafka 事务能替代 Outbox 吗？ | Broker 事务不自动包含 MySQL 本地事务；Outbox 保留持久化意图 | Task + Outbox 同事务 |
| 10. Lease 到期旧 Worker 回来怎么办？ | TTL 只能限制时间窗口；Generation/Fencing 阻止旧代状态提交 | Claim、generation fencing |
| 11. Saga 和 TCC 的使用场景有什么不同？ | 外部 API 不支持 prepare 时，用补偿/幂等而非 XA | `CloudWorkflowService` |
| 12. Delete 2xx 为何还要 Observe？ | 幂等删除、最终一致、资源清理后再释放 Quota | `forwardDeleteOne`, `runDeleteStep` |
| 13. JWT 与 RBAC 的作用分别是什么？ | 验证身份真实性 vs 资源权限/租户归属 | `control_plane_oidc.go` |
| 14. 为什么业务端必须复用相同 Idempotency-Key？ | 网络超时后首次请求可能已接受，重试键不稳定会创建第二个订单 | `pkg/cloudclient`, `taskflowctl` |
| 15. 如何用 pprof / Go Benchmark / Race Detector？ | 控制变量、CPU heap block mutex、竞态与性能不同；指标不能无条件外推线上 | `PERFORMANCE_REPORT.md`, `go test -race` |

## 面试时的验证与诚实边界

1. **已做**：本地 Go 全包测试、`go vet`、Race Detector、真实 MySQL/Keycloak/Kind 故障注入。
2. **未做成线上证明**：GitHub 远程 Actions 完整运行（本地 Commit 不等于远程 CI）；72h 完整持续压测；正式生产 Keycloak HA 与真实企业客户。
3. **性能数字不得混用**：任务排空速度 `task/s` 不等于 HTTP API `req/s`；GPUFlow 合成 `decisions/s` 不等于 GPU 训练吞吐；Keycloak 40 RPS GET 测试不能推断资源订单写入吞吐。
4. **面试展示顺序**：先业务问题（跨系统订单）→ 正确性（Saga/Outbox/Quota）→ 故障数据（Pod kill / no oversell）→ 工程交付（CLI、OIDC、测试）；不要上来堆中间件名词。
