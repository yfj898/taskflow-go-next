# TaskFlow-Go 完整项目链路学习手册

> 目标：让你能够从“我知道项目做了很多功能”，提升到“我能从一个 HTTP 请求开始，把数据、队列、Worker、AI、重试、DLQ、控制面、HA/DR 全链路讲清楚”。
>
> 本手册基于当前 `taskflow-go` 仓库真实代码整理。代码片段均来自现有实现，必要时为了讲解只截取关键部分，不改变原逻辑。

---

## 目录

1. 项目整体定位
2. 先看全局架构：系统到底有哪些进程和依赖
3. 启动链：`cmd/server/main.go` 如何把所有模块装起来
4. HTTP 接入层：Gin Router / Middleware / Readiness
5. 任务创建链：HTTP → Service → MySQL Task + Outbox
6. 为什么必须用 Transactional Outbox
7. Outbox Dispatcher：如何把数据库事件可靠投递到队列
8. Queue 抽象：Redis Streams 与 Kafka
9. Redis Streams 消费：Consumer Group、PEL、XAutoClaim
10. Worker Pool：Claim、Lease、Heartbeat、幂等、Retry
11. 任务状态机与 Enqueue Generation Fencing
12. Reconciler：为什么 MQ 之外还需要数据库兜底
13. gRPC 执行链：API 进程与 Executor Worker 如何解耦
14. Executor Router：普通任务和 AI 任务如何分流
15. AI Gateway：Provider、Routing、Fallback、Circuit Breaker
16. AI 并发、Backpressure、SLO、Fair Scheduling、Load Shedding
17. AI Budget / Quota / Route Audit
18. Poison Message 与 DLQ
19. DLQ Archive / S3 / MinIO / Restore
20. Control Plane：Bearer / OIDC / JWKS / RBAC
21. Control Plane Audit / Hash Chain / SIEM
22. Promotion / RPO / Break-glass / Governance
23. MySQL HA：Writer Failover、GTID Replica、PITR
24. Multi-region DR：Region A → Region B
25. Observability：Metrics / Tracing / SLO / Grafana
26. Graceful Shutdown、K8s、KEDA
27. 供应链安全：SBOM / Trivy / Cosign
28. 故障场景总表
29. 你最应该先掌握的 10 个函数
30. 从请求到完成的完整时序图
31. 面试时如何用 3 分钟讲完整链路
32. 学习顺序与源码阅读路线

---

# 1. 项目整体定位

TaskFlow-Go 是一个“可靠异步任务平台 + AI 调度基础设施”。

最核心的数据流只有一条：

```text
Client
  ↓ HTTP
Gin API
  ↓
TaskService
  ↓
MySQL Transaction
  ├─ tasks
  └─ outbox_events
       ↓
Outbox Dispatcher
       ↓
Redis Streams / Kafka
       ↓
Worker Pool
       ↓
Claim task + lease
       ↓
Executor
  ├─ 普通任务：Demo / business executor
  └─ AI任务：AI Router → Provider
       ↓
success / retry / failed
       ↓
MySQL 状态落盘
```

围绕这条主链，项目又增加了四套“保护层”：

```text
可靠性层：Outbox / Retry / Reconciler / DLQ / Archive
性能层：pprof / batch / prepared SQL / connection pool
治理层：OIDC / RBAC / Audit / SIEM / Governance
生产层：K8s / SLO / MySQL HA / PITR / Multi-region DR / SBOM
```

理解项目时不要先背 Phase 1~45，而是先把“主链”掌握，再学习保护层。

---

# 2. 全局架构：系统有哪些进程

## 2.1 TaskFlow API 进程

入口：

```text
cmd/server/main.go
```

负责：

- HTTP API
- MySQL 持久化
- Redis/Kafka Queue
- Outbox Dispatcher
- Worker Pool
- Reconciler
- Control Plane
- SIEM / Governance workers
- Metrics / pprof / readiness

## 2.2 Executor Worker 进程

入口：

```text
cmd/executor-worker/main.go
```

负责：

- gRPC Server
- 实际 Executor
- AI Provider Router
- AI concurrency / quota / load shedding
- gRPC health
- Prometheus metrics

API 进程可以设置：

```text
EXECUTION_BACKEND=local
```

直接本地执行，也可以：

```text
EXECUTION_BACKEND=grpc
```

调用独立 Executor Worker。

## 2.3 基础依赖

```text
MySQL      最终状态真相 / Transaction / Outbox / Audit
Redis      Streams Queue / distributed coordination
Kafka      可替换 Queue backend
MinIO/S3   DLQ archive object storage
Prometheus metrics
Grafana    dashboard
Alertmanager alerts
```

---

# 3. 启动链：`cmd/server/main.go`

这是理解整个项目最重要的文件。

源码：

```text
cmd/server/main.go
```

## 3.1 加载配置

```go
func main() {
    cfg := config.Load()
    if err := cfg.Validate(); err != nil {
        log.Fatalf("invalid config: %v", err)
    }
```

作用：

```text
环境变量
   ↓
Config
   ↓
Validate
```

很多生产级系统问题，本质上应该在启动阶段 fail-fast，而不是运行到一半才报错。

## 3.2 初始化 MySQL

```go
db, err := repository.NewMySQL(
    cfg.MySQLDSN,
    cfg.MySQLMaxOpenConns,
    cfg.MySQLMaxIdleConns,
    cfg.MySQLSkipDefaultTx,
)
```

Phase42 后，如果配置多个 writer candidate：

```go
db, selected, err = repository.NewMySQLWithFailover(
    cfg.MySQLDSN,
    cfg.MySQLFailoverDSNs,
    cfg.MySQLMaxOpenConns,
    cfg.MySQLMaxIdleConns,
    cfg.MySQLSkipDefaultTx,
    time.Duration(cfg.MySQLConnectTimeoutMS)*time.Millisecond,
)
```

理解：

```text
candidate 0: primary
candidate 1: failover writer
candidate 2: ...

逐个 open + PingContext
第一个可用 writer 被选中
```

注意：这是“启动时 writer failover”，不是运行期无脑自动切主。

## 3.3 Repository + Migration

```go
taskRepo := repository.NewTaskRepository(db)
if err := taskRepo.Migrate(); err != nil {
    log.Fatalf("migrate tasks: %v", err)
}
```

Repository 是数据访问层，负责：

- Task
- Outbox
- DLQ
- Audit
- AI Governance
- SIEM
- Promotion
- Governance receipt

## 3.4 初始化 Redis

```go
rdb := queue.NewRedisClient(
    cfg.RedisAddr,
    cfg.RedisPassword,
    cfg.RedisDB,
)

if err := rdb.Ping(context.Background()).Err(); err != nil {
    log.Fatalf("connect redis: %v", err)
}
```

## 3.5 Queue Backend 选择

```go
var taskQueue queue.TaskQueue

switch strings.ToLower(strings.TrimSpace(cfg.QueueBackend)) {
case "redis", "redis-streams", "redis_streams":
    taskQueue = queue.NewRedisTaskQueue(rdb, cfg.RedisQueueKey)

case "kafka":
    kafkaQueue := queue.NewKafkaTaskQueue(
        strings.Split(cfg.KafkaBrokers, ","),
        cfg.KafkaTopic,
        cfg.KafkaGroup,
    )
    taskQueue = kafkaQueue
}
```

这体现一个重要设计：

> Worker 和 Service 不应该依赖 Redis 的具体 API，而应该依赖 `TaskQueue` 接口。

## 3.6 初始化执行器

```go
switch cfg.ExecutionBackend {
case "local":
    exec = executor.NewTaskExecutorWithRouting(...)

case "grpc":
    grpcExec, err := executor.NewGRPCExecutor(...)
    exec = grpcExec
    executorReady = grpcExec.Ready
}
```

## 3.7 启动三个核心后台组件

```go
pool := worker.NewPool(...)
pool.Start(ctx)

outboxDispatcher := outbox.New(...)
outboxDispatcher.Start(ctx)

reconcileRunner := reconciler.New(...)
reconcileRunner.Start(reconcileCtx)
```

一定记住：

```text
Pool         = 消费队列并执行任务
Dispatcher   = DB Outbox → Queue
Reconciler   = DB 扫描兜底恢复
```

这三个组件组成可靠异步执行闭环。

---

# 4. HTTP 接入层

源码：

```text
internal/handler/router.go
internal/handler/middleware.go
```

## 4.1 Router 中间件

```go
r := gin.New()
r.Use(requestID(), traceHTTP())

if accessLog {
    r.Use(gin.Logger())
}

r.Use(
    gin.Recovery(),
    observeHTTP(),
    rateLimit(rateRPS, rateBurst),
)
```

职责：

```text
request ID
trace
panic recovery
metrics
rate limit
```

## 4.2 Liveness

```go
r.GET("/health/live", func(c *gin.Context) {
    c.JSON(http.StatusOK, gin.H{"status": "ok"})
})
```

只表示进程活着。

## 4.3 Readiness

```go
r.GET("/health/ready", func(c *gin.Context) {
    sqlDB, err := db.DB()

    if err != nil ||
        sqlDB.PingContext(c.Request.Context()) != nil ||
        rdb.Ping(c.Request.Context()).Err() != nil {

        c.JSON(http.StatusServiceUnavailable,
            gin.H{"status": "not_ready"})
        return
    }
```

当使用 gRPC executor 时，还会检查 executor health。

所以：

```text
liveness：进程要不要重启
readiness：现在能不能接业务流量
```

---

# 5. 任务创建完整链：HTTP → Service → Task + Outbox

这是整个项目最核心的一条链。

## 5.1 HTTP API

源码：

```text
internal/handler/router.go
```

```go
v1.POST("/tasks", func(c *gin.Context) {
    var in service.CreateTaskInput

    if err := c.ShouldBindJSON(&in); err != nil {
        c.JSON(http.StatusBadRequest,
            gin.H{"error": "invalid json"})
        return
    }

    task, err := taskService.Create(
        c.Request.Context(),
        in,
    )

    if err != nil {
        ...
    }

    c.JSON(http.StatusCreated, task)
})
```

## 5.2 Service 层

源码：

```text
internal/service/task_service.go
```

输入：

```go
type CreateTaskInput struct {
    Name    string `json:"name"`
    Type    string `json:"type"`
    Payload string `json:"payload"`
}
```

核心代码：

```go
func (s *TaskService) Create(
    ctx context.Context,
    in CreateTaskInput,
) (*domain.Task, error) {

    task := &domain.Task{
        Name: in.Name,
        Type: in.Type,
        Payload: in.Payload,
        Status: domain.TaskPending,
        NextRunAt: time.Now(),
        EnqueueGeneration: 1,
    }

    if err := s.repo.CreateWithOutbox(ctx, task); err != nil {
        return nil, err
    }

    return task, nil
}
```

注意：Service 没有直接 `queue.Push()`。

这是非常关键的设计。

---

# 6. Transactional Outbox

源码：

```text
internal/repository/task_repository.go
internal/domain/outbox.go
```

## 6.1 为什么不能直接 DB + Redis

错误设计：

```go
db.Create(task)
queue.Push(task.ID)
```

失败窗口：

```text
DB commit 成功
↓
进程崩溃
↓
Queue Push 没执行
↓
数据库有任务，但永远不会执行
```

反过来也会有：Queue 成功、DB rollback。

## 6.2 项目中的真实实现

```go
func (r *TaskRepository) CreateWithOutbox(
    ctx context.Context,
    task *domain.Task,
) error {

    tx, err := sqlDB.BeginTx(ctx, nil)
    if err != nil {
        return err
    }

    defer func() { _ = tx.Rollback() }()
```

先 INSERT Task：

```go
result, err := tx.StmtContext(
    ctx,
    r.insertTaskStmt,
).ExecContext(
    ctx,
    task.Name,
    task.Type,
    task.Payload,
    task.Status,
    ...,
)
```

拿到 Task ID：

```go
id, err := result.LastInsertId()
task.ID = uint64(id)
```

再 INSERT Outbox：

```go
_, err = tx.StmtContext(
    ctx,
    r.insertOutboxStmt,
).ExecContext(
    ctx,
    task.ID,
    domain.OutboxTaskCreated,
    now,
    now,
)
```

最后：

```go
return tx.Commit()
```

因此数据库保证：

```text
Task + Outbox
要么都成功
要么都失败
```

## 6.3 为什么这里用了 raw `database/sql`

源码注释已经说明原因：

```go
// This is the hottest write path in the service.
// Keep the same atomic task+outbox transaction semantics
// while avoiding GORM's reflection-heavy Create callbacks.
```

这是性能优化的重要亮点：

```text
不是删除事务
不是删除 Outbox
而是只把热点 INSERT 换成 prepared SQL
```

---

# 7. Outbox Dispatcher

源码：

```text
internal/outbox/dispatcher.go
```

职责：

```text
扫描未发布 Outbox
↓
Claim lease
↓
生成 queue Message
↓
批量 Push
↓
确认 published
```

## 7.1 Start

```go
func (d *Dispatcher) Start(ctx context.Context) {
    go func() {
        ticker := time.NewTicker(d.interval)

        d.drain(ctx)

        for {
            select {
            case <-ctx.Done():
                return
            case <-ticker.C:
                d.drain(ctx)
            }
        }
    }()
}
```

## 7.2 批量 Claim

```go
events, err := d.repo.ClaimOutboxBatch(
    ctx,
    now,
    now.Add(d.lease),
    d.batch,
)
```

这里的 lease 是为了避免多个 Dispatcher 重复处理同一批 Outbox。

## 7.3 Enqueue Fencing

```go
claim, state, err := d.repo.ClaimTaskEnqueue(
    ctx,
    events[i].TaskID,
    1,
    now,
    now.Add(d.enqueueLease),
)
```

然后：

```go
messages = append(messages, queue.Message{
    TaskID: claim.TaskID,
    Generation: claim.Generation,
})
```

Generation 是后面防止旧消息误执行的关键。

## 7.4 批量发布

```go
publishErr := d.queue.PushBatch(pubCtx, messages)
```

成功后：

```go
confirmErr := d.repo.ConfirmTaskEnqueuedAndOutboxBatch(
    settleCtx,
    claims,
    publishIDs,
    time.Now(),
)
```

## 7.5 最重要的可靠性结论

即使：

```text
Broker publish 成功
↓
DB confirm 之前宕机
```

重启后仍有可能再次发送。

因此：

```text
Outbox = at-least-once
并不是神奇 exactly-once
```

所以消费者必须幂等。

---

# 8. Queue 抽象

源码：

```text
internal/queue/task_queue.go
internal/queue/redis.go
internal/queue/kafka.go
```

核心接口：

```go
type TaskQueue interface {
    Push(ctx context.Context, message Message) error
    PushBatch(ctx context.Context, messages []Message) error
    Pop(ctx context.Context) (Delivery, error)
    Ack(ctx context.Context, deliveryID string) error
}
```

消息结构：

```go
type Message struct {
    TaskID     uint64
    Generation uint64
}
```

为什么队列里只放 TaskID + Generation，而不直接放完整任务？

因为：

```text
MySQL 才是 source of truth
Queue 只是 delivery mechanism
```

消费者拿到 ID 后重新从 DB 读取任务，避免 MQ 内数据与数据库长期漂移。

---

# 9. Redis Streams 消费

源码主要在：

```text
internal/queue/task_queue.go
```

## 9.1 Consumer Group

```go
err := q.client.XGroupCreateMkStream(
    ctx,
    q.key,
    q.group,
    "0",
).Err()
```

## 9.2 新消息消费

```go
streams, err := q.client.XReadGroup(
    ctx,
    &redis.XReadGroupArgs{
        Group: q.group,
        Consumer: q.consumer,
        Streams: []string{q.key, ">"},
        Count: 1,
        Block: 2 * time.Second,
    },
).Result()
```

## 9.3 Worker 崩溃后的消息恢复

```go
messages, _, err := q.client.XAutoClaim(
    ctx,
    &redis.XAutoClaimArgs{
        Stream: q.key,
        Group: q.group,
        Consumer: q.consumer,
        MinIdle: q.claimIdle,
        Start: "0-0",
        Count: 1,
    },
).Result()
```

含义：

```text
Worker A 消费消息
↓
没 ACK 就挂掉
↓
消息进入 PEL
↓
idle 超过阈值
↓
Worker B XAutoClaim
↓
重新执行
```

## 9.4 ACK

```go
func (q *RedisTaskQueue) Ack(
    ctx context.Context,
    deliveryID string,
) error {
    return q.client.XAck(
        ctx,
        q.key,
        q.group,
        deliveryID,
    ).Err()
}
```

---

# 10. Worker Pool：项目核心消费执行逻辑

源码：

```text
internal/worker/pool.go
```

这是第二个必须重点读的文件。

## 10.1 启动固定数量 goroutine

```go
for i := 0; i < p.size; i++ {
    p.wg.Add(1)
    go p.runWorker(intakeCtx, ctx, i+1)
}
```

## 10.2 从 Queue Pop

```go
delivery, err := p.queue.Pop(intakeCtx)
```

## 10.3 处理 Poison Message

```go
var poison *queue.PoisonDeliveryError

if errors.As(err, &poison) {
    if p.quarantinePoison(poison) {
        _ = p.queue.Ack(
            context.Background(),
            poison.DeliveryID,
        )
    }
    continue
}
```

坏消息不会无限循环重试，而是进入 DLQ。

## 10.4 Claim Task：真正的业务幂等核心

```go
claimed, err := p.repo.Claim(
    ctx,
    taskID,
    generation,
    time.Now().Add(p.lease),
)
```

只有 claim 成功的 Worker 才能继续执行。

其他 Worker 收到重复消息：

```go
if !claimed {
    ...
    return true, nil
}
```

表示这个消息可以 ACK 掉，不重复执行业务。

## 10.5 重新读取 Task

```go
task, err := p.repo.Get(ctx, taskID)
```

所以：

```text
Queue 中不信任完整业务状态
DB 才是最终状态来源
```

## 10.6 Heartbeat

执行过程中：

```go
heartbeatDone := make(chan error, 1)
go p.heartbeat(
    execCtx,
    cancelExec,
    taskID,
    heartbeatDone,
)
```

作用是持续续租 `lease_until`。

如果续租失败：

```go
execErr = fmt.Errorf(
    "worker lease lost: %w",
    heartbeatErr,
)
```

防止已经失去所有权的 Worker 继续无条件写最终结果。

## 10.7 真正执行

```go
execution, execErr = executor.RunDetailed(
    p.exec,
    execCtx,
    task,
)
```

普通 task 和 AI task 的差异从这里之后交给 Executor Router。

## 10.8 成功状态迁移

```go
updated, err := p.repo.TransitionWithAIRouteAuditAndBudget(
    ctx,
    taskID,
    domain.TaskRunning,
    domain.TaskSuccess,
    map[string]any{
        "result": result,
        "last_error": "",
        "lease_until": nil,
    },
    ...,
)
```

注意条件是：

```text
running → success
```

而不是任意状态都能改。

## 10.9 Retryable Error

```go
if task.RetryCount < p.maxRetries {
    delay := retryBackoff(task.RetryCount)

    updated, err := p.repo.TransitionWithAIRouteAuditAndBudget(
        settleCtx,
        taskID,
        domain.TaskRunning,
        domain.TaskPending,
        map[string]any{
            "retry_count": task.RetryCount + 1,
            "last_error": execErr.Error(),
            "next_run_at": time.Now().Add(delay),
            "lease_until": nil,
        },
        ...,
    )
}
```

这时并不是 Worker 自己 sleep 后再执行，而是：

```text
running → pending
next_run_at = future
↓
Reconciler 之后重新入队
```

---

# 11. Task 状态机与 Enqueue Generation

核心状态：

```text
pending
   ↓ claim
running
   ├─ success
   ├─ failed
   └─ pending (retry)
```

Generation 用于解决“旧入队消息迟到”的问题。

例如：

```text
Generation 1 已入队
↓
任务 retry
↓
Generation 2 又入队
↓
旧 Generation 1 晚到
```

如果没有 generation fencing，旧消息可能重复触发。

所以 Queue Message 携带：

```go
type Message struct {
    TaskID     uint64
    Generation uint64
}
```

Repository Claim 时同时检查当前 generation。

面试里可以解释为：

> 我不仅做 task status CAS，还给“入队动作”增加 generation，使延迟到达的旧消息天然失效。

---

# 12. Reconciler：第二条可靠性路径

源码：

```text
internal/reconciler/reconciler.go
```

如果系统只依赖 MQ，会存在问题：

```text
Outbox / Queue / Worker 任意一层异常
可能导致任务长期停留在 DB
```

所以项目让 MySQL 仍然是最终真相，并定期扫描。

## 12.1 恢复 stale running

```go
recovered, err := r.repo.RecoverStaleRunning(
    ctx,
    now,
    now.Add(-r.staleRunning),
)
```

典型情况：

```text
Worker claim task
status=running
↓
Worker进程崩溃
↓
lease过期
↓
Reconciler 恢复为 pending
```

## 12.2 扫描 Ready Pending

```go
tasks, err := r.repo.FindReadyPending(
    ctx,
    time.Now(),
    r.batchSize,
)
```

## 12.3 再次入队

```go
claim, state, err := r.repo.ClaimTaskEnqueue(...)

if state != repository.EnqueueClaimAcquired {
    continue
}

err = r.queue.Push(
    ctx,
    queue.Message{
        TaskID: claim.TaskID,
        Generation: claim.Generation,
    },
)
```

所以完整可靠性模型是：

```text
Fast Path:
Outbox → MQ → Worker

Safety Net:
MySQL → Reconciler → MQ
```

---

# 13. gRPC 执行链

源码：

```text
internal/executor/grpc.go
cmd/executor-worker/main.go
internal/rpc/executor_service.go
```

## 13.1 API 端 Client

```go
grpcExec, err := executor.NewGRPCExecutor(
    cfg.ExecutorGRPCTarget,
    time.Duration(cfg.ExecutorTimeoutMS)*time.Millisecond,
)
```

## 13.2 Ready 检查

```go
resp, err := e.health.Check(
    checkCtx,
    &grpc_health_v1.HealthCheckRequest{
        Service: rpc.ExecutorServiceName,
    },
)
```

## 13.3 Deadline

```go
callCtx, cancel := context.WithTimeout(
    traceCtx,
    e.timeoutFor(task.Type),
)
defer cancel()
```

没有 deadline 的远程调用很危险，因为下游卡死可能导致 goroutine 永久堆积。

## 13.4 Circuit Breaker

调用前：

```go
if err := e.breaker.Allow(); err != nil {
    return "", Retryable(err)
}
```

失败时：

```go
e.breaker.Failure()
```

成功：

```go
e.breaker.Success()
```

## 13.5 Executor Worker Server

```go
rpc.RegisterExecutorServiceServer(
    server,
    executor.NewGRPCServer(taskExecutor),
)
```

执行时：

```go
execution, execErr := RunDetailed(
    s.exec,
    ctx,
    task,
)
```

API 和执行层因此可以独立扩缩容。

---

# 14. Executor Router：普通任务 vs AI 任务

源码：

```text
internal/executor/executor.go
internal/executor/router.go
```

## 14.1 Executor 接口

```go
type Executor interface {
    Execute(
        ctx context.Context,
        task *domain.Task,
    ) (string, error)
}
```

## 14.2 Router

```go
type Router struct {
    fallback Executor
    handlers map[string]Executor
}
```

注册 AI：

```go
router.Register(
    AITaskType,
    NewAIProviderExecutorWithRouting(...),
)
```

执行：

```go
if exec, ok := r.handlers[
    strings.ToLower(strings.TrimSpace(task.Type))
]; ok {
    return RunDetailed(exec, ctx, task)
}

return RunDetailed(r.fallback, ctx, task)
```

所以核心 Queue / Worker 不需要知道 AI 的具体逻辑。

这是良好的模块隔离。

---

# 15. AI Gateway：Provider Routing

核心源码：

```text
internal/executor/ai_http.go
internal/executor/ai_openai.go
internal/executor/ai_routing.go
internal/executor/breaker.go
```

## 15.1 Provider 配置

```go
type AIProviderConfig struct {
    Name                 string
    Kind                 string
    Endpoint             string
    BearerToken          string
    Model                string
    Capabilities         []string
    InputCostPer1M       float64
    OutputCostPer1M      float64
    MaxConcurrency       int
    GlobalMaxConcurrency int
}
```

这说明路由不仅看 endpoint，还可以考虑：

- model
- capability
- cost
- concurrency

## 15.2 Routing Strategy

```go
const (
    AIRoutingBalanced    = "balanced"
    AIRoutingCost        = "cost"
    AIRoutingLatency     = "latency"
    AIRoutingReliability = "reliability"
)
```

## 15.3 Runtime 状态

```go
type aiProviderRuntime struct {
    provider AIProvider
    info     AIProviderInfo
    breaker  *CircuitBreaker

    calls       uint64
    failures    uint64
    ewmaLatency time.Duration
}
```

所以路由可以使用“动态运行状态”，而不仅仅是静态权重。

## 15.4 Candidate Ranking

```go
candidates, err := r.rankCandidates(
    req,
    strategy,
    defaultOutputTokens,
)
```

Audit 中记录：

```go
type AIRouteCandidateAudit struct {
    Provider         string
    Model            string
    Score            float64
    EstimatedCostUSD float64
    EWMALatencyMS    float64
    FailureRate      float64
    CircuitState     string
}
```

这是项目和普通“LLM proxy”最明显的差别之一。

---

# 16. AI Backpressure / SLO / Fair Scheduling / Load Shedding

源码：

```text
internal/executor/ai_backpressure.go
internal/executor/ai_load_shedding.go
internal/executor/ai_distributed_concurrency.go
internal/executor/ai_hedge.go
internal/executor/ai_tenant_quota.go
```

## 16.1 SLO Class

```go
const (
    AISLORealtime = "realtime"
    AISLOStandard = "standard"
    AISLOBatch    = "batch"
)
```

默认策略：

```go
Realtime:
    QueueWait: 50ms
    Timeout:   3s

Standard:
    QueueWait: 1s
    Timeout:   15s

Batch:
    QueueWait: 5s
    Timeout:   30s
```

不同业务不应该统一一个 timeout。

## 16.2 Per-model Concurrency Gate

```go
type modelConcurrencyGate struct {
    mu     sync.Mutex
    limit  int
    active int
    waiters []*modelSlotWaiter
}
```

直接有空位：

```go
if g.active < g.limit {
    g.active++
    return 0, g.release, nil
}
```

没空位则进入等待队列：

```go
w := &modelSlotWaiter{
    priority: priority,
    tenantID: tenantID,
    seq: g.nextSeq,
    enqueued: time.Now(),
    ready: make(chan struct{}, 1),
}
```

超时：

```go
case <-timer.C:
    ...
    return time.Since(started), nil, ErrAIBackpressure
```

## 16.3 Tenant Fairness

如果一个租户连续赢太多次：

```go
if g.fair.Enabled &&
   g.lastTenant != "" &&
   g.consecutiveWins >= g.fair.MaxConsecutivePerTenant {
    ...
}
```

会尝试旋转到其他 tenant。

目的：

```text
防止 noisy neighbor
```

## 16.4 Starvation Prevention

等待越久，effective priority 会提升：

```go
steps := int(
    now.Sub(waiter.enqueued) /
    g.fair.StarvationAfter,
)

priority := waiter.priority - steps
```

这是一种 aging 思路。

## 16.5 Adaptive Load Shedding

系统根据：

```text
queued
failureRate
maxLatency
```

判断：

```text
normal
degraded
overloaded
```

核心：

```go
if queued >= config.OverloadedQueueDepth ||
   failureRate >= config.OverloadedFailureRate ||
   maxLatency >= config.OverloadedLatency {

    state = AIPressureOverloaded
}
```

过载时 Batch 直接 shed：

```go
case AISLOBatch:
    return policy,
        Retryable(fmt.Errorf(
            "%w: pressure=%s slo=%s",
            ErrAILoadShed,
            state,
            sloClass,
        ))
```

这里体现的思想是：

> 过载时保护系统，而不是“所有请求都接进来然后一起超时”。

---

# 17. AI Budget / Quota / Route Audit

相关源码：

```text
internal/domain/ai_budget.go
internal/domain/ai_governance.go
internal/service/ai_budget.go
internal/service/ai_governance.go
internal/repository/ai_budget.go
internal/repository/ai_governance.go
```

Worker 执行 AI Task 前：

```go
if p.budget != nil {
    reservation, execErr = p.budget.ReserveForTask(
        execCtx,
        task,
    )
}
```

成功后从 execution metadata 中读取：

```text
actual tokens
actual USD
```

并结算 reservation。

同时 `AIRouteDecisionAudit` 保存：

```text
strategy
candidate scores
selected provider
estimated cost
actual cost
tokens
latency
fallback count
hedge winner
```

所以系统可以回答：

> 为什么这个任务选择了某个模型？花了多少钱？是否 fallback？

---

# 18. Poison Message 与 DLQ

相关源码：

```text
internal/domain/dead_letter.go
internal/service/dead_letter.go
internal/repository/dead_letter.go
internal/worker/pool.go
```

## 18.1 Poison Message

例如 Redis message 没有 `task_id`：

```go
return Delivery{}, &PoisonDeliveryError{
    Backend: "redis",
    FailureClass: "decode",
    Reason: "stream message missing task_id",
}
```

Worker 收到：

```go
if errors.As(err, &poison) {
    if p.quarantinePoison(poison) {
        p.queue.Ack(...)
    }
}
```

流程：

```text
坏消息
↓
写 DLQ durable record
↓
ACK 原 queue message
↓
不再无限 poison loop
```

## 18.2 Orphan Message

Queue 中 TaskID 存在，但数据库任务已经不存在：

```go
if errors.Is(getErr, repository.ErrTaskNotFound) {
    return false, &queue.PoisonDeliveryError{
        FailureClass: "orphan",
        Reason: "queue delivery references missing task",
    }
}
```

---

# 19. DLQ Archive / S3 / MinIO

源码：

```text
internal/service/dead_letter.go
internal/archive/dead_letter.go
internal/archive/s3.go
```

## 19.1 Archive 不是简单文件复制

流程：

```text
Claim archive lease
↓
Marshal package
↓
SHA256 digest
↓
Put object
↓
Get object read-back
↓
校验 size + SHA256
↓
Confirm archive DB record
↓
复制到 replica sink
```

关键代码：

```go
data, digest, err := archivepkg.MarshalDeadLetterPackage(...)

objectKey := fmt.Sprintf(
    "dead-letters/v1/%d/%s.json",
    claim.DeadLetterID,
    digest,
)

if err := s.archiveSink.Put(
    ctx,
    objectKey,
    data,
); err != nil {
    ...
}
```

读取回校验：

```go
readBack, err := s.archiveSink.Get(ctx, objectKey)

if int64(len(readBack)) != int64(len(data)) ||
   archivepkg.SHA256Hex(readBack) != digest {
    return ErrDeadLetterArchiveIntegrity
}
```

## 19.2 S3 Sink

```go
type S3Sink struct {
    client *minio.Client
    bucket string
}
```

Put：

```go
_, err := s.client.PutObject(
    ctx,
    s.bucket,
    key,
    bytes.NewReader(data),
    int64(len(data)),
    minio.PutObjectOptions{
        ContentType: "application/json",
    },
)
```

MinIO 只是测试/本地 release evidence 使用的 S3-compatible backend。

---

# 20. Control Plane：Bearer / OIDC / JWKS / RBAC

源码：

```text
internal/handler/control_plane_auth.go
internal/handler/control_plane_oidc.go
```

## 20.1 Bearer Auth

支持：

```text
bearer
oidc
hybrid
```

```go
mode := strings.ToLower(
    strings.TrimSpace(config.Mode),
)

bearerEnabled :=
    mode == "bearer" || mode == "hybrid"

oidcEnabled :=
    mode == "oidc" || mode == "hybrid"
```

Token 不直接普通字符串比较，而是：

```go
actual := sha256.Sum256([]byte(provided))

activeMatch := subtle.ConstantTimeCompare(
    activeExpected[:],
    actual[:],
)
```

## 20.2 OIDC JWT Verify

JWT 必须：

```text
3 segments
RS256
kid
```

```go
if header.Alg != "RS256" ||
   strings.TrimSpace(header.Kid) == "" {
    return "", nil,
        fmt.Errorf("JWT requires RS256 and kid")
}
```

签名校验：

```go
digest := sha256.Sum256(
    []byte(parts[0] + "." + parts[1]),
)

if err := rsa.VerifyPKCS1v15(
    key,
    crypto.SHA256,
    digest[:],
    signature,
); err != nil {
    return "", nil,
        fmt.Errorf("JWT signature verification failed")
}
```

然后校验：

```text
iss
aud
exp
nbf
sub
```

## 20.3 JWKS Rotation

先查 cache：

```go
key, err := v.key(ctx, header.Kid, false)
```

如果 unknown kid：

```go
key, err = v.key(ctx, header.Kid, true)
```

强制刷新 JWKS。

## 20.4 Group → Role

```go
roles := v.roles(claims[v.groupsClaim])
```

RBAC：

```go
required := requiredControlPlaneRoles(
    c.Request.Method,
    c.FullPath(),
)
```

例如：

```text
requester
approver
incident-commander
governance-reviewer
control-plane-reader
control-plane-operator
```

---

# 21. Audit / Hash Chain / SIEM

相关源码：

```text
internal/domain/control_plane_audit.go
internal/handler/control_plane_audit.go
internal/service/control_plane_audit.go
internal/repository/control_plane_audit.go
internal/siem/delivery.go
```

Control Plane 的高权限操作会经过 audit middleware。

Service 提供：

```go
func (s *ControlPlaneAuditService) Begin(...)
func (s *ControlPlaneAuditService) Complete(...)
func (s *ControlPlaneAuditService) Verify(...)
func (s *ControlPlaneAuditService) Export(...)
```

后续 Phase 增加：

```text
append-only sequence
record hash
previous hash
checkpoint
SIEM delivery
delivery receipt
DLQ
```

核心目的不是日志更多，而是：

```text
可审计
可验证篡改
可外部留证
```

---

# 22. Promotion / RPO / Break-glass / Governance

源码重点：

```text
internal/siem/promotion_workflow.go
internal/siem/receiver_cluster.go
internal/siem/receiver_control.go
internal/siem/governance.go
internal/siem/governance_reconcile.go
internal/repository/control_plane_siem_promotion.go
internal/repository/control_plane_siem_governance.go
```

这一块是项目最深的治理功能，不建议第一遍就全部背。

## 22.1 Promotion Intent

先申请：

```go
CreateControlPlaneSIEMPromotionIntent(...)
```

再审批：

```go
ApproveControlPlaneSIEMPromotionIntentWithQuorum(...)
```

然后执行：

```go
ExecutePromotionIntent(...)
```

## 22.2 执行前先检查 Governance Fence

```go
governanceStatus, err := w.GovernanceStatus(ctx)

if governanceStatus.Fenced {
    return result,
        fmt.Errorf(
            "receiver promotion fenced by governance debt policy: %s",
            governanceStatus.FenceReason,
        )
}
```

## 22.3 RPO Fence

```go
if status.RPOFenced {
    return result,
        fmt.Errorf(
            "receiver promotion fenced by RPO policy: %s",
            status.RPOFenceReason,
        )
}
```

并要求：

```go
if status.PrimaryHighWatermark !=
   status.SecondaryHighWatermark {
    return result,
        fmt.Errorf("receiver promotion target is not caught up")
}
```

## 22.4 Break-glass

紧急路径要求：

```text
incident-commander role
incident ID
reason
expected term
receiver convergence
```

即使是紧急切换，也不是“直接改一个变量”。

## 22.5 Governance Receipt

Phase38~40 增加：

```text
primary governance replica
secondary governance replica
signed receipt
receipt quorum
membership epoch
key generation
previous key grace window
durable anti-rollback floor
```

例如 key generation 必须匹配：

```go
if receipt.ReceiptKeyGeneration !=
   verification.generation {

    return "", fmt.Errorf(
        "receipt key generation mismatch got=%d want=%d",
        receipt.ReceiptKeyGeneration,
        verification.generation,
    )
}
```

---

# 23. MySQL HA：GTID / Snapshot / PITR

源码：

```text
internal/repository/mysql.go
docker-compose.yml
scripts/phase42-mysql-ha-pitr-smoke.sh
```

## 23.1 Writer Startup Failover

```go
for index, dsn := range candidates {
    db, err := openMySQL(...)
    ...

    ctx, cancel := context.WithTimeout(
        context.Background(),
        connectTimeout,
    )

    err = sqlDB.PingContext(ctx)

    if err == nil {
        return db, index, nil
    }
}
```

## 23.2 Replica 为什么需要 Snapshot Bootstrap

不能对已有生产主库直接启动空 replica：

```text
主库可能已经有很多历史数据
binlog 当前只有增量 ALTER/UPDATE
空库没有基础表
```

正确做法：

```text
consistent mysqldump
↓
restore replica
↓
GTID auto-position
↓
START REPLICA
```

## 23.3 Read Only Fence

Replica catch up 后：

```text
read_only=ON
super_read_only=ON
```

避免误写。

## 23.4 PITR

项目实际验证：

```text
consistent backup
↓
记录 binlog file / position
↓
产生 post-backup transaction
↓
独立 recovery MySQL restore snapshot
↓
从 backup position 重放 binlog
↓
恢复目标事务
```

---

# 24. Multi-region DR

源码：

```text
scripts/phase45-multiregion-failover-smoke.sh
deploy/multiregion/global-router.conf
docs/runbooks/MULTI_REGION_FAILOVER.md
```

结构：

```text
Global Endpoint
      ↓
Nginx Router
  ├─ Region A TaskFlow (primary)
  └─ Region B TaskFlow (backup)

Region A MySQL Writer
      ↓ GTID
Region B MySQL Replica
```

故障流程：

```text
1. A 正常写入 durable marker
2. 等 B 追平
3. 停止/fence A TaskFlow writer
4. 停止 A MySQL
5. B STOP REPLICA
6. B super_read_only=OFF
7. B TaskFlow 使用 B DB 启动
8. Global Router 切到 B
9. 验证故障前任务仍可读
10. 验证新 POST 可写
```

真实演练证据：

```text
rpo_lost_tasks = 0
rto_seconds = 51
```

注意：这是本地 Docker 多区域故障演练，不应描述成公网生产 SLA。

## 24.1 Router Runtime DNS

最终修复后配置使用 Docker runtime DNS：

```nginx
resolver 127.0.0.11 valid=1s ipv6=off;

upstream taskflow_regions {
    zone taskflow_regions 64k;
    server taskflow:8080 resolve max_fails=1 fail_timeout=1s;
    server taskflow-region-b:8080 resolve backup;
}
```

原因：如果 Region A 容器已经消失，Nginx 启动时静态解析 `taskflow` 会直接失败，导致备用站虽然健康但 Router 自己起不来。

---

# 25. Observability

相关源码：

```text
internal/metrics/metrics.go
internal/metrics/db.go
internal/telemetry/telemetry.go
internal/telemetry/grpc.go
prometheus.yml
deploy/observability/*
docs/runbooks/CONTROL_PLANE_SLO.md
```

项目主要观察：

```text
HTTP request count / latency
Task success / retry / failed
Worker inflight
Queue length / pending / lag
Outbox delivery
Reconciler recovery
gRPC latency/error
AI provider latency/cost/tokens
AI queue/concurrency/pressure
DLQ / archive
SIEM / governance
MySQL pool
```

Tracing 则贯穿：

```text
HTTP
↓ traceparent 写入 Task
Queue
↓
Worker Extract
↓
gRPC Inject / Extract
↓
AI Provider HTTP
```

---

# 26. Graceful Shutdown / K8s / KEDA

Executor Worker 收到 SIGTERM：

```go
healthServer.SetServingStatus(
    rpc.ExecutorServiceName,
    grpc_health_v1.HealthCheckResponse_NOT_SERVING,
)
```

然后：

```go
server.GracefulStop()
```

超时才：

```go
server.Stop()
```

API 侧 Worker Pool：

```go
func (p *Pool) BeginDrain() {
    if !p.draining.CompareAndSwap(false, true) {
        return
    }

    if p.intakeCancel != nil {
        p.intakeCancel()
    }
}
```

含义：

```text
停止 intake
但让已经 claim 的任务继续 settle
```

这是优雅下线的核心。

---

# 27. Supply Chain Security

相关文件：

```text
Dockerfile
scripts/phase44-supply-chain-smoke.sh
.github/workflows/supply-chain.yml
deploy/k8s/kustomization.release.example.yaml
```

包括：

```text
non-root UID 10001
OCI labels
Syft SPDX SBOM
Trivy fixed-CRITICAL gate
digest-pinned deployment
GitHub OIDC keyless Cosign
provenance attestation
```

真实本机 Phase44 扫描最终 fixed CRITICAL = 0。

---

# 28. 故障场景总表

| 故障 | 系统怎么处理 | 核心模块 |
|---|---|---|
| DB 成功、MQ 未发送 | Outbox 后台继续发送 | `CreateWithOutbox` + Dispatcher |
| MQ 重复消息 | Task Claim CAS + generation | Worker + Repository |
| Worker 消费后宕机 | Redis PEL + XAutoClaim | Redis Queue |
| Worker running 时宕机 | lease 过期 + Reconciler | Worker + Reconciler |
| Provider 429/5xx | retry / fallback | AI Router |
| Provider 卡死 | timeout + circuit breaker | gRPC / AI |
| 模型并发打满 | queue wait + backpressure | model gate |
| 系统过载 | load shedding | AI Load Shedding |
| 大租户占满模型 | tenant fairness / quota | AI Scheduler |
| Queue malformed message | Poison quarantine → DLQ | Worker / DLQ |
| DLQ 主存储损坏 | replica archive fallback | Archive |
| Bearer 泄漏/轮换 | active + previous credential | Control Plane Auth |
| OIDC key 轮换 | unknown kid refresh JWKS | OIDC verifier |
| 未授权高权限操作 | RBAC 403 | OIDC RBAC |
| 审计被篡改 | hash-chain verification | Audit |
| Receiver 未追平就切换 | RPO fence | Promotion Workflow |
| Governance debt | promotion fence | Governance |
| Replica receipt key 回滚 | durable generation floor | Governance Phase40 |
| MySQL primary 不可用 | startup writer candidate failover | MySQL |
| 误操作数据 | snapshot + binlog PITR | MySQL HA |
| Region A 整体故障 | fence A → promote B → route B | Phase45 DR |

---

# 29. 你最应该先掌握的 10 个函数

如果时间有限，先把下面十个函数看懂。

## 1. `TaskService.Create`

```text
internal/service/task_service.go
```

理解任务如何被构造。

## 2. `TaskRepository.CreateWithOutbox`

```text
internal/repository/task_repository.go
```

整个可靠任务系统的原点。

## 3. `Dispatcher.publishClaimed`

```text
internal/outbox/dispatcher.go
```

理解 Outbox 如何真正进入 MQ。

## 4. `RedisTaskQueue.Pop`

```text
internal/queue/task_queue.go
```

理解 XAutoClaim + XReadGroup。

## 5. `Pool.execute`

```text
internal/worker/pool.go
```

最重要函数：Claim、Execute、Retry、Success 全在这里。

## 6. `Reconciler.runOnce`

```text
internal/reconciler/reconciler.go
```

理解为什么 MQ 之外还需要 DB 兜底。

## 7. `GRPCExecutor.Execute`

```text
internal/executor/grpc.go
```

理解 deadline / breaker / gRPC status。

## 8. `AIProviderRouter.ExecuteDetailed`

```text
internal/executor/ai_routing.go
```

理解 AI routing 总入口。

## 9. `oidcVerifier.verify`

```text
internal/handler/control_plane_oidc.go
```

理解 JWT / JWKS / RBAC。

## 10. `DeliveryWorker.ExecutePromotionIntent`

```text
internal/siem/promotion_workflow.go
```

理解生产级 failover 为什么要有审批、RPO、fence。

---

# 30. 从 HTTP 请求到任务完成的完整时序

以一个普通任务为例：

```text
① Client
POST /api/v1/tasks
        │
        ▼
② Gin Router
解析 JSON
        │
        ▼
③ TaskService.Create
构造 pending task
generation=1
        │
        ▼
④ TaskRepository.CreateWithOutbox
BEGIN
  INSERT tasks
  INSERT outbox_events
COMMIT
        │
        ▼
⑤ HTTP 201 返回
此时 Queue 甚至可以暂时挂掉
        │
        ▼
⑥ Outbox Dispatcher
ClaimOutboxBatch
ClaimTaskEnqueue
        │
        ▼
⑦ Redis Streams / Kafka
PushBatch(taskID,generation)
        │
        ▼
⑧ Dispatcher confirm
标记 published / enqueued generation
        │
        ▼
⑨ Worker Pool
queue.Pop
        │
        ▼
⑩ Redis XReadGroup 或 XAutoClaim
得到 Delivery
        │
        ▼
⑪ repo.Claim
pending → running
写 lease_until
        │
        ├── Claim失败
        │     → duplicate/obsolete
        │     → ACK
        │
        ▼
⑫ DB Get Task
        │
        ▼
⑬ heartbeat goroutine
持续续 lease
        │
        ▼
⑭ Executor.RunDetailed
        │
        ├─ local executor
        │
        └─ gRPC executor
                │
                ▼
           Executor Worker
                │
                ├─ 普通 task
                └─ AI task → AI Router → Provider
        │
        ▼
⑮ 执行结果
        │
        ├─ success
        │    running → success
        │
        ├─ retryable
        │    running → pending
        │    retry_count++
        │    next_run_at=future
        │
        └─ permanent/max retry
             running → failed
        │
        ▼
⑯ ACK Queue Delivery
```

如果 Retry：

```text
pending + next_run_at
↓
Reconciler 扫描
↓
generation++ / enqueue claim
↓
再次 Push Queue
↓
Worker 再执行
```

如果 Worker 挂了：

```text
running lease过期
↓
Reconciler RecoverStaleRunning
↓
pending
↓
重新入队
```

---

# 31. AI Task 的完整时序

```text
Client
POST task type=ai_generate
        │
        ▼
Task + Outbox
        │
        ▼
Queue
        │
        ▼
Worker Claim
        │
        ▼
Budget Reserve
        │
        ▼
Executor Router
识别 type=ai_generate
        │
        ▼
AIProviderRouter.ExecuteDetailed
        │
        ├─ parse tenant/model/capability/SLO/max_cost
        ├─ load pressure
        ├─ apply load shedding
        ├─ rank providers
        ├─ check circuit breaker
        ├─ acquire per-model gate
        ├─ distributed concurrency
        ├─ tenant quota
        ├─ optional hedge
        └─ fallback
        │
        ▼
AI Provider HTTP/OpenAI-compatible API
        │
        ▼
usage / token / cost / latency
        │
        ▼
AIRouteDecisionAudit
        │
        ▼
Worker settle budget + route audit + Task success
```

---

# 32. 面试时 3 分钟讲完整项目

可以按下面顺序：

## 第一段：核心链路

> TaskFlow-Go 是一个 Go 实现的可靠异步任务和 AI 调度平台。任务创建时我没有直接“写 MySQL 再发 Redis”，而是把 Task 和 Outbox Event 放在同一个 MySQL 事务中。后台 Dispatcher 再把 Outbox 批量投递到 Redis Streams 或 Kafka，从而解决 DB 和 MQ 双写一致性问题。

## 第二段：消费可靠性

> MQ 采用 at-least-once，所以 Worker 不能假设消息只来一次。Worker 执行前会通过数据库状态和 enqueue generation 做原子 Claim，只有成功把 pending 转成 running 的实例获得执行权。Worker 还有 lease 和 heartbeat；如果执行进程崩溃，Reconciler 会检测过期 running task，将其恢复为 pending 并重新入队。

## 第三段：AI

> AI task 通过 Executor Router 进入 AI Gateway。Gateway 不只是 HTTP 代理，而是做 provider/model routing、fallback、cost/token budget、模型级并发、tenant quota、公平调度、backpressure、load shedding 和 hedged request，因此可以在模型 429、延迟升高或系统过载时进行降级和保护。

## 第四段：生产化

> 后续我又做了 OIDC/JWT/RBAC、审计和 SIEM、MySQL GTID 主从与 PITR、Prometheus/Grafana SLO、多区域故障切换和 SBOM/Trivy。最终多区域 Docker 演练实现故障前任务零丢失，RTO 51 秒。性能上通过 pprof 优化，在保留 Transactional Outbox 语义的情况下，create-task 吞吐提升约 31.9%。

---

# 33. 学习优先级

不要一次试图理解 45 个 Phase。

## 第一阶段：必须熟练

1. `router.go` 的 POST `/tasks`
2. `TaskService.Create`
3. `CreateWithOutbox`
4. `Dispatcher.publishClaimed`
5. `TaskQueue`
6. `RedisTaskQueue.Pop`
7. `Pool.execute`
8. `Reconciler.runOnce`

只掌握这 8 个点，你就已经可以把“可靠异步任务系统”完整讲清。

## 第二阶段：Go 后端岗位重点

9. gRPC Executor
10. Circuit Breaker
11. DLQ
12. Archive / S3
13. MySQL GTID / PITR
14. Graceful Drain / K8s
15. Prometheus / SLO

## 第三阶段：AI 应用岗位重点

16. AI Router
17. Provider ranking
18. fallback
19. per-model concurrency
20. SLO classes
21. tenant quota/fairness
22. load shedding
23. hedge
24. cost/token budget

## 第四阶段：加分项

25. OIDC/JWKS/RBAC
26. Audit hash chain
27. SIEM
28. Promotion workflow
29. Governance receipts
30. Multi-region DR

---

# 34. 最后需要记住的系统设计原则

## 原则 1：数据库状态和消息投递不要做裸双写

```text
Transactional Outbox
```

## 原则 2：at-least-once 系统必须做业务幂等

```text
Task Claim CAS
+ generation fencing
```

## 原则 3：Queue 不是最终真相

```text
MySQL = source of truth
Queue = delivery mechanism
```

## 原则 4：快路径之外必须有恢复路径

```text
Outbox/MQ = fast path
Reconciler = safety net
```

## 原则 5：过载时不能只靠“多开 goroutine”

```text
Concurrency limit
Backpressure
Quota
Load shedding
```

## 原则 6：HA 最危险的是 split-brain

```text
先 fence old writer
再 promote new writer
```

## 原则 7：高权限操作需要身份、审批、审计、证据

```text
OIDC
RBAC
Audit
Approval
Governance
```

---

# 35. 源码导航速查表

| 模块 | 文件 |
|---|---|
| API 启动装配 | `cmd/server/main.go` |
| Executor Worker 启动 | `cmd/executor-worker/main.go` |
| HTTP Router | `internal/handler/router.go` |
| Task Service | `internal/service/task_service.go` |
| Task/Outbox Repository | `internal/repository/task_repository.go` |
| Outbox Dispatcher | `internal/outbox/dispatcher.go` |
| Queue Interface | `internal/queue/task_queue.go` |
| Redis Client | `internal/queue/redis.go` |
| Kafka Queue | `internal/queue/kafka.go` |
| Worker | `internal/worker/pool.go` |
| Reconciler | `internal/reconciler/reconciler.go` |
| gRPC Executor | `internal/executor/grpc.go` |
| Executor Interface | `internal/executor/executor.go` |
| Task Router | `internal/executor/router.go` |
| AI Routing | `internal/executor/ai_routing.go` |
| AI Backpressure | `internal/executor/ai_backpressure.go` |
| AI Load Shedding | `internal/executor/ai_load_shedding.go` |
| AI Hedge | `internal/executor/ai_hedge.go` |
| AI Distributed Concurrency | `internal/executor/ai_distributed_concurrency.go` |
| AI Tenant Quota | `internal/executor/ai_tenant_quota.go` |
| DLQ Service | `internal/service/dead_letter.go` |
| S3 Archive | `internal/archive/s3.go` |
| Bearer Auth | `internal/handler/control_plane_auth.go` |
| OIDC/JWKS/RBAC | `internal/handler/control_plane_oidc.go` |
| Audit | `internal/service/control_plane_audit.go` |
| SIEM Delivery | `internal/siem/delivery.go` |
| Promotion Workflow | `internal/siem/promotion_workflow.go` |
| Governance | `internal/siem/governance.go` |
| MySQL Connection | `internal/repository/mysql.go` |
| Phase42 HA/PITR | `scripts/phase42-mysql-ha-pitr-smoke.sh` |
| Phase45 Multi-region | `scripts/phase45-multiregion-failover-smoke.sh` |
| Global Router | `deploy/multiregion/global-router.conf` |
| Metrics | `internal/metrics/metrics.go` |
| Telemetry | `internal/telemetry/telemetry.go` |

---

# 36. 建议你实际动手跟一遍

第一遍不要看治理功能，自己跟一个 task：

```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H 'Content-Type: application/json' \
  -d '{
    "name":"learn-chain",
    "type":"compute",
    "payload":"hello"
  }'
```

然后按顺序在代码里打断点/日志：

```text
handler/router.go
TaskService.Create
CreateWithOutbox
Dispatcher.publishClaimed
RedisTaskQueue.PushBatch
RedisTaskQueue.Pop
Pool.execute
repo.Claim
executor.RunDetailed
Transition → success
Ack
```

第二遍使用：

```json
{"type":"fail_once"}
```

观察：

```text
running → pending
retry_count++
next_run_at
Reconciler
重新入队
第二次成功
```

第三遍再跟：

```json
{"type":"ai_generate"}
```

进入 AI Router。

只要把这三遍完整跟完，这个项目的核心链路就会真正属于你，而不是停留在“我知道做过这些 Phase”。

