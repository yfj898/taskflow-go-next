# TaskFlow Go — Interview Guide

## 60-second answer to “tell me about this project”

TaskFlow started as a Go asynchronous task service and I deliberately pushed it toward production failure scenarios. The core uses MySQL as the source of truth, Transactional Outbox to avoid the database/queue dual-write gap, Redis Streams or Kafka for transport, and gRPC executor workers. On top of that I added AI-provider routing and multi-tenant resource controls, Kubernetes/KEDA scaling, DLQ/archive governance, OIDC/JWT RBAC, tamper-evident audit and SIEM/governance HA. I also implemented MySQL GTID replica bootstrap, PITR and a Region A-to-B promotion drill. I kept the work evidence-driven: the final single-host release capture had a 5,781.98 RPS median with 26.816 ms P95, and the local multi-region drill showed zero durable task loss with a 51-second RTO.

## Three-minute architecture walkthrough

1. **Write path:** `POST /tasks` writes task + Outbox in one MySQL transaction.
2. **Delivery path:** the Outbox dispatcher publishes to Redis Streams or Kafka; the queue is treated as at-least-once.
3. **Execution path:** consumers use conditional SQL claims/leases and invoke independently scalable gRPC executors; reconciliation recovers stale work.
4. **AI path:** AI tasks route through provider/model policies, budget/quota, distributed concurrency and adaptive shedding rather than bypassing scheduling controls.
5. **Control plane:** privileged operations use bearer or OIDC/JWT RBAC and write durable hash-chained audit evidence.
6. **Governance path:** SIEM receipts, receiver failover, external governance quorum and key-generation fencing prevent stale or divergent control-plane evidence from silently passing.
7. **Operations path:** Prometheus recording rules, burn-rate alerts, Alertmanager, Grafana and runbooks provide the SRE layer.
8. **DR path:** MySQL snapshot bootstrap + GTID replication feeds Region B; promotion fences A, makes B writable and restores one global endpoint.

## Questions an interviewer is likely to ask

### Why Transactional Outbox instead of writing DB then publishing directly?

Because those are two independent side effects. If the DB commit succeeds and queue publish fails, a direct dual write can lose the publish intent. With Outbox, the task and publish intent share one DB transaction; queue delivery can be retried later.

### Does Outbox give exactly-once execution?

No. The design assumes at-least-once publication/delivery. Safety comes from idempotent state transitions: only a valid pending generation can claim work, so a duplicate message does not imply duplicate business execution.

### Why keep a reconciler if the queue already retries?

They protect different failure surfaces. Queue retry handles message delivery; the reconciler repairs durable state when leases expire, work is stranded, or publish/recovery timing diverges.

### How did you optimize performance without weakening correctness?

I profiled first, then optimized the hot SQL/Outbox path and batching. I did not remove the transaction or reliability checks. I repeated fixed-hardware fresh-stack runs and kept raw evidence, so the performance change can be separated from feature changes.

### Why seed the MySQL replica before starting GTID replication?

A new empty replica cannot replay only later GTID/binlog transactions if the primary already contains schema/data from earlier history. A consistent snapshot establishes the baseline; GTID then catches up incremental changes.

### What is the difference between startup DSN failover and runtime database failover?

Startup DSN failover chooses a reachable writer when the process starts. It does not magically migrate an existing live SQL connection pool to a new writer. Regional promotion explicitly fences the old writer, promotes the replica, and starts/restarts the application against the promoted DSN.

### Why did the global router need runtime DNS resolution?

If Nginx resolves both upstream names only at startup, a missing primary container can make Nginx itself fail to start even though the backup is healthy. Docker DNS runtime resolution lets the router stay up, log the failed primary resolution and continue to the backup.

### Why have governance receipts and key generations at all?

The control plane can look healthy while external governance evidence is stale or divergent. Per-replica signed receipts, membership epochs and monotonically increasing key generations let TaskFlow distinguish a valid current quorum from stale replicas or rolled-back credentials.

### What would you change for a real cloud deployment?

Use a managed MySQL-compatible HA service or operator, real cross-region networking/storage replication, external OIDC, real KMS/Vault, production ingress/DNS health checks, secret management and region-specific observability. The repository demonstrates the mechanisms and failure semantics, not a claim that local Docker equals a managed cloud platform.

## Five-minute demo flow

1. show `/health/ready` and `/metrics`;
2. create a task and point to task + Outbox atomicity;
3. show queue/worker metrics and gRPC execution;
4. open Grafana/SLO rules or one governance status endpoint;
5. show the Phase 42 PITR smoke and Phase 45 failover evidence JSON;
6. finish with `make gate` / static guardrails to demonstrate reproducibility.

Avoid spending the demo on all 45 phases. Present the system as four stories: **reliable delivery, AI workload governance, privileged control-plane integrity, and recovery/operations**.

## Best deep-dive topics by role

### Go backend

Transactional Outbox, SQL claims/leases, Redis/Kafka semantics, gRPC worker separation, pprof optimization, MySQL GTID/PITR.

### Cloud-native / SRE

KEDA, drain fencing, SLO burn rates, Alertmanager/Grafana, DLQ lifecycle, regional promotion and runbooks.

### AI application engineer

Provider routing, fallback semantics, usage/cost accounting, budget/quota, distributed concurrency, adaptive shedding, fairness and audit/governance around AI operations.

## Numbers worth remembering

- final five-run release median: **5,781.98 RPS**
- release P95: **26.816 ms**
- release P99: **36.290 ms**
- Phase 45 tested RPO: **0 lost durable tasks**
- Phase 45 tested RTO: **51 s**

Always qualify the first three as single-host release benchmark evidence and the last two as the local tested DR topology.
