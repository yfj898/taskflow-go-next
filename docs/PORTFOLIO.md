# TaskFlow Go — Portfolio Summary

## 30-second project pitch

TaskFlow is a production-oriented Go asynchronous task platform that I evolved from a basic queue-backed service into a reliability-focused system covering Transactional Outbox, Redis/Kafka, gRPC workers, AI-provider routing and quota control, Kubernetes/KEDA, tamper-evident control-plane audit, SIEM/governance HA, MySQL GTID/PITR, SRE observability, software-supply-chain checks and a multi-region failover drill.

The main engineering theme is not feature count; it is making failure behavior explicit and testable. The repository includes executable smoke tests for queue outages, duplicate delivery, worker recovery, governance consistency faults, database recovery and regional promotion.

## Resume-ready version — Go backend / cloud-native

- Built a Go asynchronous task platform using **Gin, MySQL, Transactional Outbox, Redis Streams/Kafka and gRPC workers**, using conditional SQL claims, leases and reconciliation to make at-least-once delivery safe under duplicate messages and worker failure.
- Drove **pprof-based hot-path optimization** while retaining reliability semantics; the final five-run single-host release capture recorded a median **5,781.98 RPS**, **26.816 ms P95** and **36.290 ms P99**, with zero HTTP errors in the measured set.
- Implemented platform reliability across **Kubernetes/KEDA autoscaling, drain/rollout fencing, DLQ lifecycle, MinIO/S3-compatible archive replication, Prometheus SLO burn-rate alerts, Alertmanager and Grafana** with executable release-evidence checks.
- Added **MySQL GTID replica bootstrap, startup writer failover, verified backup + binary-log PITR and Region A→B promotion**; the local failover drill recorded **0 lost durable tasks** and **51 s RTO** through one global endpoint.

## Resume-ready version — AI application engineer

- Built an AI task execution layer on top of a durable Go scheduler, adding **OpenAI-compatible provider adapters, model/provider routing, retryable fallback, token/cost accounting and trace propagation** without coupling provider logic into the scheduling core.
- Added **budget/quota admission control, SLO classes, per-model distributed concurrency, adaptive load shedding, hedged requests and tenant-fair scheduling**, turning AI calls into governable multi-tenant workloads rather than direct synchronous API requests.
- Hardened privileged AI/control-plane operations with **OIDC/JWT RBAC, tamper-evident audit, signed SIEM delivery and maker-checker governance remediation**, including signed multi-replica receipt quorum and key anti-rollback fencing.

## STAR framing for interviews

### Reliability / Transactional Outbox

**Situation:** creating a task in MySQL and publishing to a queue creates a dual-write failure window.

**Task:** ensure accepted work remains recoverable even when the queue is unavailable.

**Action:** committed the task row and Outbox row in one transaction; moved publishing to a retryable dispatcher; used conditional task claims and enqueue generations so duplicate publishes remain safe; added stale-work reconciliation.

**Result:** queue outages no longer lose accepted task intent, and duplicate delivery does not require transport-level exactly-once semantics.

### Performance without deleting reliability

**Situation:** the durable create path had avoidable overhead.

**Task:** increase throughput without removing MySQL transactions, Outbox semantics, metrics or traceability.

**Action:** profiled with pprof, optimized the hot INSERT path and Outbox batching, then repeated fresh-stack fixed-hardware runs.

**Result:** the final release evidence recorded a median **5,781.98 RPS**, **26.816 ms P95**, **36.290 ms P99** and zero HTTP errors in the five-run set.

### Database/region DR

**Situation:** application-level HA is insufficient if the database writer or an entire region is lost.

**Task:** demonstrate a recoverable writer path with measurable evidence.

**Action:** implemented snapshot-seeded GTID replication, replica read-only fencing, verified backup/PITR, Region A fencing, Region B promotion and runtime-resolved global routing.

**Result:** the local regional drill preserved the durable pre-failure task (`rpo_lost_tasks=0`) and restored the global endpoint in **51 seconds**.

## Technical stack

Go, Gin, GORM/database/sql, MySQL 8 GTID/binlog, Redis Streams, Kafka, gRPC, Docker Compose, Kubernetes, KEDA, MinIO/S3-compatible storage, Prometheus, Alertmanager, Grafana, OpenTelemetry, OIDC/JWT/JWKS, Vault/AWS-KMS-compatible signing paths, Syft, Trivy and Cosign workflow integration.

## Proof links

- architecture: `docs/ARCHITECTURE.md`
- interview walkthrough: `docs/INTERVIEW_GUIDE.md`
- performance: `docs/PERFORMANCE_RELEASE_2026-09-07.md`
- SRE runbook: `docs/runbooks/CONTROL_PLANE_SLO.md`
- regional DR runbook: `docs/runbooks/MULTI_REGION_FAILOVER.md`
- all phase/release checks: `scripts/`

## Claims to avoid

Do not describe the benchmark as a production SLA, the local DR drill as a real cross-cloud deployment, or the GitHub OIDC signing workflow as locally executed keyless signing. The strongest version of the project is the version where every statement can be tied back to code or an executable evidence path.
