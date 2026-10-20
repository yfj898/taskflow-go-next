# TaskFlow production SLO runbook

## Service objectives

- HTTP availability target: 99.9% over the rolling 30-day release window.
- HTTP latency target: p99 below 1 second for the aggregate API surface; individual AI execution SLOs remain governed by their Phase 6 class budgets.
- Queue observability must remain fresh. A stale queue metric is treated as unknown capacity, never as an empty queue.
- Governance safety fences are correctness signals, not availability noise. Do not disable a fence to make an alert green.

## HTTP availability burn

1. Confirm the 5-minute and 1-hour error ratios in the TaskFlow Grafana dashboard.
2. Split `taskflow_http_requests_total` by `path` and `status`; identify whether failures are application, MySQL, Redis, executor, or external-provider related.
3. Check `/health/ready`, MySQL connectivity, Redis health, executor readiness and recent deployment events.
4. If a new release introduced the burn, roll back the deployment before changing data or governance state.
5. Preserve audit/SIEM evidence before destructive remediation.

## Latency

1. Compare HTTP p99 with executor and AI provider latency.
2. Check queue backlog, fair-scheduler pressure, distributed concurrency and load-shedding metrics.
3. Use pprof only after ruling out dependency latency; collect CPU and heap profiles before changing concurrency.
4. Prefer reverting a regression over raising timeout/SLO thresholds.

## Queue observability

1. If `taskflow_queue_stats_fresh` is zero, verify Redis/Kafka reachability and collector errors.
2. Do not scale down based on a stale backlog value.
3. Verify pending/lag state directly at the broker before restarting consumers.
4. After recovery, confirm the freshness gauge returns to one and the last-success timestamp advances.

## DLQ

1. Inspect failure class/state and oldest age before purging anything.
2. Use controlled replay only when the task generation is still authoritative.
3. For capacity pressure, run archive-first lifecycle cleanup and verify archive integrity/replication before hot-row deletion.

## Governance fence

1. Read `/api/v1/control-plane/siem/governance/status` and determine whether the fence is dead evidence, replica receipt debt, open remediation or SLO debt.
2. Never clear durable evidence manually in SQL.
3. Use Phase 35-40 remediation/approval flows. Preserve maker-checker separation and operation-key idempotency.
4. For replica receipt debt, verify membership epoch, receipt key generation and previous-key retirement window before retrying.
5. Break-glass promotion is an incident action and must retain its audit/incident evidence.

## Cloud Saga

1. Check `taskflow_cloud_saga_runnable_workflows` and the workflow history endpoint for the affected Resource; identify whether the workflow is provision, resize, delete or compensation.
2. Compare `taskflow_cloud_saga_workflow_claims_total{outcome="claimed"}` with `outcome="busy"`. Busy claims are expected during replica contention, but a workflow should still make durable progress after the active lease expires or the owner finishes the step.
3. Inspect `taskflow_cloud_saga_step_operations_total` by bounded `kind`, `step`, `direction` and `outcome` labels. Do not add tenant/resource IDs as Prometheus labels.
4. For Provision/Resize failures, verify the persisted compensation step before changing external Kubernetes objects manually. For Delete, preserve fail-forward semantics: do not recreate a Deployment/PVC/Service merely to make the workflow look reversible.
5. Do not release tenant quota manually while any external bundle object still exists. A successful Kubernetes DELETE acknowledgement is insufficient; confirm the object is absent.
6. If a controller replica dies with a lease, wait for lease expiry or verify that another replica can claim the workflow. Do not clear `lease_until` directly in SQL during ordinary recovery.

Reproducible cluster drills:

```bash
make cloud-control-kind-e2e
make cloud-control-kind-chaos
```

The ordinary E2E verifies two-replica lease contention and Provision/Resize/Delete convergence. The Chaos drill additionally kills the workflow owner after a real Kubernetes side effect and injects a post-success provider error, proving lease-expiry takeover and outcome-unknown compensation without manually editing MySQL state.

When `CLOUD_SAGA_OBSERVE_BACKOFF_MS` is enabled, a running provider step may intentionally remain runnable-but-not-due until its persisted `next_reconcile_at`. Do not clear this field manually merely because the scan loop sees no immediate work: the deadline is part of the anti-polling control policy and survives controller handoff. Investigate a genuinely stuck workflow by comparing `next_reconcile_at`, `lease_until`, provider object state, and step attempt counters together.

For bucketed quota accounting, `quota_bucket_id` is ownership state, not a load-balancing hint after admission. A live Resource must release/compensate against that exact bucket. Do not migrate a tenant between single-row and bucketed accounting while usage is non-zero; the repository deliberately rejects that operation.

## Recovery exit criteria

- readiness is healthy;
- queue statistics are fresh;
- HTTP burn-rate recording rules are back inside budget;
- no unexpected DLQ growth is present;
- governance status is unfenced for ordinary operation;
- cloud lifecycle Saga backlog is converging and no workflow is stuck beyond the alert window;
- the incident timeline, operator actions and release version are retained in the audit trail.
