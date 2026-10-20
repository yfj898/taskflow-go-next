# Cloud Saga Structural Optimization — 2026-09-23

This evidence package follows the 2026-09-22 scale/A-B work and tests two structural changes against the bottlenecks measured there:

1. **true tenant quota buckets / sub-ledgers**, instead of serializing every reservation through one tenant usage row;
2. **durable `next_reconcile_at` exponential observation backoff**, instead of allowing every controller/worker to poll a running Kubernetes step on every scan.

The benchmark remains a **single-host Kind control-plane experiment**, not a production capacity or SLA claim.

## 1. Quota buckets: correctness design

`bucketed` accounting partitions one tenant hard limit into multiple independent rows in `cloud_tenant_quota_buckets`. The per-bucket limits sum exactly to the tenant hard quota, so the system cannot admit more aggregate CPU/memory/replicas than the configured tenant limit.

Each admitted `CloudResource` persists its selected `quota_bucket_id`. Resize keeps that bucket, and delete/compensation release to the same row. A quota accounting-mode change is rejected while usage is non-zero, preventing a live resource from losing ownership of its reservation ledger.

### Concurrency bug found during implementation

The first bucket prototype used a hash start bucket and then tried multiple bucket `UPDATE`s in a ring. Real MySQL concurrency testing exposed `Error 1213: Deadlock found when trying to get lock`: transactions could touch multiple buckets in different orders, while the default `REPEATABLE READ` range locking also introduced gap-lock interactions.

The final design therefore uses two constraints:

- **one bucket lock per admission transaction**;
- bucketed MySQL admission runs at **READ COMMITTED**, while quota correctness comes from an explicit locking read and atomic update of the selected bucket row rather than repeatable-read snapshots.

New resources first select one eligible bucket with `FOR UPDATE SKIP LOCKED`; if all eligible rows are only temporarily locked, the transaction performs one blocking locking read. Existing resources select only their persisted bucket. This removes cross-bucket lock cycles while preserving exact release semantics.

### Real MySQL invariants

`cloud-control-quota-smoke.sh` passes the following concurrent checks:

- 160 concurrent bucketed reservations with hard capacity for 80: **80 admitted / 80 rejected**, no oversubscription;
- all requests deliberately hash to the same initial bucket: the first 8 use available capacity across 4 buckets, the ninth is rejected only after aggregate hard quota is exhausted;
- delete releases the exact persisted reservation bucket;
- mode migration with live bucket usage is rejected.

The real MySQL Saga integration suite additionally proves that a failed Provision enters compensation and releases the **same `quota_bucket_id`** originally charged; both that bucket and aggregate tenant usage return to zero.

## 2. Quota admission performance A/B

Each point sends 600 accepted Provision orders through two API replicas with Saga controllers disabled. Concurrency is 16/64/128. The same run compares `locked`, `atomic_versioned`, and 16-bucket `bucketed` modes.

### Hot tenant

| Mode | C16 RPS / P95 | C64 RPS / P95 | C128 RPS / P95 | C128 row-lock waits |
| --- | --- | --- | --- | ---: |
| locked | 116.85 / 164.8 ms | 134.61 / 573.6 ms | 111.23 / 1344.0 ms | 1,132 |
| atomic_versioned | 113.82 / 175.0 ms | 107.75 / 670.6 ms | **132.35 / 1128.8 ms** | 1,115 |
| bucketed | **141.38 / 132.7 ms** | 121.56 / 665.1 ms | 115.31 / 1329.0 ms | **653** |

At hot-tenant c128, bucketed accounting reduced InnoDB row-lock waits from `1132` to `653` (**-42.3%**), but throughput moved only `111.23 -> 115.31 orders/s` (**+3.7%**) and P95 only `1344 -> 1329 ms` (**-1.1%**).

That result is important: the quota-row bottleneck was substantially removed, but the end-to-end transaction immediately exposed the next shared costs in Resource/Order/Workflow/Step persistence and MySQL commit/IO. **Lower lock contention did not automatically translate into proportional request throughput.**

`atomic_versioned` produced a larger c128 point improvement (`111.23 -> 132.35 orders/s`, P95 `1344 -> 1129 ms`) but was worse than locked at c64 in the same run. It is therefore not reported as a universally superior mode.

### 32-tenant load

The bucketed c128 point reached `161.17 orders/s / P95 964.9 ms`, compared with locked `116.80 / 1362.4 ms` in this run. This is useful relative evidence but should not be treated as a production capacity claim: earlier local runs showed material point-to-point variation, and the benchmark runs on one Kind control plane and one host.

Raw data: [`cloud-saga-structural-admission-20260923.csv`](cloud-saga-structural-admission-20260923.csv).

## 3. Durable Saga observation backoff

The previous optimized 8-worker controller hid Kubernetes wait time by polling aggressively. At four controller replicas, it reached about the same effective workflow throughput as the baseline while issuing more than 200 Compute attempts per workflow.

The structural fix adds `cloud_workflows.next_reconcile_at` as durable state. A Workflow is claimable only when both conditions are true:

```text
lease is free/expired
AND
next_reconcile_at is NULL or due
```

When a Kubernetes step returns `Ready=false` (or a retryable delete/compensation error), the coordinator persists an exponential delay. Another Pod or controller replica sees the same deadline after a crash/handoff, so this is not an in-process `sleep`.

The real MySQL integration suite verifies that a Workflow with a future `next_reconcile_at` cannot be processed by either batch reconciliation or direct reconciliation; after moving the deadline into the past another controller can claim it, and a successful step clears the schedule.

## 4. Backoff A/B: 0 / 40 / 60 / 100 ms base

All cases use the deliberately aggressive `optimized + 8 workers` path so the experiment measures whether durable scheduling can control the previously observed polling storm. The exponential delay is capped at 2000 ms.

| Backoff | Controllers | Workflow/s | P95 completion | Compute attempts/WF |
| ---: | ---: | ---: | ---: | ---: |
| 0 ms | 1 | 5.374 | 16.532 s | 62.42 |
| 0 ms | 2 | 5.374 | 16.539 s | 121.80 |
| 0 ms | 4 | **5.403** | **16.478 s** | **206.36** |
| 40 ms | 1 | 4.948 | 18.159 s | 11.96 |
| 40 ms | 2 | 5.119 | 17.556 s | 12.44 |
| 40 ms | 4 | **5.164** | **17.380 s** | **12.48** |
| 60 ms | 1 | 5.186 | 17.356 s | 11.49 |
| 60 ms | 2 | 4.829 | 16.820 s | 11.79 |
| 60 ms | 4 | 4.865 | 18.419 s | 11.79 |
| 100 ms | 1 | 4.914 | 18.290 s | 10.92 |
| 100 ms | 2 | 4.982 | 18.010 s | 11.27 |
| 100 ms | 4 | 5.043 | 17.846 s | 11.18 |

The 40 ms case is the best **observed trade-off in this run**. At 4 controllers it changes:

- effective throughput: `5.403 -> 5.164 workflows/s` (**-4.4%**);
- P95: `16.478 -> 17.380s` (**+5.5%**);
- Compute attempts: `206.36 -> 12.48/WF` (**-94.0%**).

More importantly, polling pressure no longer scales with replica count: `11.96 -> 12.44 -> 12.48 attempts/WF` for 1/2/4 controllers, compared with `62.42 -> 121.80 -> 206.36` without backoff.

The 60 ms run had a tail outlier at 2/4 replicas, while 100 ms consistently reduced polling but cost more throughput. Because these are single local runs, 40 ms is kept as an **experimental candidate**, not promoted to the production default solely from this benchmark.

Raw data: [`cloud-saga-structural-controller-20260923.csv`](cloud-saga-structural-controller-20260923.csv).

## 5. Engineering decision

The experiments separate three different conclusions:

1. **True quota buckets are structurally correct and materially reduce the hot quota-row component of lock contention.** They do not solve the entire admission transaction bottleneck.
2. **Persistent observation backoff fixes the controller polling-amplification failure mode.** A new replica obeys the same persisted deadline, so horizontal scaling no longer multiplies Observe requests for the same waiting step.
3. **The conservative defaults remain unchanged for now.** `locked + baseline + worker=1 + backoff=0` remains the default deployment profile; bucketed accounting and aggressive worker/backoff combinations remain explicit opt-in modes until broader soak and multi-node control-plane testing is complete.

The next controller step should be event-driven observation (Kubernetes Watch/Informer) or a hybrid watch + timeout fallback. The next admission step should profile the remaining Resource/Order/Workflow/Step write path now that quota-row contention has been reduced.

## Reproduction

Correctness:

```bash
sh scripts/cloud-control-quota-smoke.sh
sh scripts/cloud-control-saga-smoke.sh
```

Scale benchmark:

```bash
make cloud-control-scale-benchmark
```

The benchmark harness uses a global `flock` so two local scale runs cannot silently compete for the same host and contaminate results.
