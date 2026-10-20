# Cloud Saga A/B Optimization — 2026-09-22

This document freezes an A/B experiment against the previously measured scale bottlenecks:

1. hot-tenant quota admission contention;
2. Saga controller DB round trips and replica scaling.

The run used the same real Kind/MySQL/Kubernetes control-plane benchmark and completed with `rc=0`.

## Environment

- Kind / Kubernetes: `v1.35.0`
- MySQL: `8.4.11`
- host: Intel Core i9-9900K, 8 cores / 16 threads
- admission: `600` requests per point, concurrency `16 / 64 / 128`
- controller: `90` real Kubernetes workflows per point
- controller replicas: `1 / 2 / 4`
- controller interval: `20ms`

Before the benchmark, correctness was revalidated on real MySQL:

- `atomic_versioned` quota admission with 32 concurrent requests and capacity for only 8 reservations produced exactly **8 admitted / 24 rejected**, with no oversubscription;
- optimized Saga batch prefetch/fused writes and worker-pool convergence tests passed.

## A. Quota admission: locked vs atomic_versioned

`locked` uses an explicit `SELECT ... FOR UPDATE`, checks availability, then updates usage. `atomic_versioned` replaces that read-lock-write sequence with one conditional atomic update:

```sql
UPDATE cloud_tenant_quotas
SET cpu_milli_used = cpu_milli_used + ?,
    memory_mib_used = memory_mib_used + ?,
    replica_used = replica_used + ?,
    version = version + 1
WHERE tenant_id = ?
  AND cpu_milli_used + ? <= cpu_milli_limit
  AND memory_mib_used + ? <= memory_mib_limit
  AND replica_used + ? <= replica_limit;
```

### Hot tenant

| Concurrency | Locked RPS | Atomic RPS | Δ throughput | Locked P95 | Atomic P95 | Δ P95 | Locked waits | Atomic waits |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 16 | 116.40 | 128.34 | **+10.3%** | 172.77 ms | 150.20 ms | **-13.1%** | 832 | 775 |
| 64 | 124.61 | 124.30 | -0.2% | 658.68 ms | 632.48 ms | **-4.0%** | 989 | 938 |
| 128 | 109.66 | 115.35 | **+5.2%** | 1381.97 ms | 1266.22 ms | **-8.4%** | 1122 | 1086 |

At c128, accumulated InnoDB row-lock time decreased from `485173ms` to `458752ms` (**-5.4%**).

### 32-tenant sharded load

The same optimization did **not** provide a stable gain after quota-row contention had already been distributed across tenants:

| Concurrency | Locked RPS | Atomic RPS | Locked P95 | Atomic P95 |
| ---: | ---: | ---: | ---: | ---: |
| 16 | 111.14 | 131.95 | 164.89 ms | 151.50 ms |
| 64 | 109.29 | 120.79 | 647.62 ms | 647.38 ms |
| 128 | **130.60** | 127.29 | 1203.54 ms | 1196.86 ms |

### Interpretation

The versioned conditional update reduces the critical section and helps a hot quota row, but it does **not** remove the fundamental serialization point: one tenant still has one accounting row. At c128 the improvement is useful but modest, so this experiment does not justify claiming that hot-tenant contention has been solved.

The next materially different design would be true quota reservation buckets/sub-ledgers. That requires preserving strict global no-oversubscription and recording the reservation bucket on the resource/order so release and compensation are exact; it should be treated as a separate design, not as a cosmetic SQL change.

## B. Saga DB path: legacy vs batch claim vs optimized

Three DB modes were compared:

- `legacy`: list runnable rows, then claim/reload each workflow individually;
- `baseline`: `SELECT ... FOR UPDATE SKIP LOCKED` batch claim, then individual workflow state loads/writes;
- `optimized`: batch claim + batch preload of Resource/Order/Steps + fused MySQL updates; tested with worker concurrency 1 and 8.

### 1 controller

| Mode | Workers | WF/s | P95 | vs legacy throughput | Compute attempts/WF |
| --- | ---: | ---: | ---: | ---: | ---: |
| legacy | 1 | 4.617 | 19.445 s | 1.000× | 7.17 |
| baseline | 1 | 5.175 | 17.334 s | **1.121×** | 8.51 |
| optimized | 1 | 5.255 | 17.002 s | **1.138×** | 10.62 |
| optimized | 8 | 5.384 | 16.557 s | **1.166×** | **56.40** |

Batch claim alone therefore improved single-controller throughput by about **12.1%** and P95 by about **10.9%**. Fused writes added only a small incremental gain. An 8-worker pool pushed single-controller throughput to **+16.6% vs legacy**, but at the cost of an 8× increase in compute polling.

### Replica scaling

| Mode | Workers | 1 controller | 2 controllers | 4 controllers | 4-controller efficiency |
| --- | ---: | ---: | ---: | ---: | ---: |
| legacy | 1 | 4.617 | 5.086 | 5.409 | 29.3% |
| baseline | 1 | 5.175 | 5.302 | 5.397 | 26.1% |
| optimized | 1 | 5.255 | 5.322 | 5.392 | 25.7% |
| optimized | 8 | 5.384 | 5.363 | 5.397 | **25.1%** |

The 8-worker case is especially important:

```text
controllers:      1       2        4
workflow/s:     5.384   5.363    5.397
compute polls: 56.4    119.9     201.0 per workflow
```

Going from one to two controllers produced **negative scaling**, while Kubernetes polling more than doubled. Four controllers generated about **201 compute attempts per workflow** without improving effective throughput.

### Interpretation

The A/B result separates useful and harmful optimizations:

1. **Batch claim is useful at low replica counts.** It removes repeated DB claim contention and improves the single-controller path.
2. **Fused writes are a small optimization, not a new scaling regime.** They save DB round trips but do not move the shared-control-plane ceiling much.
3. **Unbounded worker concurrency is counterproductive.** It hides wait time by polling Kubernetes more aggressively, causing a polling storm instead of useful throughput scaling.
4. **The next controller optimization should be backoff/event-driven observation, not more workers.** A per-step minimum re-observe interval, exponential backoff, watch/informer integration, or a delayed runnable timestamp would reduce futile SSA/Observe loops.

## Production decision from this experiment

This experiment does **not** change defaults automatically.

- Keep `CLOUD_QUOTA_ADMISSION_MODE=locked` as the conservative default until the atomic path receives broader lifecycle soak coverage, although `atomic_versioned` is a viable targeted optimization for hot tenants.
- Keep `CLOUD_SAGA_DB_MODE=baseline` as the default. Batch claim has a clear benefit without the polling amplification of the aggressive worker-pool case.
- Keep `CLOUD_SAGA_WORKER_CONCURRENCY=1`. The w8 experiment is evidence of a throughput/pressure trade-off, not a production recommendation.

## Next experiments

The two bottlenecks now have clearer next actions:

1. **Quota:** implement true bucketed reservation/sub-ledger accounting with exact bucket ownership on each resource; compare against `atomic_versioned` under one hot tenant while preserving global no-oversubscription.
2. **Saga:** add observe backoff / next-reconcile scheduling so a `running` Compute step is not polled every 20ms by every available worker. Re-run 1/2/4 controller curves and require both higher WF/s and lower attempts/WF before considering it an improvement.

The key result is that the A/B work found both an optimization that helps and an optimization that merely shifts pressure. That distinction is more useful than reporting a higher single-point benchmark number.
