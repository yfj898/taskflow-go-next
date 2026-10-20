# Cloud Saga Scale Benchmark — 2026-09-22

This report freezes the first real-Kind scale benchmark for the cloud lifecycle Saga. It is a **single-host control-plane benchmark**, not a production capacity claim.

## Environment

- Kind / Kubernetes: `v1.35.0`
- MySQL: `8.4.11`
- host CPU: Intel Core i9-9900K, 8 cores / 16 threads
- TaskFlow image: `sha256:5465efd48388a9c48c7e32dfd567bf4e25d3bd071b7e321c81db833328f48c09`
- order-admission requests per point: `600`
- order-admission concurrency: `16 / 64 / 128`
- hot-tenant case: `1` tenant / one quota row
- sharded case: `32` tenants / 32 quota rows
- controller scaling: `90` real Kubernetes workflows per point
- controller replicas: `1 / 2 / 4`

The benchmark target is:

```bash
make cloud-control-scale-benchmark
```

The harness first runs the ordinary real-Kind E2E baseline. For admission measurements, Saga controllers are disabled so HTTP latency measures accepted order persistence and quota admission rather than Kubernetes convergence. For controller scaling, all workflows are persisted first and fenced with a future lease, then released at one common database timestamp so 1/2/4 replicas start from the same backlog.

## 1. High-concurrency order admission

All six points accepted all `600/600` requests with no transport errors.

| Tenant mode | Concurrency | Orders/s | P50 ms | P95 ms | P99 ms | Row-lock waits Δ | Row-lock time Δ ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Hot, 1 tenant | 16 | 128.50 | 118.995 | 163.849 | 177.631 | 867 | 58,549 |
| Hot, 1 tenant | 64 | 107.72 | 606.332 | 687.799 | 700.512 | 870 | 320,411 |
| Hot, 1 tenant | 128 | 103.51 | 1162.365 | 1487.288 | 1613.373 | 1,125 | 516,117 |
| Sharded, 32 tenants | 16 | 124.04 | 125.956 | 161.396 | 163.877 | 599 | 57,803 |
| Sharded, 32 tenants | 64 | 123.30 | 505.175 | 636.423 | 642.371 | 615 | 280,776 |
| Sharded, 32 tenants | 128 | 117.59 | 1057.789 | 1341.583 | 1439.617 | 634 | 458,701 |

### Hot-tenant finding

At concurrency `128`, spreading the same request volume across 32 tenant quota rows instead of one hot quota row produced:

- throughput: `103.51 -> 117.59 orders/s`, **+13.61%**;
- P95: `1487.29 -> 1341.58 ms`, **-9.80%**;
- InnoDB row-lock waits: `1125 -> 634`, **-43.64%**;
- accumulated row-lock time: `516117 -> 458701 ms`, **-11.12%**.

At concurrency `64`, sharding improved throughput by **14.46%**, reduced P95 by **7.47%**, and reduced row-lock waits by **29.31%**.

The important result is not that sharding makes contention disappear. It does not: order creation still touches shared MySQL indexes/tables and performs multiple transactional writes. The evidence shows that the per-tenant quota row becomes a material additional serialization point under high concurrency.

The Hot Tenant curve also shows saturation clearly: increasing concurrency from `16` to `128` reduced throughput from `128.50` to `103.51 orders/s` while P95 increased from `163.85 ms` to `1487.29 ms`. More callers past the saturation point create queueing rather than useful throughput.

Raw curve: [`cloud-saga-scale-admission-20260922.csv`](cloud-saga-scale-admission-20260922.csv).

## 2. Controller replica scaling

Controller scaling uses 90 persisted workflows. Each workflow creates a real Kubernetes Deployment with `replicas=0`, exercising database workflow transitions, Kubernetes Server-Side Apply and Observe while avoiding workload-Pod scheduling as an uncontrolled variable.

| Controllers | Workflow/s | P50 completion s | P95 s | P99 s | Speedup vs 1 | Scaling efficiency |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 4.531 | 18.586 | 19.825 | 19.861 | 1.000x | 100.0% |
| 2 | 5.093 | 16.392 | 17.616 | 17.671 | 1.124x | 56.2% |
| 4 | 5.402 | 14.789 | 16.466 | 16.661 | 1.192x | 29.8% |

Going from 1 to 2 controllers improved workflow throughput by **12.40%** and reduced P95 completion latency by **11.15%**. Going from 1 to 4 improved throughput by only **19.21%** while reducing P95 by **16.95%**.

This is deliberately reported as **sub-linear scaling**, not as a success story about replica count. By 4 replicas, measured scaling efficiency is only `29.8%`. The controller itself is therefore not the only bottleneck. Shared MySQL state transitions, Kubernetes apiserver/SSA/Observe latency, and the sequential per-workflow state machine become dominant.

Raw curve: [`cloud-saga-controller-scaling-20260922.csv`](cloud-saga-controller-scaling-20260922.csv).

## 3. Engineering conclusion

The scale evidence changes the next optimization target:

1. **Do not scale Controller replicas blindly.** 1->2->4 improves tail latency, but throughput quickly approaches a shared-control-plane ceiling.
2. **Hot Tenant quota admission needs explicit contention treatment.** Possible next experiments are quota reservation buckets/sub-ledgers, optimistic versioned admission, or per-tenant serialized admission workers; any change must preserve strict no-oversubscription semantics.
3. **Batching is more promising than more replicas for Workflow progression.** Current reconciliation advances only one durable transition per workflow and repeatedly crosses MySQL/Kubernetes boundaries. Safe batch discovery/claim and reduced DB round trips should be profiled before raising replica count further.
4. **Separate API admission SLO from resource convergence SLO.** Admission can remain around 100+ orders/s on this host while convergence is around 5 workflows/s; exposing them as one latency metric would hide the actual bottleneck.

## Boundary

These are local single-node Kind measurements on one i9-9900K host. They prove relative behavior and bottleneck shape for this implementation; they are not a production private-cloud SLA and should not be extrapolated to a multi-node Kubernetes control plane without a separate run.

## Follow-up A/B optimization

The bottlenecks identified here were subsequently tested with explicit A/B modes: conditional atomic/versioned quota admission, batch `SKIP LOCKED` workflow claim, batch state prefetch/fused DB writes, and controller-local worker concurrency. The frozen results are in [`CLOUD_SAGA_AB_OPTIMIZATION_20260922.md`](CLOUD_SAGA_AB_OPTIMIZATION_20260922.md). The follow-up shows that quota atomic updates provide a modest hot-tenant improvement, batch claim helps at low controller counts, and aggressive worker concurrency mainly amplifies Kubernetes polling rather than increasing effective multi-replica throughput.
