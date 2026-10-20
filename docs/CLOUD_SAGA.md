# Durable Multi-Resource Lifecycle Saga

TaskFlow's cloud-control layer includes persistent Sagas for a concrete multi-resource lifecycle: **provision, resize and delete**.

```text
Quota Admission
      |
      v
Kubernetes Service (network)
      |
      v
PersistentVolumeClaim (storage)
      |
      v
Deployment (compute)
      |
      v
Commit Resource + Order
```

The workflow is intentionally durable. It is not implemented as one in-memory function that calls APIs in sequence. `cloud_workflows` stores the coordinator state and `cloud_workflow_steps` stores the state, attempts, compensation attempts, output and error for every step. A Resource may own a sequence of workflows over time; one controller tick advances **at most one durable transition**, so another process can resume from MySQL after a crash.

## 1. Why Saga instead of a database transaction

MySQL can atomically persist TaskFlow's own rows, but it cannot include Kubernetes Service/PVC/Deployment mutations in the same ACID transaction. A provisioning request can therefore fail after some external resources already exist.

The Saga splits the order into locally atomic and idempotent steps:

| Position | Forward action | Compensation |
| ---: | --- | --- |
| 0 | Reserve tenant CPU/memory/replica quota in MySQL | Release charged quota |
| 1 | Server-Side Apply Kubernetes Service | Delete Service and confirm `404` |
| 2 | Server-Side Apply PVC | Delete PVC and confirm `404` |
| 3 | Server-Side Apply Deployment and observe readiness | Delete Deployment and confirm `404` |
| 4 | Commit Resource/Order terminal state in MySQL | No external compensation; transient DB failure is retried |

The reverse compensation order is therefore:

```text
Compute -> Storage -> Network -> Quota
```

## 2. Persisted state machine

Workflow states:

```text
pending -> running -> succeeded
             |
             +-> compensating -> compensated
```

Step states:

```text
pending -> running -> succeeded
             |
             +-> failed -> compensating -> compensated

optional step: pending -> skipped
```

A workflow owns a lease (`lease_until`). Multiple TaskFlow replicas may scan the same workflow table, but only one can claim a runnable workflow at a time. Provider operations are also idempotent, so a lease expiry or process crash does not require exactly-once RPC semantics.

The Resource created by this path is marked `orchestration_mode=saga`. The normal single-resource reconciler excludes it while the multi-resource workflow is in control, preventing two independent controllers from mutating the same Deployment during provisioning.

## 3. Ambiguous provider failures

An important Saga edge case is a timeout after the provider has already applied the side effect:

```text
TaskFlow ---- PATCH PVC ----> Kubernetes
          X response lost
```

From TaskFlow's point of view the storage step failed, but the PVC may exist. For that reason a **failed provider step is also compensable**. During rollback, TaskFlow calls the idempotent delete for the failed step before compensating earlier succeeded steps.

This avoids the common leak where a Saga compensates only steps recorded as `succeeded` and ignores a side effect whose success response was lost.

## 4. Conservative compensation and quota release

Kubernetes DELETE returning `2xx` does not prove the object is gone; it may only mean deletion has started. Each compensation performs:

```text
DELETE object
   |
   v
GET object
   |
   +-- 2xx --> keep step = compensating, retry later
   |
   `-- 404 --> mark step = compensated
```

Quota is the final compensation step. CPU/memory/replica capacity remains charged until Deployment, PVC and Service compensations have completed. This prevents another order from consuming capacity while the failed workload may still exist.

## 5. API

Configure tenant quota first, then submit a provisioning workflow:

```bash
curl -X POST http://localhost:8080/api/v1/cloud/workflows/provision \
  -H 'Authorization: Bearer ...' \
  -H 'Idempotency-Key: provision-team-a-demo-001' \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant_id":"team-a",
    "name":"demo",
    "provider":"kubernetes",
    "spec":{
      "compute":{
        "namespace":"default",
        "image":"nginx:1.27",
        "replicas":2,
        "cpu":"500m",
        "memory":"256Mi",
        "container_port":8080
      },
      "network":{
        "enabled":true,
        "port":80,
        "target_port":8080
      },
      "storage":{
        "enabled":true,
        "size":"1Gi",
        "mount_path":"/data"
      }
    }
  }'
```

Inspect all persisted steps:

```bash
curl -H 'Authorization: Bearer ...' \
  http://localhost:8080/api/v1/cloud/workflows/1
```

For deterministic debugging, one transition can be driven manually:

```bash
curl -X POST -H 'Authorization: Bearer ...' \
  http://localhost:8080/api/v1/cloud/workflows/1/reconcile
```

Normal operation uses the background Saga controller.

Resize an already active Saga-managed Resource:

```bash
curl -X POST http://localhost:8080/api/v1/cloud/resources/1/workflows/resize \
  -H 'Authorization: Bearer ...' \
  -H 'Idempotency-Key: resize-team-a-demo-002' \
  -H 'Content-Type: application/json' \
  -d '{
    "spec":{
      "compute":{
        "namespace":"default",
        "image":"nginx:1.27",
        "replicas":4,
        "cpu":"600m",
        "memory":"341Mi",
        "container_port":8080
      },
      "network":{"enabled":true,"port":8081,"target_port":8080},
      "storage":{"enabled":true,"size":"1Gi","mount_path":"/data"}
    }
  }'
```

Delete the whole bundle:

```bash
curl -X POST http://localhost:8080/api/v1/cloud/resources/1/workflows/delete \
  -H 'Authorization: Bearer ...' \
  -H 'Idempotency-Key: delete-team-a-demo-003'
```

List the lifecycle history for one Resource:

```bash
curl -H 'Authorization: Bearer ...' \
  'http://localhost:8080/api/v1/cloud/resources/1/workflows?limit=20&offset=0'
```

## 6. Restart behavior

Suppose the process stops after the Service step committed:

```text
quota   succeeded
network succeeded
storage pending
compute pending
commit  pending
```

No in-memory continuation is required. A new controller instance reads these rows, claims the workflow lease and starts from `storage`. It does not replay the already-completed network step.

If the process stops during a `running` provider step, the new instance retries that step. Server-Side Apply and delete are idempotent, which converts the unknown RPC outcome into a safe retry.

## 7. Reproducible evidence

Provider-level tests exercise real HTTP semantics for Kubernetes Service, PVC and Deployment operations, including post-delete `404` confirmation.

The durable coordinator is tested against **real MySQL 8.4**:

```bash
make cloud-control-saga-smoke
```

The smoke verifies:

- quota is already charged before the first external step;
- one transition is persisted before a simulated process restart;
- a fresh coordinator resumes at the next pending step;
- the successful path reaches `workflow=succeeded`, `order=succeeded`, `resource=active`;
- a storage forward failure moves the workflow to `compensating`;
- the failed storage step is itself compensated because its external outcome may be ambiguous;
- compensation runs in reverse order;
- network compensation is injected to fail once and is retried after a simulated restart;
- quota is not released while compensation remains incomplete;
- terminal rollback reaches `workflow=compensated`, `order=failed`, `resource=deleted`, with quota usage returned to zero.

It also exercises a complete post-provision lifecycle on the **same Resource**:

```text
Provision succeeded
      |
      v
Resize up succeeded
      |
      v
Resize larger -> Compute failure
      |
      v
restore previous Compute + Service + quota
      |
      v
Delete -> Storage delete fails once
      |
      v
retry delete forward (no resource recreation)
      |
      v
external objects gone -> quota=0 -> deleted
```

The test explicitly checks that Compute `Ensure` call count does **not** increase during delete retry, proving the delete workflow is fail-forward rather than compensation-by-recreation. It also checks that workflow history contains Provision, successful Resize, compensated Resize and Delete entries for the same Resource.

### Real Kind / Kubernetes E2E

The provider path is also validated against a real Kind Kubernetes `v1.35.0` cluster with **two TaskFlow replicas**, real MySQL 8.4, real Service/PVC/Deployment objects, and the public HTTP control-plane APIs:

```bash
make cloud-control-kind-e2e
```

The frozen 2026-09-22 run produced **2 successful reconcile responses and 14 lease-conflict `409` responses** from 16 simultaneous requests split across the two Pods. Each Pod reported `claimed=1 / busy=7` through `taskflow_cloud_saga_workflow_claims_total`. The same Resource then completed Provision → Resize → Delete; the real Kubernetes objects disappeared before quota was released, and the final CPU/memory/replica usage was zero.

The first two-replica run also exposed an `AutoMigrate` race that single-replica tests could not reveal. Schema migration is now serialized across Pods with MySQL `GET_LOCK`, and the clean-schema rerun started **2/2 TaskFlow Pods with restart count 0**. See [`CLOUD_SAGA_KIND_E2E_EVIDENCE.md`](CLOUD_SAGA_KIND_E2E_EVIDENCE.md) for the frozen evidence and timings.

The final validation also switches **both** TaskFlow Pods from manual Saga reconciliation to the background controller (`CLOUD_SAGA_CONTROLLER_ENABLED=true`, 200 ms scan interval), submits a second Resource, and makes no manual `/reconcile` calls. The two background controllers automatically converge its Provision workflow, then automatically converge its Delete workflow and return tenant CPU/memory/replica usage to zero. This separates “the lease works when directly stressed” from “the normal multi-replica controller loop actually makes progress.”

### Real Kind crash / outcome-unknown evidence

The lifecycle is also exercised under controlled cluster faults:

```bash
make cloud-control-kind-chaos
```

The frozen run kills the owning TaskFlow Pod **after** its real Kubernetes Service has been created but before the Network step is committed. A competing Pod first receives `409 Busy`; after lease expiry it retries the persisted `running` step, and the Service UID remains unchanged across takeover. A second scenario applies the Service successfully and then injects an error, forcing the workflow into compensation; the `failed` Network step itself is deleted and marked `compensated`, with tenant quota returning to zero. Full evidence is in [`CLOUD_SAGA_KIND_CHAOS_EVIDENCE.md`](CLOUD_SAGA_KIND_CHAOS_EVIDENCE.md).

### Scale benchmark: admission contention and controller replicas

The same real-Kind control plane also has a repeatable scale target:

```bash
make cloud-control-scale-benchmark
```

For API admission, controllers are disabled so the benchmark isolates order/workflow/quota persistence. At concurrency 128, a single hot tenant achieved `103.51 orders/s` with P95 `1487.29 ms` and 1,125 InnoDB row-lock waits, while spreading the same 600 requests across 32 tenants achieved `117.59 orders/s`, P95 `1341.58 ms`, and 634 row-lock waits. This confirms that strict per-tenant quota serialization becomes a material hotspot under high concurrency.

For convergence, 90 real Kubernetes workflows were released from a common persisted backlog. Controller throughput was `4.531 / 5.093 / 5.402 workflows/s` at `1 / 2 / 4` replicas, corresponding to only `1.124x` and `1.192x` speedup. P95 completion improved from `19.825s` to `17.616s` and `16.466s`, but 4-replica scaling efficiency was only `29.8%`. The result therefore points to shared MySQL/Kubernetes and per-workflow sequential transitions as the next bottleneck rather than simply asking for more controller replicas. See [`CLOUD_SAGA_SCALE_BENCHMARK_20260922.md`](CLOUD_SAGA_SCALE_BENCHMARK_20260922.md).

### Structural follow-up: bucketed quota and durable observation scheduling

The next experiment replaces the single hot quota ledger with hard-limit sub-ledgers and adds a persisted `next_reconcile_at` gate for Kubernetes observation. The quota bucket chosen for a Resource is stored on that Resource, so resize/delete/compensation cannot release from a different bucket. Real MySQL tests verify strict no-oversubscription, hash-skew fallback across buckets, accounting-mode migration fencing, and exact same-bucket Saga compensation.

The first multi-bucket probing design deadlocked under MySQL concurrency (`1213`). The final path deliberately locks at most one bucket per transaction and uses `READ COMMITTED` for bucketed admission to avoid REPEATABLE READ range/gap-lock cycles. At hot c128 this reduced row-lock waits `1132 -> 653`, while throughput improved only `111.23 -> 115.31 orders/s`; the remaining admission cost is therefore outside the quota row itself.

For controller observation, `next_reconcile_at` is stored in MySQL and participates in both list/claim predicates, so a replacement controller cannot poll a step before its persisted deadline. With optimized 8-worker controllers, a 40 ms exponential base changed four-controller polling from `206.36` to `12.48` Compute attempts/workflow (`-94.0%`) while throughput changed `5.403 -> 5.164 workflows/s`. Full evidence and tuning points are in [`CLOUD_SAGA_STRUCTURAL_OPTIMIZATION_20260923.md`](CLOUD_SAGA_STRUCTURAL_OPTIMIZATION_20260923.md).

## 8. Resize semantics

Resize persists both the last successful `ActualBundleSpec` and the requested `DesiredBundleSpec`. Admission calculates the target CPU/memory/replica demand and conservatively charges:

```text
charged = max(current charge, target demand)
```

So upscale reserves capacity before touching Kubernetes, while downscale keeps the old higher charge until the new Deployment actually converges.

If a forward resize step fails, the Saga switches to compensation and uses `PreviousSpec` as the restore point. Compute is Server-Side Applied back to the previous Deployment spec, the previous Service configuration is restored (or the newly-created Service is deleted if the old bundle had networking disabled), and only then is any additional quota reservation released.

Storage configuration is intentionally immutable during this resize workflow. PVC expansion/shrink has storage-class- and filesystem-specific semantics, so TaskFlow does not pretend a generic rollback-safe PVC resize exists. This is an explicit correctness boundary rather than a missing validation.

## 9. Delete semantics: fail-forward, not rollback

Delete is deliberately different from Provision/Resize. Once a user has submitted an accepted delete intent, an API timeout must **not** cause TaskFlow to recreate resources in an attempt to "roll back" deletion.

The delete state machine is:

```text
Delete Deployment -> confirm absent
        |
        v
Delete PVC -> confirm absent
        |
        v
Delete Service -> confirm absent
        |
        v
Release quota
        |
        v
Commit resource=deleted
```

Provider failures leave the step `running` with the last error and release the workflow lease. A later controller pass retries the same idempotent delete. Quota remains charged until all external resources are confirmed absent. This is a **fail-forward convergent workflow**, not a compensating delete transaction.

## 10. Operability

`GET /api/v1/cloud/resources/:id/workflows` exposes the order/workflow history for portal and debugging use. The controller also exports the low-cardinality Prometheus gauge:

```text
taskflow_cloud_saga_runnable_workflows
```

Provider forward/delete steps expose bounded `kind/step/direction/outcome` counters and latency histograms; tenant IDs and resource IDs are intentionally not Prometheus labels, avoiding high-cardinality metric explosions.

The current Prometheus configuration and Saga alert rules are parsed by `promtool`; the frozen E2E validation found 12 valid recording/alert rules.

## 11. Current boundary

Implemented in this Saga today:

- transactional quota admission;
- Kubernetes Service as the concrete network resource;
- Kubernetes PVC as the concrete storage resource;
- Kubernetes Deployment as the concrete compute resource;
- persistent coordinator + step journal;
- multi-replica workflow lease;
- idempotent provider retries;
- failed-step compensation for ambiguous RPC outcomes;
- reverse compensation with retry;
- deletion confirmation before quota release;
- background and manual reconciliation APIs.
- sequential workflow history per Resource;
- post-success Compute/Service resize with previous-bundle rollback;
- fail-forward whole-bundle delete with delayed quota release;
- runnable Saga backlog and bounded provider-step metrics.

Not claimed yet:

- VM/VPC/block-volume APIs from a real IaaS provider;
- generic PVC resize/shrink and rollback across arbitrary StorageClasses/filesystems;
- cross-cluster placement or migration;
- a general-purpose BPM/workflow DSL.

The implementation is therefore accurately described as a **durable multi-resource Kubernetes lifecycle Saga for provision/resize/delete**, not a complete private-cloud IaaS workflow engine.
