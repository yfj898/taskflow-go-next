# Cloud Lifecycle Saga — Real Kind E2E Evidence

Frozen validation date: **2026-09-22**.

This evidence complements the real-MySQL fault-injection smoke in `CLOUD_SAGA.md`. It validates the lifecycle Saga against a real Kubernetes API and two simultaneously running TaskFlow replicas rather than a fake provider.

## Environment

- Kind `v0.32.0`
- Kubernetes `v1.35.0`
- MySQL `8.4`
- Redis `8-alpine`
- TaskFlow API replicas: `2`
- Workload: `nginx:1.27-alpine`
- Namespace: `taskflow-cloud-saga`
- Provider objects: Kubernetes `Service`, `PersistentVolumeClaim`, `Deployment`

The test is reproducible with:

```bash
make cloud-control-kind-e2e
```

The script rebuilds the current TaskFlow image, starts from a clean namespace and empty MySQL schema, deploys two TaskFlow replicas, drives the real HTTP control plane, and validates Kubernetes and quota state directly.

## 1. Multi-replica schema migration

The first two-replica Kind run exposed a real startup race: both Pods executed GORM `AutoMigrate` concurrently and one failed with MySQL `Error 1050: Table ... already exists`.

The fix wraps the full migration window in a MySQL advisory lock held by a dedicated connection:

```text
GET_LOCK('taskflow:schema:migrate', 120)
        |
        v
AutoMigrate + index normalization
        |
        v
RELEASE_LOCK('taskflow:schema:migrate')
```

After the fix, the clean-schema E2E started both replicas concurrently. Both reached Ready without a restart:

```text
replica A: ready=true, restarts=0, started=2026-09-22T05:32:19Z
replica B: ready=true, restarts=0, started=2026-09-22T05:32:19Z
both listening by 2026-09-22T05:32:21Z
```

This is a database-level lock, not a process-local mutex, so it serializes migration across Pods.

## 2. Workflow lease contention

With the background Saga controller disabled for deterministic testing, the same workflow was reconciled concurrently through the two individual TaskFlow Pods.

Sixteen requests produced:

```text
HTTP 200: 2
HTTP 409 Busy: 14
```

The per-Pod Prometheus counters independently recorded:

```text
replica A: claimed=1, busy=7
replica B: claimed=1, busy=7
aggregate: claimed=2, busy=14
```

This demonstrates that multiple API/controller replicas can race on the same durable workflow while the MySQL workflow lease fences concurrent execution.

## 3. Real Kubernetes lifecycle

The test then drove the same Resource through three durable workflows.

### Provision

Requested bundle:

```text
Deployment replicas = 1
Service port = 8080
PVC = 64Mi
```

The workflow persisted at `2026-09-22T05:32:22.768Z` and completed at `05:32:29.015Z`. The test verified the real Service, PVC and Deployment through the Kubernetes API and waited for the workload Deployment to converge.

### Resize

Target bundle:

```text
Deployment replicas = 2
CPU = 60m x 2
Memory = 96Mi x 2
Service port = 8081
```

The resize workflow persisted at `05:32:29.998Z` and completed at `05:32:35.947Z`. Kubernetes reported two replicas and Service port `8081`. The quota ledger converged to:

```text
cpu_milli_used = 120
memory_mib_used = 192
replica_used = 2
```

### Delete

The delete workflow persisted at `05:32:40.227Z` and completed at `05:33:09.220Z`. The longer duration is expected because the workflow does not treat a DELETE acknowledgement as completion; it waits until the external objects are actually absent.

At completion:

```text
Deployment: absent
PVC: absent
Service: absent

cpu_milli_used = 0
memory_mib_used = 0
replica_used = 0
```

The workflow history for Resource `1` was:

```text
delete_bundle     succeeded
resize_bundle     succeeded
provision_bundle  succeeded
```

## 4. Observability evidence

The Saga exports bounded metrics including:

```text
taskflow_cloud_saga_runnable_workflows
taskflow_cloud_saga_workflow_claims_total{outcome=...}
taskflow_cloud_saga_step_operations_total{kind,step,direction,outcome}
taskflow_cloud_saga_step_duration_seconds{kind,step,direction}
```

Tenant IDs and Resource IDs are deliberately excluded from metric labels.

Prometheus `promtool` validated the current configuration and rules:

```text
prometheus.yml: SUCCESS
alerts.yml: 12 rules found, SUCCESS
```

The Saga-specific alerts include a stuck runnable-workflow backlog alert and a provider-step error-ratio alert, with remediation guidance in `docs/runbooks/CONTROL_PLANE_SLO.md`.

## 5. Background-controller convergence

The final frozen run also validates the normal controller mode rather than only the deterministic manual-reconcile path. Both TaskFlow replicas were rolled to:

```text
CLOUD_SAGA_CONTROLLER_ENABLED=true
CLOUD_SAGA_RECONCILE_INTERVAL_MS=200
```

A second Resource was then submitted through the public API. No manual `/reconcile` request was issued for either of its workflows. The two background controllers automatically completed:

```text
workflow 4  provision_bundle  succeeded
created   2026-09-22T05:39:39.977Z
completed 2026-09-22T05:39:46.795Z

workflow 5  delete_bundle     succeeded
created   2026-09-22T05:39:47.089Z
completed 2026-09-22T05:39:47.392Z
```

At the end of the run:

```text
TaskFlow replica A: Ready=true, restarts=0
TaskFlow replica B: Ready=true, restarts=0
tenant quota: cpu=0, memory=0, replicas=0
automatic-test Deployment: absent
```

This proves two separate properties: direct concurrent requests are fenced by the durable workflow lease, and the ordinary two-replica background controller loop can independently discover and converge accepted lifecycle orders.

### 2026-09-22 revalidation after compensation-metric completion

The complete E2E was rerun after adding provider-step compensation metrics. The clean namespace again produced the same contention result:

```text
HTTP 200:      2
HTTP 409 Busy: 14
busy metric:   14
```

The persisted workflow rows from that run were:

```text
1  provision_bundle  succeeded  05:51:02.260 -> 05:51:09.463
2  resize_bundle     succeeded  05:51:10.614 -> 05:51:17.490
3  delete_bundle     succeeded  05:51:22.705 -> 05:51:50.460
4  provision_bundle  succeeded  05:51:57.882 -> 05:52:01.743
5  delete_bundle     succeeded  05:52:01.868 -> 05:52:02.107
```

Final MySQL state:

```text
Resource 1  kind-saga-app   desired=deleted actual=deleted quota=0/0/0
Resource 2  kind-saga-auto  desired=deleted actual=deleted quota=0/0/0
Tenant kind-saga-tenant      cpu=0 memory=0 replicas=0
```

No `kind-saga-app` or `kind-saga-auto` business Deployment, Service or PVC remained in the namespace. The final automatic Provision/Delete pair was driven only by the two background Saga controllers; no manual reconcile request was used for workflows 4 or 5.

## 6. Evidence boundary

This E2E proves a real single-cluster Kubernetes lifecycle control path and multi-replica TaskFlow coordination. It does **not** claim VM/VPC/cloud-disk semantics, multi-cluster scheduling, or production multi-region Kubernetes availability. Those remain separate extensions.
