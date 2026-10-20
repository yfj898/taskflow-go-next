# Cloud Resource Control Plane

TaskFlow now contains an opt-in resource control plane for a concrete private-cloud-style lifecycle: **create → resize → delete** a Kubernetes `Deployment` through a durable order model and a desired/actual-state reconciliation loop.

This layer is deliberately separate from the original task queue. The queue proves reliable asynchronous execution; the cloud resource layer proves how the same failure semantics map to a real control-plane object.

## 1. Domain model

`cloud_resources` is the source of truth for desired and observed state:

```text
tenant + provider + name      unique resource identity
desired_spec                  user requested deployment spec
actual_spec                   last provider observation
desired_state                 active | deleted
actual_state                  pending | provisioning | active | deleting | deleted | error
generation                    monotonically increasing desired version
observed_generation           latest converged version
reconcile_lease_until         multi-replica reconcile fence
```

`cloud_orders` records each user-visible mutation:

```text
create | resize | delete
pending -> running -> succeeded
                    \-> superseded
```

Every mutation accepts `Idempotency-Key`. Reusing the same key for the same operation returns the original order instead of creating a second mutation. A newer resize/delete generation atomically marks older unfinished orders `superseded`, so rapid consecutive changes do not leave permanently-pending historical orders.

`cloud_tenant_quotas` provides strongly-consistent admission accounting for each tenant:

```text
cpu_milli_limit / cpu_milli_used
memory_mib_limit / memory_mib_used
replica_limit / replica_used
```

Create and upscale mutations reserve additional quota in the **same MySQL transaction** that persists the resource/order. The quota row is locked with `SELECT ... FOR UPDATE`, so concurrent requests cannot both observe the same remaining capacity. Downscale and delete deliberately do **not** release capacity at admission time: the old workload may still exist in Kubernetes. Capacity is released only when the provider reports convergence, preventing temporary overcommit between desired-state acceptance and actual-state completion.

## 2. Reconciliation semantics

The controller loop is:

```text
API
 |
 v
MySQL desired state + order
 |
 v
conditional reconcile lease (resource_id + generation)
 |
 v
Kubernetes server-side apply / delete
 |
 v
GET Deployment actual state
 |
 +-- not converged --> persist progress, release lease, retry later
 |
 `-- converged ----> observed_generation = generation
                     order = succeeded
```

The important correctness boundary is the conditional update on the current `generation`. If a resize arrives while an older reconcile is in flight, that stale reconcile cannot advance `observed_generation` for the newer desired state.

The reconcile lease prevents two TaskFlow API replicas from concurrently issuing provider mutations for the same resource generation. Provider calls themselves are still designed to be idempotent: Kubernetes uses server-side apply and delete is safe to repeat.

Convergence is not treated as permanent. Active resources are periodically selected again after a drift interval (currently 60 seconds) and server-side apply is repeated. This makes out-of-band deletion or spec edits self-healing instead of leaving `observed_generation == generation` forever while the real cluster has drifted.

Quota convergence follows the same rule. For an upscale, the higher target is charged before the order is accepted. For a downscale, the old higher charge is retained until the new Deployment generation is actually available. For delete, all charged CPU/memory/replicas remain accounted until the Deployment is confirmed absent. A transient Kubernetes error therefore cannot accidentally free capacity for another request while the original workload may still be running.

## 3. Kubernetes provider

The current provider manages namespaced `apps/v1 Deployment` objects using the Kubernetes REST API directly. It does not rely on `kubectl` or shell execution.

Desired spec example:

```json
{
  "namespace": "default",
  "image": "nginx:1.27",
  "replicas": 3,
  "cpu": "500m",
  "memory": "256Mi",
  "container_port": 8080
}
```

Provider convergence requires Kubernetes to report its current Deployment generation as observed and `availableReplicas >= spec.replicas`. Deletion converges when the Deployment GET returns `404`.

## 4. Enable in Kubernetes

The feature is disabled by default so existing TaskFlow deployments keep their current behavior.

Set:

```yaml
CLOUD_CONTROL_ENABLED: "true"
CLOUD_CONTROL_KUBE_NAMESPACE: "default"
```

`deploy/k8s/cloud-control-rbac.yaml` creates a dedicated ServiceAccount and a namespace-scoped Role for the concrete provider resources used by the control plane: Deployments, Services and PersistentVolumeClaims. `deploy/k8s/deployment.yaml` uses that ServiceAccount. In-cluster API host, token and CA are discovered from the standard Kubernetes service environment and projected ServiceAccount files.

For a non-cluster development endpoint, the provider also accepts:

```text
CLOUD_CONTROL_KUBE_API_URL
CLOUD_CONTROL_KUBE_TOKEN
CLOUD_CONTROL_KUBE_CA_FILE
```

## 5. API walkthrough

The routes reuse TaskFlow control-plane authentication, OIDC/RBAC and audit middleware.

Configure tenant quota first:

```bash
curl -X PUT http://localhost:8080/api/v1/cloud/tenants/team-a/quota \
  -H 'Authorization: Bearer ...' \
  -H 'Content-Type: application/json' \
  -d '{
    "cpu_milli_limit":8000,
    "memory_mib_limit":16384,
    "replica_limit":20
  }'

curl -H 'Authorization: Bearer ...' \
  http://localhost:8080/api/v1/cloud/tenants/team-a/quota
```

The GET response includes both `used` and `available` values. Requests that exceed any quota dimension return HTTP `409` and are rolled back atomically; no resource/order is persisted and no partial quota charge remains.

Create:

```bash
curl -X POST http://localhost:8080/api/v1/cloud/resources \
  -H 'Authorization: Bearer ...' \
  -H 'Idempotency-Key: create-demo-001' \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant_id":"team-a",
    "name":"demo",
    "kind":"deployment",
    "provider":"kubernetes",
    "spec":{"namespace":"default","image":"nginx:1.27","replicas":2,"cpu":"250m","memory":"128Mi"}
  }'
```

Resize:

```bash
curl -X POST http://localhost:8080/api/v1/cloud/resources/1/resize \
  -H 'Authorization: Bearer ...' \
  -H 'Idempotency-Key: resize-demo-002' \
  -H 'Content-Type: application/json' \
  -d '{"spec":{"namespace":"default","image":"nginx:1.27","replicas":5,"cpu":"500m","memory":"256Mi"}}'
```

Observe resource and order history:

```bash
curl -H 'Authorization: Bearer ...' http://localhost:8080/api/v1/cloud/resources/1
curl -H 'Authorization: Bearer ...' http://localhost:8080/api/v1/cloud/resources/1/orders
```

Delete:

```bash
curl -X DELETE http://localhost:8080/api/v1/cloud/resources/1 \
  -H 'Authorization: Bearer ...' \
  -H 'Idempotency-Key: delete-demo-003'
```

## 6. Failure scenarios worth demonstrating

For interviews or release evidence, exercise these instead of only showing a happy-path CRUD demo:

1. Send the same create request twice with the same `Idempotency-Key`; verify there is one order/resource.
2. Submit resize generation N+1 and N+2 rapidly; verify N+1 becomes `superseded` and only N+2 needs to converge.
3. Run two TaskFlow API replicas; verify the conditional reconcile lease allows only one provider mutation for a resource generation.
4. Make the Kubernetes API return 500/timeout; verify the resource becomes `error`, the order records the last error, and the next controller pass retries.
5. Kill TaskFlow after server-side apply but before MySQL convergence; on restart, the provider observation should converge the existing Deployment rather than create a duplicate.
6. Delete or edit the Deployment manually while desired state remains `active`; verify periodic drift reconciliation recreates/restores it without requiring a new user order.
7. Give a tenant room for only one replica, submit two creates concurrently, and verify exactly one commits while the other receives quota-exceeded; the real-MySQL smoke test exercises this row-lock race.
8. Upscale and then immediately downscale; verify the larger quota charge is retained until the downscale actually converges, then released to the smaller target.
9. Submit delete and verify quota remains charged while the Deployment is still present, then returns to zero only after provider-confirmed deletion.

These scenarios demonstrate the control-plane property that matters most: **a user mutation is represented as durable state and can be retried or reconciled after partial failure instead of depending on one successful synchronous RPC.**

## 7. Current scope and next production steps

Implemented now:

- Resource + Order persistence in MySQL.
- Desired/actual state with generation fencing.
- Idempotency keys and superseded-order handling.
- Per-resource reconcile lease for multi-replica API deployments.
- Periodic drift reconciliation after initial convergence.
- Tenant CPU/memory/replica quota admission with `SELECT ... FOR UPDATE` serialization.
- Atomic quota reservation in the same transaction as Resource + Order persistence.
- Conservative downscale/delete accounting with release only after provider convergence.
- Real Kubernetes REST provider for Deployment apply/observe/delete.
- Background reconciliation loop plus manual reconcile endpoint.
- Existing control-plane auth/audit middleware on the new API.
- Namespace-scoped Kubernetes RBAC manifest.
- Durable multi-resource lifecycle Saga: provision, resize and delete across quota/Service/PVC/Deployment with restart recovery; resize restores the previous bundle on failure, while delete is fail-forward until resources disappear. See [`CLOUD_SAGA.md`](CLOUD_SAGA.md).

Intentionally not claimed yet:

- VM/network/block-storage providers.
- Cross-provider quota dimensions beyond the current Kubernetes Deployment CPU/memory/replica model.
- Mutable PVC resize/shrink workflow and external IaaS VM/VPC/block-volume adapters.
- A production multi-cluster provider registry.

Those are the next layers if the project is extended from a Kubernetes resource control plane toward a complete private-cloud portal backend.

## 8. Reproducible quota evidence

The normal unit suite tests quota quantity calculation without external services. A separate integration smoke uses a **real MySQL 8.4** instance to verify transaction and row-lock semantics:

```bash
make cloud-control-quota-smoke
```

The smoke covers:

- initial quota creation and usage accounting;
- create admission and over-quota rollback;
- upscale reservation;
- downscale delayed release;
- refusing to lower a tenant limit below currently charged usage;
- delete delayed release;
- two concurrent creates racing for one remaining replica, with exactly one admitted.
