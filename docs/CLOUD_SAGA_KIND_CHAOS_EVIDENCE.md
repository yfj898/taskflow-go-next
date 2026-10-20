# Cloud Lifecycle Saga — Real Kind Chaos Evidence

Frozen validation date: **2026-09-22**.

This evidence extends `CLOUD_SAGA_KIND_E2E_EVIDENCE.md` from normal multi-replica convergence into two failure windows that are difficult to validate with unit tests alone:

1. the workflow owner Pod dies **after** Kubernetes has accepted an external side effect but **before** the Saga step is committed to MySQL;
2. Kubernetes has accepted the side effect, but the provider returns an error to the coordinator, creating an explicit **outcome-unknown** condition.

The reproducible target is:

```bash
make cloud-control-kind-chaos
```

The target first runs the ordinary real-Kind E2E baseline, then uses a separate Kind cluster for fault injection. Fault injection is opt-in through `CLOUD_SAGA_FAULT_INJECTION_ENABLED=true`; normal deployments keep it disabled.

## Environment

- Kind / Kubernetes: `v1.35.0`
- TaskFlow replicas: `2`
- MySQL: `8.4`
- Redis: `8-alpine`
- real Kubernetes provider objects: `Service`, `Deployment`
- test workflow lease: `10s`
- injected post-side-effect delay: `20s`

The shorter lease and artificial delay are test controls only. They make the crash window deterministic without changing the production default lease (`30s`).

## 1. Crash after external side effect

The test creates a Provision workflow whose first external step is Network/Service. One specific TaskFlow Pod owns the manual reconcile request.

```text
Pod A claims workflow
        |
        v
network step -> running
        |
        v
Kubernetes Service is successfully created
        |
        v
fault wrapper delays return for 20s
        |
        +---- Pod B reconcile -> HTTP 409 Busy
        |
        v
Pod A is force deleted
```

The HTTP request attached to Pod A ended with code `000` / empty reply because the process was killed. Before the kill, the competing Pod returned `HTTP 409 Busy`.

After the owner died, MySQL retained the durable workflow and `network=running` state until the workflow lease expired. The surviving replica then reconciled the same Network step again. Server-Side Apply was idempotent, so the Service was not replaced. Its Kubernetes UID before and after takeover was identical:

```text
dc5e44e3-a4ca-48a7-801b-56a0b8bd21f8
```

The Network step's attempt count advanced to `2`, proving that the new replica actually re-executed the persisted step rather than silently marking it complete. The Provision workflow then continued to Compute/Commit and succeeded; its cleanup Delete workflow also succeeded.

## 2. Explicit outcome-unknown after successful API side effect

The second test removes timing ambiguity. The fault wrapper delegates to the real Kubernetes provider first, waits for the Service apply to succeed, and only then returns a synthetic error:

```text
EnsureNetwork -> Kubernetes success
        |
        v
Service exists in the real cluster
        |
        v
synthetic error returned to Saga
```

Immediately after that error, workflow `8` persisted:

```text
workflow.status = compensating
order.status    = running
network.status  = failed
network.attempt_count = 1
network.last_error = "injected outcome-unknown after successful network side effect"
```

The important detail is that a `failed` forward step is included in compensation. The coordinator therefore does **not** assume `error == no side effect`.

After compensation:

```text
workflow.status = compensated
order.status    = failed
resource.actual_state = deleted

network.status = compensated
network.compensation_attempt_count = 1

Service: absent
Deployment: absent
tenant quota: cpu=0 memory=0 replicas=0
```

## 3. Metrics and operability

The injected provider error enters the same bounded production observability path:

```text
taskflow_cloud_saga_step_operations_total{
  kind="provision_bundle",
  step="network",
  direction="forward",
  outcome="error"
}
```

Fault injection does not add tenant/resource identifiers to metric labels.

## 4. Fault-injection boundary

The fault wrapper is deliberately narrow and disabled by default. It supports only reproducible E2E behavior after a successful `EnsureNetwork`, `EnsureStorage`, or `EnsureCompute` call:

```text
CLOUD_SAGA_FAULT_INJECTION_ENABLED=true
CLOUD_SAGA_FAULT_AFTER_SUCCESS_STEP=network|storage|compute
CLOUD_SAGA_FAULT_AFTER_SUCCESS_DELAY_MS=<milliseconds>
CLOUD_SAGA_FAULT_AFTER_SUCCESS_ERROR=true|false
```

It is not a production retry mechanism and is not used when the enable flag is false. The normal Kubernetes provider remains unchanged.

## 5. Frozen result

The final run exited with:

```text
CLOUD_CONTROL_KIND_CHAOS_OK
crash-after-side-effect: busy-before-kill=409 service_uid_preserved=dc5e44e3-a4ca-48a7-801b-56a0b8bd21f8
lease-expiry takeover: surviving replica resumed workflow after owner pod death
outcome-unknown: external Service existed before synthetic error and failed step was compensated
final quota: cpu=0 memory=0 replicas=0
```

Together with the normal Kind E2E, this provides cluster-level evidence for both healthy multi-replica convergence and crash/outcome-unknown recovery.
