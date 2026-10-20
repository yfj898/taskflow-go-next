# GPUFlow: AI workload scheduling extension

GPUFlow is the AI-infrastructure scheduling layer built on top of TaskFlow's existing reliable task execution, SLO, fair scheduling, backpressure, Kubernetes/KEDA and observability foundation.

## What is implemented

- Multi-resource placement using GPU, CPU and memory vectors.
- GPU-class constraints, e.g. A100 jobs never land on L4 nodes.
- Atomic gang placement for multi-replica training/inference jobs.
- Per-tenant GPU quota enforcement before placement.
- Priority-aware preemption, restricted to victims that are both preemptible and checkpointable.
- Best-fit GPU packing with deterministic node ordering.
- Explicit release accounting for capacity and tenant quota recovery.
- Multi-tenant pending queue with weighted fair share, priority aging and a maximum consecutive-dispatch guard.
- Queue scan skips temporarily quota/capacity-blocked tenants instead of head-of-line blocking all work.
- Checkpoint-safe preemption victims are automatically returned to the pending queue and can resume after capacity is released.
- Queue observability includes pending depth, per-tenant dispatch counts, requeue count, queue wait and normalized Jain fairness.
- KubeRay `RayJob` deployment example with GPU resources and accelerator node selection.
- Synthetic scheduler benchmark (`cmd/gpuflow-bench`) for reproducible control-plane throughput measurements.
- Weighted-fairness benchmark (`cmd/gpuflow-fairness-bench`) for reproducible multi-tenant share measurements.

## Scheduling contract

Each job declares:

```text
tenant + priority + GPU type
replicas × (GPU + CPU + memory)
preemptible + checkpointable
```

Scheduling runs in four gates:

```text
validate request
  -> enforce tenant GPU quota
  -> atomic gang best-fit placement
  -> if needed, checkpoint-safe lower-priority preemption
```

The scheduler never partially commits a gang job. If all replicas cannot be placed, the tentative placement is discarded. Preemption is only considered for strictly lower-priority jobs and only when the victim is marked both `preemptible` and `checkpointable`.

Pending jobs are managed by a separate `FairQueue` layer:

```text
enqueue
  -> effective priority = base priority + bounded aging boost
  -> weighted fair-share comparison across tenants
  -> max-consecutive-tenant guard
  -> skip quota/capacity-blocked candidate
  -> Scheduler.Schedule()
  -> if a checkpoint-safe victim is preempted, requeue victim
```

This separation is intentional: the placement core answers "can this gang fit, and where?" while the queue answers "which tenant/job should get the next scheduling opportunity?". It keeps placement correctness independent from queue policy and makes both layers benchmarkable in isolation.

## Fairness and recovery evidence

The final weighted-fairness benchmark used tenant weights `1:1:2` and 1,200 dispatches per run. All three reproduced runs dispatched exactly `300 / 300 / 600` jobs, corresponding to `25% / 25% / 50%` share and a weight-normalized **Jain fairness index of 1.000**. The median FairQueue decision rate was **1,258.78 decisions/s** with **0.87% CV**.

The FairQueue benchmark is intentionally reported separately from the raw placement benchmark. The placement-only core reached a median **5,626.05 decisions/s** on the 100,000-job / 64-node workload, while the FairQueue layer performs additional sorting, aging and weighted-share decisions.

An HTTP recovery smoke also validated the preemption lifecycle: a low-priority checkpointable job was running, an urgent job preempted it, the victim was automatically returned to the pending queue, and releasing the urgent job caused the victim to be dispatched again. Observed metrics included `gpuflow_preemptions_total 1`, `gpuflow_queue_requeued_total 1`, and final `gpuflow_queue_pending 0`.

## Why this is an AI Infra project

The original TaskFlow already provides durable asynchronous execution, retry/reconcile/DLQ, SLO classes, distributed tenant concurrency, fair scheduling, KEDA, Prometheus/Grafana, MinIO/S3 checkpoint-style object storage and failure recovery. GPUFlow adds the missing AI-compute control-plane semantics: accelerator-aware placement, gang scheduling, quota and checkpoint-safe preemption.

The included RayJob is an integration boundary rather than a claim that a physical multi-GPU KubeRay cluster was available during local validation. Real cluster claims should only be added after a GPU Kubernetes environment is exercised.

Likewise, the scheduler and FairQueue throughput figures are **control-plane synthetic benchmarks**, not model-training throughput, GPU utilization measurements or physical-cluster scaling results.
