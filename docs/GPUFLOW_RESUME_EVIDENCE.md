# GPUFlow resume evidence

This file records only measurements that were actually reproduced locally. Do not convert scheduler simulation numbers into GPU training throughput claims.

Raw three-run benchmark data is committed at `docs/gpuflow-benchmark-final-20260914.json`.
Raw FairQueue data is committed at `docs/gpuflow-fairness-final-20260914.json`.

Validated on 2026-09-14 in an isolated Linux container using Go 1.26.8.

## Correctness gate

- `go test ./internal/gpuflow`: **PASS**
- `go vet ./internal/gpuflow ./cmd/gpuflow-server ./cmd/gpuflow-bench ./cmd/gpuflow-fairness-bench`: **PASS**
- `go build ./cmd/gpuflow-server ./cmd/gpuflow-bench ./cmd/gpuflow-fairness-bench`: **PASS**
- `gofmt -d internal/gpuflow cmd/gpuflow-server cmd/gpuflow-bench cmd/gpuflow-fairness-bench`: **clean**
- KubeRay example parsed successfully as `ray.io/v1 RayJob` using PyYAML 6.0.1.

## Control-plane API smoke

The standalone `cmd/gpuflow-server` exposes schedule/release/snapshot/RayJob endpoints plus Prometheus text metrics. A local smoke run used two 8-GPU A100 nodes and one 4-GPU L4 node:

- admitted an 8-GPU low-priority checkpointable job;
- admitted a second 4-GPU low-priority job;
- admitted an urgent 8-GPU job and preempted the first low-priority job;
- generated a `ray.io/v1 RayJob` with 2 GPU workers;
- exported `gpuflow_schedule_*`, `gpuflow_preemptions_total`, per-node GPU usage and per-tenant GPU usage metrics.

Observed metrics after the smoke path included `gpuflow_schedule_accepted_total 3` and `gpuflow_preemptions_total 1`.

The final FairQueue HTTP smoke additionally validated a complete checkpoint/recovery loop:

1. a low-priority checkpointable job was dispatched;
2. a higher-priority urgent job preempted it;
3. the preempted victim was automatically returned to the pending queue;
4. releasing the urgent job triggered automatic redispatch of the low-priority job.

Final observable state included:

- `gpuflow_preemptions_total 1`
- `gpuflow_queue_requeued_total 1`
- `gpuflow_queue_pending 0`

This is a control-plane recovery semantic test. It does not claim that a physical training process checkpoint was restored on a GPU cluster.

## Scheduler benchmark

Command (repeated three times against the final code):

```bash
go run ./cmd/gpuflow-bench -jobs 100000 -nodes 64
```

Synthetic topology and workload:

- 64 GPU nodes, 8 GPUs per node.
- Mixed A100/L4 accelerator classes.
- Four tenants with GPU quotas.
- 100,000 mixed jobs with 1-4 gang replicas.
- Priority mix includes urgent jobs that may preempt lower-priority jobs only when the victim is both preemptible and checkpointable.

Measured three-run result:

| Metric | Result |
| --- | ---: |
| Scheduling decisions per run | 100,000 |
| Median elapsed | 17,774.465 ms |
| Median control-plane throughput | **5,626.05 decisions/s** |
| Throughput CV | **3.55%** |
| Median successfully scheduled | 44,972 |
| Median capacity rejected | 54,266 |
| Median quota rejected | 762 |
| Median checkpoint-safe preemptions | **17,953** |
| Peak concurrent running jobs | 301 |

The benchmark intentionally drives the synthetic cluster into saturation, so rejection is expected and is part of the scheduler decision workload. These are scheduler/control-plane measurements, not GPU kernel, token-generation or model-training throughput.

## Multi-tenant FairQueue benchmark

The pending-queue layer adds policy that is deliberately separate from gang placement:

- per-tenant weights;
- base priority plus bounded waiting-time aging;
- maximum consecutive dispatches from one tenant;
- quota/capacity-blocked candidate skipping to avoid global head-of-line blocking;
- checkpoint-safe preemption-victim requeue;
- pending, dispatch, requeue, wait and Jain-fairness metrics.

Benchmark command:

```bash
go run ./cmd/gpuflow-fairness-bench -dispatches 1200
```

Workload:

- tenants `team-a`, `team-b`, `team-c`;
- weights `1 : 1 : 2`;
- 1,200 successful dispatch opportunities per run;
- expected long-run share `25% : 25% : 50%`.

Three reproduced runs:

| Run | team-a | team-b | team-c | Weighted Jain fairness | Decision rate |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 300 | 300 | 600 | **1.000** | 1,258.78 decisions/s |
| 2 | 300 | 300 | 600 | **1.000** | 1,276.32 decisions/s |
| 3 | 300 | 300 | 600 | **1.000** | 1,256.06 decisions/s |

Summary:

| Metric | Result |
| --- | ---: |
| Observed tenant share | **25% / 25% / 50%** |
| Normalized Jain fairness | **1.000** |
| Median FairQueue decision rate | **1,258.78 decisions/s** |
| Throughput CV | **0.87%** |

The queue benchmark uses synthetic control-plane work. Queue waiting-time values generated from its logical benchmark clock are intentionally not used as real-system latency claims.

## Resume-safe wording template

> Extended a production-oriented Go task platform into a GPU workload control plane with accelerator-aware multi-resource placement, atomic gang scheduling, per-tenant quota, checkpoint-safe preemption and a weighted FairQueue with aging/requeue semantics. The placement core sustained a median **5,626 decisions/s** across three **100,000-job / 64-node** synthetic runs; a separate `1:1:2` multi-tenant queue benchmark reproduced the exact **25% / 25% / 50%** target share with normalized **Jain fairness = 1.000**, while an HTTP smoke verified preempted checkpoint-safe jobs are automatically requeued and redispatched after capacity recovery.
