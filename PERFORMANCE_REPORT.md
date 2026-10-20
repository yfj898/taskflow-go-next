# TaskFlow Performance Report

Date: 2026-09-04

This report records the local performance investigation used to tune TaskFlow. The goal is not to claim a synthetic maximum QPS number; it is to show a reproducible profiling loop: load -> pprof -> identify a bottleneck -> change one thing -> rerun -> verify that the asynchronous pipeline still converges.

## Environment

- Host: 16 logical CPUs, ~62 GiB RAM
- Go toolchain: 1.26.8
- MySQL: 8.4
- Redis: 7.4
- Transport: Redis Streams
- Internal execution: gRPC executor-worker
- Task type: `compute`, simulated executor latency ~150 ms/task
- Load generator: 100 concurrent HTTP clients using persistent sessions
- API: `POST /api/v1/tasks`
- Error rate in the reported runs: 0%

All benchmark services ran in an isolated Docker network. MySQL/Redis data was reset between A/B runs unless the run explicitly measured drain behavior.

## 1. pprof: task creation hot path

The first CPU profile showed that request processing was spending a large amount of time inside GORM callbacks on the hottest write path.

### Before

100 VU, 12 s, 32 consumer workers:

| Metric | Baseline |
| --- | ---: |
| Throughput | 769.3 req/s |
| Mean | 128.1 ms |
| P50 | 112.6 ms |
| P95 | 265.7 ms |
| P99 | 364.8 ms |
| Errors | 0 |

CPU pprof included:

```text
gorm.io/gorm.(*processor).Execute   cumulative ~39.8%
database/sql.withLock               cumulative ~16.3%
```

### Change

`CreateWithOutbox` still uses one MySQL transaction, but the two inserts on every request now use prepared/raw `database/sql` statements instead of two reflection-heavy GORM `Create` callback chains.

Atomicity did not change:

```text
BEGIN
  INSERT tasks
  INSERT outbox_events
COMMIT
```

### After the hot-path change

Same 100-VU profiling workload:

| Metric | Raw SQL hot path | Change vs baseline |
| --- | ---: | ---: |
| Throughput | 1068.3 req/s | +38.9% |
| Mean | 91.6 ms | -28.4% |
| P50 | 77.7 ms | -31.1% |
| P95 | 204.0 ms | -23.2% |
| P99 | 290.6 ms | -20.3% |
| Errors | 0 | unchanged |

In the post-change CPU profile, `gorm.(*processor).Execute` disappeared from the top-20 CPU nodes. The dominant cost moved to syscall / MySQL network I/O and runtime scheduling.

This run is useful as profiling evidence. Because local load tests have run-to-run variance, the resume headline should use the end-to-end Outbox result below rather than presenting +38.9% as a universal production guarantee.

## 2. Correctness of the performance metric: Outbox backlog

Higher API QPS alone was misleading. The original Outbox dispatcher processed one event at a time:

```text
claim one event in a transaction
-> publish one broker message
-> update one event as published
-> repeat
```

Under load the API could accept requests faster than the durable publish loop could move them to Redis Streams.

With the pre-batch implementation, 100 VU for 10 s with 128 workers produced:

```text
created tasks             16,606
success at load end          833
pending/running           15,773
unpublished outbox        15,832
```

After another 10 seconds with no new requests:

```text
success                    1,841
pending/running           14,765
unpublished outbox        14,807
```

That means the old Outbox path was publishing only about 102.5 events/s and completing about 100.8 tasks/s while draining. A high HTTP ingest number here was mostly deferred work, not end-to-end capacity.

## 3. Outbox batching optimization

The dispatcher was changed to operate on batches of up to 100 events:

```text
SELECT batch FOR UPDATE SKIP LOCKED
-> one batch lease update
-> Redis pipeline / Kafka WriteMessages batch
-> one batch mark-published update
```

Failure semantics remain at-least-once. If a batch publish is partially successful but settlement fails, the entire batch can be delivered again; task execution is still protected by the database `pending -> running` conditional claim.

### Result

Final controlled run, 100 VU, 10 s, 128 workers:

| Metric | Final batch version |
| --- | ---: |
| Requests created | 14,612 |
| Throughput | 1455.2 req/s |
| Mean | 68.3 ms |
| P50 | 59.1 ms |
| P95 | 144.4 ms |
| P99 | 219.6 ms |
| HTTP errors | 0 |
| Unpublished outbox at load end | 0 |

State exactly at the end of the 10-second load:

```text
success   7,454
running     127
pending   7,030
outbox        0
```

The remaining queue backlog was execution capacity, not an Outbox reliability bottleneck. It drained completely in 9.769 seconds:

```text
success            14,612
failed                  0
pending/running          0
unpublished outbox       0
```

Drain throughput after the request burst was about 732.7 task/s, compared with about 100.8 task/s before Outbox batching: approximately **7.3x higher**.

End-to-end effective throughput across request generation plus complete drain was about **737.6 task/s** for this local benchmark.

## 4. Worker concurrency tuning

The executor intentionally sleeps about 150 ms for `compute` tasks, so pure service-time math suggests:

```text
required concurrency ~= arrival_rate * service_time
```

However, increasing workers also increases MySQL claim/transition traffic. Tests confirmed that worker count must be tuned rather than maximized blindly.

- 32 workers: API stayed fast, but execution backlog grew quickly; theoretical executor capacity is only ~213 task/s before database overhead.
- 128 workers: best balance in the measured runs; 1455 req/s ingest with P95 144 ms and full drain in 9.769 s.
- 192 workers: backlog drained very quickly, but API throughput/latency degraded because worker state updates contended with request/outbox writes in MySQL.

The project therefore keeps worker count configurable and includes queue-backlog metrics plus a KEDA example instead of hard-coding the benchmark's 128-worker value as a universal default.

## 5. Post-optimization pprof observations

After the hot-path and Outbox changes, CPU profiles were dominated by syscall / MySQL network I/O rather than application computation. During a pure 128-worker drain, 5,345 pending tasks reached zero within the 5-second profiling window; Go CPU samples were low and mostly in database/network syscalls.

The goroutine profile during load was also consistent with I/O-bound behavior: many goroutines were waiting in `database/sql` transaction and MySQL connection paths rather than burning CPU in application loops.

This is the stopping point for local code-level optimization. Further gains should target database architecture/capacity (separate service pools, replicas/sharding where appropriate), executor horizontal scaling, and production-like network/storage instead of micro-optimizing Go instructions.

## 6. Access logging experiment

Synchronous Gin access logging was made configurable with `ACCESS_LOG`. Production Docker/Kubernetes examples disable per-request access logs while retaining Prometheus metrics, request IDs and tracing.

The local A/B result for logging was noisy and did not show a stable throughput win, so **no percentage improvement from disabling access logs should be quoted**. The option remains useful for reducing log volume and avoiding stdout backpressure at high request rates.

## Resume-safe summary

A defensible resume bullet based on the measured experiment is:

> Used pprof to identify ORM callback overhead on the task-creation hot path and replaced reflection-heavy writes with transactional prepared SQL; redesigned the Transactional Outbox from per-event round trips to batched claim + Redis Pipeline + batch settlement. In a local 100-concurrency benchmark, eliminated ~15k unpublished Outbox backlog and increased post-burst task drain throughput from ~101 task/s to ~733 task/s (~7.3x), with 0 request errors.

Do not present the local 1455 req/s figure as a production SLA. It is a reproducible development benchmark on the hardware/configuration listed above.
