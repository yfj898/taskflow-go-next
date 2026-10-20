# TaskFlow performance report

Date: 2026-09-04

## Goal

Measure the real task-creation hot path under load, use `pprof` to identify CPU/allocation hotspots, optimize only evidence-backed bottlenecks, and repeat the benchmark under the same conditions.

## 2026-09-07 post-Phase-21 release verification

The final release-performance capture after the Phase 20-21 archive lifecycle,
replication and real MinIO/S3 work completed with five fresh-stack runs on the
same i9-9900K host and the same 100-client / 15-second / `ACCESS_LOG=false`
method as the 2026-09-06 baseline.

- Median throughput: **5,781.98 RPS**.
- Median P95: **26.816 ms**.
- Median P99: **36.290 ms**.
- Total measured responses: **427,554 HTTP 201**.
- Transport errors: **0**.
- Throughput CV: **3.65%**; the run set was noisier than the previous baseline.

The median shows no performance regression, but the host started this capture
at a substantially higher load average than the earlier baseline, so the small
median improvement is not presented as a controlled optimization gain. The
full comparison, stability caveat and current-vs-baseline pprof analysis are in
[`PERFORMANCE_RELEASE_2026-09-07.md`](PERFORMANCE_RELEASE_2026-09-07.md), with
raw evidence under `perf/post-phase21-fixed-hardware-20260907/`.

## 2026-09-06 fixed-hardware stability baseline

The repeated-run verification was captured before the Phase 12 autoscaling
work. Five independent fresh-stack runs on the same i9-9900K host used 100
concurrent keep-alive clients for 15 seconds each with `ACCESS_LOG=false`.

- Median throughput: **5,698.62 RPS**, CV **1.06%**.
- Median P95: **27.694 ms**, CV **1.19%**.
- Median P99: **38.070 ms**, CV **3.24%**.
- Total measured responses: **428,418 HTTP 201**.
- Transport errors: **0**.

The detailed report is
[`PERFORMANCE_STABILITY_2026-09-06.md`](PERFORMANCE_STABILITY_2026-09-06.md),
with the generated summary, environment capture and all five sets of pprof
artifacts under `perf/phase11-fixed-hardware-20260906/`.

The `phase11-` directory prefix is a historical artifact name from before the
runtime-governance work was assigned the Phase 11 number; it is retained so
existing evidence links remain stable.

The five-run throughput/P95/P99 medians are all within 1% of the 2026-09-04
single optimized sample below. The multi-run median is now the preferred current
stability baseline; the older before/after table remains the optimization-delta
evidence.

The benchmark keeps the production-oriented path enabled:

- Gin REST API
- MySQL task persistence
- Transactional Outbox in the same MySQL transaction
- Redis Streams queue
- goroutine consumer pool
- gRPC executor worker
- Prometheus middleware
- OpenTelemetry context propagation

## 2026-09-04 before/after optimization verification

The controlled run on 2026-09-04 is the primary before/after optimization data
set. It used a fresh isolated Docker stack, 100 concurrent keep-alive clients, a
1 second warm-up and a 15 second measured window. All requests completed with
HTTP 201 and there were no transport errors.

| Metric | Baseline | Optimized | Change |
| --- | ---: | ---: | ---: |
| Requests / 15s | 64,284 | 84,752 | +31.8% |
| Throughput | 4,280.05 RPS | 5,643.98 RPS | **+31.9%** |
| Average latency | 23.342 ms | 17.702 ms | **-24.2%** |
| P50 latency | 21.524 ms | 16.733 ms | **-22.3%** |
| P95 latency | 38.903 ms | 27.881 ms | **-28.3%** |
| P99 latency | 57.523 ms | 38.253 ms | **-33.5%** |
| Max latency | 165.250 ms | 97.201 ms | **-41.2%** |
| HTTP errors | 0 | 0 | unchanged |
| API CPU sample | 161.77% | 178.96% | +10.6% |
| MySQL CPU sample | 314.87% | 376.55% | +19.6% |

The higher absolute CPU samples in the optimized run are expected because the
service completes 31.9% more requests in the same wall-clock interval. After
normalizing by throughput, requests per sampled API CPU percentage improved by
about 19.2%, and requests per sampled MySQL CPU percentage improved by about
10.3%.

The baseline 10 second CPU profile collected 14.10 CPU-seconds of samples. The
strongest evidence was database and syscall pressure rather than executor or
Redis pressure:

- `internal/runtime/syscall/linux.Syscall6`: 39.65% flat CPU.
- `TaskRepository.CreateWithOutbox`: 31.84% cumulative CPU.
- `database/sql.(*DB).retry`: 24.11% cumulative CPU.
- `database/sql.withLock`: 17.02% cumulative CPU.
- the baseline API emitted 64,799 log lines during the measured run.

The final optimized path combines prepared hot-path SQL, batched Transactional
Outbox claim/publish/settle, reduced redundant implicit transactions and
optional synchronous access logging. The transaction that atomically creates
the task and its Outbox event is retained.

## Current verification method

- Fresh MySQL, Redis, API and executor containers for each measured run.
- Each case is destroyed before the next case so queued work cannot steal CPU or disk I/O from later measurements.
- 4 keep-alive clients for a 1 second warm-up.
- 100 concurrent HTTP keep-alive clients for 15 seconds.
- `POST /api/v1/tasks` with a small `compute` task payload.
- API rate limit raised to 10,000 RPS / 20,000 burst so the limiter is not the benchmark ceiling.
- 32 queue consumers.
- 10 second CPU profile captured during the measured window.
- Heap profile captured after the run with `gc=1`.
- Container CPU/memory sampled during the load window.

This is a single-host Docker benchmark. It is useful for before/after engineering decisions; it is not a claim about production cluster capacity.

## Baseline pprof findings

The baseline implementation used two GORM `Create` calls inside the task + outbox transaction and recorded OpenTelemetry spans even when no exporter was configured.

CPU profile:

- `TaskRepository.CreateWithOutbox`: 48.19% cumulative CPU.
- GORM create processor: 36.48% cumulative CPU.
- MySQL/socket syscalls dominated flat CPU.
- Repeated MySQL statement prepare/execute/close appeared on the request path.

Allocation profile:

- 1,146.12 MiB cumulative allocations during the measured process lifetime.
- GORM `Statement.clone`: 175.57 MiB flat.
- GORM `Statement.AddClause`: 99.57 MiB flat.
- GORM `ConvertToCreateValues`: 79.55 MiB flat.

## Optimizations

### 1. Remove GORM reflection from the hottest insert path

`CreateWithOutbox` now uses `database/sql` for the two INSERT statements while preserving one explicit MySQL transaction. Task creation and outbox creation remain atomic.

GORM is still used for migrations and the less frequent query/update paths; the optimization is intentionally limited to the profiled hotspot.

### 2. Do not record spans when there is no exporter

Previously `OTEL_LOG_SPANS=false` still configured `AlwaysSample`, so every request allocated a recording span that was never exported.

The default no-exporter mode now uses `NeverSample`. Trace context propagation remains enabled. Turning the log exporter on restores full recording.

### 3. Cache hot-path prepared statements

The first raw-SQL profile still showed:

- `PrepareContext`: 9.24% cumulative CPU.
- statement close: 7.77% cumulative CPU.

The repository now prepares the task INSERT and outbox INSERT once per SQL connection pool and uses `tx.StmtContext` inside each transaction.

After the change:

- `PrepareContext`: 0.59% cumulative CPU.
- statement close: 0.75% cumulative CPU.

The database transaction boundary and Transactional Outbox semantics are unchanged.

## Earlier 30-second tuning run (historical)

The following result came from an earlier 30-second tuning pass on the same
project. It is retained as engineering history, but should not be mixed with the
current 15-second verification numbers above when writing a resume or quoting a
single performance result.

That historical pass used a 2-second warm-up, a 30-second measured window and a
20-second CPU profile. The current `make perf` defaults intentionally reproduce
the 15-second primary verification method instead.

| Metric | Baseline | Optimized | Change |
| --- | ---: | ---: | ---: |
| Requests / 30s | 45,850 | 87,345 | +90.5% |
| Throughput | 1,525.49 RPS | 2,907.15 RPS | **+90.6%** |
| Average latency | 65.479 ms | 34.375 ms | **-47.5%** |
| P50 latency | 60.027 ms | 24.417 ms | **-59.3%** |
| P95 latency | 107.173 ms | 86.606 ms | **-19.2%** |
| P99 latency | 170.071 ms | 137.315 ms | **-19.3%** |
| Max latency | 470.246 ms | 435.155 ms | -7.5% |
| HTTP errors | 0 | 0 | unchanged |
| App memory sample | 30.61 MiB | 31.64 MiB | +3.4% |
| Cumulative alloc space | 1,146.12 MiB | 916.57 MiB | -20.0% total while processing 90.5% more requests |
| Alloc / completed request | ~25.60 KiB | ~10.75 KiB | **-58.0%** |

The optimized run consumes more absolute MySQL and API CPU because it completes nearly twice as many requests in the same 30 second window. The useful efficiency signal is throughput and latency at zero error rate, plus allocation per completed request.

## Earlier parameter interpolation experiment

An earlier version tested `interpolateParams=true` before the hot path was
redesigned. That isolated experiment regressed and was not treated as evidence
of an optimization. The current code may use parameter interpolation for
non-prepared driver calls, but the measured improvement must not be attributed
to that flag by itself; the material changes were the prepared task/outbox
inserts and batched Outbox processing.

Keeping rejected experiments in the report matters: the final numbers are the result of profiling and measurement, not a list of theoretically faster switches.

## Reproduce

Build the current image:

```bash
docker build --build-arg GOPROXY=https://goproxy.cn,direct -t taskflow-go:perf .
```

Run a fresh benchmark stack:

```bash
make perf
```

Results and profiles default to `/tmp/taskflow-perf/`.

For repeated-run stability measurement, build once and execute the same isolated
case multiple times:

```bash
RUNS=5 make perf-multi
```

The multi-run harness writes every run under `run-N/`, records host/Docker and
benchmark image details in `environment.txt`, and emits `summary.json` plus
`summary.md` with the median, sample variance, standard deviation and
coefficient of variation for RPS and latency metrics. The fixed-hardware
five-run result was captured on 2026-09-06 and is documented above; it does not
retroactively convert the 2026-09-04 before/after experiment into a multi-run
comparison.

Inspect the CPU profile with the exact TaskFlow binary used for the run, for example:

```bash
go tool pprof -top ./taskflow /tmp/taskflow-perf/cpu.pprof
```

Useful follow-up views:

```bash
go tool pprof -top -cum ./taskflow /tmp/taskflow-perf/cpu.pprof
go tool pprof -top -alloc_space ./taskflow /tmp/taskflow-perf/heap.pprof
```

## Interview narrative

1. Pressure testing showed the create endpoint was database-bound rather than goroutine-bound.
2. CPU and allocation profiles pointed to GORM reflection/callback construction on the two-write Transactional Outbox path.
3. Replacing only that hot path with raw `database/sql` preserved transaction semantics while reducing allocation pressure.
4. A second profile exposed repeated statement preparation, so the two INSERT statements were cached and reused inside transactions.
5. OpenTelemetry was also recording spans with no exporter, so default sampling was changed to avoid wasted allocations.
6. In the latest controlled verification run, throughput increased from 4.28k
   to 5.64k RPS (+31.9%), P95 fell from 38.9ms to 27.9ms (-28.3%), and P99 fell
   from 57.5ms to 38.3ms (-33.5%), with zero HTTP errors in both runs.

