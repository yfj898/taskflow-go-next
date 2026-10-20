# TaskFlow performance summary

Date: 2026-09-04

This directory is the stable, Git-friendly summary of the real profiling and
load-testing work. The canonical detailed reports remain:

- `../docs/PERFORMANCE_REPORT.md` — request hot-path pprof and controlled A/B.
- `../PERFORMANCE_REPORT.md` — Transactional Outbox batching and drain capacity.

Binary CPU/heap profiles are generated under `/tmp` by the benchmark harness
instead of being committed to Git.

## Primary controlled A/B

Fresh isolated Docker stacks, 100 persistent keep-alive clients, 1 second
warm-up, 15 second measured window, MySQL 8.4, Redis 7.4 and gRPC executor.
All requests returned HTTP 201 with no transport errors.

| Metric | Baseline | Optimized | Change |
| --- | ---: | ---: | ---: |
| Requests / 15s | 64,284 | 84,752 | +31.8% |
| Throughput | 4,280.05 RPS | 5,643.98 RPS | **+31.9%** |
| Average latency | 23.342 ms | 17.702 ms | **-24.2%** |
| P50 | 21.524 ms | 16.733 ms | **-22.3%** |
| P95 | 38.903 ms | 27.881 ms | **-28.3%** |
| P99 | 57.523 ms | 38.253 ms | **-33.5%** |
| Max | 165.250 ms | 97.201 ms | **-41.2%** |
| HTTP errors | 0 | 0 | unchanged |
| API CPU sample | 161.77% | 178.96% | +10.6% absolute |
| MySQL CPU sample | 314.87% | 376.55% | +19.6% absolute |

Absolute CPU rises because the optimized service completes 31.9% more work in
the same wall-clock window. Normalized efficiency improved by about 19.2% in
requests per sampled API CPU percentage and about 10.3% for MySQL.

## pprof evidence

The baseline 10 second CPU profile collected 14.10 CPU-seconds. The dominant
cost was the request/database path, not executor computation:

- Linux `Syscall6`: 39.65% flat CPU.
- `TaskRepository.CreateWithOutbox`: 31.84% cumulative CPU in the primary run.
- `database/sql.(*DB).retry`: 24.11% cumulative CPU.
- `database/sql.withLock`: 17.02% cumulative CPU.
- the baseline emitted 64,799 synchronous access-log lines.

An earlier focused profile before replacing the hottest GORM writes showed:

- `TaskRepository.CreateWithOutbox`: 48.19% cumulative CPU.
- GORM create processor: 36.48% cumulative CPU.
- 1,146.12 MiB cumulative allocation space.
- `gorm.Statement.clone`: 175.57 MiB flat allocation.
- `gorm.Statement.AddClause`: 99.57 MiB flat allocation.
- `ConvertToCreateValues`: 79.55 MiB flat allocation.

After moving only the task+outbox hot inserts to prepared `database/sql`, the
next profile showed repeated prepare/close overhead. Caching those statements
reduced:

- `PrepareContext`: 9.24% -> 0.59% cumulative CPU.
- statement close: 7.77% -> 0.75% cumulative CPU.

The explicit transaction that atomically creates the task and Transactional
Outbox row remains intact.

## Outbox end-to-end result

HTTP ingest alone was not treated as the final performance metric. Before
batching, the single-event Outbox loop left 15,832 unpublished events after a
100-concurrency burst and drained only about 100.8 completed tasks/s.

The dispatcher was changed to:

```text
SELECT batch FOR UPDATE SKIP LOCKED
-> one batch lease update
-> Redis pipeline / Kafka WriteMessages batch
-> one batch settlement update
```

With batches up to 100 events, the final controlled 100-concurrency run had:

- 14,612 requests created in 10 seconds.
- 1,455.2 req/s ingest.
- 0 unpublished Outbox events at load end.
- all 14,612 tasks eventually succeeded.
- post-burst drain throughput about 732.7 task/s.
- previous drain throughput about 100.8 task/s.
- **~7.3x faster drain** with the same at-least-once/idempotent semantics.

## Optimizations kept

1. Prepared raw `database/sql` only on the profiled task + outbox INSERT path.
2. Batched Outbox claim/publish/settle operations.
3. `NeverSample` when no OpenTelemetry exporter is enabled; trace propagation
   still remains enabled.
4. Cached prepared task/outbox INSERT statements.
5. Configurable synchronous Gin access logging. No standalone performance
   percentage is attributed to disabling logs because the isolated logging A/B
   was noisy.
6. Queue backlog and database/sql pool metrics so later tuning is evidence-based.

## Reproduce

Primary benchmark:

```bash
make perf
```

Explicit configuration A/B harness:

```bash
make perf-ab
```

`make perf-ab` compares runtime configuration on the **current** source tree;
it does not recreate the historical reflection-heavy GORM INSERT path or the
single-event Outbox dispatcher. The primary before/after numbers above come
from the preserved controlled source-version experiments documented in the two
canonical reports.

Runtime profiles default to `/tmp/taskflow-perf/` or
`/tmp/taskflow-perf-ab/`.

## Resume-safe statement

> Used pprof to identify ORM callback/allocation overhead on the task-creation
> hot path, replaced only the two hot writes with transactional prepared SQL,
> and batched Transactional Outbox publishing. In a controlled local
> 100-concurrency benchmark, throughput increased from 4.28k to 5.64k RPS
> (+31.9%), P95 decreased from 38.9ms to 27.9ms (-28.3%), with zero request
> errors; Outbox post-burst drain throughput improved from ~101 to ~733 task/s
> (~7.3x).

These are single-host development benchmark results, not a production SLA.
