# TaskFlow post-Phase-21 release performance evidence

Date: 2026-09-07

## Purpose

This capture closes the final post-Phase-21 release-performance item after the
DLQ archive lifecycle, replication and real MinIO/S3 primary+replica work. The
goal is to verify that the expanded reliability path did not regress the
task-creation hot path and to retain fresh CPU/heap profiles for the release
revision.

The raw/generated evidence is stored under
`perf/post-phase21-fixed-hardware-20260907/`.

## Method

The release run intentionally matches the 2026-09-06 fixed-hardware baseline:

- same 16-logical-CPU Intel Core i9-9900K host;
- same Docker 29.1.3 engine and `powersave` CPU governor;
- five independent fresh Docker stacks;
- 100 concurrent keep-alive clients;
- 15 second measured window after a 1 second warm-up;
- `ACCESS_LOG=false`;
- 10 second CPU pprof plus post-run heap pprof;
- MySQL persistence + Transactional Outbox + Redis Streams + gRPC executor.

The benchmark harness needed one post-Phase-21 compatibility fix before the
release capture: the archive subsystem is enabled by default, while its source
default `./data/dlq-archive` resolves to an unwritable location for the
non-root runtime user when the benchmark container starts at `/`. The harness
now explicitly sets `QUEUE_DLQ_ARCHIVE_DIR=/var/lib/taskflow/dlq-archive`, a
directory created and owned by the runtime user in the Docker image. A static
regression test also checks this contract and the no-access-log benchmark
default.

## Result

All five measured runs completed successfully. Across the full capture,
**427,554 requests returned HTTP 201 and there were zero transport errors**.

| Metric | 2026-09-06 baseline | 2026-09-07 post-Phase-21 | Median change |
| --- | ---: | ---: | ---: |
| Throughput | 5,698.62 RPS | **5,781.98 RPS** | +1.46% |
| Average latency | 17.531 ms | **17.277 ms** | -1.45% |
| P50 latency | 16.532 ms | **16.433 ms** | -0.60% |
| P95 latency | 27.694 ms | **26.816 ms** | -3.17% |
| P99 latency | 38.070 ms | **36.290 ms** | -4.68% |
| Transport errors | 0 | **0** | unchanged |

The release revision therefore shows **no task-creation throughput or latency
regression** after the Phase 20-21 archive work.

## Stability caveat

The release capture is noisier than the 2026-09-06 baseline:

| Metric | 2026-09-06 CV | 2026-09-07 CV |
| --- | ---: | ---: |
| Throughput | 1.06% | 3.65% |
| P95 | 1.19% | 8.65% |
| P99 | 3.24% | 16.49% |

Run 2 was the main outlier at 5,340.72 RPS / 32.241 ms P95 / 50.213 ms P99,
while Runs 1, 3 and 4 were between 5,781.98 and 5,841.24 RPS with P95 below
26.82 ms. No run produced an HTTP or transport error.

The environment capture also recorded a much higher pre-run host load average
(`5.30 5.73 3.46`) than the earlier baseline (`0.61 0.66 0.80`). For that
reason, the small median improvement is not claimed as a controlled optimization
win. The defensible release conclusion is that no regression was observed on
the same hardware and benchmark path despite the noisier host state.

## pprof comparison

The post-Phase-21 median-throughput run is Run 1. Its CPU profile remains
dominated by syscall/runtime/database I/O rather than the archive subsystem:

- `internal/runtime/syscall/linux.Syscall6`: 42.97% flat CPU;
- `runtime.futex`: 5.29% flat CPU;
- no archive package function appears in the top 18 flat CPU nodes.

The previous median-throughput baseline run (2026-09-06 Run 5) showed the same
shape, with `linux.Syscall6` at 40.00% flat CPU. The post-Phase-21 profile
therefore does not expose a new application CPU hotspot attributable to archive
replication or object lifecycle work.

Heap `alloc_space` for the median run was **897.54 MB**, versus **944.28 MB** in
the previous median run (about 5% lower). `TaskService.Create` cumulative
allocation remained effectively flat at 304.61 MB versus 305.13 MB. This is
consistent with the archive work staying off the task-creation hot path.

Raw CPU and heap profiles for all five runs are retained alongside
`summary.json`, `summary.md`, `environment.txt`, container samples and request
results in `perf/post-phase21-fixed-hardware-20260907/`.

## Release conclusion

The final planned post-Phase-21 performance item is complete. The current
release keeps the original Transactional Outbox task-creation semantics,
supports the Phase 20-21 archive durability features, and preserves the prior
fixed-hardware task-creation performance envelope with zero measured request
errors.
