# TaskFlow fixed-hardware multi-run stability report

Date: 2026-09-06

## Purpose

This report records the repeated fixed-hardware stability baseline captured on
2026-09-06. It is intended to answer a
different question from the earlier before/after optimization experiment:

- the 2026-09-04 report shows that the optimized implementation is faster than
  the measured baseline;
- this report shows that the current optimized implementation produces
  repeatable results across independent fresh-stack runs on the same host.

These results are local development benchmark evidence, not a production SLA.

## Fixed conditions

- Host CPU: Intel Core i9-9900K, 8 cores / 16 logical CPUs.
- Host memory: about 62 GiB.
- CPU governor captured at start: `powersave`.
- Docker Engine: 29.1.3.
- Benchmark image: `taskflow-go:perf`.
- Image ID: `sha256:541fe05651c6c5aea5c35f82ec8e4cfaeb2f65d0a7ce9eaa14a755f939072015`.
- Fresh MySQL 8.4, Redis 7.4, API and executor containers for every run.
- 4 keep-alive clients for a 1 second warm-up.
- 100 concurrent keep-alive clients for a 15 second measured window.
- 10 second CPU profile captured during each measured run.
- `ACCESS_LOG=false` to match the production-oriented Compose/Kubernetes
  configuration while retaining Prometheus metrics and tracing propagation.
- Five independent measured runs with a 3 second cooldown between runs.

The complete captured host/Docker metadata is in
`perf/phase11-fixed-hardware-20260906/environment.txt`.

The directory keeps its original `phase11-` prefix for artifact stability. The
project phase numbering was subsequently corrected: Phase 11 is runtime tenant
quota governance and Phase 12 is queue-aware autoscaling guardrails.

## Result

Across the five runs, all **428,418** measured requests returned HTTP 201 and
the load generator recorded **zero transport errors**.

| Metric | Median | Mean | Std dev | CV |
| --- | ---: | ---: | ---: | ---: |
| Requests / 15s | 85,570 | 85,683.6 | 911.4 | 1.06% |
| Throughput | **5,698.62 RPS** | 5,706.20 RPS | 60.47 | **1.06%** |
| Average latency | **17.531 ms** | 17.510 ms | 0.185 ms | **1.05%** |
| P50 latency | **16.532 ms** | 16.475 ms | 0.153 ms | **0.93%** |
| P95 latency | **27.694 ms** | 27.862 ms | 0.333 ms | **1.19%** |
| P99 latency | **38.070 ms** | 38.720 ms | 1.255 ms | **3.24%** |
| Max latency | 100.031 ms | 107.843 ms | 24.401 ms | 22.63% |

The core throughput and percentile-latency signals are tightly grouped. Maximum
latency is intentionally not used as a stability headline because one-off tail
events make its 22.63% coefficient of variation much noisier than P95/P99.

## Per-run data

| Run | Requests | RPS | Avg ms | P50 ms | P95 ms | P99 ms | Max ms | Errors |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 86,948 | 5,789.98 | 17.256 | 16.265 | 27.554 | 37.567 | 80.301 | 0 |
| 2 | 84,777 | 5,646.03 | 17.695 | 16.658 | 28.138 | 39.540 | 100.031 | 0 |
| 3 | 86,217 | 5,741.78 | 17.401 | 16.380 | 27.694 | 37.909 | 96.312 | 0 |
| 4 | 84,906 | 5,654.61 | 17.669 | 16.538 | 28.296 | 40.513 | 144.129 | 0 |
| 5 | 85,570 | 5,698.62 | 17.531 | 16.532 | 27.626 | 38.070 | 118.442 | 0 |

The machine-readable aggregate is
`perf/phase11-fixed-hardware-20260906/summary.json`; the generated table is in
`perf/phase11-fixed-hardware-20260906/summary.md`.

## Consistency with the earlier optimized sample

The 2026-09-04 optimized verification sample reported 5,643.98 RPS, P95
27.881 ms and P99 38.253 ms. The new five-run medians differ by only:

- throughput: +0.97%;
- P95 latency: -0.67%;
- P99 latency: -0.48%.

That agreement is useful evidence that the earlier optimized sample was not an
isolated high-throughput outlier. The new multi-run median should be preferred
when describing the current implementation's local performance stability; the
2026-09-04 before/after table remains the evidence for the optimization delta.

## Resource observation

The one-shot `docker stats` samples across the five load windows were also
consistent:

- API CPU: about 178% to 188%, memory about 31 MiB;
- MySQL CPU: about 367% to 387%, memory about 573-578 MiB;
- executor CPU: about 6%, memory about 9-11 MiB;
- Redis CPU: about 3.6-3.9%, memory about 4.5-6.3 MiB.

This remains consistent with the previous pprof conclusion that task creation is
primarily database/network-I/O constrained rather than executor-CPU constrained.

Representative Phase 11 CPU profiles were also parsed with the exact `/taskflow`
binary copied from benchmark image
`sha256:541fe05651c6c5aea5c35f82ec8e4cfaeb2f65d0a7ce9eaa14a755f939072015`:

- run 3: `internal/runtime/syscall/linux.Syscall6` was 38.74% flat CPU;
- run 5: `internal/runtime/syscall/linux.Syscall6` was 40.00% flat CPU.

The consistency of those independent profiles supports the same stopping point
as the earlier tuning work: further performance work should focus on database,
network/storage capacity and horizontal scaling before attempting Go-level
micro-optimizations.

## Profiles and reproducibility

Every run retained all expected evidence:

- `warmup.json`
- `load.json`
- `cpu.pprof`
- `heap.pprof`
- `goroutine.txt`
- `docker-stats.txt`
- `app.log`

Rebuild and repeat the same case with:

```bash
docker build --build-arg GOPROXY=https://goproxy.cn,direct -t taskflow-go:perf .
RUNS=5 ACCESS_LOG=false make perf-multi
```

`scripts/perf-multi.sh` also records the benchmark image ID and creation time,
so a future run can be tied back to the exact container image used for the
measurement.

