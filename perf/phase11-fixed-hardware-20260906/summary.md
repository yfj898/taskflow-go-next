# TaskFlow multi-run benchmark

Generated: 2026-09-06T10:13:40.211675+00:00

Runs: 5
Concurrency: 100
Median measured duration: 15.016 s

## Aggregate stability

| Metric | Median | Mean | Sample variance | Std dev | CV |
| --- | ---: | ---: | ---: | ---: | ---: |
| Requests | 85570.000 | 85683.600 | 830678.300 | 911.416 | 1.06% |
| Throughput (RPS) | 5698.620 | 5706.204 | 3656.110 | 60.466 | 1.06% |
| Average latency (ms) | 17.531 | 17.510 | 0.034 | 0.185 | 1.05% |
| P50 latency (ms) | 16.532 | 16.475 | 0.023 | 0.153 | 0.93% |
| P95 latency (ms) | 27.694 | 27.862 | 0.111 | 0.333 | 1.19% |
| P99 latency (ms) | 38.070 | 38.720 | 1.574 | 1.255 | 3.24% |
| Max latency (ms) | 100.031 | 107.843 | 595.391 | 24.401 | 22.63% |

## Per-run results

| Run | Requests | RPS | Avg ms | P50 ms | P95 ms | P99 ms | Max ms | Errors |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 86948 | 5789.98 | 17.256 | 16.265 | 27.554 | 37.567 | 80.301 | 0 |
| 2 | 84777 | 5646.03 | 17.695 | 16.658 | 28.138 | 39.540 | 100.031 | 0 |
| 3 | 86217 | 5741.78 | 17.401 | 16.380 | 27.694 | 37.909 | 96.312 | 0 |
| 4 | 84906 | 5654.61 | 17.669 | 16.538 | 28.296 | 40.513 | 144.129 | 0 |
| 5 | 85570 | 5698.62 | 17.531 | 16.532 | 27.626 | 38.070 | 118.442 | 0 |

## Delivery outcome

HTTP status totals: `{"201": 428418}`

Transport error totals: `{}`

`environment.txt` in the same directory records the host/container environment used for the run.
