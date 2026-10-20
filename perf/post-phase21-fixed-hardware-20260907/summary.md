# TaskFlow multi-run benchmark

Generated: 2026-09-07T15:44:43.343898+00:00

Runs: 5
Concurrency: 100
Median measured duration: 15.017 s

## Aggregate stability

| Metric | Median | Mean | Sample variance | Std dev | CV |
| --- | ---: | ---: | ---: | ---: | ---: |
| Requests | 86835.000 | 85510.800 | 9753588.700 | 3123.074 | 3.65% |
| Throughput (RPS) | 5781.980 | 5694.308 | 43141.388 | 207.705 | 3.65% |
| Average latency (ms) | 17.277 | 17.565 | 0.446 | 0.668 | 3.80% |
| P50 latency (ms) | 16.433 | 16.497 | 0.052 | 0.228 | 1.38% |
| P95 latency (ms) | 26.816 | 27.960 | 5.849 | 2.418 | 8.65% |
| P99 latency (ms) | 36.290 | 38.936 | 41.241 | 6.422 | 16.49% |
| Max latency (ms) | 82.180 | 137.871 | 7623.115 | 87.310 | 63.33% |

## Per-run results

| Run | Requests | RPS | Avg ms | P50 ms | P95 ms | P99 ms | Max ms | Errors |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 86835 | 5781.98 | 17.277 | 16.433 | 26.816 | 36.290 | 82.180 | 0 |
| 2 | 80192 | 5340.72 | 18.709 | 16.857 | 32.241 | 50.213 | 246.387 | 0 |
| 3 | 87523 | 5828.59 | 17.142 | 16.373 | 26.568 | 34.436 | 67.182 | 0 |
| 4 | 87718 | 5841.24 | 17.104 | 16.264 | 26.701 | 35.868 | 74.488 | 0 |
| 5 | 85286 | 5679.01 | 17.592 | 16.559 | 27.473 | 37.873 | 219.117 | 0 |

## Delivery outcome

HTTP status totals: `{"201": 427554}`

Transport error totals: `{}`

`environment.txt` in the same directory records the host/container environment used for the run.
