# pprof summaries

Binary profiles are generated into `/tmp/taskflow-perf-ab/<case>/` during the
benchmark. This directory contains the stable text output used for review and
Git history:

- `baseline-cpu-top.txt`
- `optimized-cpu-top.txt`
- `baseline-heap-top.txt`
- `optimized-heap-top.txt`
- goroutine summary excerpts when they reveal a bottleneck

Generate a CPU top report, for example:

```bash
go tool pprof -top /tmp/taskflow-perf-ab/baseline/cpu.pprof
```
