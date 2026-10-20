#!/usr/bin/env sh
set -eu

IMAGE="${IMAGE:-taskflow-go:perf}"
CONCURRENCY="${CONCURRENCY:-100}"
DURATION="${DURATION:-30}"
PROFILE_SECONDS="${PROFILE_SECONDS:-20}"
ROOT="${PERF_ROOT:-/tmp/taskflow-perf-ab}"

run_case() {
  name="$1"
  access_log="$2"
  skip_default_tx="$3"
  dsn_options="$4"
  port="$5"
  pprof_port="$6"

  printf '\n=== %s ===\n' "$name"
  CASE="taskflow-perf-${name}" \
  IMAGE="$IMAGE" \
  HTTP_PORT="$port" \
  PPROF_PORT="$pprof_port" \
  CONCURRENCY="$CONCURRENCY" \
  DURATION="$DURATION" \
  PROFILE_SECONDS="$PROFILE_SECONDS" \
  PERF_OUTPUT_DIR="${ROOT}/${name}" \
  ACCESS_LOG="$access_log" \
  MYSQL_SKIP_DEFAULT_TX="$skip_default_tx" \
  MYSQL_DSN_OPTIONS="$dsn_options" \
  sh scripts/perf-benchmark.sh
}

# Runtime-control case. This intentionally does NOT pretend to reconstruct the
# historical pre-prepared-SQL/pre-batched-Outbox source revision; those measured
# results are preserved in docs/PERFORMANCE_REPORT.md and PERFORMANCE_REPORT.md.
run_case baseline true false "&interpolateParams=true" 18084 16064

# Production-style runtime configuration. Both cases use the current prepared
# SQL + batched Outbox implementation so this isolates operational toggles.
run_case optimized false true "&interpolateParams=true" 18085 16065

printf '\nA/B artifacts are under %s\n' "$ROOT"
