#!/usr/bin/env sh
set -eu

RUNS="${RUNS:-5}"
COOLDOWN_SEC="${COOLDOWN_SEC:-3}"
CASE_PREFIX="${CASE_PREFIX:-taskflow-multi}"
OUTPUT_DIR="${PERF_MULTI_OUTPUT_DIR:-/tmp/taskflow-perf-multi-$(date -u +%Y%m%dT%H%M%SZ)}"
IMAGE="${IMAGE:-taskflow-go:perf}"
ACCESS_LOG="${ACCESS_LOG:-false}"

case "$RUNS" in
  ''|*[!0-9]*)
    echo "RUNS must be a positive integer" >&2
    exit 2
    ;;
esac
if [ "$RUNS" -lt 1 ]; then
  echo "RUNS must be >= 1" >&2
  exit 2
fi

mkdir -p "$OUTPUT_DIR"

{
  echo "captured_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "runs=$RUNS"
  echo "concurrency=${CONCURRENCY:-100}"
  echo "duration=${DURATION:-15}"
  echo "warmup_concurrency=${WARMUP_CONCURRENCY:-4}"
  echo "warmup_duration=${WARMUP_DURATION:-1}"
  echo "profile_seconds=${PROFILE_SECONDS:-10}"
  echo "access_log=$ACCESS_LOG"
  echo "image=$IMAGE"
  if docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "image_id=$(docker image inspect "$IMAGE" --format '{{.Id}}')"
    echo "image_created=$(docker image inspect "$IMAGE" --format '{{.Created}}')"
  fi
  echo "uname=$(uname -a)"
  if [ -r /proc/loadavg ]; then
    echo "loadavg_before=$(cat /proc/loadavg)"
  fi
  if [ -r /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor ]; then
    echo "cpu_governor=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)"
  fi
  if command -v nproc >/dev/null 2>&1; then
    echo "logical_cpus=$(nproc)"
  fi
  if command -v lscpu >/dev/null 2>&1; then
    echo "--- lscpu ---"
    lscpu
  fi
  if [ -r /proc/meminfo ]; then
    echo "--- meminfo ---"
    grep -E '^(MemTotal|MemAvailable):' /proc/meminfo || true
  fi
  echo "--- docker ---"
  docker version 2>/dev/null || true
} >"$OUTPUT_DIR/environment.txt"

i=1
while [ "$i" -le "$RUNS" ]; do
  run_dir="$OUTPUT_DIR/run-$i"
  case_name="${CASE_PREFIX}-$$-$i"
  echo "== benchmark run $i/$RUNS =="
  PERF_OUTPUT_DIR="$run_dir" \
    CASE="$case_name" \
    IMAGE="$IMAGE" \
    ACCESS_LOG="$ACCESS_LOG" \
    sh scripts/perf-benchmark.sh

  if [ "$i" -lt "$RUNS" ] && [ "$COOLDOWN_SEC" != "0" ]; then
    sleep "$COOLDOWN_SEC"
  fi
  i=$((i + 1))
done

python3 scripts/perf_multi.py "$OUTPUT_DIR"
echo "multi-run results: $OUTPUT_DIR"
