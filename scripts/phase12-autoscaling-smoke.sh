#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"

restore() {
  docker compose unpause redis >/dev/null 2>&1 || true
  docker compose up -d redis mysql executor-worker >/dev/null 2>&1 || true
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
}
trap restore EXIT

metric_value() {
  metric="$1"
  curl -fsS "$BASE_URL/metrics" | awk -v metric="$metric" '$1 == metric { print $2; exit }'
}

wait_metric() {
  metric="$1"
  wanted="$2"
  attempts="${3:-40}"
  i=0
  while [ "$i" -lt "$attempts" ]; do
    value="$(metric_value "$metric" 2>/dev/null || true)"
    if [ "$value" = "$wanted" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.1
  done
  echo "metric $metric did not become $wanted" >&2
  return 1
}

docker compose up -d redis mysql executor-worker >/dev/null
QUEUE_STATS_INTERVAL_MS=200 \
docker compose up -d --no-deps --force-recreate taskflow >/dev/null

i=0
while [ "$i" -lt 80 ]; do
  if curl -fsS "$BASE_URL/health/ready" >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  sleep 0.1
done
test "$i" -lt 80

wait_metric taskflow_queue_stats_fresh 1
last_success_before="$(metric_value taskflow_queue_stats_last_success_timestamp_seconds)"
errors_before="$(metric_value taskflow_queue_stats_errors_total)"
test -n "$last_success_before"
test "$last_success_before" != "0"

# Freeze Redis without destroying Stream/group state. Queue stats must become
# stale while the HTTP metrics endpoint stays available.
docker compose pause redis >/dev/null
wait_metric taskflow_queue_stats_fresh 0 50

last_success_stale="$(metric_value taskflow_queue_stats_last_success_timestamp_seconds)"
errors_stale="$(metric_value taskflow_queue_stats_errors_total)"
test "$last_success_stale" = "$last_success_before"
awk -v before="$errors_before" -v after="$errors_stale" 'BEGIN { exit !(after > before) }'

# Recovery must happen in-process; no TaskFlow restart is allowed here.
docker compose unpause redis >/dev/null
wait_metric taskflow_queue_stats_fresh 1 50
last_success_after="$(metric_value taskflow_queue_stats_last_success_timestamp_seconds)"
awk -v before="$last_success_before" -v after="$last_success_after" 'BEGIN { exit !(after >= before) }'

python3 scripts/phase12-keda-check.py
echo "phase12 queue-aware autoscaling guardrails: ok"
