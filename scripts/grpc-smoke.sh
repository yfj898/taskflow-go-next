#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"

wait_ready() {
  i=0
  while [ "$i" -lt 30 ]; do
    if curl -fsS "$BASE_URL/health/ready" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  echo "service did not become ready" >&2
  return 1
}

create_task() {
  type="$1"
  response="$(curl -fsS -X POST "$BASE_URL/api/v1/tasks" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"grpc-smoke-$type\",\"type\":\"$type\",\"payload\":\"smoke\"}")"
  printf '%s' "$response" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p'
}

wait_status() {
  id="$1"
  expected="$2"
  i=0
  while [ "$i" -lt 30 ]; do
    task="$(curl -fsS "$BASE_URL/api/v1/tasks/$id")"
    case "$task" in
      *"\"status\":\"$expected\""*) printf '%s\n' "$task"; return 0 ;;
      *'"status":"failed"'*)
        if [ "$expected" != "failed" ]; then
          printf '%s\n' "$task" >&2
          return 1
        fi
        ;;
    esac
    i=$((i + 1))
    sleep 1
  done
  echo "task $id did not reach $expected" >&2
  return 1
}

wait_ready

id="$(create_task compute)"
wait_status "$id" success >/dev/null
echo "grpc compute: ok"

id="$(create_task fail_once)"
task="$(wait_status "$id" success)"
printf '%s' "$task" | grep -q '"retry_count":1'
echo "grpc transient retry: ok"

id="$(create_task always_fail)"
task="$(wait_status "$id" failed)"
printf '%s' "$task" | grep -q '"retry_count":0'
echo "grpc permanent failure: ok"

curl -fsS "$BASE_URL/metrics" | grep -q 'taskflow_grpc_executor_calls_total'
echo "grpc metrics: ok"
