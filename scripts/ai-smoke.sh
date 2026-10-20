#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"

wait_ready() {
  i=0
  while [ "$i" -lt 30 ]; do
    if curl -fsS "$BASE_URL/health/ready" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 1
  done
  return 1
}

wait_status() {
  id="$1"
  i=0
  while [ "$i" -lt 30 ]; do
    task="$(curl -fsS "$BASE_URL/api/v1/tasks/$id")"
    case "$task" in
      *'"status":"success"'*) printf '%s' "$task"; return 0 ;;
      *'"status":"failed"'*) printf '%s' "$task" >&2; return 1 ;;
    esac
    i=$((i + 1)); sleep 1
  done
  return 1
}

wait_ready
response="$(curl -fsS -X POST "$BASE_URL/api/v1/tasks" -H 'Content-Type: application/json' -d '{"name":"ai-smoke","type":"ai_generate","payload":"hello"}')"
id="$(printf '%s' "$response" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')"
test -n "$id"
task="$(wait_status "$id")"
printf '%s' "$task" | grep -q 'mock-ai:hello'
echo "ai task pipeline: ok"
