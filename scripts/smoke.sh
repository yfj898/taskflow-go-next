#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"

echo "checking readiness"
curl -fsS "$BASE_URL/health/ready"
echo

echo "creating task"
response="$(curl -fsS -X POST "$BASE_URL/api/v1/tasks" \
  -H 'Content-Type: application/json' \
  -d '{"name":"smoke","type":"compute","payload":"hello"}')"
echo "$response"

task_id="$(printf '%s' "$response" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')"
if [ -z "$task_id" ]; then
  echo "failed to parse task id" >&2
  exit 1
fi

i=0
while [ "$i" -lt 20 ]; do
  task="$(curl -fsS "$BASE_URL/api/v1/tasks/$task_id")"
  echo "$task"
  case "$task" in
    *'"status":"success"'*) exit 0 ;;
    *'"status":"failed"'*) exit 1 ;;
  esac
  i=$((i + 1))
  sleep 1
done

echo "task did not settle before timeout" >&2
exit 1
