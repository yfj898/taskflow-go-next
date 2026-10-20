#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
TOKEN="${CONTROL_PLANE_BEARER_TOKEN:-phase22-control-plane-token}"
phase="setup"

wait_ready() {
  i=0
  while [ "$i" -lt 60 ]; do
    if [ "$(curl -sS -o /dev/null -w '%{http_code}' "$BASE_URL/health/ready" || true)" = "200" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

cleanup() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "Phase 22 control-plane auth smoke failed during phase: $phase" >&2
    docker compose logs --tail 200 taskflow >&2 2>/dev/null || true
  fi
  CONTROL_PLANE_AUTH_ENABLED=false CONTROL_PLANE_BEARER_TOKEN= \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT

phase="start dependencies"
docker compose up -d mysql redis executor-worker >/dev/null

phase="fail-closed startup validation"
if docker run --rm \
  -e CONTROL_PLANE_AUTH_ENABLED=true \
  -e CONTROL_PLANE_BEARER_TOKEN= \
  taskflow-go:local >/tmp/phase22-misconfigured.log 2>&1; then
  echo "misconfigured control-plane auth unexpectedly started" >&2
  exit 1
fi
grep -q 'CONTROL_PLANE_AUTH_ENABLED=true requires CONTROL_PLANE_BEARER_TOKEN' /tmp/phase22-misconfigured.log

phase="start authenticated taskflow"
CONTROL_PLANE_AUTH_ENABLED=true CONTROL_PLANE_BEARER_TOKEN="$TOKEN" \
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null
wait_ready

phase="public health and task data plane"
test "$(curl -sS -o /dev/null -w '%{http_code}' "$BASE_URL/health/live")" = "200"
task_response="$(curl -fsS -X POST "$BASE_URL/api/v1/tasks" \
  -H 'Content-Type: application/json' \
  -d '{"name":"phase22-public-task","type":"compute","payload":"auth-data-plane"}')"
task_id="$(printf '%s' "$task_response" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')"
test -n "$task_id"
test "$(curl -sS -o /dev/null -w '%{http_code}' "$BASE_URL/api/v1/tasks/$task_id")" = "200"

phase="unauthenticated control plane rejected"
test "$(curl -sS -o /tmp/phase22-routing-unauth.json -w '%{http_code}' "$BASE_URL/api/v1/ai/routing-policy")" = "401"
test "$(curl -sS -o /tmp/phase22-dlq-unauth.json -w '%{http_code}' "$BASE_URL/api/v1/queue/dead-letters?limit=1")" = "401"
test "$(curl -sS -o /tmp/phase22-routing-wrong.json -w '%{http_code}' \
  -H 'Authorization: Bearer wrong-token' "$BASE_URL/api/v1/ai/routing-policy")" = "401"

phase="authenticated control plane allowed"
test "$(curl -sS -o /tmp/phase22-routing-auth.json -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" "$BASE_URL/api/v1/ai/routing-policy")" = "200"
test "$(curl -sS -o /tmp/phase22-dlq-auth.json -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" "$BASE_URL/api/v1/queue/dead-letters?limit=1")" = "200"

phase="authenticated governance mutation"
policy_fields="$(python3 - <<'PY'
import json
with open('/tmp/phase22-routing-auth.json', encoding='utf-8') as f:
    policy = json.load(f)
print(policy.get('strategy', 'balanced'))
print(policy.get('default_output_tokens', 256))
PY
)"
strategy="$(printf '%s\n' "$policy_fields" | sed -n '1p')"
default_output_tokens="$(printf '%s\n' "$policy_fields" | sed -n '2p')"
test -n "$strategy"
test "$default_output_tokens" -gt 0
test "$(curl -sS -o /tmp/phase22-routing-update.json -w '%{http_code}' \
  -X PUT "$BASE_URL/api/v1/ai/routing-policy" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"strategy\":\"$strategy\",\"default_output_tokens\":$default_output_tokens}")" = "200"

python3 scripts/phase22-control-plane-auth-check.py
phase="complete"
echo "Phase 22 control-plane Bearer auth and fail-closed validation: ok"
