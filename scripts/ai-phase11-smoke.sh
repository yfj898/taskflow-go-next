#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
METRICS_URL="${EXECUTOR_METRICS_URL:-http://127.0.0.1:19093/metrics}"
PREFIX="phase11:ai:concurrency"

restore() {
  rm -f /tmp/taskflow-phase11-bad.json >/dev/null 2>&1 || true
  curl -fsS -X DELETE "$BASE_URL/api/v1/ai/tenant-quota-policy" >/dev/null 2>&1 || true
  docker compose --profile ai-smoke stop ai-gateway-slow >/dev/null 2>&1 || true
  docker compose --profile ai-smoke up -d --force-recreate executor-worker taskflow >/dev/null 2>&1 || true
  docker compose exec -T redis redis-cli --scan --pattern "${PREFIX}:*" 2>/dev/null | while IFS= read -r key; do
    [ -n "$key" ] && docker compose exec -T redis redis-cli DEL "$key" >/dev/null 2>&1 || true
  done
}
trap restore EXIT

wait_ready() {
  i=0
  while [ "$i" -lt 100 ]; do
    if curl -fsS "$BASE_URL/health/ready" >/dev/null 2>&1 && curl -fsS "$METRICS_URL" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.1
  done
  return 1
}

create_task() {
  prompt="$1"
  tenant="$2"
  response="$(curl -fsS -X POST "$BASE_URL/api/v1/tasks" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"ai-phase11-$prompt\",\"type\":\"ai_generate\",\"payload\":\"{\\\"prompt\\\":\\\"$prompt\\\",\\\"tenant_id\\\":\\\"$tenant\\\",\\\"slo_class\\\":\\\"realtime\\\"}\"}")"
  printf '%s' "$response" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p'
}

wait_status() {
  id="$1"
  wanted="$2"
  i=0
  while [ "$i" -lt 160 ]; do
    task="$(curl -fsS "$BASE_URL/api/v1/tasks/$id")"
    case "$task" in
      *"\"status\":\"$wanted\""*) printf '%s' "$task"; return 0 ;;
    esac
    i=$((i + 1))
    sleep 0.05
  done
  printf '%s\n' "$task" >&2
  return 1
}

wait_tenant_lease() {
  i=0
  while [ "$i" -lt 100 ]; do
    keys="$(docker compose exec -T redis redis-cli --scan --pattern "${PREFIX}:tenant:*" | tr -d '\r')"
    if [ -n "$keys" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.025
  done
  return 1
}

AI_GATEWAY_SLOW_DELAY_MS=1500 \
docker compose --profile ai-smoke up -d --force-recreate redis mysql ai-gateway-slow >/dev/null

AI_PROVIDER=mock-slow \
AI_PROVIDER_KIND=gateway \
AI_ENDPOINT=http://ai-gateway-slow:9000/generate \
AI_MODEL=slow-v1 \
AI_CAPABILITIES=text,reasoning \
AI_INPUT_COST_PER_1M=0.1 \
AI_OUTPUT_COST_PER_1M=0.1 \
AI_MAX_CONCURRENCY=4 \
AI_GLOBAL_MAX_CONCURRENCY=0 \
AI_FALLBACK_ENDPOINT= \
AI_DISTRIBUTED_TENANT_QUOTA=false \
AI_TENANT_GLOBAL_MAX_CONCURRENCY=0 \
AI_TENANT_CONCURRENCY_OVERRIDES= \
AI_CONCURRENCY_REDIS_PREFIX="$PREFIX" \
AI_DISTRIBUTED_LEASE_MS=3000 \
AI_ADAPTIVE_LOAD_SHEDDING=false \
AI_HEDGE_ENABLED=false \
AI_SLO_REALTIME_QUEUE_MS=150 \
AI_SLO_REALTIME_TIMEOUT_MS=6000 \
AI_TIMEOUT_MS=7000 \
docker compose --profile ai-smoke up -d --force-recreate executor-worker taskflow >/dev/null

wait_ready
curl -fsS -X DELETE "$BASE_URL/api/v1/ai/tenant-quota-policy" >/dev/null

executor_before="$(docker inspect taskflow-go-executor-worker-1 --format '{{.Id}}')"

# Runtime writes are strict: malformed overrides are rejected instead of being
# silently ignored like optional environment overrides.
bad_code="$(curl -sS -o /tmp/taskflow-phase11-bad.json -w '%{http_code}' \
  -X PUT "$BASE_URL/api/v1/ai/tenant-quota-policy" \
  -H 'Content-Type: application/json' \
  -d '{"enabled":true,"default_limit":1,"overrides":"broken"}')"
test "$bad_code" = "400"

policy="$(curl -fsS -X PUT "$BASE_URL/api/v1/ai/tenant-quota-policy" \
  -H 'Content-Type: application/json' \
  -d '{"enabled":true,"default_limit":1,"overrides":"tenant-vip=2"}')"
printf '%s' "$policy" | grep -q '"tenant_quota_managed":true'
printf '%s' "$policy" | grep -q '"tenant_quota_enabled":true'
printf '%s' "$policy" | grep -q '"tenant_quota_default_limit":1'
printf '%s' "$policy" | grep -q '"tenant_quota_overrides":"tenant-vip=2"'
policy_version="$(printf '%s' "$policy" | sed -n 's/.*"version":\([0-9][0-9]*\).*/\1/p')"
test -n "$policy_version"

# Runtime policy is durable in MySQL. Recreate only the API/queue process and
# verify the executor process is unchanged while the policy reloads.
docker compose --profile ai-smoke up -d --no-deps --force-recreate taskflow >/dev/null
wait_ready
executor_after_reload="$(docker inspect taskflow-go-executor-worker-1 --format '{{.Id}}')"
test "$executor_before" = "$executor_after_reload"
persisted="$(curl -fsS "$BASE_URL/api/v1/ai/tenant-quota-policy")"
printf '%s' "$persisted" | grep -q '"tenant_quota_managed":true'
printf '%s' "$persisted" | grep -q "\"version\":$policy_version"

holder_id="$(create_task runtime-holder tenant-a)"
wait_tenant_lease

overflow_id="$(create_task runtime-overflow tenant-a)"
overflow_task="$(wait_status "$overflow_id" failed)"
printf '%s' "$overflow_task" | grep -q 'tenant concurrency quota saturated'

audits="$(curl -fsS "$BASE_URL/api/v1/tasks/$overflow_id/ai-route-audits")"
printf '%s' "$audits" | grep -q '"tenant_quota_source":"runtime"'
printf '%s' "$audits" | grep -q '"tenant_concurrency_limit":1'
printf '%s' "$audits" | grep -q "\"policy_version\":$policy_version"
wait_status "$holder_id" success | grep -q 'slow-ai:runtime-holder'

# Reset governance to unmanaged. The executor still must not restart, and its
# Phase 10 static config is disabled, so no tenant-scoped Redis lease is made.
reset="$(curl -fsS -X DELETE "$BASE_URL/api/v1/ai/tenant-quota-policy")"
printf '%s' "$reset" | grep -q '"tenant_quota_managed":false'
executor_after_reset="$(docker inspect taskflow-go-executor-worker-1 --format '{{.Id}}')"
test "$executor_before" = "$executor_after_reset"

reset_a="$(create_task reset-a tenant-a)"
wait_status "$reset_a" running >/dev/null
sleep 0.1
keys="$(docker compose exec -T redis redis-cli --scan --pattern "${PREFIX}:tenant:*" | tr -d '\r')"
test -z "$keys"
reset_b="$(create_task reset-b tenant-a)"
wait_status "$reset_b" running >/dev/null
wait_status "$reset_a" success | grep -q 'slow-ai:reset-a'
wait_status "$reset_b" success | grep -q 'slow-ai:reset-b'

echo "ai phase11 runtime-governed tenant quota policy: ok"
