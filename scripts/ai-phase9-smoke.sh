#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
METRICS_URL="${EXECUTOR_METRICS_URL:-http://127.0.0.1:19093/metrics}"

restore() {
  docker compose --profile ai-smoke up -d --force-recreate executor-worker >/dev/null 2>&1 || true
  docker compose --profile ai-smoke stop ai-gateway-slow >/dev/null 2>&1 || true
}
trap restore EXIT

wait_ready() {
  i=0
  while [ "$i" -lt 80 ]; do
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
  slo="$3"
  response="$(curl -fsS -X POST "$BASE_URL/api/v1/tasks" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"ai-phase9-$prompt\",\"type\":\"ai_generate\",\"payload\":\"{\\\"prompt\\\":\\\"$prompt\\\",\\\"tenant_id\\\":\\\"$tenant\\\",\\\"slo_class\\\":\\\"$slo\\\"}\"}")"
  printf '%s' "$response" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p'
}

wait_status() {
  id="$1"
  wanted="$2"
  i=0
  while [ "$i" -lt 200 ]; do
    task="$(curl -fsS "$BASE_URL/api/v1/tasks/$id")"
    case "$task" in
      *"\"status\":\"$wanted\""*) printf '%s' "$task"; return 0 ;;
      *'"status":"failed"'*) printf '%s\n' "$task" >&2; return 1 ;;
    esac
    i=$((i + 1))
    sleep 0.05
  done
  return 1
}

wait_running() {
  id="$1"
  i=0
  while [ "$i" -lt 120 ]; do
    task="$(curl -fsS "$BASE_URL/api/v1/tasks/$id")"
    case "$task" in
      *'"status":"running"'*) return 0 ;;
      *'"status":"failed"'*|*'"status":"success"'*) printf '%s\n' "$task" >&2; return 1 ;;
    esac
    i=$((i + 1))
    sleep 0.05
  done
  return 1
}

wait_queued_metric() {
  i=0
  while [ "$i" -lt 80 ]; do
    if curl -fsS "$METRICS_URL" | awk '
      /^taskflow_ai_model_concurrency_queued\{/ && /provider="mock-slow"/ && /model="slow-v1"/ {
        if ($NF + 0 > 0) found=1
      }
      END { exit found ? 0 : 1 }
    '; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.05
  done
  return 1
}

updated_at() {
  printf '%s' "$1" | sed -n 's/.*"updated_at":"\([^"]*\)".*/\1/p'
}

AI_GATEWAY_SLOW_DELAY_MS="1500" \
docker compose --profile ai-smoke up -d --force-recreate ai-gateway-slow >/dev/null

AI_PROVIDER="mock-slow" \
AI_PROVIDER_KIND="gateway" \
AI_ENDPOINT="http://ai-gateway-slow:9000/generate" \
AI_MODEL="slow-v1" \
AI_CAPABILITIES="text,reasoning" \
AI_INPUT_COST_PER_1M="0.1" \
AI_OUTPUT_COST_PER_1M="0.1" \
AI_MAX_CONCURRENCY="1" \
AI_FALLBACK_ENDPOINT="" \
AI_HEDGE_ENABLED="false" \
AI_TENANT_FAIR_SCHEDULING="true" \
AI_TENANT_MAX_CONSECUTIVE="1" \
AI_TENANT_STARVATION_MS="100" \
AI_SLO_REALTIME_QUEUE_MS="10000" \
AI_SLO_REALTIME_TIMEOUT_MS="15000" \
AI_SLO_STANDARD_QUEUE_MS="10000" \
AI_SLO_STANDARD_TIMEOUT_MS="15000" \
AI_SLO_BATCH_QUEUE_MS="10000" \
AI_SLO_BATCH_TIMEOUT_MS="15000" \
docker compose --profile ai-smoke up -d --force-recreate executor-worker taskflow >/dev/null

wait_ready

# Same-SLO fairness. tenant-a owns the slot and queues two more requests first;
# tenant-b joins later. With max-consecutive=1, tenant-b must be admitted before
# the older tenant-a waiters when the holder releases the slot.
holder_id="$(create_task holder tenant-a standard)"
wait_running "$holder_id"
sleep 0.15

a1_id="$(create_task a-1 tenant-a standard)"
wait_running "$a1_id"
wait_queued_metric
sleep 0.15
a2_id="$(create_task a-2 tenant-a standard)"
wait_running "$a2_id"
sleep 0.15
b1_id="$(create_task b-1 tenant-b standard)"
wait_running "$b1_id"
sleep 0.15

holder_task="$(wait_status "$holder_id" success)"
b1_task="$(wait_status "$b1_id" success)"
a1_task="$(wait_status "$a1_id" success)"
a2_task="$(wait_status "$a2_id" success)"
printf '%s' "$holder_task" | grep -q 'slow-ai:holder'
printf '%s' "$b1_task" | grep -q 'slow-ai:b-1'

b1_time="$(updated_at "$b1_task")"
a1_time="$(updated_at "$a1_task")"
a2_time="$(updated_at "$a2_task")"
first_after_holder="$(printf '%s\n%s\n%s\n' "$b1_time" "$a1_time" "$a2_time" | sort | head -n 1)"
test "$first_after_holder" = "$b1_time"

# Starvation protection. A batch request waits for more than two 100ms aging
# intervals (priority 2 -> effective priority 0), then a fresh realtime request
# arrives. The aged batch must be admitted first when the holder releases.
starve_holder_id="$(create_task starve-holder tenant-a realtime)"
wait_running "$starve_holder_id"
sleep 0.15
old_batch_id="$(create_task old-batch tenant-b batch)"
wait_running "$old_batch_id"
sleep 0.25
new_realtime_id="$(create_task new-realtime tenant-a realtime)"
wait_running "$new_realtime_id"
sleep 0.15

wait_status "$starve_holder_id" success >/dev/null
old_batch_task="$(wait_status "$old_batch_id" success)"
new_realtime_task="$(wait_status "$new_realtime_id" success)"
old_batch_time="$(updated_at "$old_batch_task")"
new_realtime_time="$(updated_at "$new_realtime_task")"
first_starved="$(printf '%s\n%s\n' "$old_batch_time" "$new_realtime_time" | sort | head -n 1)"
test "$first_starved" = "$old_batch_time"

metrics="$(curl -fsS "$METRICS_URL")"
printf '%s\n' "$metrics" | grep 'taskflow_ai_tenant_fair_scheduling_total' | grep 'action="tenant_rotated"' >/dev/null
printf '%s\n' "$metrics" | grep 'taskflow_ai_tenant_fair_scheduling_total' | grep 'action="starvation_promoted"' >/dev/null

audits="$(curl -fsS "$BASE_URL/api/v1/tasks/$b1_id/ai-route-audits")"
printf '%s' "$audits" | grep -q '"tenant_fair_scheduling":true'
printf '%s' "$audits" | grep -q '"tenant_max_consecutive":1'
printf '%s' "$audits" | grep -q '"tenant_starvation_ms":100'

echo "ai phase9 tenant fair scheduling + starvation protection: ok"
