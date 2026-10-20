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
  while [ "$i" -lt 40 ]; do
    if curl -fsS "$BASE_URL/health/ready" >/dev/null 2>&1 && curl -fsS "$METRICS_URL" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

create_task() {
  payload="$1"
  response="$(curl -fsS -X POST "$BASE_URL/api/v1/tasks" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"ai-phase6\",\"type\":\"ai_generate\",\"payload\":$payload}")"
  printf '%s' "$response" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p'
}

wait_status() {
  id="$1"
  wanted="$2"
  i=0
  while [ "$i" -lt 40 ]; do
    task="$(curl -fsS "$BASE_URL/api/v1/tasks/$id")"
    case "$task" in
      *"\"status\":\"$wanted\""*) printf '%s' "$task"; return 0 ;;
      *'"status":"failed"'*)
        if [ "$wanted" = "failed" ]; then printf '%s' "$task"; return 0; fi
        printf '%s\n' "$task" >&2
        return 1
        ;;
    esac
    i=$((i + 1))
    sleep 1
  done
  return 1
}

docker compose --profile ai-smoke up -d ai-gateway ai-gateway-slow >/dev/null

AI_PROVIDER="mock-slow" \
AI_PROVIDER_KIND="gateway" \
AI_ENDPOINT="http://ai-gateway-slow:9000/generate" \
AI_MODEL="slow-v1" \
AI_CAPABILITIES="text,reasoning" \
AI_INPUT_COST_PER_1M="0.1" \
AI_OUTPUT_COST_PER_1M="0.1" \
AI_MAX_CONCURRENCY="1" \
AI_FALLBACK_PROVIDER="mock-fast" \
AI_FALLBACK_KIND="gateway" \
AI_FALLBACK_ENDPOINT="http://ai-gateway:9000/generate" \
AI_FALLBACK_MODEL="mock-v1" \
AI_FALLBACK_CAPABILITIES="text,reasoning" \
AI_FALLBACK_INPUT_COST_PER_1M="10" \
AI_FALLBACK_OUTPUT_COST_PER_1M="10" \
AI_FALLBACK_MAX_CONCURRENCY="1" \
AI_ROUTING_STRATEGY="cost" \
AI_SLO_REALTIME_QUEUE_MS="100" \
AI_SLO_REALTIME_TIMEOUT_MS="2000" \
AI_SLO_STANDARD_QUEUE_MS="1500" \
AI_SLO_STANDARD_TIMEOUT_MS="4000" \
docker compose --profile ai-smoke up -d --force-recreate executor-worker >/dev/null

wait_ready

# Occupy the single slow-model slot with a pinned standard request.
holder_id="$(create_task '"{\"prompt\":\"holder\",\"model\":\"slow-v1\",\"slo_class\":\"standard\"}"')"
test -n "$holder_id"
sleep 0.15

# Realtime first tries the cheaper slow provider, hits its 100ms queue budget,
# then falls back to the available fast provider.
overflow_id="$(create_task '"{\"prompt\":\"overflow\",\"slo_class\":\"realtime\",\"routing_strategy\":\"cost\"}"')"
test -n "$overflow_id"
overflow_task="$(wait_status "$overflow_id" success)"
printf '%s' "$overflow_task" | grep -q 'mock-ai:overflow'

holder_task="$(wait_status "$holder_id" success)"
printf '%s' "$holder_task" | grep -q 'slow-ai:holder'

metrics="$(curl -fsS "$METRICS_URL")"
printf '%s\n' "$metrics" | grep 'taskflow_ai_backpressure_total' | grep 'provider="mock-slow"' | grep 'model="slow-v1"' | grep 'slo_class="realtime"' | grep 'outcome="backpressure"' >/dev/null
printf '%s\n' "$metrics" | grep 'taskflow_ai_route_selections_total' | grep 'provider="mock-fast"' | grep 'reason="fallback"' >/dev/null
printf '%s\n' "$metrics" | grep 'taskflow_ai_model_queue_wait_seconds_count' | grep 'provider="mock-slow"' | grep 'slo_class="realtime"' >/dev/null

# Now remove the fallback and force a provider execution deadline. This is a
# distinct SLO failure, not provider-health failure or queue saturation.
AI_PROVIDER="mock-slow" \
AI_PROVIDER_KIND="gateway" \
AI_ENDPOINT="http://ai-gateway-slow:9000/generate" \
AI_MODEL="slow-v1" \
AI_CAPABILITIES="text,reasoning" \
AI_INPUT_COST_PER_1M="0.1" \
AI_OUTPUT_COST_PER_1M="0.1" \
AI_MAX_CONCURRENCY="1" \
AI_FALLBACK_ENDPOINT="" \
AI_SLO_REALTIME_QUEUE_MS="50" \
AI_SLO_REALTIME_TIMEOUT_MS="150" \
docker compose --profile ai-smoke up -d --force-recreate executor-worker >/dev/null
wait_ready

deadline_id="$(create_task '"{\"prompt\":\"deadline\",\"model\":\"slow-v1\",\"slo_class\":\"realtime\"}"')"
deadline_task="$(wait_status "$deadline_id" failed)"
printf '%s' "$deadline_task" | grep -q 'AI SLO deadline exceeded'

metrics="$(curl -fsS "$METRICS_URL")"
printf '%s\n' "$metrics" | grep 'taskflow_ai_provider_calls_total' | grep 'provider="mock-slow"' | grep 'result="slo_deadline"' >/dev/null
printf '%s\n' "$metrics" | grep 'taskflow_ai_slo_requests_total' | grep 'slo_class="realtime"' | grep 'result="failed"' >/dev/null

echo "ai phase6 SLO priority + per-model concurrency + backpressure: ok"
