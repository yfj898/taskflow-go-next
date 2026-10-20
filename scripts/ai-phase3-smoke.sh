#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
METRICS_URL="${EXECUTOR_METRICS_URL:-http://127.0.0.1:19093/metrics}"

restore_worker() {
  docker compose --profile ai-smoke up -d --force-recreate executor-worker >/dev/null 2>&1 || true
}
trap restore_worker EXIT

wait_ready() {
  i=0
  while [ "$i" -lt 30 ]; do
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
    -d "{\"name\":\"ai-phase3\",\"type\":\"ai_generate\",\"payload\":$payload}")"
  printf '%s' "$response" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p'
}

wait_result() {
  id="$1"
  i=0
  while [ "$i" -lt 30 ]; do
    task="$(curl -fsS "$BASE_URL/api/v1/tasks/$id")"
    case "$task" in
      *'"status":"success"'*) printf '%s' "$task"; return 0 ;;
      *'"status":"failed"'*) printf '%s\n' "$task" >&2; return 1 ;;
    esac
    i=$((i + 1))
    sleep 1
  done
  return 1
}

docker compose --profile ai-smoke up -d ai-gateway ai-gateway-alt >/dev/null

AI_PROVIDER="openai-primary" \
AI_PROVIDER_KIND="openai" \
AI_ENDPOINT="http://ai-gateway:9000" \
AI_MODEL="mock-v1" \
AI_CAPABILITIES="text,reasoning" \
AI_INPUT_COST_PER_1M="10" \
AI_OUTPUT_COST_PER_1M="30" \
AI_FALLBACK_PROVIDER="openai-cheap" \
AI_FALLBACK_KIND="openai" \
AI_FALLBACK_ENDPOINT="http://ai-gateway-alt:9000" \
AI_FALLBACK_MODEL="mock-alt-v1" \
AI_FALLBACK_CAPABILITIES="text" \
AI_FALLBACK_INPUT_COST_PER_1M="0.1" \
AI_FALLBACK_OUTPUT_COST_PER_1M="0.5" \
AI_ROUTING_STRATEGY="cost" \
docker compose --profile ai-smoke up -d --force-recreate executor-worker >/dev/null

wait_ready

id="$(create_task '"{\"prompt\":\"route-me\",\"routing_strategy\":\"cost\"}"')"
test -n "$id"
task="$(wait_result "$id")"
printf '%s' "$task" | grep -q 'alt-ai:route-me'

id="$(create_task '"{\"prompt\":\"model-pin\",\"model\":\"mock-v1\",\"routing_strategy\":\"cost\"}"')"
test -n "$id"
task="$(wait_result "$id")"
printf '%s' "$task" | grep -q 'mock-ai:model-pin'

id="$(create_task '"{\"prompt\":\"think\",\"capability\":\"reasoning\"}"')"
test -n "$id"
task="$(wait_result "$id")"
printf '%s' "$task" | grep -q 'mock-ai:think'

metrics="$(curl -fsS "$METRICS_URL")"
printf '%s\n' "$metrics" | grep 'taskflow_ai_route_selections_total' | grep 'provider="openai-cheap"' | grep 'strategy="cost"' | grep 'reason="selected"' >/dev/null
printf '%s\n' "$metrics" | grep 'taskflow_ai_route_selections_total' | grep 'provider="openai-primary"' | grep 'strategy="cost"' >/dev/null
printf '%s\n' "$metrics" | grep 'taskflow_ai_tokens_total' | grep 'provider="openai-cheap"' | grep 'kind="total"' >/dev/null
printf '%s\n' "$metrics" | grep 'taskflow_ai_cost_usd_total' | grep 'provider="openai-cheap"' >/dev/null

# Provider-level circuit breaker integration: the first task reaches the
# primary and falls back after a retryable 503. With threshold=1, the second
# task must skip the open primary circuit and go straight to the fallback.
docker compose --profile ai-smoke up -d ai-gateway-fail ai-gateway-alt >/dev/null
AI_PROVIDER="circuit-primary" \
AI_PROVIDER_KIND="gateway" \
AI_ENDPOINT="http://ai-gateway-fail:9000/generate" \
AI_MODEL="circuit-primary-v1" \
AI_CAPABILITIES="text" \
AI_INPUT_COST_PER_1M="0.1" \
AI_OUTPUT_COST_PER_1M="0.1" \
AI_FALLBACK_PROVIDER="circuit-fallback" \
AI_FALLBACK_KIND="gateway" \
AI_FALLBACK_ENDPOINT="http://ai-gateway-alt:9000/generate" \
AI_FALLBACK_MODEL="circuit-fallback-v1" \
AI_FALLBACK_CAPABILITIES="text" \
AI_FALLBACK_INPUT_COST_PER_1M="10" \
AI_FALLBACK_OUTPUT_COST_PER_1M="10" \
AI_ROUTING_STRATEGY="cost" \
AI_PROVIDER_CIRCUIT_FAILURES="1" \
AI_PROVIDER_CIRCUIT_OPEN_MS="60000" \
docker compose --profile ai-smoke up -d --force-recreate executor-worker >/dev/null

wait_ready

id="$(create_task '"circuit-one"')"
test -n "$id"
task="$(wait_result "$id")"
printf '%s' "$task" | grep -q 'alt-ai:circuit-one'

id="$(create_task '"circuit-two"')"
test -n "$id"
task="$(wait_result "$id")"
printf '%s' "$task" | grep -q 'alt-ai:circuit-two'

metrics="$(curl -fsS "$METRICS_URL")"
printf '%s\n' "$metrics" | grep 'taskflow_ai_provider_calls_total' | grep 'provider="circuit-primary"' | grep 'result="retryable_error"' >/dev/null
printf '%s\n' "$metrics" | grep 'taskflow_ai_provider_circuit_skips_total' | grep 'provider="circuit-primary"' >/dev/null
printf '%s\n' "$metrics" | grep 'taskflow_ai_provider_calls_total' | grep 'provider="circuit-fallback"' | grep 'result="success"' >/dev/null

echo "ai phase3 openai-compatible + smart routing + provider circuit: ok"
