#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
METRICS_URL="${EXECUTOR_METRICS_URL:-http://127.0.0.1:19093/metrics}"

initial_strategy=""
initial_tokens=""

restore() {
  AI_HEDGE_ENABLED=false docker compose --profile ai-smoke up -d --force-recreate executor-worker >/dev/null 2>&1 || true
  docker compose --profile ai-smoke stop ai-gateway-slow >/dev/null 2>&1 || true
  if [ -n "$initial_strategy" ] && [ -n "$initial_tokens" ]; then
    curl -fsS -X PUT "$BASE_URL/api/v1/ai/routing-policy" \
      -H 'Content-Type: application/json' \
      -d "{\"strategy\":\"$initial_strategy\",\"default_output_tokens\":$initial_tokens}" \
      >/dev/null 2>&1 || true
  fi
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
  prompt="$1"
  disable_hedge="$2"
  response="$(curl -fsS -X POST "$BASE_URL/api/v1/tasks" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"ai-phase8-$prompt\",\"type\":\"ai_generate\",\"payload\":\"{\\\"prompt\\\":\\\"$prompt\\\",\\\"slo_class\\\":\\\"realtime\\\",\\\"disable_hedge\\\":$disable_hedge}\"}")"
  printf '%s' "$response" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p'
}

wait_result() {
  id="$1"
  i=0
  while [ "$i" -lt 40 ]; do
    task="$(curl -fsS "$BASE_URL/api/v1/tasks/$id")"
    case "$task" in
      *'"status":"success"'*) printf '%s' "$task"; return 0 ;;
      *'"status":"failed"'*) printf '%s\n' "$task" >&2; return 1 ;;
    esac
    i=$((i + 1))
    sleep 0.2
  done
  return 1
}

docker compose --profile ai-smoke up -d ai-gateway ai-gateway-slow >/dev/null
wait_ready

initial_policy="$(curl -fsS "$BASE_URL/api/v1/ai/routing-policy")"
initial_strategy="$(printf '%s' "$initial_policy" | sed -n 's/.*"strategy":"\([^"]*\)".*/\1/p')"
initial_tokens="$(printf '%s' "$initial_policy" | sed -n 's/.*"default_output_tokens":\([0-9][0-9]*\).*/\1/p')"
test -n "$initial_strategy"
test -n "$initial_tokens"

curl -fsS -X PUT "$BASE_URL/api/v1/ai/routing-policy" \
  -H 'Content-Type: application/json' \
  -d '{"strategy":"cost","default_output_tokens":256}' >/dev/null

AI_PROVIDER="mock-slow" \
AI_PROVIDER_KIND="gateway" \
AI_ENDPOINT="http://ai-gateway-slow:9000/generate" \
AI_MODEL="slow-v1" \
AI_CAPABILITIES="text,reasoning" \
AI_INPUT_COST_PER_1M="0.1" \
AI_OUTPUT_COST_PER_1M="0.1" \
AI_MAX_CONCURRENCY="4" \
AI_GLOBAL_MAX_CONCURRENCY="0" \
AI_FALLBACK_PROVIDER="mock-fast" \
AI_FALLBACK_KIND="gateway" \
AI_FALLBACK_ENDPOINT="http://ai-gateway:9000/generate" \
AI_FALLBACK_MODEL="mock-v1" \
AI_FALLBACK_CAPABILITIES="text,reasoning" \
AI_FALLBACK_INPUT_COST_PER_1M="10" \
AI_FALLBACK_OUTPUT_COST_PER_1M="10" \
AI_FALLBACK_MAX_CONCURRENCY="4" \
AI_FALLBACK_GLOBAL_MAX_CONCURRENCY="0" \
AI_ROUTING_STRATEGY="cost" \
AI_ADAPTIVE_LOAD_SHEDDING="false" \
AI_HEDGE_ENABLED="true" \
AI_HEDGE_DELAY_MS="100" \
AI_HEDGE_CANCEL_WAIT_MS="150" \
AI_HEDGE_MAX_EXTRA_COST_USD="0.01" \
docker compose --profile ai-smoke up -d --force-recreate executor-worker >/dev/null

wait_ready

hedged_id="$(create_task hedge-win false)"
hedged_task="$(wait_result "$hedged_id")"
printf '%s' "$hedged_task" | grep -q 'mock-ai:hedge-win'

audits="$(curl -fsS "$BASE_URL/api/v1/tasks/$hedged_id/ai-route-audits")"
printf '%s' "$audits" | grep -q '"hedge_triggered":true'
printf '%s' "$audits" | grep -q '"hedge_winner":"mock-fast"'
printf '%s' "$audits" | grep -q '"result":"hedge_winner"'

metrics="$(curl -fsS "$METRICS_URL")"
printf '%s\n' "$metrics" | grep 'taskflow_ai_hedge_requests_total' | grep 'outcome="winner_secondary"' >/dev/null
printf '%s\n' "$metrics" | grep -q 'taskflow_ai_hedge_delay_seconds_count 1'

nohedge_id="$(create_task no-hedge true)"
nohedge_task="$(wait_result "$nohedge_id")"
printf '%s' "$nohedge_task" | grep -q 'slow-ai:no-hedge'
nohedge_audits="$(curl -fsS "$BASE_URL/api/v1/tasks/$nohedge_id/ai-route-audits")"
if printf '%s' "$nohedge_audits" | grep -q '"hedge_triggered":true'; then
  echo "disable_hedge task unexpectedly launched a hedge" >&2
  exit 1
fi

echo "ai phase8 hedged requests + tail latency control: ok"
