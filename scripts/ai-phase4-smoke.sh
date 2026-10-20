#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
METRICS_URL="${EXECUTOR_METRICS_URL:-http://127.0.0.1:19093/metrics}"

initial_strategy=""
initial_tokens=""

restore() {
  docker compose --profile ai-smoke up -d --force-recreate executor-worker >/dev/null 2>&1 || true
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
  payload_json="$1"
  response="$(curl -fsS -X POST "$BASE_URL/api/v1/tasks" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"ai-phase4\",\"type\":\"ai_generate\",\"payload\":$payload_json}")"
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
docker compose --profile ai-smoke up -d --force-recreate executor-worker >/dev/null

wait_ready

initial_policy="$(curl -fsS "$BASE_URL/api/v1/ai/routing-policy")"
initial_strategy="$(printf '%s' "$initial_policy" | sed -n 's/.*"strategy":"\([^"]*\)".*/\1/p')"
initial_tokens="$(printf '%s' "$initial_policy" | sed -n 's/.*"default_output_tokens":\([0-9][0-9]*\).*/\1/p')"
test -n "$initial_strategy"
test -n "$initial_tokens"

# Warm one call per model so the executor has real latency EWMA for replay.
id="$(create_task '"{\"prompt\":\"warm-primary\",\"model\":\"mock-v1\"}"')"
wait_result "$id" | grep -q 'mock-ai:warm-primary'
id="$(create_task '"{\"prompt\":\"warm-cheap\",\"model\":\"mock-alt-v1\"}"')"
wait_result "$id" | grep -q 'alt-ai:warm-cheap'

cost_policy="$(curl -fsS -X PUT "$BASE_URL/api/v1/ai/routing-policy" \
  -H 'Content-Type: application/json' \
  -d '{"strategy":"cost","default_output_tokens":256}')"
cost_version="$(printf '%s' "$cost_policy" | sed -n 's/.*"version":\([0-9][0-9]*\).*/\1/p')"
test -n "$cost_version"

cost_id="$(create_task '"phase4-cost"')"
cost_task="$(wait_result "$cost_id")"
printf '%s' "$cost_task" | grep -q 'alt-ai:phase4-cost'

audits="$(curl -fsS "$BASE_URL/api/v1/tasks/$cost_id/ai-route-audits")"
printf '%s' "$audits" | grep -q '"selected_provider":"openai-cheap"'
printf '%s' "$audits" | grep -q '"strategy":"cost"'
printf '%s' "$audits" | grep -q "\"policy_version\":$cost_version"
audit_id="$(printf '%s' "$audits" | sed -n 's/.*"items":\[{"id":\([0-9][0-9]*\).*/\1/p')"
test -n "$audit_id"

replay_cost="$(curl -fsS -X POST "$BASE_URL/api/v1/tasks/$cost_id/ai-route-audits/$audit_id/replay" \
  -H 'Content-Type: application/json' -d '{"strategy":"cost"}')"
printf '%s' "$replay_cost" | grep -q '"replay_provider":"openai-cheap"'
printf '%s' "$replay_cost" | grep -q '"same_selection":true'

replay_latency="$(curl -fsS -X POST "$BASE_URL/api/v1/tasks/$cost_id/ai-route-audits/$audit_id/replay" \
  -H 'Content-Type: application/json' -d '{"strategy":"latency"}')"
printf '%s' "$replay_latency" | grep -q '"replay_provider":"openai-primary"'
printf '%s' "$replay_latency" | grep -q '"same_selection":false'

reliability_policy="$(curl -fsS -X PUT "$BASE_URL/api/v1/ai/routing-policy" \
  -H 'Content-Type: application/json' \
  -d '{"strategy":"reliability","default_output_tokens":256}')"
reliability_version="$(printf '%s' "$reliability_policy" | sed -n 's/.*"version":\([0-9][0-9]*\).*/\1/p')"
test -n "$reliability_version"
test "$reliability_version" -gt "$cost_version"

hot_id="$(create_task '"phase4-hot"')"
hot_task="$(wait_result "$hot_id")"
printf '%s' "$hot_task" | grep -q 'mock-ai:phase4-hot'
hot_audits="$(curl -fsS "$BASE_URL/api/v1/tasks/$hot_id/ai-route-audits")"
printf '%s' "$hot_audits" | grep -q '"selected_provider":"openai-primary"'
printf '%s' "$hot_audits" | grep -q '"strategy":"reliability"'
printf '%s' "$hot_audits" | grep -q "\"policy_version\":$reliability_version"

# Recreate only the API process. Policy must reload from MySQL and still be
# propagated to the unchanged executor worker on the next task.
docker compose --profile ai-smoke up -d --force-recreate taskflow >/dev/null
wait_ready
persisted_policy="$(curl -fsS "$BASE_URL/api/v1/ai/routing-policy")"
printf '%s' "$persisted_policy" | grep -q '"strategy":"reliability"'
printf '%s' "$persisted_policy" | grep -q "\"version\":$reliability_version"

persisted_id="$(create_task '"phase4-persisted"')"
persisted_task="$(wait_result "$persisted_id")"
printf '%s' "$persisted_task" | grep -q 'mock-ai:phase4-persisted'

echo "ai phase4 durable policy + audit + replay: ok"
