#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
METRICS_URL="${EXECUTOR_METRICS_URL:-http://127.0.0.1:19093/metrics}"

restore_worker() {
  docker compose --profile ai-smoke up -d --force-recreate executor-worker >/dev/null 2>&1 || true
}
trap restore_worker EXIT

docker compose --profile ai-smoke up -d ai-gateway ai-gateway-fail >/dev/null

AI_ENDPOINT="http://ai-gateway-fail:9000/generate" \
AI_FALLBACK_ENDPOINT="http://ai-gateway:9000/generate" \
docker compose --profile ai-smoke up -d --force-recreate executor-worker >/dev/null

i=0
while [ "$i" -lt 30 ]; do
  if curl -fsS "$BASE_URL/health/ready" >/dev/null 2>&1 && curl -fsS "$METRICS_URL" >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "$i" -lt 30

BASE_URL="$BASE_URL" sh scripts/ai-smoke.sh

metrics="$(curl -fsS "$METRICS_URL")"
printf '%s\n' "$metrics" | grep 'taskflow_ai_provider_calls_total' | grep 'provider="mock-primary"' | grep 'result="retryable_error"' >/dev/null
printf '%s\n' "$metrics" | grep 'taskflow_ai_provider_calls_total' | grep 'provider="mock-fallback"' | grep 'result="success"' >/dev/null
printf '%s\n' "$metrics" | grep 'taskflow_ai_fallback_total' | grep 'from="mock-primary"' | grep 'to="mock-fallback"' >/dev/null
printf '%s\n' "$metrics" | grep 'taskflow_ai_tokens_total' | grep 'provider="mock-fallback"' | grep 'kind="total"' >/dev/null
printf '%s\n' "$metrics" | grep 'taskflow_ai_cost_usd_total' | grep 'provider="mock-fallback"' >/dev/null

echo "ai phase2 provider fallback + usage metrics: ok"
