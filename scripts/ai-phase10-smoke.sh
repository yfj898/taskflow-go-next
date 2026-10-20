#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

PREFIX="phase10:ai:concurrency"

restore() {
  docker compose --profile phase7-smoke rm -sf executor-worker-alt >/dev/null 2>&1 || true
  docker compose --profile ai-smoke stop ai-gateway-slow >/dev/null 2>&1 || true
  docker compose --profile ai-smoke up -d --force-recreate executor-worker taskflow >/dev/null 2>&1 || true
  docker compose exec -T redis redis-cli --scan --pattern "${PREFIX}:*" 2>/dev/null | while IFS= read -r key; do
    [ -n "$key" ] && docker compose exec -T redis redis-cli DEL "$key" >/dev/null 2>&1 || true
  done
}
trap restore EXIT

AI_GATEWAY_SLOW_DELAY_MS=1500 \
docker compose --profile ai-smoke --profile phase7-smoke up -d redis ai-gateway-slow >/dev/null

docker compose exec -T redis redis-cli --scan --pattern "${PREFIX}:*" | while IFS= read -r key; do
  [ -n "$key" ] && docker compose exec -T redis redis-cli DEL "$key" >/dev/null
done

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
AI_DISTRIBUTED_TENANT_QUOTA=true \
AI_TENANT_GLOBAL_MAX_CONCURRENCY=1 \
AI_TENANT_CONCURRENCY_OVERRIDES=tenant-vip=2 \
AI_CONCURRENCY_REDIS_PREFIX="$PREFIX" \
AI_DISTRIBUTED_LEASE_MS=3000 \
AI_ADAPTIVE_LOAD_SHEDDING=false \
AI_HEDGE_ENABLED=false \
AI_SLO_STANDARD_QUEUE_MS=200 \
AI_SLO_STANDARD_TIMEOUT_MS=5000 \
AI_TIMEOUT_MS=6000 \
docker compose --profile ai-smoke --profile phase7-smoke up -d --force-recreate executor-worker executor-worker-alt >/dev/null

i=0
while [ "$i" -lt 80 ]; do
  if curl -fsS http://127.0.0.1:19093/metrics >/dev/null 2>&1 && curl -fsS http://127.0.0.1:19194/metrics >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  sleep 0.1
done
test "$i" -lt 80

client_log="$(mktemp)"
trap 'rm -f "$client_log"; restore' EXIT
docker run --rm --network host taskflow-go:local \
  /ai-phase10-client 127.0.0.1:19091 127.0.0.1:19192 \
  >"$client_log" 2>&1 &
client_pid=$!

# Inspect the tenant lease while the holder is still executing. The key must
# exist, but its suffix must be a hash rather than the plaintext tenant id.
i=0
keys=""
while [ "$i" -lt 80 ]; do
  keys="$(docker compose exec -T redis redis-cli --scan --pattern "${PREFIX}:tenant:*" | tr -d '\r')"
  if [ -n "$keys" ]; then
    break
  fi
  i=$((i + 1))
  sleep 0.025
done
test "$i" -lt 80
if printf '%s\n' "$keys" | grep -q 'tenant-a'; then
  echo "tenant id leaked into Redis concurrency key" >&2
  exit 1
fi

wait "$client_pid"
grep -q 'phase10 distributed tenant quota: ok' "$client_log"

metrics="$(curl -fsS http://127.0.0.1:19194/metrics)"
printf '%s\n' "$metrics" | grep 'taskflow_ai_tenant_distributed_concurrency_total' | grep 'provider="mock-slow"' | grep 'model="slow-v1"' | grep 'outcome="tenant_backpressure"' >/dev/null

# Tenant identifiers must not become Prometheus labels or plaintext Redis key
# components. The Redis namespace contains only the hashed tenant suffix.
if printf '%s\n' "$metrics" | grep 'taskflow_ai_tenant_distributed_concurrency_total' | grep -q 'tenant='; then
  echo "tenant id leaked into Phase 10 metric labels" >&2
  exit 1
fi
echo "ai phase10 distributed tenant concurrency quota: ok"
