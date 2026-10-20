#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

PREFIX="phase7:ai:concurrency"
BUILDER_IMAGE="taskflow-go:phase7-builder"

restore() {
  docker compose --profile phase7-smoke rm -sf executor-worker-alt >/dev/null 2>&1 || true
  docker compose --profile ai-smoke stop ai-gateway-slow >/dev/null 2>&1 || true
  docker compose --profile ai-smoke up -d --force-recreate executor-worker >/dev/null 2>&1 || true
  docker compose exec -T redis redis-cli --scan --pattern "${PREFIX}:*" 2>/dev/null | while IFS= read -r key; do
    [ -n "$key" ] && docker compose exec -T redis redis-cli DEL "$key" >/dev/null 2>&1 || true
  done
}
trap restore EXIT

docker compose --profile ai-smoke --profile phase7-smoke up -d redis ai-gateway ai-gateway-slow >/dev/null
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
AI_GLOBAL_MAX_CONCURRENCY=1 \
AI_FALLBACK_PROVIDER=mock-fast \
AI_FALLBACK_KIND=gateway \
AI_FALLBACK_ENDPOINT=http://ai-gateway:9000/generate \
AI_FALLBACK_MODEL=mock-v1 \
AI_FALLBACK_CAPABILITIES=text,reasoning \
AI_FALLBACK_INPUT_COST_PER_1M=10 \
AI_FALLBACK_OUTPUT_COST_PER_1M=10 \
AI_FALLBACK_MAX_CONCURRENCY=4 \
AI_FALLBACK_GLOBAL_MAX_CONCURRENCY=0 \
AI_ROUTING_STRATEGY=cost \
AI_SLO_REALTIME_QUEUE_MS=100 \
AI_SLO_REALTIME_TIMEOUT_MS=2500 \
AI_SLO_STANDARD_QUEUE_MS=1000 \
AI_SLO_STANDARD_TIMEOUT_MS=4000 \
AI_DISTRIBUTED_LEASE_MS=600 \
AI_CONCURRENCY_REDIS_PREFIX="$PREFIX" \
AI_ADAPTIVE_LOAD_SHEDDING=true \
AI_HEDGE_ENABLED=false \
docker compose --profile ai-smoke --profile phase7-smoke up -d --force-recreate executor-worker executor-worker-alt >/dev/null

i=0
while [ "$i" -lt 40 ]; do
  if curl -fsS http://127.0.0.1:19093/metrics >/dev/null 2>&1 && curl -fsS http://127.0.0.1:19194/metrics >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  sleep 0.25
done
test "$i" -lt 40

docker build --target build \
  --build-arg GOPROXY="${GOPROXY:-https://goproxy.cn,direct}" \
  -t "$BUILDER_IMAGE" . >/dev/null

docker run --rm --network host "$BUILDER_IMAGE" \
  /out/ai-phase7-client shared 127.0.0.1:19091 127.0.0.1:19192 \
  | grep -q 'phase7 distributed concurrency: ok'

metrics="$(curl -fsS http://127.0.0.1:19194/metrics)"
printf '%s\n' "$metrics" | grep 'taskflow_ai_global_concurrency_total' | grep 'provider="mock-slow"' | grep 'model="slow-v1"' | grep 'outcome="global_backpressure"' >/dev/null
printf '%s\n' "$metrics" | grep 'taskflow_ai_global_concurrency_limit' | grep 'provider="mock-slow"' | grep ' 1$' >/dev/null

# Crash recovery: start a slow call on worker A, wait until its Redis lease is
# visible, then SIGKILL the worker. Worker B must reclaim the expired lease.
docker run --rm --network host "$BUILDER_IMAGE" \
  /out/ai-phase7-client call 127.0.0.1:19091 crash-holder slow-v1 standard slow-ai:crash-holder \
  >/tmp/taskflow-phase7-crash-client.log 2>&1 &
crash_client_pid=$!

i=0
while [ "$i" -lt 40 ]; do
  leases="$(docker compose exec -T redis redis-cli ZCARD "${PREFIX}:mock-slow:slow-v1" | tr -d '\r')"
  if [ "${leases:-0}" -ge 1 ]; then
    break
  fi
  i=$((i + 1))
  sleep 0.025
done
test "$i" -lt 40

docker compose kill -s KILL executor-worker >/dev/null
wait "$crash_client_pid" 2>/dev/null || true
sleep 0.8

docker run --rm --network host "$BUILDER_IMAGE" \
  /out/ai-phase7-client call 127.0.0.1:19192 recovered slow-v1 standard slow-ai:recovered \
  | grep -q 'slow-ai:recovered'

# Adaptive load shedding: one local model slot is occupied, two batch requests
# queue, and the next batch request is rejected before provider execution.
AI_PROVIDER=mock-slow \
AI_PROVIDER_KIND=gateway \
AI_ENDPOINT=http://ai-gateway-slow:9000/generate \
AI_MODEL=slow-v1 \
AI_CAPABILITIES=text,reasoning \
AI_MAX_CONCURRENCY=1 \
AI_GLOBAL_MAX_CONCURRENCY=0 \
AI_FALLBACK_ENDPOINT= \
AI_ADAPTIVE_LOAD_SHEDDING=true \
AI_PRESSURE_DEGRADED_QUEUE=1 \
AI_PRESSURE_OVERLOADED_QUEUE=2 \
AI_PRESSURE_DEGRADED_LATENCY_MS=10000 \
AI_PRESSURE_OVERLOADED_LATENCY_MS=20000 \
AI_SLO_STANDARD_QUEUE_MS=2000 \
AI_SLO_STANDARD_TIMEOUT_MS=4000 \
AI_SLO_BATCH_QUEUE_MS=3000 \
AI_SLO_BATCH_TIMEOUT_MS=5000 \
AI_HEDGE_ENABLED=false \
docker compose --profile phase7-smoke up -d --force-recreate executor-worker-alt >/dev/null

i=0
while [ "$i" -lt 40 ]; do
  if curl -fsS http://127.0.0.1:19194/metrics >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  sleep 0.1
done
test "$i" -lt 40

docker run --rm --network host "$BUILDER_IMAGE" \
  /out/ai-phase7-client shed 127.0.0.1:19192 \
  | grep -q 'phase7 adaptive load shedding: ok'

metrics="$(curl -fsS http://127.0.0.1:19194/metrics)"
printf '%s\n' "$metrics" | grep 'taskflow_ai_load_shedding_total' | grep 'slo_class="batch"' | grep 'pressure="overloaded"' | grep 'action="reject"' >/dev/null
printf '%s\n' "$metrics" | grep 'taskflow_ai_system_pressure_state{state="overloaded"}' >/dev/null

echo "ai phase7 distributed concurrency + crash recovery + adaptive load shedding: ok"

