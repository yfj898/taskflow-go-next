#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
EXECUTOR_METRICS_URL="${EXECUTOR_METRICS_URL:-http://127.0.0.1:19093/metrics}"
phase="setup"

cleanup_tasks() {
  docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow >/dev/null 2>&1 <<'SQL' || true
DELETE FROM ai_route_audits WHERE task_id IN (SELECT id FROM tasks WHERE name LIKE 'phase13-%');
DELETE FROM outbox_events WHERE task_id IN (SELECT id FROM tasks WHERE name LIKE 'phase13-%');
DELETE FROM tasks WHERE name LIKE 'phase13-%';
SQL
}

restore() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "phase13 smoke failed during phase: $phase" >&2
    docker compose --profile ai-smoke ps -a taskflow executor-worker ai-gateway-slow redis mysql >&2 2>/dev/null || true
    echo "--- taskflow logs ---" >&2
    docker logs --tail 120 taskflow-go-taskflow-1 >&2 2>/dev/null || true
    echo "--- executor logs ---" >&2
    docker logs --tail 120 taskflow-go-executor-worker-1 >&2 2>/dev/null || true
  fi
  docker compose --profile ai-smoke up -d redis mysql >/dev/null 2>&1 || true
  docker compose --profile ai-smoke stop taskflow executor-worker >/dev/null 2>&1 || true
  docker compose --profile ai-smoke stop ai-gateway-slow >/dev/null 2>&1 || true
  docker compose --profile phase7-smoke rm -sf executor-worker-alt >/dev/null 2>&1 || true
  cleanup_tasks
  docker compose --profile ai-smoke up -d --force-recreate executor-worker taskflow >/dev/null 2>&1 || true
  return "$rc"
}
trap restore EXIT

metric_value() {
  url="$1"
  metric="$2"
  curl -fsS "$url" | awk -v metric="$metric" '$1 == metric { print $2; exit }'
}

wait_metric() {
  url="$1"
  metric="$2"
  wanted="$3"
  attempts="${4:-160}"
  i=0
  while [ "$i" -lt "$attempts" ]; do
    value="$(metric_value "$url" "$metric" 2>/dev/null || true)"
    if [ "$value" = "$wanted" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.05
  done
  echo "metric $metric did not become $wanted" >&2
  return 1
}

wait_ready() {
  i=0
  while [ "$i" -lt 120 ]; do
    if curl -fsS "$BASE_URL/health/ready" >/dev/null 2>&1 && curl -fsS "$EXECUTOR_METRICS_URL" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.1
  done
  return 1
}

create_task() {
  prompt="$1"
  response="$(curl -fsS -X POST "$BASE_URL/api/v1/tasks" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"phase13-$prompt\",\"type\":\"ai_generate\",\"payload\":\"{\\\"prompt\\\":\\\"$prompt\\\",\\\"slo_class\\\":\\\"standard\\\"}\"}")"
  printf '%s' "$response" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p'
}

wait_task_status() {
  id="$1"
  wanted="$2"
  attempts="${3:-240}"
  i=0
  task=""
  while [ "$i" -lt "$attempts" ]; do
    task="$(curl -fsS "$BASE_URL/api/v1/tasks/$id" 2>/dev/null || true)"
    case "$task" in
      *"\"status\":\"$wanted\""*) printf '%s' "$task"; return 0 ;;
      *'"status":"failed"'*) printf '%s\n' "$task" >&2; return 1 ;;
    esac
    i=$((i + 1))
    sleep 0.05
  done
  printf '%s\n' "$task" >&2
  return 1
}

wait_container_exit_zero() {
  name="$1"
  i=0
  while [ "$i" -lt 240 ]; do
    state="$(docker inspect "$name" --format '{{.State.Status}} {{.State.ExitCode}}' 2>/dev/null || true)"
    case "$state" in
      'exited 0') return 0 ;;
      exited\ *) echo "$name exited non-zero: $state" >&2; return 1 ;;
    esac
    i=$((i + 1))
    sleep 0.05
  done
  echo "$name did not exit during drain window" >&2
  return 1
}

db_status() {
  id="$1"
  docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT status FROM tasks WHERE id=$id" 2>/dev/null | tr -d '\r'
}

start_slow_executor() {
  AI_PROVIDER=mock-slow \
  AI_PROVIDER_KIND=gateway \
  AI_ENDPOINT=http://ai-gateway-slow:9000/generate \
  AI_MODEL=slow-v1 \
  AI_CAPABILITIES=text,reasoning \
  AI_MAX_CONCURRENCY=2 \
  AI_FALLBACK_ENDPOINT= \
  AI_ADAPTIVE_LOAD_SHEDDING=false \
  AI_HEDGE_ENABLED=false \
  AI_SLO_STANDARD_QUEUE_MS=1000 \
  AI_SLO_STANDARD_TIMEOUT_MS=9000 \
  AI_TIMEOUT_MS=9000 \
  DRAIN_TIMEOUT_MS=7000 \
  docker compose --profile ai-smoke up -d --force-recreate executor-worker >/dev/null
}

start_taskflow() {
  WORKER_COUNT=1 \
  AI_TIMEOUT_MS=9000 \
  DRAIN_FENCE_MS=500 \
  DRAIN_TIMEOUT_MS=7000 \
  QUEUE_STATS_INTERVAL_MS=500 \
  docker compose --profile ai-smoke up -d --no-deps --force-recreate taskflow >/dev/null
}

cleanup_tasks
phase="start slow stack"
AI_GATEWAY_SLOW_DELAY_MS=4000 \
docker compose --profile ai-smoke up -d --force-recreate redis mysql ai-gateway-slow >/dev/null
start_slow_executor
start_taskflow
wait_ready

# Part A: executor drain. An active RPC must finish while health flips away
# from SERVING and new RPC admission is fenced by gRPC GracefulStop.
phase="executor holder running"
executor_task="$(create_task executor-holder)"
test -n "$executor_task"
wait_task_status "$executor_task" running >/dev/null
wait_metric "$EXECUTOR_METRICS_URL" taskflow_executor_inflight 1

phase="executor draining"
docker kill --signal=TERM taskflow-go-executor-worker-1 >/dev/null
wait_metric "$EXECUTOR_METRICS_URL" taskflow_process_draining 1
test "$(docker inspect taskflow-go-executor-worker-1 --format '{{.State.Running}}')" = "true"

i=0
while [ "$i" -lt 60 ]; do
  code="$(curl -sS -o /tmp/phase13-ready.json -w '%{http_code}' "$BASE_URL/health/ready" || true)"
  if [ "$code" = "503" ]; then
    break
  fi
  i=$((i + 1))
  sleep 0.05
done
test "$i" -lt 60

wait_task_status "$executor_task" success >/dev/null
wait_container_exit_zero taskflow-go-executor-worker-1
docker logs taskflow-go-executor-worker-1 2>&1 | grep -q 'executor-worker drain completed'

# Restart the executor with the same slow backend for the TaskFlow drain half.
start_slow_executor
wait_ready

# Part B: TaskFlow queue-consumer drain. The already-running holder must finish,
# while a follower that has not been claimed must remain pending until restart.
phase="taskflow holder running"
holder="$(create_task taskflow-holder)"
test -n "$holder"
wait_task_status "$holder" running >/dev/null
follower="$(create_task taskflow-follower)"
test -n "$follower"
wait_task_status "$follower" pending >/dev/null
wait_metric "$BASE_URL/metrics" taskflow_worker_inflight 1

phase="taskflow draining"
docker kill --signal=TERM taskflow-go-taskflow-1 >/dev/null
wait_metric "$BASE_URL/metrics" taskflow_process_draining 1
wait_metric "$BASE_URL/metrics" taskflow_worker_inflight 1
test "$(docker inspect taskflow-go-taskflow-1 --format '{{.State.Running}}')" = "true"

i=0
while [ "$i" -lt 50 ]; do
  code="$(curl -sS -o /tmp/phase13-draining-ready.json -w '%{http_code}' "$BASE_URL/health/ready" || true)"
  if [ "$code" = "503" ] && grep -q '"reason":"draining"' /tmp/phase13-draining-ready.json; then
    break
  fi
  i=$((i + 1))
  sleep 0.05
done
test "$i" -lt 50

wait_container_exit_zero taskflow-go-taskflow-1
docker logs taskflow-go-taskflow-1 2>&1 | grep -q 'taskflow drain completed'
test "$(db_status "$holder")" = "success"
test "$(db_status "$follower")" = "pending"

# Restarting a healthy consumer must pick up the fenced follower normally.
phase="follower recovery"
start_taskflow
wait_ready
wait_task_status "$follower" success >/dev/null

python3 scripts/phase13-k8s-check.py
rm -f /tmp/phase13-ready.json /tmp/phase13-draining-ready.json
phase="complete"
echo "phase13 graceful scale-down drain fencing: ok"
