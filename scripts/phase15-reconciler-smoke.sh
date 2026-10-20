#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

MAIN_URL="${BASE_URL:-http://127.0.0.1:8080}"
PEER_URL="${PHASE15_PEER_URL:-http://127.0.0.1:18080}"
QUEUE_KEY="phase15:queue"
LOCK_KEY="phase15:reconciler:leader"
PEER="taskflow-phase15-peer"
phase="setup"

cleanup_db() {
  docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow >/dev/null 2>&1 <<'SQL' || true
DELETE FROM ai_route_audits WHERE task_id IN (SELECT id FROM tasks WHERE name LIKE 'phase15-%');
DELETE FROM outbox_events WHERE task_id IN (SELECT id FROM tasks WHERE name LIKE 'phase15-%');
DELETE FROM tasks WHERE name LIKE 'phase15-%';
SQL
}

restore() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "phase15 smoke failed during phase: $phase" >&2
    docker logs --tail 120 taskflow-go-taskflow-1 >&2 2>/dev/null || true
    docker logs --tail 120 "$PEER" >&2 2>/dev/null || true
  fi
  docker rm -f "$PEER" >/dev/null 2>&1 || true
  docker compose up -d redis mysql executor-worker >/dev/null 2>&1 || true
  cleanup_db
  docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" "$LOCK_KEY" >/dev/null 2>&1 || true
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  return "$rc"
}
trap restore EXIT

wait_ready_url() {
  url="$1"
  i=0
  while [ "$i" -lt 160 ]; do
    if curl -fsS "$url/health/ready" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.1
  done
  return 1
}

metric_value() {
  url="$1"
  metric="$2"
  outcome="${3:-}"
  if [ -n "$outcome" ]; then
    curl -fsS "$url/metrics" | awk -v metric="$metric" -v outcome="$outcome" '
      index($0, metric "{") == 1 && index($0, "outcome=\"" outcome "\"") { print $NF; exit }
    '
  else
    curl -fsS "$url/metrics" | awk -v metric="$metric" '$1 == metric { print $2; exit }'
  fi
}

wait_coordination() {
  i=0
  while [ "$i" -lt 160 ]; do
    main_acquired="$(metric_value "$MAIN_URL" taskflow_reconciler_coordination_total acquired 2>/dev/null || true)"
    peer_acquired="$(metric_value "$PEER_URL" taskflow_reconciler_coordination_total acquired 2>/dev/null || true)"
    main_contended="$(metric_value "$MAIN_URL" taskflow_reconciler_coordination_total contended 2>/dev/null || true)"
    peer_contended="$(metric_value "$PEER_URL" taskflow_reconciler_coordination_total contended 2>/dev/null || true)"
    if awk -v ma="${main_acquired:-0}" -v pa="${peer_acquired:-0}" -v mc="${main_contended:-0}" -v pc="${peer_contended:-0}" 'BEGIN { exit !(((ma+0)>0 && (pc+0)>0) || ((pa+0)>0 && (mc+0)>0)) }'; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.1
  done
  return 1
}

insert_tasks() {
  prefix="$1"
  count="$2"
  i=1
  while [ "$i" -le "$count" ]; do
    docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -e \
      "INSERT INTO tasks (name,type,payload,status,result,retry_count,last_error,trace_parent,trace_state,next_run_at,lease_until,created_at,updated_at) VALUES ('phase15-${prefix}-${i}','compute','phase15','pending','',0,'','','',NOW(),NULL,NOW(),NOW());" >/dev/null
    i=$((i + 1))
  done
}

wait_tasks_success() {
  prefix="$1"
  count="$2"
  i=0
  while [ "$i" -lt 240 ]; do
    success="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT COUNT(*) FROM tasks WHERE name LIKE 'phase15-${prefix}-%' AND status='success'" 2>/dev/null | tr -d '\r')"
    if [ "$success" = "$count" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.1
  done
  return 1
}

stream_len() {
  docker compose exec -T redis redis-cli XLEN "$QUEUE_KEY" 2>/dev/null | tr -d '\r'
}

network_name() {
  docker inspect taskflow-go-taskflow-1 --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}'
}

phase="start coordinated replicas"
docker compose up -d redis mysql executor-worker >/dev/null
cleanup_db
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" "$LOCK_KEY" >/dev/null

REDIS_QUEUE_KEY="$QUEUE_KEY" \
RECONCILE_SEC=1 \
RECONCILE_DISTRIBUTED_COORDINATION=true \
RECONCILE_LOCK_KEY="$LOCK_KEY" \
RECONCILE_LEASE_MS=5000 \
WORKER_COUNT=8 \
docker compose up -d --no-deps --force-recreate taskflow >/dev/null

wait_ready_url "$MAIN_URL"
NETWORK="$(network_name)"
docker rm -f "$PEER" >/dev/null 2>&1 || true
docker run -d --name "$PEER" --network "$NETWORK" -p 127.0.0.1:18080:8080 \
  -e HTTP_ADDR=:8080 \
  -e PPROF_ADDR=127.0.0.1:6060 \
  -e 'MYSQL_DSN=taskflow:taskflow@tcp(mysql:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true' \
  -e REDIS_ADDR=redis:6379 \
  -e REDIS_QUEUE_KEY="$QUEUE_KEY" \
  -e QUEUE_BACKEND=redis \
  -e EXECUTION_BACKEND=grpc \
  -e EXECUTOR_GRPC_TARGET=executor-worker:9091 \
  -e WORKER_COUNT=8 \
  -e RECONCILE_SEC=1 \
  -e RECONCILE_DISTRIBUTED_COORDINATION=true \
  -e RECONCILE_LOCK_KEY="$LOCK_KEY" \
  -e RECONCILE_LEASE_MS=5000 \
  -e QUEUE_STATS_INTERVAL_MS=500 \
  -e DRAIN_FENCE_MS=100 \
  -e DRAIN_TIMEOUT_MS=5000 \
  taskflow-go:local >/dev/null

wait_ready_url "$PEER_URL"
wait_coordination

main_acquired="$(metric_value "$MAIN_URL" taskflow_reconciler_coordination_total acquired 2>/dev/null || true)"
if awk -v value="${main_acquired:-0}" 'BEGIN { exit !((value+0)>0) }'; then
  leader="taskflow-go-taskflow-1"
  survivor_url="$PEER_URL"
else
  leader="$PEER"
  survivor_url="$MAIN_URL"
fi

phase="single enqueue under two replicas"
insert_tasks first 20
wait_tasks_success first 20
test "$(stream_len)" = "20"

main_enqueued="$(metric_value "$MAIN_URL" taskflow_reconciler_enqueued_tasks_total 2>/dev/null || true)"
peer_enqueued="$(metric_value "$PEER_URL" taskflow_reconciler_enqueued_tasks_total 2>/dev/null || true)"
awk -v main="${main_enqueued:-0}" -v peer="${peer_enqueued:-0}" 'BEGIN { exit !((main+peer)==20) }'

phase="leader failover"
survivor_acquired_before="$(metric_value "$survivor_url" taskflow_reconciler_coordination_total acquired 2>/dev/null || true)"
docker kill --signal=TERM "$leader" >/dev/null
i=0
while [ "$i" -lt 120 ]; do
  survivor_acquired_after="$(metric_value "$survivor_url" taskflow_reconciler_coordination_total acquired 2>/dev/null || true)"
  if awk -v before="${survivor_acquired_before:-0}" -v after="${survivor_acquired_after:-0}" 'BEGIN { exit !((after+0)>(before+0)) }'; then
    break
  fi
  i=$((i + 1))
  sleep 0.1
done
test "$i" -lt 120

before="$(stream_len)"
insert_tasks failover 5
wait_tasks_success failover 5
after="$(stream_len)"
test "$after" -eq $((before + 5))

lock_ttl="$(docker compose exec -T redis redis-cli PTTL "$LOCK_KEY" 2>/dev/null | tr -d '\r')"
test "$lock_ttl" -gt 0

python3 scripts/phase15-coordination-check.py
phase="complete"
echo "phase15 distributed reconciler coordination: ok"
