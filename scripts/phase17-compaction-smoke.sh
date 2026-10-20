#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
QUEUE_KEY="phase17:queue"
LOCK_KEY="phase17:reconciler:leader"
GROUP="taskflow-workers"
phase="setup"

cleanup_db() {
  docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow >/dev/null 2>&1 <<'SQL' || true
DELETE FROM ai_route_audits WHERE task_id IN (SELECT id FROM tasks WHERE name LIKE 'phase17-%');
DELETE FROM outbox_events WHERE task_id IN (SELECT id FROM tasks WHERE name LIKE 'phase17-%');
DELETE FROM tasks WHERE name LIKE 'phase17-%';
SQL
}

restore() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "phase17 smoke failed during phase: $phase" >&2
    docker logs --tail 180 taskflow-go-taskflow-1 >&2 2>/dev/null || true
  fi
  docker compose up -d redis mysql executor-worker >/dev/null 2>&1 || true
  cleanup_db
  docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" "$LOCK_KEY" >/dev/null 2>&1 || true
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  return "$rc"
}
trap restore EXIT

wait_ready() {
  i=0
  while [ "$i" -lt 200 ]; do
    if curl -fsS "$BASE_URL/health/ready" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.1
  done
  return 1
}

start_taskflow() {
  retention_ms="$1"
  max_entries="$2"
  REDIS_QUEUE_KEY="$QUEUE_KEY" \
  EXECUTION_BACKEND=local \
  WORKER_COUNT=1 \
  OUTBOX_INTERVAL_MS=60000 \
  RECONCILE_SEC=60 \
  RECONCILE_DISTRIBUTED_COORDINATION=true \
  RECONCILE_LOCK_KEY="$LOCK_KEY" \
  RECONCILE_LEASE_MS=5000 \
  QUEUE_STATS_INTERVAL_MS=200 \
  QUEUE_COMPACTION_ENABLED=true \
  QUEUE_COMPACTION_INTERVAL_MS=200 \
  QUEUE_ACK_RETENTION_MS="$retention_ms" \
  QUEUE_ACK_RETENTION_MAX_ENTRIES="$max_entries" \
  DRAIN_FENCE_MS=100 \
  DRAIN_TIMEOUT_MS=5000 \
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null
  wait_ready
}

insert_task() {
  name="$1"
  docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse \
    "INSERT INTO tasks (name,type,payload,status,result,retry_count,last_error,trace_parent,trace_state,enqueue_generation,enqueued_generation,enqueue_lease_until,next_run_at,lease_until,created_at,updated_at) VALUES ('$name','compute','phase17','pending','',0,'','','',1,1,NULL,NOW(),NULL,NOW(),NOW()); SELECT LAST_INSERT_ID();" 2>/dev/null | tail -n 1 | tr -d '\r'
}

enqueue_task() {
  id="$1"
  docker compose exec -T redis redis-cli XADD "$QUEUE_KEY" '*' task_id "$id" enqueue_generation 1 >/dev/null
}

wait_prefix_success() {
  prefix="$1"
  want="$2"
  i=0
  while [ "$i" -lt 240 ]; do
    got="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT COUNT(*) FROM tasks WHERE name LIKE '${prefix}%' AND status='success'" 2>/dev/null | tr -d '\r')"
    if [ "$got" = "$want" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.1
  done
  return 1
}

wait_task_success() {
  id="$1"
  i=0
  while [ "$i" -lt 160 ]; do
    status="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT status FROM tasks WHERE id=$id" 2>/dev/null | tr -d '\r')"
    if [ "$status" = "success" ]; then
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

pending_count() {
  docker compose exec -T redis redis-cli --raw XPENDING "$QUEUE_KEY" "$GROUP" 2>/dev/null | sed -n '1p' | tr -d '\r'
}

wait_stream_at_most() {
  max="$1"
  i=0
  while [ "$i" -lt 200 ]; do
    len="$(stream_len)"
    if [ "$len" -le "$max" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.1
  done
  return 1
}

wait_pending() {
  want="$1"
  i=0
  while [ "$i" -lt 200 ]; do
    got="$(pending_count)"
    if [ "$got" = "$want" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.1
  done
  return 1
}

entry_generation() {
  entry_id="$1"
  docker compose exec -T redis redis-cli --json XRANGE "$QUEUE_KEY" "$entry_id" "$entry_id" 2>/dev/null | \
    python3 -c 'import json,sys
rows=json.load(sys.stdin)
if not rows:
    print("")
else:
    fields=rows[0][1]
    values=dict(zip(fields[::2], fields[1::2]))
    print(values.get("enqueue_generation", ""))'
}

phase="start dependencies"
docker compose up -d redis mysql executor-worker >/dev/null
cleanup_db
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" "$LOCK_KEY" >/dev/null

# Scenario A: ordinary ACKed history is bounded by count.
phase="bounded acknowledged history"
start_taskflow 0 5
i=1
while [ "$i" -le 12 ]; do
  id="$(insert_task "phase17-baseline-$i")"
  enqueue_task "$id"
  i=$((i + 1))
done
wait_prefix_success "phase17-baseline-" 12
wait_pending 0
wait_stream_at_most 5
test "$(stream_len)" -le 5

trimmed="$(curl -fsS "$BASE_URL/metrics" | awk '$1 == "taskflow_queue_compaction_trimmed_entries_total" {print $2; exit}')"
awk -v value="${trimmed:-0}" 'BEGIN { exit !((value+0)>0) }'

# Scenario B: a delivered-but-unacked entry survives compaction, then the real
# worker recovers it through XAUTOCLAIM and the stream returns to the bound.
phase="pending preservation and crash recovery"
docker compose stop taskflow >/dev/null
protected_id="$(insert_task phase17-protected)"
protected_entry="$(docker compose exec -T redis redis-cli XADD "$QUEUE_KEY" '*' task_id "$protected_id" enqueue_generation 1 2>/dev/null | tr -d '\r')"
docker compose exec -T redis redis-cli XREADGROUP GROUP "$GROUP" phase17-crashed COUNT 1 STREAMS "$QUEUE_KEY" '>' >/dev/null
test "$(pending_count)" = "1"
test "$(entry_generation "$protected_entry")" = "1"

i=1
while [ "$i" -le 12 ]; do
  id="$(insert_task "phase17-followup-$i")"
  enqueue_task "$id"
  i=$((i + 1))
done
start_taskflow 0 5
wait_prefix_success "phase17-followup-" 12
sleep 0.8
test "$(pending_count)" = "1"
test "$(entry_generation "$protected_entry")" = "1"
test "$(stream_len)" -gt 5

docker compose exec -T redis redis-cli XCLAIM "$QUEUE_KEY" "$GROUP" phase17-crashed 0 "$protected_entry" IDLE 60000 JUSTID >/dev/null
wait_task_success "$protected_id"
wait_pending 0
wait_stream_at_most 5
test "$(stream_len)" -le 5

# Scenario C: every consumer group protects its own undelivered work.
phase="multi-group undelivered preservation"
docker compose exec -T redis redis-cli XGROUP CREATE "$QUEUE_KEY" phase17-audit 0 >/dev/null
i=1
while [ "$i" -le 10 ]; do
  id="$(insert_task "phase17-multigroup-$i")"
  enqueue_task "$id"
  i=$((i + 1))
done
wait_prefix_success "phase17-multigroup-" 10
sleep 0.8
test "$(stream_len)" -gt 5
docker compose exec -T redis redis-cli XGROUP DESTROY "$QUEUE_KEY" phase17-audit >/dev/null
wait_stream_at_most 5

# Scenario D: time retention independently compacts old ACKed history when the
# count limit is not reached. One last-delivered safety anchor remains.
phase="time based retention"
docker compose stop taskflow >/dev/null
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" "$LOCK_KEY" >/dev/null
start_taskflow 500 1000
i=1
while [ "$i" -le 4 ]; do
  docker compose exec -T redis redis-cli XADD "$QUEUE_KEY" '*' task_id "$((9900000 + i))" enqueue_generation 1 >/dev/null
  i=$((i + 1))
done
wait_pending 0
sleep 1
wait_stream_at_most 1
test "$(stream_len)" -le 1

python3 scripts/phase17-compaction-check.py
phase="complete"
echo "phase17 Redis Stream acknowledged-history compaction: ok"
