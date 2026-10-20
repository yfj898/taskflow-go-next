#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
QUEUE_KEY="phase16:queue"
LOCK_KEY="phase16:reconciler:leader"
phase="setup"

cleanup_db() {
  docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow >/dev/null 2>&1 <<'SQL' || true
DELETE FROM ai_route_audits WHERE task_id IN (SELECT id FROM tasks WHERE name LIKE 'phase16-%');
DELETE FROM outbox_events WHERE task_id IN (SELECT id FROM tasks WHERE name LIKE 'phase16-%');
DELETE FROM tasks WHERE name LIKE 'phase16-%';
SQL
}

restore() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "phase16 smoke failed during phase: $phase" >&2
    docker logs --tail 160 taskflow-go-taskflow-1 >&2 2>/dev/null || true
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

create_task() {
  name="$1"
  type="$2"
  response="$(curl -fsS -X POST "$BASE_URL/api/v1/tasks" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"$name\",\"type\":\"$type\",\"payload\":\"phase16\"}")"
  printf '%s' "$response" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p'
}

wait_status() {
  id="$1"
  want="$2"
  limit="${3:-200}"
  i=0
  while [ "$i" -lt "$limit" ]; do
    status="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT status FROM tasks WHERE id=$id" 2>/dev/null | tr -d '\r')"
    if [ "$status" = "$want" ]; then
      return 0
    fi
    if [ "$status" = "failed" ] && [ "$want" != "failed" ]; then
      return 1
    fi
    i=$((i + 1))
    sleep 0.1
  done
  return 1
}

wait_outbox_published() {
  id="$1"
  limit="${2:-100}"
  i=0
  while [ "$i" -lt "$limit" ]; do
    published="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT COUNT(*) FROM outbox_events WHERE task_id=$id AND published_at IS NOT NULL" 2>/dev/null | tr -d '\r')"
    if [ "$published" = "1" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.1
  done
  return 1
}

stream_count() {
  id="$1"
  generation="$2"
  docker compose exec -T redis redis-cli --json XRANGE "$QUEUE_KEY" - + 2>/dev/null | \
    python3 -c 'import json,sys
task_id=sys.argv[1]; generation=sys.argv[2]
rows=json.load(sys.stdin)
count=0
for _, fields in rows:
    values=dict(zip(fields[::2], fields[1::2]))
    if values.get("task_id")==task_id and values.get("enqueue_generation")==generation:
        count += 1
print(count)' "$id" "$generation"
}

start_taskflow() {
  reconcile_sec="$1"
  outbox_ms="$2"
  REDIS_QUEUE_KEY="$QUEUE_KEY" \
  EXECUTION_BACKEND=local \
  WORKER_COUNT=1 \
  RECONCILE_SEC="$reconcile_sec" \
  RECONCILE_DISTRIBUTED_COORDINATION=true \
  RECONCILE_LOCK_KEY="$LOCK_KEY" \
  RECONCILE_LEASE_MS=5000 \
  ENQUEUE_VISIBILITY_LEASE_MS=3000 \
  OUTBOX_INTERVAL_MS="$outbox_ms" \
  DRAIN_FENCE_MS=100 \
  DRAIN_TIMEOUT_MS=5000 \
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null
  wait_ready
}

phase="start dependencies"
docker compose up -d redis mysql executor-worker >/dev/null
cleanup_db
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" "$LOCK_KEY" >/dev/null

# Scenario A: the reconciler wins generation 1 while the outbox is deliberately
# slower. The outbox must observe broker visibility and settle without another
# Stream append. Keeping one worker occupied leaves the target pending through
# several reconcile scans, proving repeated scans do not append duplicates.
phase="cross-path dedupe and retry generation"
start_taskflow 1 1500
holder_id="$(create_task phase16-holder slow)"
test -n "$holder_id"
wait_status "$holder_id" running 80

target_id="$(create_task phase16-fail-once fail_once)"
test -n "$target_id"
sleep 3
target_state="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT CONCAT(status,':',enqueue_generation,':',enqueued_generation) FROM tasks WHERE id=$target_id" 2>/dev/null | tr -d '\r')"
test "$target_state" = "pending:1:1"
test "$(stream_count "$target_id" 1)" = "1"

outbox_published="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT COUNT(*) FROM outbox_events WHERE task_id=$target_id AND published_at IS NOT NULL" 2>/dev/null | tr -d '\r')"
test "$outbox_published" = "1"

wait_status "$target_id" success 180
retry_state="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT CONCAT(status,':',retry_count,':',enqueue_generation,':',enqueued_generation) FROM tasks WHERE id=$target_id" 2>/dev/null | tr -d '\r')"
test "$retry_state" = "success:1:2:2"
test "$(stream_count "$target_id" 1)" = "1"
test "$(stream_count "$target_id" 2)" = "1"

# Scenario B: a stale generation-1 delivery must not claim a task whose current
# pending generation is 2. Only the matching generation-2 delivery may run it.
phase="stale generation execution fence"
docker compose stop taskflow >/dev/null
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" "$LOCK_KEY" >/dev/null
docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -e \
  "INSERT INTO tasks (name,type,payload,status,result,retry_count,last_error,trace_parent,trace_state,enqueue_generation,enqueued_generation,enqueue_lease_until,next_run_at,lease_until,created_at,updated_at) VALUES ('phase16-stale','compute','phase16','pending','',0,'','','',2,2,NULL,NOW(),NULL,NOW(),NOW());" >/dev/null
stale_id="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT id FROM tasks WHERE name='phase16-stale' ORDER BY id DESC LIMIT 1" 2>/dev/null | tr -d '\r')"
start_taskflow 60 60000
docker compose exec -T redis redis-cli XADD "$QUEUE_KEY" '*' task_id "$stale_id" enqueue_generation 1 >/dev/null
sleep 1
stale_status="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT status FROM tasks WHERE id=$stale_id" 2>/dev/null | tr -d '\r')"
test "$stale_status" = "pending"
docker compose exec -T redis redis-cli XADD "$QUEUE_KEY" '*' task_id "$stale_id" enqueue_generation 2 >/dev/null
wait_status "$stale_id" success 80

# Scenario C: simulate a publisher crash after obtaining the DB enqueue lease
# but before broker I/O. The task must remain pending while the lease is live,
# then be rediscovered and published after the lease expires.
phase="expired enqueue lease recovery"
docker compose stop taskflow >/dev/null
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" "$LOCK_KEY" >/dev/null
docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -e \
  "INSERT INTO tasks (name,type,payload,status,result,retry_count,last_error,trace_parent,trace_state,enqueue_generation,enqueued_generation,enqueue_lease_until,next_run_at,lease_until,created_at,updated_at) VALUES ('phase16-lease-recovery','compute','phase16','pending','',0,'','','',3,2,DATE_ADD(NOW(), INTERVAL 2 SECOND),NOW(),NULL,NOW(),NOW());" >/dev/null
lease_id="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT id FROM tasks WHERE name='phase16-lease-recovery' ORDER BY id DESC LIMIT 1" 2>/dev/null | tr -d '\r')"
start_taskflow 1 60000
sleep 0.7
lease_status="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT status FROM tasks WHERE id=$lease_id" 2>/dev/null | tr -d '\r')"
test "$lease_status" = "pending"
test "$(stream_count "$lease_id" 3)" = "0"
wait_status "$lease_id" success 80
test "$(stream_count "$lease_id" 3)" = "1"

# Scenario D: a publisher may crash after broker acceptance while a worker
# consumes the message before the MySQL visibility confirmation. The durable
# non-pending task state proves that generation reached a worker, so a later
# outbox retry must converge the visibility marker and event without publishing
# another broker message or retrying the outbox row forever.
phase="execution proves broker visibility"
docker compose stop taskflow >/dev/null
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" "$LOCK_KEY" >/dev/null
docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -e \
  "INSERT INTO tasks (name,type,payload,status,result,retry_count,last_error,trace_parent,trace_state,enqueue_generation,enqueued_generation,enqueue_lease_until,next_run_at,lease_until,created_at,updated_at) VALUES ('phase16-executed-before-confirm','compute','phase16','success','already executed',0,'','','',1,0,NULL,NOW(),NULL,NOW(),NOW());" >/dev/null
executed_id="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT id FROM tasks WHERE name='phase16-executed-before-confirm' ORDER BY id DESC LIMIT 1" 2>/dev/null | tr -d '\r')"
docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -e \
  "INSERT INTO outbox_events (task_id,event_type,attempts,last_error,published_at,lease_until,created_at,updated_at) VALUES ($executed_id,'task.created',0,'',NULL,NULL,NOW(),NOW());" >/dev/null
start_taskflow 60 100
wait_outbox_published "$executed_id" 80
executed_state="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT CONCAT(status,':',enqueue_generation,':',enqueued_generation) FROM tasks WHERE id=$executed_id" 2>/dev/null | tr -d '\r')"
test "$executed_state" = "success:1:1"
test "$(stream_count "$executed_id" 1)" = "0"

python3 scripts/phase16-fencing-check.py
phase="complete"
echo "phase16 per-task enqueue visibility and generation fencing: ok"
