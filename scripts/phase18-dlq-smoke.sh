#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
QUEUE_KEY="phase18:queue"
LOCK_KEY="phase18:reconciler:leader"
GROUP="taskflow-workers"
phase="setup"

cleanup_db() {
  docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow >/dev/null 2>&1 <<SQL || true
DELETE FROM queue_dead_letter_replay_audits WHERE source_queue = '$QUEUE_KEY';
DELETE FROM queue_dead_letters WHERE source_queue = '$QUEUE_KEY';
DELETE FROM ai_route_audits WHERE task_id IN (SELECT id FROM tasks WHERE name LIKE 'phase18-%');
DELETE FROM outbox_events WHERE task_id IN (SELECT id FROM tasks WHERE name LIKE 'phase18-%');
DELETE FROM tasks WHERE name LIKE 'phase18-%';
SQL
}

restore() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "phase18 smoke failed during phase: $phase" >&2
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

wait_dlq_class() {
  class="$1"
  want="$2"
  i=0
  while [ "$i" -lt 200 ]; do
    got="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT COUNT(*) FROM queue_dead_letters WHERE source_queue='$QUEUE_KEY' AND failure_class='$class'" 2>/dev/null | tr -d '\r')"
    if [ "$got" -ge "$want" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.1
  done
  return 1
}

pending_count() {
  docker compose exec -T redis redis-cli --raw XPENDING "$QUEUE_KEY" "$GROUP" 2>/dev/null | sed -n '1p' | tr -d '\r'
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

wait_pending_zero() {
  i=0
  while [ "$i" -lt 120 ]; do
    got="$(pending_count || true)"
    if [ "${got:-0}" = "0" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.1
  done
  return 1
}

insert_task() {
  name="$1"
  generation="$2"
  docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse \
    "INSERT INTO tasks (name,type,payload,status,result,retry_count,last_error,trace_parent,trace_state,enqueue_generation,enqueued_generation,enqueue_lease_until,next_run_at,lease_until,created_at,updated_at) VALUES ('$name','compute','phase18','pending','',0,'','','',$generation,0,NULL,NOW(),NULL,NOW(),NOW()); SELECT LAST_INSERT_ID();" 2>/dev/null | tail -n 1 | tr -d '\r'
}

wait_task_status() {
  id="$1"
  want="$2"
  i=0
  while [ "$i" -lt 160 ]; do
    got="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT status FROM tasks WHERE id=$id" 2>/dev/null | tr -d '\r')"
    if [ "$got" = "$want" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.1
  done
  return 1
}

phase="start isolated queue"
docker compose up -d redis mysql executor-worker >/dev/null
cleanup_db
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" "$LOCK_KEY" >/dev/null
REDIS_QUEUE_KEY="$QUEUE_KEY" \
EXECUTION_BACKEND=local \
WORKER_COUNT=1 \
OUTBOX_INTERVAL_MS=60000 \
RECONCILE_SEC=60 \
RECONCILE_LOCK_KEY="$LOCK_KEY" \
RECONCILE_LEASE_MS=5000 \
QUEUE_COMPACTION_ENABLED=false \
QUEUE_DLQ_REPLAY_LEASE_MS=3000 \
DRAIN_FENCE_MS=100 \
DRAIN_TIMEOUT_MS=5000 \
docker compose up -d --no-deps --force-recreate taskflow >/dev/null
wait_ready

# Scenario A: malformed envelope is durably quarantined before the original
# Redis delivery is ACKed. It must not hot-loop into duplicate DLQ rows.
phase="malformed envelope quarantine"
bad_entry="$(docker compose exec -T redis redis-cli XADD "$QUEUE_KEY" '*' task_id not-a-number enqueue_generation 1 marker phase18-bad 2>/dev/null | tr -d '\r')"
test -n "$bad_entry"
wait_dlq_class decode 1
wait_pending_zero
sleep 0.5
decode_count="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT COUNT(*) FROM queue_dead_letters WHERE source_queue='$QUEUE_KEY' AND failure_class='decode'" 2>/dev/null | tr -d '\r')"
test "$decode_count" = "1"
raw="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT raw_envelope FROM queue_dead_letters WHERE source_queue='$QUEUE_KEY' AND source_delivery_id='$bad_entry'" 2>/dev/null | tr -d '\r')"
printf '%s' "$raw" | grep -q 'not-a-number'

# Scenario B: a syntactically valid envelope that references no durable MySQL
# task is poison too; it must be diagnosed rather than silently ACKed.
phase="orphan delivery quarantine"
docker compose exec -T redis redis-cli XADD "$QUEUE_KEY" '*' task_id 999999999 enqueue_generation 1 marker phase18-orphan >/dev/null
wait_dlq_class orphan 1
wait_pending_zero

# Scenario C: repair a malformed generation at replay time. The API validates
# the corrected task generation against MySQL before publishing it back through
# the normal Phase 16 queue envelope.
phase="controlled repaired replay"
repair_task="$(insert_task phase18-repair 3)"
repair_entry="$(docker compose exec -T redis redis-cli XADD "$QUEUE_KEY" '*' task_id "$repair_task" enqueue_generation broken marker phase18-repair 2>/dev/null | tr -d '\r')"
wait_dlq_class decode 2
repair_dlq="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT id FROM queue_dead_letters WHERE source_queue='$QUEUE_KEY' AND source_delivery_id='$repair_entry'" 2>/dev/null | tr -d '\r')"
test -n "$repair_dlq"

replayed="$(curl -fsS -X POST "$BASE_URL/api/v1/queue/dead-letters/$repair_dlq/replay" \
  -H 'Content-Type: application/json' \
  -d "{\"task_id\":$repair_task,\"enqueue_generation\":3}")"
printf '%s' "$replayed" | grep -q '"replay_state":"replayed"'
printf '%s' "$replayed" | grep -q '"replay_count":1'
wait_task_status "$repair_task" success

repair_visibility="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT CONCAT(status,':',enqueue_generation,':',enqueued_generation) FROM tasks WHERE id=$repair_task" 2>/dev/null | tr -d '\r')"
test "$repair_visibility" = "success:3:3"
test "$(stream_count "$repair_task" 3)" = "1"

replay_row="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT CONCAT(replay_state,':',replay_count,':',replayed_at IS NOT NULL) FROM queue_dead_letters WHERE id=$repair_dlq" 2>/dev/null | tr -d '\r')"
test "$replay_row" = "replayed:1:1"

# Scenario D: an operator cannot use replay to bypass Phase 16. A generation
# that is stale relative to the current pending task is rejected before queue
# publication and leaves both task and DLQ state unchanged.
phase="stale generation replay rejection"
stale_task="$(insert_task phase18-stale 4)"
bad_dlq="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT id FROM queue_dead_letters WHERE source_queue='$QUEUE_KEY' AND source_delivery_id='$bad_entry'" 2>/dev/null | tr -d '\r')"
code="$(curl -sS -o /tmp/phase18-stale.json -w '%{http_code}' -X POST "$BASE_URL/api/v1/queue/dead-letters/$bad_dlq/replay" \
  -H 'Content-Type: application/json' \
  -d "{\"task_id\":$stale_task,\"enqueue_generation\":3}")"
test "$code" = "400"
wait_task_status "$stale_task" pending
bad_state="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT CONCAT(replay_state,':',replay_count) FROM queue_dead_letters WHERE id=$bad_dlq" 2>/dev/null | tr -d '\r')"
test "$bad_state" = "quarantined:0"

# Scenario E: replay cannot bypass an existing Phase 16 visibility marker. The
# service refuses to publish another copy when the current generation is
# already recorded as broker-visible, even if an operator asks explicitly.
phase="already visible replay rejection"
visible_task="$(insert_task phase18-visible 5)"
docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -e "UPDATE tasks SET enqueued_generation=5 WHERE id=$visible_task" >/dev/null 2>&1
visible_dlq="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "INSERT INTO queue_dead_letters (source_backend,source_queue,source_delivery_id,failure_class,failure_reason,raw_envelope,task_id,enqueue_generation,replay_state,replay_count,last_replay_error,created_at,updated_at) VALUES ('redis','$QUEUE_KEY','phase18-visible-manual','decode','manual replay guard','{\\\"task_id\\\":\\\"$visible_task\\\"}',${visible_task},5,'quarantined',0,'',NOW(),NOW()); SELECT LAST_INSERT_ID();" 2>/dev/null | tail -n 1 | tr -d '\r')"
before_visible_count="$(stream_count "$visible_task" 5)"
code="$(curl -sS -o /tmp/phase18-visible.json -w '%{http_code}' -X POST "$BASE_URL/api/v1/queue/dead-letters/$visible_dlq/replay" -H 'Content-Type: application/json' -d '{}')"
test "$code" = "409"
test "$(stream_count "$visible_task" 5)" = "$before_visible_count"
visible_state="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow taskflow -Nse "SELECT CONCAT(replay_state,':',replay_count) FROM queue_dead_letters WHERE id=$visible_dlq" 2>/dev/null | tr -d '\r')"
test "$visible_state" = "quarantined:1"

phase="api and metrics"
listing="$(curl -fsS "$BASE_URL/api/v1/queue/dead-letters?limit=10")"
printf '%s' "$listing" | grep -q 'phase18-bad'
printf '%s' "$listing" | grep -q '"failure_class":"orphan"'
quarantined="$(curl -fsS "$BASE_URL/metrics" | awk '$1 ~ /^taskflow_queue_poison_deliveries_total\{.*outcome="quarantined"/ {sum += $2} END {print sum+0}')"
replay_success="$(curl -fsS "$BASE_URL/metrics" | awk '$1 == "taskflow_queue_dead_letter_replays_total{outcome=\"success\"}" {print $2; exit}')"
replay_rejected="$(curl -fsS "$BASE_URL/metrics" | awk '$1 == "taskflow_queue_dead_letter_replays_total{outcome=\"rejected\"}" {print $2; exit}')"
awk -v value="$quarantined" 'BEGIN { exit !((value+0)>=3) }'
awk -v value="${replay_success:-0}" 'BEGIN { exit !((value+0)>=1) }'
awk -v value="${replay_rejected:-0}" 'BEGIN { exit !((value+0)>=1) }'

python3 scripts/phase18-dlq-check.py
phase="complete"
echo "phase18 poison-message quarantine and controlled replay: ok"
