#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
QUEUE_KEY="phase19:queue"
LOCK_KEY="phase19:reconciler:leader"
DB_NAME="taskflow_phase19_smoke"
TEST_MYSQL_DSN="taskflow:taskflow@tcp(mysql:3306)/$DB_NAME?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
phase="setup"

root_sql() {
  docker compose exec -T mysql mysql -uroot -proot -Nse "$1" 2>/dev/null | tr -d '\r'
}

prepare_db() {
  root_sql "DROP DATABASE IF EXISTS $DB_NAME; CREATE DATABASE $DB_NAME CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci; GRANT ALL PRIVILEGES ON $DB_NAME.* TO 'taskflow'@'%';"
}

cleanup_db() {
  root_sql "DROP DATABASE IF EXISTS $DB_NAME" >/dev/null 2>&1 || true
}

restore() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "phase19 smoke failed during phase: $phase" >&2
    docker logs --tail 200 taskflow-go-taskflow-1 >&2 2>/dev/null || true
  fi
  docker compose stop taskflow >/dev/null 2>&1 || true
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

sql() {
  docker compose exec -T mysql mysql -utaskflow -ptaskflow "$DB_NAME" -Nse "$1" 2>/dev/null | tr -d '\r'
}

wait_sql_eq() {
  query="$1"
  want="$2"
  i=0
  while [ "$i" -lt 200 ]; do
    got="$(sql "$query")"
    if [ "$got" = "$want" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.1
  done
  echo "wait_sql_eq got=$got want=$want query=$query" >&2
  return 1
}

insert_task() {
  name="$1"
  generation="$2"
  sql "INSERT INTO tasks (name,type,payload,status,result,retry_count,last_error,trace_parent,trace_state,enqueue_generation,enqueued_generation,enqueue_lease_until,next_run_at,lease_until,created_at,updated_at) VALUES ('$name','compute','phase19','pending','',0,'','','',$generation,0,NULL,NOW(),NULL,NOW(),NOW()); SELECT LAST_INSERT_ID();" | tail -n 1
}

insert_dlq() {
  delivery="$1"
  state="$2"
  task_id="$3"
  generation="$4"
  created_expr="$5"
  replayed_expr="$6"
  lease_expr="$7"
  sql "INSERT INTO queue_dead_letters (source_backend,source_queue,source_delivery_id,failure_class,failure_reason,raw_envelope,task_id,enqueue_generation,replay_state,replay_count,replay_lease_until,last_replay_error,replayed_at,created_at,updated_at) VALUES ('redis','$QUEUE_KEY','$delivery','decode','phase19','{}',$task_id,$generation,'$state',0,$lease_expr,'',$replayed_expr,$created_expr,NOW()); SELECT LAST_INSERT_ID();" | tail -n 1
}

start_taskflow() {
  MYSQL_DSN="$TEST_MYSQL_DSN" \
  REDIS_QUEUE_KEY="$QUEUE_KEY" \
  EXECUTION_BACKEND=local \
  WORKER_COUNT=1 \
  OUTBOX_INTERVAL_MS=60000 \
  RECONCILE_SEC=60 \
  RECONCILE_LOCK_KEY="$LOCK_KEY" \
  QUEUE_COMPACTION_ENABLED=false \
  QUEUE_DLQ_LIFECYCLE_ENABLED=true \
  QUEUE_DLQ_LIFECYCLE_INTERVAL_MS=200 \
  QUEUE_DLQ_REPLAYED_RETENTION_MS=3600000 \
  QUEUE_DLQ_QUARANTINED_RETENTION_MS=3600000 \
  QUEUE_DLQ_REPLAY_AUDIT_RETENTION_MS=3600000 \
  QUEUE_DLQ_MAX_ENTRIES=3 \
  QUEUE_DLQ_REPLAY_LEASE_MS=3000 \
  ENQUEUE_VISIBILITY_LEASE_MS=3000 \
  DRAIN_FENCE_MS=100 \
  DRAIN_TIMEOUT_MS=5000 \
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null
  wait_ready
}

wait_metric_line() {
  pattern="$1"
  i=0
  while [ "$i" -lt 100 ]; do
    if curl -fsS "$BASE_URL/metrics" | grep -q "$pattern"; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.1
  done
  echo "metric not observed: $pattern" >&2
  curl -fsS "$BASE_URL/metrics" 2>/dev/null | grep '^taskflow_queue_dead_letter_' >&2 || true
  sql "SELECT replay_state, COUNT(*) FROM queue_dead_letters WHERE source_queue='$QUEUE_KEY' GROUP BY replay_state ORDER BY replay_state" >&2 || true
  return 1
}

phase="start stack"
docker compose up -d redis mysql executor-worker >/dev/null
prepare_db
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" "$LOCK_KEY" >/dev/null

start_taskflow

# Scenario A: successful replay writes an append-only audit. Manual purge removes
# the parent DLQ but leaves its replay audit available for forensic inspection.
phase="replay audit survives manual purge"
task_id="$(insert_task phase19-replay 2)"
dlq_id="$(insert_dlq phase19-replay quarantined "$task_id" 2 'NOW()' 'NULL' 'NULL')"
curl -fsS -X POST "$BASE_URL/api/v1/queue/dead-letters/$dlq_id/replay" >/tmp/phase19-replay.json
grep -q '"replay_state":"replayed"' /tmp/phase19-replay.json
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letter_replay_audits WHERE dead_letter_id=$dlq_id AND outcome='success'" 1
audits="$(curl -fsS "$BASE_URL/api/v1/queue/dead-letters/$dlq_id/replay-audits")"
printf '%s' "$audits" | grep -q '"outcome":"success"'
code="$(curl -sS -o /tmp/phase19-delete.json -w '%{http_code}' -X DELETE "$BASE_URL/api/v1/queue/dead-letters/$dlq_id")"
test "$code" = "204"
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE id=$dlq_id" 0
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letter_replay_audits WHERE dead_letter_id=$dlq_id" 1
wait_metric_line '^taskflow_queue_dead_letter_purged_total{reason="manual"}'

# Scenario B: explicit purge must not delete replaying evidence, regardless of
# operator intent. This avoids racing a broker publish/database settlement.
phase="active replay purge protection"
active_id="$(insert_dlq phase19-active replaying NULL NULL 'DATE_SUB(NOW(), INTERVAL 2 DAY)' 'NULL' 'DATE_ADD(NOW(), INTERVAL 5 MINUTE)')"
code="$(curl -sS -o /tmp/phase19-active.json -w '%{http_code}' -X DELETE "$BASE_URL/api/v1/queue/dead-letters/$active_id")"
test "$code" = "409"
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE id=$active_id" 1

# Scenario B2: lifecycle selection must not race a concurrent transition from
# quarantined to replaying. Hold the row lock while several lifecycle cycles
# occur, then transition it to replaying before commit. SKIP LOCKED plus the
# delete-time state predicate must preserve the row.
phase="concurrent replay transition protection"
docker compose stop taskflow >/dev/null
race_id="$(insert_dlq phase19-race quarantined NULL NULL 'DATE_SUB(NOW(), INTERVAL 2 DAY)' 'NULL' 'NULL')"
marker="phase19-race-$race_id"
(
  docker compose exec -T mysql mysql -utaskflow -ptaskflow "$DB_NAME" >/dev/null 2>&1 <<SQL
START TRANSACTION;
SELECT id FROM queue_dead_letters WHERE id=$race_id FOR UPDATE;
SELECT GET_LOCK('$marker', 0);
DO SLEEP(1);
UPDATE queue_dead_letters SET replay_state='replaying', replay_lease_until=DATE_ADD(NOW(), INTERVAL 5 MINUTE) WHERE id=$race_id;
COMMIT;
SELECT RELEASE_LOCK('$marker');
SQL
) &
locker_pid=$!
i=0
while [ "$i" -lt 100 ]; do
  holder="$(sql "SELECT COALESCE(IS_USED_LOCK('$marker'), 0)")"
  if [ "$holder" != "0" ]; then
    break
  fi
  i=$((i + 1))
  sleep 0.05
done
test "${holder:-0}" != "0"
start_taskflow
wait "$locker_pid"
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE id=$race_id AND replay_state='replaying'" 1

# Scenario C: age retention deletes old replayed/quarantined evidence but never
# the replaying row. An old replay audit is independently expired.
phase="age retention"
old_replayed="$(insert_dlq phase19-old-replayed replayed NULL NULL 'DATE_SUB(NOW(), INTERVAL 2 DAY)' 'DATE_SUB(NOW(), INTERVAL 2 DAY)' 'NULL')"
old_quarantined="$(insert_dlq phase19-old-quarantined quarantined NULL NULL 'DATE_SUB(NOW(), INTERVAL 2 DAY)' 'NULL' 'NULL')"
sql "INSERT INTO queue_dead_letter_replay_audits (dead_letter_id,source_backend,source_queue,source_delivery_id,failure_class,task_id,enqueue_generation,outcome,failure_reason,created_at) VALUES ($active_id,'redis','$QUEUE_KEY','phase19-old-audit','decode',0,0,'rejected','old',DATE_SUB(NOW(), INTERVAL 2 DAY));"
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE id IN ($old_replayed,$old_quarantined)" 0
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letter_replay_audits WHERE source_queue='$QUEUE_KEY' AND source_delivery_id='phase19-old-audit'" 0
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE id=$active_id" 1
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE id=$race_id" 1

# Scenario D: count capacity evicts only replayed terminal evidence. Fresh
# unresolved quarantine is preserved while old terminal rows make room.
phase="capacity bound"
fresh_quarantined="$(insert_dlq phase19-cap-quarantined quarantined NULL NULL 'NOW()' 'NULL' 'NULL')"
i=1
while [ "$i" -le 5 ]; do
  insert_dlq "phase19-cap-$i" replayed NULL NULL 'NOW()' 'NOW()' 'NULL' >/dev/null
  i=$((i + 1))
done
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE source_queue='$QUEUE_KEY'" 3
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE id=$active_id" 1
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE id=$race_id" 1
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE id=$fresh_quarantined AND replay_state='quarantined'" 1

# Scenario E: if protected unresolved/live rows alone exceed the count target,
# lifecycle reports over-limit rather than deleting evidence to force the cap.
phase="protected over limit"
protected_quarantined="$(insert_dlq phase19-protected-quarantined quarantined NULL NULL 'NOW()' 'NULL' 'NULL')"
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE source_queue='$QUEUE_KEY'" 4
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE id=$protected_quarantined AND replay_state='quarantined'" 1

phase="metrics"
wait_metric_line '^taskflow_queue_dead_letter_purged_total{reason="retention"}'
wait_metric_line '^taskflow_queue_dead_letter_purged_total{reason="capacity"}'
wait_metric_line '^taskflow_queue_dead_letters{class="decode",state="replaying"} 2$'
wait_metric_line '^taskflow_queue_dead_letter_oldest_age_seconds{class="decode",state="replaying"}'
wait_metric_line '^taskflow_queue_dead_letter_over_limit 1$'

python3 scripts/phase19-dlq-lifecycle-check.py
phase="complete"
echo "phase19 dead-letter lifecycle governance: ok"
