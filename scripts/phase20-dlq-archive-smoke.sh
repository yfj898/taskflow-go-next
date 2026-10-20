#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
TEST_DB="taskflow_phase20"
QUEUE_KEY="phase20:queue"
LOCK_KEY="phase20:reconciler:leader"
ARCHIVE_DIR="/var/lib/taskflow/dlq-archive/phase20-smoke"
TEST_DSN="taskflow:taskflow@tcp(mysql:3306)/$TEST_DB?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
phase="setup"

restore() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "phase20 smoke failed during phase: $phase" >&2
    docker logs --tail 250 taskflow-go-taskflow-1 >&2 2>/dev/null || true
  fi
  # Stop the isolated Phase 20 worker before deleting its Stream key. Otherwise
  # the consumer-group bootstrap can recreate an empty key after DEL.
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  docker compose exec -T taskflow rm -rf "$ARCHIVE_DIR" >/dev/null 2>&1 || true
  docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" "$LOCK_KEY" >/dev/null 2>&1 || true
  docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS $TEST_DB" >/dev/null 2>&1 || true
  return "$rc"
}
trap restore EXIT

wait_ready() {
  i=0
  while [ "$i" -lt 250 ]; do
    if curl -fsS "$BASE_URL/health/ready" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.1
  done
  return 1
}

sql() {
  docker compose exec -T mysql mysql -utaskflow -ptaskflow "$TEST_DB" -Nse "$1" 2>/dev/null | tr -d '\r'
}

wait_sql_eq() {
  query="$1"
  want="$2"
  i=0
  got=""
  while [ "$i" -lt 250 ]; do
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
  sql "INSERT INTO tasks (name,type,payload,status,result,retry_count,last_error,trace_parent,trace_state,enqueue_generation,enqueued_generation,enqueue_lease_until,next_run_at,lease_until,created_at,updated_at) VALUES ('$name','compute','phase20','pending','',0,'','','',$generation,0,NULL,NOW(),NULL,NOW(),NOW()); SELECT LAST_INSERT_ID();" | tail -n 1
}

insert_dlq() {
  delivery="$1"
  state="$2"
  task_id="$3"
  generation="$4"
  created_expr="$5"
  replayed_expr="$6"
  lease_expr="$7"
  sql "INSERT INTO queue_dead_letters (source_backend,source_queue,source_delivery_id,failure_class,failure_reason,raw_envelope,task_id,enqueue_generation,replay_state,replay_count,replay_lease_until,last_replay_error,replayed_at,created_at,updated_at) VALUES ('redis','$QUEUE_KEY','$delivery','decode','phase20-evidence','{}',$task_id,$generation,'$state',0,$lease_expr,'',$replayed_expr,$created_expr,NOW()); SELECT LAST_INSERT_ID();" | tail -n 1
}

phase="start isolated stack"
docker compose up -d redis mysql executor-worker >/dev/null
docker compose exec -T mysql mysql -uroot -proot >/dev/null <<SQL
DROP DATABASE IF EXISTS $TEST_DB;
CREATE DATABASE $TEST_DB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
GRANT ALL PRIVILEGES ON $TEST_DB.* TO 'taskflow'@'%';
FLUSH PRIVILEGES;
SQL
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" "$LOCK_KEY" >/dev/null

MYSQL_DSN="$TEST_DSN" \
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
QUEUE_DLQ_REPLAY_AUDIT_RETENTION_MS=7776000000 \
QUEUE_DLQ_MAX_ENTRIES=100 \
QUEUE_DLQ_ARCHIVE_ENABLED=true \
QUEUE_DLQ_ARCHIVE_SINK=file \
QUEUE_DLQ_ARCHIVE_DIR="$ARCHIVE_DIR" \
QUEUE_DLQ_ARCHIVE_LEASE_MS=3000 \
QUEUE_DLQ_REPLAY_LEASE_MS=3000 \
ENQUEUE_VISIBILITY_LEASE_MS=3000 \
DRAIN_FENCE_MS=100 \
DRAIN_TIMEOUT_MS=5000 \
docker compose up -d --no-deps --force-recreate taskflow >/dev/null
wait_ready

# Scenario A: archive-before-purge captures the parent and replay-audit snapshot,
# then restore creates a fresh quarantined hot row that must still use Phase 18
# controlled replay and Phase 16 generation fencing.
phase="archive purge restore replay"
task_id="$(insert_task phase20-restore 2)"
dlq_id="$(insert_dlq phase20-restore quarantined "$task_id" 2 'NOW()' 'NULL' 'NULL')"
sql "INSERT INTO queue_dead_letter_replay_audits (dead_letter_id,source_backend,source_queue,source_delivery_id,failure_class,task_id,enqueue_generation,outcome,failure_reason,created_at) VALUES ($dlq_id,'redis','$QUEUE_KEY','phase20-restore','decode',$task_id,2,'rejected','phase20-audit',NOW());"
code="$(curl -sS -o /tmp/phase20-delete.json -w '%{http_code}' -X DELETE "$BASE_URL/api/v1/queue/dead-letters/$dlq_id")"
test "$code" = "204"
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE id=$dlq_id" 0
archive_id="$(sql "SELECT id FROM queue_dead_letter_archives WHERE dead_letter_id=$dlq_id AND state='ready'")"
test -n "$archive_id"
test "$(sql "SELECT CHAR_LENGTH(sha256) FROM queue_dead_letter_archives WHERE id=$archive_id")" = "64"
test "$(sql "SELECT COUNT(*) FROM queue_dead_letter_archives WHERE id=$archive_id AND verified_at IS NOT NULL")" = "1"
object_key="$(sql "SELECT object_key FROM queue_dead_letter_archives WHERE id=$archive_id")"
stored_sha="$(sql "SELECT sha256 FROM queue_dead_letter_archives WHERE id=$archive_id")"
actual_sha="$(docker compose exec -T taskflow sha256sum "$ARCHIVE_DIR/$object_key" | awk '{print $1}')"
test "$actual_sha" = "$stored_sha"
exported="$(curl -fsS "$BASE_URL/api/v1/queue/dead-letter-archives/$archive_id/export")"
printf '%s' "$exported" | grep -q '"failure_reason":"phase20-evidence"'
printf '%s' "$exported" | grep -q '"outcome":"rejected"'
archives="$(curl -fsS "$BASE_URL/api/v1/queue/dead-letter-archives")"
printf '%s' "$archives" | grep -q "\"id\":$archive_id"

curl -fsS -X POST "$BASE_URL/api/v1/queue/dead-letter-archives/$archive_id/restore" >/tmp/phase20-restore.json
grep -q '"replay_state":"quarantined"' /tmp/phase20-restore.json
restored_id="$(sql "SELECT last_restored_dead_letter_id FROM queue_dead_letter_archives WHERE id=$archive_id")"
test -n "$restored_id"
wait_sql_eq "SELECT replay_state FROM queue_dead_letters WHERE id=$restored_id" quarantined
curl -fsS -X POST "$BASE_URL/api/v1/queue/dead-letters/$restored_id/replay" >/tmp/phase20-replay.json
wait_sql_eq "SELECT status FROM tasks WHERE id=$task_id" success
wait_sql_eq "SELECT replay_state FROM queue_dead_letters WHERE id=$restored_id" replayed
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letter_replay_audits WHERE dead_letter_id=$restored_id AND outcome='success'" 1
code="$(curl -sS -o /tmp/phase20-conflict.json -w '%{http_code}' -X POST "$BASE_URL/api/v1/queue/dead-letter-archives/$archive_id/restore")"
test "$code" = "409"

# Scenario B: corrupting an archive after it was ready must block both manual
# purge and lifecycle retention. The old verified_at cannot authorize deletion
# in a later lifecycle cycle because every cycle requires a fresh verification.
phase="integrity fail closed"
tamper_id="$(insert_dlq phase20-tamper quarantined NULL NULL 'NOW()' 'NULL' 'NULL')"
curl -fsS -X POST "$BASE_URL/api/v1/queue/dead-letters/$tamper_id/archive" >/tmp/phase20-archive.json
tamper_archive_id="$(sql "SELECT id FROM queue_dead_letter_archives WHERE dead_letter_id=$tamper_id")"
tamper_key="$(sql "SELECT object_key FROM queue_dead_letter_archives WHERE id=$tamper_archive_id")"
docker compose exec -T taskflow sh -c "printf tampered > '$ARCHIVE_DIR/$tamper_key'"
code="$(curl -sS -o /tmp/phase20-tamper-delete.json -w '%{http_code}' -X DELETE "$BASE_URL/api/v1/queue/dead-letters/$tamper_id")"
test "$code" = "422"
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE id=$tamper_id" 1
sql "UPDATE queue_dead_letters SET created_at=DATE_SUB(NOW(), INTERVAL 2 DAY) WHERE id=$tamper_id;"
sleep 1
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE id=$tamper_id" 1
code="$(curl -sS -o /tmp/phase20-tamper-restore.json -w '%{http_code}' -X POST "$BASE_URL/api/v1/queue/dead-letter-archives/$tamper_archive_id/restore")"
test "$code" = "422"

# Scenario C: lifecycle archives and verifies old terminal evidence before it
# removes the hot MySQL row.
phase="lifecycle archive before retention purge"
old_id="$(insert_dlq phase20-old replayed NULL NULL 'DATE_SUB(NOW(), INTERVAL 2 DAY)' 'DATE_SUB(NOW(), INTERVAL 2 DAY)' 'NULL')"
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE id=$old_id" 0
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letter_archives WHERE dead_letter_id=$old_id AND state='ready' AND verified_at IS NOT NULL" 1

# Scenario D: replaying evidence is protected before archive starts.
phase="replaying protection"
active_id="$(insert_dlq phase20-active replaying NULL NULL 'DATE_SUB(NOW(), INTERVAL 2 DAY)' 'NULL' 'DATE_ADD(NOW(), INTERVAL 5 MINUTE)')"
code="$(curl -sS -o /tmp/phase20-active.json -w '%{http_code}' -X DELETE "$BASE_URL/api/v1/queue/dead-letters/$active_id")"
test "$code" = "409"
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE id=$active_id" 1
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letter_archives WHERE dead_letter_id=$active_id" 0

phase="metrics and schema"
test "$(sql "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='$TEST_DB' AND table_name='queue_dead_letter_archives' AND column_name IN ('sha256','size_bytes','verified_at','object_key')")" = "4"
metrics="$(curl -fsS "$BASE_URL/metrics")"
printf '%s' "$metrics" | grep -q '^taskflow_queue_dead_letter_archives_total{outcome="success"}'
printf '%s' "$metrics" | grep -q '^taskflow_queue_dead_letter_archive_bytes_total '
printf '%s' "$metrics" | grep -q '^taskflow_queue_dead_letter_archive_integrity_failures_total '
printf '%s' "$metrics" | grep -q '^taskflow_queue_dead_letter_archive_restores_total{outcome="success"}'
printf '%s' "$metrics" | grep -q '^taskflow_queue_dead_letter_archive_restores_total{outcome="conflict"}'
printf '%s' "$metrics" | grep -q '^taskflow_queue_dead_letter_purged_total{reason="retention"}'

python3 scripts/phase20-dlq-archive-check.py
phase="complete"
echo "phase20 DLQ archive/export integrity and restore workflow: ok"
