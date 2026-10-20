#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
TEST_DB="taskflow_phase21"
QUEUE_KEY="phase21:queue"
LOCK_KEY="phase21:reconciler:leader"
PRIMARY_DIR="/var/lib/taskflow/dlq-archive/phase21-primary"
REPLICA_DIR="/var/lib/taskflow/dlq-archive-replica/phase21-replica"
TEST_DSN="taskflow:taskflow@tcp(mysql:3306)/$TEST_DB?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
phase="setup"

restore() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "phase21 smoke failed during phase: $phase" >&2
    docker logs --tail 300 taskflow-go-taskflow-1 >&2 2>/dev/null || true
  fi
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  docker compose exec -T taskflow rm -rf "$PRIMARY_DIR" "$REPLICA_DIR" >/dev/null 2>&1 || true
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

wait_file_absent() {
  path="$1"
  i=0
  while [ "$i" -lt 80 ]; do
    if ! docker compose exec -T taskflow test -e "$path" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.1
  done
  echo "file remained: $path" >&2
  return 1
}

insert_dlq() {
  delivery="$1"
  state="$2"
  created_expr="$3"
  replayed_expr="$4"
  sql "INSERT INTO queue_dead_letters (source_backend,source_queue,source_delivery_id,failure_class,failure_reason,raw_envelope,task_id,enqueue_generation,replay_state,replay_count,replay_lease_until,last_replay_error,replayed_at,created_at,updated_at) VALUES ('redis','$QUEUE_KEY','$delivery','decode','phase21-evidence','{}',NULL,NULL,'$state',0,NULL,'',$replayed_expr,$created_expr,NOW()); SELECT LAST_INSERT_ID();" | tail -n 1
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
QUEUE_DLQ_LIFECYCLE_ENABLED=false \
QUEUE_DLQ_ARCHIVE_ENABLED=true \
QUEUE_DLQ_ARCHIVE_SINK=file \
QUEUE_DLQ_ARCHIVE_DIR="$PRIMARY_DIR" \
QUEUE_DLQ_ARCHIVE_LEASE_MS=200 \
QUEUE_DLQ_ARCHIVE_REPLICA_ENABLED=true \
QUEUE_DLQ_ARCHIVE_REPLICA_SINK=file \
QUEUE_DLQ_ARCHIVE_REPLICA_DIR="$REPLICA_DIR" \
QUEUE_DLQ_ARCHIVE_MIN_COPIES=2 \
QUEUE_DLQ_ARCHIVE_OBJECT_LIFECYCLE_ENABLED=true \
QUEUE_DLQ_ARCHIVE_OBJECT_LIFECYCLE_INTERVAL_MS=200 \
QUEUE_DLQ_ARCHIVE_OBJECT_RETENTION_MS=3600000 \
QUEUE_DLQ_ARCHIVE_ORPHAN_GRACE_MS=500 \
QUEUE_DLQ_ARCHIVE_GC_BATCH=20 \
QUEUE_DLQ_ARCHIVE_ORPHAN_BATCH=50 \
QUEUE_DLQ_ARCHIVE_DRILL_INTERVAL_MS=3600000 \
QUEUE_DLQ_ARCHIVE_DRILL_BATCH=3 \
DRAIN_FENCE_MS=100 \
DRAIN_TIMEOUT_MS=5000 \
docker compose up -d --no-deps --force-recreate taskflow >/dev/null
wait_ready
docker compose exec -T taskflow rm -rf "$PRIMARY_DIR" "$REPLICA_DIR"
docker compose exec -T taskflow mkdir -p "$PRIMARY_DIR" "$REPLICA_DIR"

phase="replicated archive and purge"
dlq_id="$(insert_dlq phase21-replicated quarantined 'NOW()' 'NULL')"
curl -fsS -X POST "$BASE_URL/api/v1/queue/dead-letters/$dlq_id/archive" >/tmp/phase21-archive.json
archive_id="$(sql "SELECT id FROM queue_dead_letter_archives WHERE dead_letter_id=$dlq_id")"
wait_sql_eq "SELECT verified_copies FROM queue_dead_letter_archives WHERE id=$archive_id" 2
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letter_archive_replicas WHERE archive_id=$archive_id" 1
object_key="$(sql "SELECT object_key FROM queue_dead_letter_archives WHERE id=$archive_id")"
docker compose exec -T taskflow test -f "$PRIMARY_DIR/$object_key"
docker compose exec -T taskflow test -f "$REPLICA_DIR/$object_key"
code="$(curl -sS -o /tmp/phase21-delete.json -w '%{http_code}' -X DELETE "$BASE_URL/api/v1/queue/dead-letters/$dlq_id")"
test "$code" = "204"
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE id=$dlq_id" 0

phase="minimum copies fail closed and repair"
fail_id="$(insert_dlq phase21-copy-fail quarantined 'NOW()' 'NULL')"
docker compose exec -T taskflow rm -rf "$REPLICA_DIR"
docker compose exec -T taskflow sh -c "printf blocked > '$REPLICA_DIR'"
code="$(curl -sS -o /tmp/phase21-copy-fail.json -w '%{http_code}' -X POST "$BASE_URL/api/v1/queue/dead-letters/$fail_id/archive")"
test "$code" = "503"
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE id=$fail_id" 1
wait_sql_eq "SELECT verified_copies FROM queue_dead_letter_archives WHERE dead_letter_id=$fail_id" 1
docker compose exec -T taskflow rm -f "$REPLICA_DIR"
docker compose exec -T taskflow mkdir -p "$REPLICA_DIR"
code="$(curl -sS -o /tmp/phase21-copy-repair.json -w '%{http_code}' -X DELETE "$BASE_URL/api/v1/queue/dead-letters/$fail_id")"
test "$code" = "204"
wait_sql_eq "SELECT verified_copies FROM queue_dead_letter_archives WHERE dead_letter_id=$fail_id" 2

phase="replica fallback restore drill"
fallback_id="$(insert_dlq phase21-fallback quarantined 'NOW()' 'NULL')"
curl -fsS -X POST "$BASE_URL/api/v1/queue/dead-letters/$fallback_id/archive" >/dev/null
fallback_archive="$(sql "SELECT id FROM queue_dead_letter_archives WHERE dead_letter_id=$fallback_id")"
fallback_key="$(sql "SELECT object_key FROM queue_dead_letter_archives WHERE id=$fallback_archive")"
docker compose exec -T taskflow rm -f "$PRIMARY_DIR/$fallback_key"
code="$(curl -sS -o /tmp/phase21-drill.json -w '%{http_code}' -X POST "$BASE_URL/api/v1/queue/dead-letter-archives/$fallback_archive/drill")"
test "$code" = "204"
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letter_archives WHERE id=$fallback_archive AND last_drill_at IS NOT NULL AND last_drill_error=''" 1
curl -fsS "$BASE_URL/api/v1/queue/dead-letter-archives/$fallback_archive/export" >/tmp/phase21-fallback-export.json
grep -q 'phase21-evidence' /tmp/phase21-fallback-export.json

phase="orphan object reconciliation"
primary_orphan="$PRIMARY_DIR/dead-letters/v1/999999/orphan.json"
replica_orphan="$REPLICA_DIR/dead-letters/v1/999999/orphan.json"
docker compose exec -T taskflow sh -c "mkdir -p '$(dirname "$primary_orphan")' '$(dirname "$replica_orphan")'; printf orphan > '$primary_orphan'; printf orphan > '$replica_orphan'"
sleep 0.8
wait_file_absent "$primary_orphan"
wait_file_absent "$replica_orphan"

phase="object retention gc tombstone"
sql "UPDATE queue_dead_letter_archives SET archived_at=DATE_SUB(NOW(), INTERVAL 2 DAY), last_restored_at=NULL WHERE id=$archive_id;"
wait_sql_eq "SELECT state FROM queue_dead_letter_archives WHERE id=$archive_id" expired
wait_file_absent "$PRIMARY_DIR/$object_key"
wait_file_absent "$REPLICA_DIR/$object_key"
code="$(curl -sS -o /tmp/phase21-expired-restore.json -w '%{http_code}' -X POST "$BASE_URL/api/v1/queue/dead-letter-archives/$archive_id/restore")"
test "$code" = "409"

phase="hot dlq protects archive object"
sql "UPDATE queue_dead_letter_archives SET archived_at=DATE_SUB(NOW(), INTERVAL 2 DAY) WHERE id=$fallback_archive;"
sleep 0.8
wait_sql_eq "SELECT state FROM queue_dead_letter_archives WHERE id=$fallback_archive" ready
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE id=$fallback_id" 1
docker compose exec -T taskflow test -f "$REPLICA_DIR/$fallback_key"

phase="metrics and schema"
test "$(sql "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='$TEST_DB' AND table_name='queue_dead_letter_archive_replicas'")" = "1"
test "$(sql "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='$TEST_DB' AND table_name='queue_dead_letter_archives' AND column_name IN ('verified_copies','last_drill_at','last_drill_error','expired_at')")" = "4"
metrics="$(curl -fsS "$BASE_URL/metrics")"
printf '%s' "$metrics" | grep -q '^taskflow_queue_dead_letter_archive_replications_total{outcome="success"}'
printf '%s' "$metrics" | grep -q '^taskflow_queue_dead_letter_archive_replications_total{outcome="insufficient"}'
printf '%s' "$metrics" | grep -q '^taskflow_queue_dead_letter_archive_replica_reads_total{outcome="fallback"}'
printf '%s' "$metrics" | grep -q '^taskflow_queue_dead_letter_archive_object_gc_total{outcome="success"}'
printf '%s' "$metrics" | grep -q '^taskflow_queue_dead_letter_archive_orphans_total{outcome="deleted"}'
printf '%s' "$metrics" | grep -q '^taskflow_queue_dead_letter_archive_restore_drills_total{outcome="success"}'

python3 scripts/phase21-dlq-archive-object-check.py
phase="complete"
echo "phase21 archive object lifecycle governance: ok"
