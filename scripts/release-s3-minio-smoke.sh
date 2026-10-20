#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
TEST_DB="taskflow_release_s3"
QUEUE_KEY="release:s3:queue"
LOCK_KEY="release:s3:reconciler:leader"
BUCKET="taskflow-dlq-release"
TOOL="/tmp/taskflow-release-s3-tool"
TEST_DSN="taskflow:taskflow@tcp(mysql:3306)/$TEST_DB?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
HOST_UID="$(id -u)"
HOST_GID="$(id -g)"
phase="setup"

primary_env() {
  S3_ENDPOINT=127.0.0.1:19000 \
  S3_ACCESS_KEY=taskflow-primary \
  S3_SECRET_KEY=taskflow-primary-secret \
  S3_BUCKET="$BUCKET" S3_REGION=us-east-1 S3_SECURE=false "$TOOL" "$@"
}

replica_env() {
  S3_ENDPOINT=127.0.0.1:19002 \
  S3_ACCESS_KEY=taskflow-replica \
  S3_SECRET_KEY=taskflow-replica-secret \
  S3_BUCKET="$BUCKET" S3_REGION=us-east-1 S3_SECURE=false "$TOOL" "$@"
}

wait_minio() {
  port="$1"
  i=0
  while [ "$i" -lt 200 ]; do
    if curl -fsS "http://127.0.0.1:$port/minio/health/live" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.1
  done
  return 1
}

wait_ready() {
  i=0
  while [ "$i" -lt 300 ]; do
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
  while [ "$i" -lt 300 ]; do
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

wait_object_absent() {
  side="$1"
  key="$2"
  i=0
  while [ "$i" -lt 120 ]; do
    if [ "$side" = primary ]; then
      if ! primary_env exists "$key" >/dev/null 2>&1; then return 0; fi
    else
      if ! replica_env exists "$key" >/dev/null 2>&1; then return 0; fi
    fi
    i=$((i + 1))
    sleep 0.1
  done
  echo "S3 object remained side=$side key=$key" >&2
  return 1
}

insert_dlq() {
  delivery="$1"
  sql "INSERT INTO queue_dead_letters (source_backend,source_queue,source_delivery_id,failure_class,failure_reason,raw_envelope,task_id,enqueue_generation,replay_state,replay_count,replay_lease_until,last_replay_error,replayed_at,created_at,updated_at) VALUES ('redis','$QUEUE_KEY','$delivery','decode','release-s3-evidence','{}',NULL,NULL,'quarantined',0,NULL,'',NULL,NOW(),NOW()); SELECT LAST_INSERT_ID();" | tail -n 1
}

restore() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "real MinIO/S3 smoke failed during phase: $phase" >&2
    docker logs --tail 300 taskflow-go-taskflow-1 >&2 2>/dev/null || true
    docker compose --profile archive-s3 logs --tail 120 minio-primary minio-replica >&2 2>/dev/null || true
  fi
  docker compose --profile archive-s3 start minio-primary minio-replica >/dev/null 2>&1 || true
  wait_minio 19000 >/dev/null 2>&1 || true
  wait_minio 19002 >/dev/null 2>&1 || true
  [ -x "$TOOL" ] && primary_env remove-bucket >/dev/null 2>&1 || true
  [ -x "$TOOL" ] && replica_env remove-bucket >/dev/null 2>&1 || true
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" "$LOCK_KEY" >/dev/null 2>&1 || true
  docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS $TEST_DB" >/dev/null 2>&1 || true
  docker compose --profile archive-s3 stop minio-primary minio-replica >/dev/null 2>&1 || true
  rm -f "$TOOL"
  return "$rc"
}
trap restore EXIT

phase="build release S3 helper"
docker run --rm -v "$PWD":/src -v /tmp:/out -w /src \
  -e HOST_UID="$HOST_UID" -e HOST_GID="$HOST_GID" \
  -e GOPROXY=https://goproxy.cn,direct taskflow-go:phase7-builder \
  sh -lc '/usr/local/go/bin/go build -o /out/taskflow-release-s3-tool ./scripts/release-s3-tool && chown "$HOST_UID:$HOST_GID" /out/taskflow-release-s3-tool' >/dev/null

phase="start independent MinIO failure domains"
docker compose --profile archive-s3 up -d minio-primary minio-replica redis mysql executor-worker >/dev/null
wait_minio 19000
wait_minio 19002
primary_env ensure-bucket
replica_env ensure-bucket

phase="create isolated database"
docker compose exec -T mysql mysql -uroot -proot >/dev/null <<SQL
DROP DATABASE IF EXISTS $TEST_DB;
CREATE DATABASE $TEST_DB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
GRANT ALL PRIVILEGES ON $TEST_DB.* TO 'taskflow'@'%';
FLUSH PRIVILEGES;
SQL
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" "$LOCK_KEY" >/dev/null

phase="start TaskFlow with real S3 primary and replica"
env \
MYSQL_DSN="$TEST_DSN" REDIS_QUEUE_KEY="$QUEUE_KEY" EXECUTION_BACKEND=local WORKER_COUNT=1 \
OUTBOX_INTERVAL_MS=60000 RECONCILE_SEC=60 RECONCILE_LOCK_KEY="$LOCK_KEY" QUEUE_COMPACTION_ENABLED=false \
QUEUE_DLQ_LIFECYCLE_ENABLED=false QUEUE_DLQ_ARCHIVE_ENABLED=true QUEUE_DLQ_ARCHIVE_SINK=s3 \
QUEUE_DLQ_ARCHIVE_S3_ENDPOINT=minio-primary:9000 QUEUE_DLQ_ARCHIVE_S3_ACCESS_KEY=taskflow-primary \
QUEUE_DLQ_ARCHIVE_S3_SECRET_KEY=taskflow-primary-secret QUEUE_DLQ_ARCHIVE_S3_BUCKET="$BUCKET" \
QUEUE_DLQ_ARCHIVE_S3_REGION=us-east-1 QUEUE_DLQ_ARCHIVE_S3_SECURE=false QUEUE_DLQ_ARCHIVE_LEASE_MS=200 \
QUEUE_DLQ_ARCHIVE_REPLICA_ENABLED=true QUEUE_DLQ_ARCHIVE_REPLICA_SINK=s3 \
QUEUE_DLQ_ARCHIVE_REPLICA_S3_ENDPOINT=minio-replica:9000 QUEUE_DLQ_ARCHIVE_REPLICA_S3_ACCESS_KEY=taskflow-replica \
QUEUE_DLQ_ARCHIVE_REPLICA_S3_SECRET_KEY=taskflow-replica-secret QUEUE_DLQ_ARCHIVE_REPLICA_S3_BUCKET="$BUCKET" \
QUEUE_DLQ_ARCHIVE_REPLICA_S3_REGION=us-east-1 QUEUE_DLQ_ARCHIVE_REPLICA_S3_SECURE=false QUEUE_DLQ_ARCHIVE_MIN_COPIES=2 \
QUEUE_DLQ_ARCHIVE_OBJECT_LIFECYCLE_ENABLED=true QUEUE_DLQ_ARCHIVE_OBJECT_LIFECYCLE_INTERVAL_MS=200 \
QUEUE_DLQ_ARCHIVE_OBJECT_RETENTION_MS=3600000 QUEUE_DLQ_ARCHIVE_ORPHAN_GRACE_MS=500 \
QUEUE_DLQ_ARCHIVE_GC_BATCH=20 QUEUE_DLQ_ARCHIVE_ORPHAN_BATCH=50 QUEUE_DLQ_ARCHIVE_DRILL_INTERVAL_MS=3600000 \
DRAIN_FENCE_MS=100 DRAIN_TIMEOUT_MS=5000 \
docker compose --profile archive-s3 up -d --no-deps --force-recreate taskflow >/dev/null
wait_ready

phase="replicated S3 archive and purge"
dlq_id="$(insert_dlq release-s3-replicated)"
curl -fsS -X POST "$BASE_URL/api/v1/queue/dead-letters/$dlq_id/archive" >/tmp/release-s3-archive.json
archive_id="$(sql "SELECT id FROM queue_dead_letter_archives WHERE dead_letter_id=$dlq_id")"
wait_sql_eq "SELECT verified_copies FROM queue_dead_letter_archives WHERE id=$archive_id" 2
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letter_archive_replicas WHERE archive_id=$archive_id" 1
object_key="$(sql "SELECT object_key FROM queue_dead_letter_archives WHERE id=$archive_id")"
primary_env exists "$object_key"
replica_env exists "$object_key"
code="$(curl -sS -o /tmp/release-s3-delete.json -w '%{http_code}' -X DELETE "$BASE_URL/api/v1/queue/dead-letters/$dlq_id")"
test "$code" = "204"
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE id=$dlq_id" 0

phase="replica outage fail closed and repair"
fail_id="$(insert_dlq release-s3-replica-down)"
docker compose --profile archive-s3 stop minio-replica >/dev/null
code="$(curl -sS -o /tmp/release-s3-copy-fail.json -w '%{http_code}' -X POST "$BASE_URL/api/v1/queue/dead-letters/$fail_id/archive")"
test "$code" = "503"
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE id=$fail_id" 1
wait_sql_eq "SELECT verified_copies FROM queue_dead_letter_archives WHERE dead_letter_id=$fail_id" 1
docker compose --profile archive-s3 start minio-replica >/dev/null
wait_minio 19002
code="$(curl -sS -o /tmp/release-s3-copy-repair.json -w '%{http_code}' -X DELETE "$BASE_URL/api/v1/queue/dead-letters/$fail_id")"
test "$code" = "204"
wait_sql_eq "SELECT verified_copies FROM queue_dead_letter_archives WHERE dead_letter_id=$fail_id" 2
fail_key="$(sql "SELECT object_key FROM queue_dead_letter_archives WHERE dead_letter_id=$fail_id")"
replica_env exists "$fail_key"

phase="primary outage replica fallback"
fallback_id="$(insert_dlq release-s3-primary-down)"
curl -fsS -X POST "$BASE_URL/api/v1/queue/dead-letters/$fallback_id/archive" >/dev/null
fallback_archive="$(sql "SELECT id FROM queue_dead_letter_archives WHERE dead_letter_id=$fallback_id")"
fallback_key="$(sql "SELECT object_key FROM queue_dead_letter_archives WHERE id=$fallback_archive")"
code="$(curl -sS -o /tmp/release-s3-fallback-purge.json -w '%{http_code}' -X DELETE "$BASE_URL/api/v1/queue/dead-letters/$fallback_id")"
test "$code" = "204"
docker compose --profile archive-s3 stop minio-primary >/dev/null
code="$(curl -sS -o /tmp/release-s3-drill.json -w '%{http_code}' -X POST "$BASE_URL/api/v1/queue/dead-letter-archives/$fallback_archive/drill")"
test "$code" = "204"
curl -fsS "$BASE_URL/api/v1/queue/dead-letter-archives/$fallback_archive/export" >/tmp/release-s3-export.json
grep -q 'release-s3-evidence' /tmp/release-s3-export.json
code="$(curl -sS -o /tmp/release-s3-restore.json -w '%{http_code}' -X POST "$BASE_URL/api/v1/queue/dead-letter-archives/$fallback_archive/restore")"
test "$code" = "201"
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE source_delivery_id='release-s3-primary-down'" 1
docker compose --profile archive-s3 start minio-primary >/dev/null
wait_minio 19000
primary_env exists "$fallback_key"
replica_env exists "$fallback_key"

phase="S3 orphan reconciliation"
orphan_key="dead-letters/v1/999999/orphan.json"
primary_env put "$orphan_key" orphan
replica_env put "$orphan_key" orphan
sleep 0.8
wait_object_absent primary "$orphan_key"
wait_object_absent replica "$orphan_key"

phase="S3 retention GC tombstone"
sql "UPDATE queue_dead_letter_archives SET archived_at=DATE_SUB(NOW(), INTERVAL 2 DAY), last_restored_at=NULL WHERE id=$archive_id;"
wait_sql_eq "SELECT state FROM queue_dead_letter_archives WHERE id=$archive_id" expired
wait_object_absent primary "$object_key"
wait_object_absent replica "$object_key"
code="$(curl -sS -o /tmp/release-s3-expired-restore.json -w '%{http_code}' -X POST "$BASE_URL/api/v1/queue/dead-letter-archives/$archive_id/restore")"
test "$code" = "409"

phase="metrics and S3 receipts"
test "$(sql "SELECT sink FROM queue_dead_letter_archives WHERE id=$fallback_archive")" = "s3"
test "$(sql "SELECT sink FROM queue_dead_letter_archive_replicas WHERE archive_id=$fallback_archive")" = "replica:s3"
metrics="$(curl -fsS "$BASE_URL/metrics")"
printf '%s' "$metrics" | grep -q '^taskflow_queue_dead_letter_archive_replications_total{outcome="success"}'
printf '%s' "$metrics" | grep -q '^taskflow_queue_dead_letter_archive_replications_total{outcome="insufficient"}'
printf '%s' "$metrics" | grep -q '^taskflow_queue_dead_letter_archive_replica_reads_total{outcome="fallback"}'
printf '%s' "$metrics" | grep -q '^taskflow_queue_dead_letter_archive_object_gc_total{outcome="success"}'
printf '%s' "$metrics" | grep -q '^taskflow_queue_dead_letter_archive_orphans_total{outcome="deleted"}'
printf '%s' "$metrics" | grep -q '^taskflow_queue_dead_letter_archive_restore_drills_total{outcome="success"}'

python3 scripts/release-s3-minio-check.py
phase="complete"
echo "real MinIO/S3 primary+replica archive replication and fail-closed repair: ok"
echo "real MinIO/S3 replica fallback, orphan reconciliation and retention GC: ok"
