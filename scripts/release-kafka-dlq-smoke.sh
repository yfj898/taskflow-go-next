#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

BASE_URL="${BASE_URL:-http://127.0.0.1:18080}"
RUN_ID="${RELEASE_KAFKA_RUN_ID:-$$}"
TEST_DB="taskflow_release_kafka_${RUN_ID}"
TOPIC="taskflow.release.kafka.${RUN_ID}"
GROUP="taskflow-release-kafka-workers-${RUN_ID}"
ARCHIVE_DIR="/var/lib/taskflow/dlq-archive/release-kafka-${RUN_ID}"
TEST_DSN="taskflow:taskflow@tcp(mysql:3306)/$TEST_DB?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
phase="setup"

restore() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "real Kafka DLQ smoke failed during phase: $phase" >&2
    docker logs --tail 300 taskflow-go-taskflow-1 >&2 2>/dev/null || true
    docker compose --profile kafka logs --tail 200 kafka >&2 2>/dev/null || true
  fi
  remove_taskflow
  docker compose up -d --no-deps taskflow >/dev/null 2>&1 || true
  docker compose exec -T taskflow rm -rf "$ARCHIVE_DIR" >/dev/null 2>&1 || true
  docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS $TEST_DB" >/dev/null 2>&1 || true
  docker compose --profile kafka exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 --delete --topic "$TOPIC" >/dev/null 2>&1 || true
  return "$rc"
}
trap restore EXIT

remove_taskflow() {
  id="$(docker compose ps -aq taskflow 2>/dev/null || true)"
  if [ -z "$id" ]; then
    id="$(docker ps -aq --filter name=taskflow-go-taskflow-1 | head -n 1)"
  fi
  if [ -n "$id" ]; then
    docker rm -f "$id" >/dev/null 2>&1 || true
    i=0
    while [ "$i" -lt 100 ]; do
      if ! docker inspect "$id" >/dev/null 2>&1; then
        return 0
      fi
      i=$((i + 1))
      sleep 0.1
    done
    echo "taskflow container removal did not settle: $id" >&2
    return 1
  fi
}

wait_ready() {
  i=0
  while [ "$i" -lt 300 ]; do
    identity_ok=false
    if docker compose exec -T taskflow sh -c \
      "test \"\$QUEUE_BACKEND\" = kafka && printf '%s' \"\$MYSQL_DSN\" | grep -F '/$TEST_DB?' >/dev/null" \
      >/dev/null 2>&1; then
      identity_ok=true
    fi
    if [ "$identity_ok" = true ] && curl -fsS "$BASE_URL/health/ready" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.1
  done
  echo "isolated Kafka taskflow did not become ready: db=$TEST_DB topic=$TOPIC group=$GROUP" >&2
  return 1
}

wait_kafka() {
  i=0
  while [ "$i" -lt 120 ]; do
    if docker compose --profile kafka exec -T kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server kafka:9092 --list >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
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

kafka_lag() {
  docker compose --profile kafka exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka:9092 --describe --group "$GROUP" 2>/dev/null |
    awk -v topic="$TOPIC" '$2 == topic && $6 ~ /^[0-9]+$/ {sum += $6; found=1} END {if (found) print sum; else print -1}' |
    tr -d '\r'
}

wait_kafka_lag_zero() {
  i=0
  got=""
  while [ "$i" -lt 120 ]; do
    got="$(kafka_lag)"
    if [ "$got" = "0" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.25
  done
  echo "Kafka group lag did not converge to zero: $got" >&2
  return 1
}

produce_raw() {
  value="$1"
  printf '%s\n' "$value" | docker compose --profile kafka exec -T kafka \
    /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic "$TOPIC" >/dev/null
}

insert_task() {
  name="$1"
  generation="$2"
  sql "INSERT INTO tasks (name,type,payload,status,result,retry_count,last_error,trace_parent,trace_state,enqueue_generation,enqueued_generation,enqueue_lease_until,next_run_at,lease_until,created_at,updated_at) VALUES ('$name','compute','release-kafka','pending','',0,'','','',$generation,0,NULL,NOW(),NULL,NOW(),NOW()); SELECT LAST_INSERT_ID();" | tail -n 1
}

start_isolated_taskflow() {
  MYSQL_DSN="$TEST_DSN" \
  QUEUE_BACKEND=kafka \
  KAFKA_TOPIC="$TOPIC" \
  KAFKA_GROUP="$GROUP" \
  EXECUTION_BACKEND=local \
  WORKER_COUNT=1 \
  OUTBOX_INTERVAL_MS=100 \
  RECONCILE_SEC=3600 \
  QUEUE_COMPACTION_ENABLED=false \
  QUEUE_DLQ_LIFECYCLE_ENABLED=false \
  QUEUE_DLQ_ARCHIVE_ENABLED=true \
  QUEUE_DLQ_ARCHIVE_SINK=file \
  QUEUE_DLQ_ARCHIVE_DIR="$ARCHIVE_DIR" \
  QUEUE_DLQ_ARCHIVE_OBJECT_LIFECYCLE_ENABLED=false \
  DRAIN_FENCE_MS=100 \
  DRAIN_TIMEOUT_MS=5000 \
  TASKFLOW_HTTP_PORT=18080 \
  TASKFLOW_PPROF_PORT=16060 \
  docker compose up -d --no-deps taskflow >/dev/null
}

restart_isolated_taskflow() {
  remove_taskflow
  start_isolated_taskflow
}

phase="start Kafka and isolated database"
docker compose --profile kafka up -d mysql redis executor-worker kafka >/dev/null
wait_kafka
docker compose --profile kafka exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --create --if-not-exists --topic "$TOPIC" \
  --partitions 1 --replication-factor 1 >/dev/null
docker compose exec -T mysql mysql -uroot -proot >/dev/null <<SQL
DROP DATABASE IF EXISTS $TEST_DB;
CREATE DATABASE $TEST_DB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
GRANT ALL PRIVILEGES ON $TEST_DB.* TO 'taskflow'@'%';
FLUSH PRIVILEGES;
SQL

restart_isolated_taskflow
wait_ready

phase="normal transactional outbox through Kafka"
normal="$(curl -fsS -X POST "$BASE_URL/api/v1/tasks" -H 'Content-Type: application/json' \
  -d '{"name":"release-kafka-normal","type":"compute","payload":"release-kafka"}')"
normal_id="$(printf '%s' "$normal" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
wait_sql_eq "SELECT status FROM tasks WHERE id=$normal_id" success
wait_kafka_lag_zero

phase="malformed Kafka delivery quarantined then committed"
produce_raw "release-kafka-malformed"
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE source_backend='kafka' AND source_queue='$TOPIC' AND raw_envelope='release-kafka-malformed' AND replay_state='quarantined'" 1
bad_id="$(sql "SELECT id FROM queue_dead_letters WHERE source_backend='kafka' AND source_queue='$TOPIC' AND raw_envelope='release-kafka-malformed'")"
delivery_id="$(sql "SELECT source_delivery_id FROM queue_dead_letters WHERE id=$bad_id")"
printf '%s' "$delivery_id" | grep -Eq '^[0-9]+:[0-9]+$'
wait_kafka_lag_zero
quarantined_before="$(curl -fsS "$BASE_URL/metrics" | awk '$1 ~ /^taskflow_queue_poison_deliveries_total\{backend="kafka",class="decode",outcome="quarantined"/ {print $2; exit}')"
test "${quarantined_before:-0}" != "0"

# A restart with group lag already at zero proves the poison offset was committed;
# the same delivery must not be fetched and quarantined again.
phase="poison commit survives consumer restart"
restart_isolated_taskflow
wait_ready
sleep 1
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE source_backend='kafka' AND source_queue='$TOPIC' AND source_delivery_id='$delivery_id'" 1
wait_kafka_lag_zero

phase="archive purge restore then controlled Kafka replay"
code="$(curl -sS -o /tmp/release-kafka-delete.json -w '%{http_code}' -X DELETE "$BASE_URL/api/v1/queue/dead-letters/$bad_id")"
test "$code" = "204"
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE id=$bad_id" 0
archive_id="$(sql "SELECT id FROM queue_dead_letter_archives WHERE dead_letter_id=$bad_id AND state='ready'")"
test -n "$archive_id"
exported="$(curl -fsS "$BASE_URL/api/v1/queue/dead-letter-archives/$archive_id/export")"
printf '%s' "$exported" | grep -q 'release-kafka-malformed'
curl -fsS -X POST "$BASE_URL/api/v1/queue/dead-letter-archives/$archive_id/restore" >/tmp/release-kafka-restore.json
restored_id="$(sql "SELECT last_restored_dead_letter_id FROM queue_dead_letter_archives WHERE id=$archive_id")"
wait_sql_eq "SELECT replay_state FROM queue_dead_letters WHERE id=$restored_id" quarantined
repair_task="$(insert_task release-kafka-repair 3)"
curl -fsS -X POST "$BASE_URL/api/v1/queue/dead-letters/$restored_id/replay" \
  -H 'Content-Type: application/json' -d "{\"task_id\":$repair_task,\"enqueue_generation\":3}" >/tmp/release-kafka-replay.json
wait_sql_eq "SELECT status FROM tasks WHERE id=$repair_task" success
wait_sql_eq "SELECT replay_state FROM queue_dead_letters WHERE id=$restored_id" replayed
wait_kafka_lag_zero

phase="stale generation cannot bypass Phase 16 on Kafka"
produce_raw "release-kafka-stale"
wait_sql_eq "SELECT COUNT(*) FROM queue_dead_letters WHERE source_backend='kafka' AND source_queue='$TOPIC' AND raw_envelope='release-kafka-stale' AND replay_state='quarantined'" 1
stale_dlq="$(sql "SELECT id FROM queue_dead_letters WHERE raw_envelope='release-kafka-stale' ORDER BY id DESC LIMIT 1")"
stale_task="$(insert_task release-kafka-stale-task 2)"
code="$(curl -sS -o /tmp/release-kafka-stale.json -w '%{http_code}' -X POST "$BASE_URL/api/v1/queue/dead-letters/$stale_dlq/replay" \
  -H 'Content-Type: application/json' -d "{\"task_id\":$stale_task,\"enqueue_generation\":1}")"
test "$code" = "400"
wait_sql_eq "SELECT status FROM tasks WHERE id=$stale_task" pending
wait_sql_eq "SELECT replay_state FROM queue_dead_letters WHERE id=$stale_dlq" quarantined
curl -fsS -X POST "$BASE_URL/api/v1/queue/dead-letters/$stale_dlq/replay" \
  -H 'Content-Type: application/json' -d "{\"task_id\":$stale_task,\"enqueue_generation\":2}" >/dev/null
wait_sql_eq "SELECT status FROM tasks WHERE id=$stale_task" success
wait_kafka_lag_zero

phase="metrics and release evidence"
metrics="$(curl -fsS "$BASE_URL/metrics")"
printf '%s' "$metrics" | grep -q '^taskflow_queue_poison_deliveries_total{backend="kafka",class="decode",outcome="quarantined"}'
printf '%s' "$metrics" | grep -q '^taskflow_queue_dead_letter_replays_total{outcome="success"}'
python3 scripts/release-kafka-dlq-check.py
phase="complete"
echo "real Kafka poison quarantine, commit, archive/restore and controlled replay: ok"
