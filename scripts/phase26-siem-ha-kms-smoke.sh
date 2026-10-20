#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
ALT_URL="${PHASE26_ALT_URL:-http://127.0.0.1:18086}"
SIEM_URL="${PHASE26_SIEM_URL:-http://127.0.0.1:18084}"
KMS_URL="${PHASE26_KMS_URL:-http://127.0.0.1:19701}"
TOKEN="${PHASE26_TOKEN:-phase26-control-token}"
OPERATOR="${PHASE26_OPERATOR:-phase26-security-admin}"
SINK_TOKEN="${PHASE26_SINK_TOKEN:-phase26-sink-token}"
ACTIVE_KEY="${PHASE26_ACTIVE_KEY:-phase26-active-secret}"
ACTIVE_KEY_ID="${PHASE26_ACTIVE_KEY_ID:-phase26-key-v2}"
PREVIOUS_KEY="${PHASE26_PREVIOUS_KEY:-phase26-previous-secret}"
PREVIOUS_KEY_ID="${PHASE26_PREVIOUS_KEY_ID:-phase26-key-v1}"
KMS_KEY="${PHASE26_KMS_KEY:-phase26-kms-secret}"
KMS_KEY_ID="${PHASE26_KMS_KEY_ID:-phase26-kms-v2}"
KMS_TOKEN="${PHASE26_KMS_TOKEN:-phase26-kms-token}"
TEST_DB="${PHASE26_DB:-taskflow_phase26}"
TEST_DSN="taskflow:taskflow@tcp(mysql:3306)/${TEST_DB}?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
DEFAULT_DSN="taskflow:taskflow@tcp(mysql:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
QUEUE_KEY="${PHASE26_QUEUE_KEY:-taskflow:phase26:queue}"
COORD_KEY="${PHASE26_COORD_KEY:-taskflow:phase26:siem:leader}"
ALT_NAME="${PHASE26_ALT_NAME:-taskflow-phase26-alt}"
phase="initialization"

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 80 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.25
  done
  return 1
}

wait_sql_value() {
  sql="$1"
  predicate="$2"
  i=0
  while [ "$i" -lt 120 ]; do
    value="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse "$sql" 2>/dev/null || true)"
    if [ -n "$value" ] && sh -c "[ \"$value\" $predicate ]"; then
      printf '%s' "$value"
      return 0
    fi
    i=$((i + 1))
    sleep 0.25
  done
  return 1
}

metric_value() {
  url="$1"
  name="$2"
  curl -fsS "$url/metrics" | awk -v n="$name" '$1 == n {print $2; exit}'
}

wait_metric() {
  url="$1"
  name="$2"
  want="$3"
  i=0
  while [ "$i" -lt 100 ]; do
    value="$(metric_value "$url" "$name" 2>/dev/null || true)"
    if [ "$value" = "$want" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.2
  done
  return 1
}

cleanup() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "Phase 26 SIEM HA/KMS smoke failed during phase: $phase" >&2
    docker compose --profile phase26-smoke logs --tail 200 taskflow siem-sink kms-signer >&2 2>/dev/null || true
    docker logs --tail 100 "$ALT_NAME" >&2 2>/dev/null || true
  fi
  docker rm -f "$ALT_NAME" >/dev/null 2>&1 || true
  CONTROL_PLANE_AUTH_ENABLED=false \
    CONTROL_PLANE_BEARER_TOKEN= \
    CONTROL_PLANE_PREVIOUS_BEARER_TOKEN= \
    CONTROL_PLANE_OPERATOR_ID=operator \
    CONTROL_PLANE_PREVIOUS_OPERATOR_ID= \
    CONTROL_PLANE_AUDIT_ENABLED=true \
    CONTROL_PLANE_AUDIT_RETENTION_ENABLED=true \
    CONTROL_PLANE_SIEM_DELIVERY_ENABLED=false \
    CONTROL_PLANE_SIEM_ENDPOINT= \
    CONTROL_PLANE_SIEM_BEARER_TOKEN= \
    CONTROL_PLANE_SIEM_SIGNING_KEY= \
    CONTROL_PLANE_SIEM_PREVIOUS_SIGNING_KEY= \
    CONTROL_PLANE_SIEM_PREVIOUS_SIGNING_KEY_ID= \
    CONTROL_PLANE_SIEM_REMOTE_SIGNER_ENDPOINT= \
    CONTROL_PLANE_SIEM_REMOTE_SIGNER_BEARER_TOKEN= \
    CONTROL_PLANE_SIEM_DISTRIBUTED_COORDINATION=false \
    MYSQL_DSN="$DEFAULT_DSN" \
    REDIS_QUEUE_KEY=taskflow:queue \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  docker compose exec -T redis redis-cli DEL "$COORD_KEY" "${COORD_KEY}:fence" "$QUEUE_KEY" >/dev/null 2>&1 || true
  docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}" >/dev/null 2>&1 || true
  docker compose --profile phase26-smoke rm -sf siem-sink kms-signer >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT

phase="validate remote signer fail-closed configuration"
if docker run --rm \
  -e CONTROL_PLANE_SIEM_DELIVERY_ENABLED=true \
  -e CONTROL_PLANE_SIEM_ENDPOINT=http://siem-sink:8080/events \
  -e CONTROL_PLANE_SIEM_SIGNING_PROVIDER=remote \
  -e CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$KMS_KEY_ID" \
  taskflow-go:local >/tmp/phase26-invalid-remote.log 2>&1; then
  echo "remote SIEM signer unexpectedly started without endpoint" >&2
  exit 1
fi
grep -q 'requires CONTROL_PLANE_SIEM_REMOTE_SIGNER_ENDPOINT' /tmp/phase26-invalid-remote.log

phase="start isolated schema SIEM receiver and remote KMS signer"
SIEM_SIGNING_KEYS="${ACTIVE_KEY_ID}=${ACTIVE_KEY},${PREVIOUS_KEY_ID}=${PREVIOUS_KEY},${KMS_KEY_ID}=${KMS_KEY}" \
CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
CONTROL_PLANE_SIEM_REMOTE_SIGNER_BEARER_TOKEN="$KMS_TOKEN" \
KMS_SIGNING_KEYS="${KMS_KEY_ID}=${KMS_KEY}" \
  docker compose --profile phase26-smoke up -d mysql redis executor-worker siem-sink kms-signer >/dev/null
wait_http "$SIEM_URL/health"
wait_http "$KMS_URL/health"
docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}; CREATE DATABASE ${TEST_DB}; GRANT ALL PRIVILEGES ON ${TEST_DB}.* TO 'taskflow'@'%'; FLUSH PRIVILEGES" >/dev/null
docker compose exec -T redis redis-cli DEL "$COORD_KEY" "${COORD_KEY}:fence" "$QUEUE_KEY" >/dev/null

phase="create retained audit while SIEM delivery is disabled"
CONTROL_PLANE_AUTH_ENABLED=true \
  CONTROL_PLANE_BEARER_TOKEN="$TOKEN" \
  CONTROL_PLANE_OPERATOR_ID="$OPERATOR" \
  CONTROL_PLANE_AUDIT_ENABLED=true \
  CONTROL_PLANE_AUDIT_RETENTION_ENABLED=false \
  CONTROL_PLANE_SIEM_DELIVERY_ENABLED=false \
  MYSQL_DSN="$TEST_DSN" \
  REDIS_QUEUE_KEY="$QUEUE_KEY" \
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null
wait_http "$BASE_URL/health/ready"
curl -fsS -H "Authorization: Bearer $TOKEN" -H 'X-Request-ID: phase26-backfill' "$BASE_URL/api/v1/ai/routing-policy" >/tmp/phase26-backfill.json
backfill_seq="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse "SELECT chain_sequence FROM ${TEST_DB}.control_plane_audits WHERE request_id='phase26-backfill' LIMIT 1")"
[ -n "$backfill_seq" ]
[ "$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE chain_sequence=${backfill_seq}")" = 0 ]

phase="enable active plus previous HMAC signatures with Redis fencing leadership"
CONTROL_PLANE_AUTH_ENABLED=true \
  CONTROL_PLANE_BEARER_TOKEN="$TOKEN" \
  CONTROL_PLANE_OPERATOR_ID="$OPERATOR" \
  CONTROL_PLANE_AUDIT_ENABLED=true \
  CONTROL_PLANE_AUDIT_RETENTION_ENABLED=false \
  CONTROL_PLANE_SIEM_DELIVERY_ENABLED=true \
  CONTROL_PLANE_SIEM_ENDPOINT=http://siem-sink:8080/events \
  CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
  CONTROL_PLANE_SIEM_SIGNING_PROVIDER=hmac \
  CONTROL_PLANE_SIEM_SIGNING_KEY="$ACTIVE_KEY" \
  CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$ACTIVE_KEY_ID" \
  CONTROL_PLANE_SIEM_PREVIOUS_SIGNING_KEY="$PREVIOUS_KEY" \
  CONTROL_PLANE_SIEM_PREVIOUS_SIGNING_KEY_ID="$PREVIOUS_KEY_ID" \
  CONTROL_PLANE_SIEM_DISTRIBUTED_COORDINATION=true \
  CONTROL_PLANE_SIEM_COORDINATION_KEY="$COORD_KEY" \
  CONTROL_PLANE_SIEM_COORDINATION_LEASE_MS=1500 \
  CONTROL_PLANE_SIEM_DELIVERY_INTERVAL_MS=100 \
  CONTROL_PLANE_SIEM_DELIVERY_LEASE_MS=1200 \
  CONTROL_PLANE_SIEM_GC_ENABLED=false \
  MYSQL_DSN="$TEST_DSN" \
  REDIS_QUEUE_KEY="$QUEUE_KEY" \
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null
wait_http "$BASE_URL/health/ready"
wait_metric "$BASE_URL" taskflow_control_plane_siem_leader 1
wait_sql_value "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE chain_sequence=${backfill_seq} AND state='delivered'" '-ge 1' >/dev/null
curl -fsS "$SIEM_URL/stats" >/tmp/phase26-rotation.json
python3 - <<'PY'
import json
s = json.load(open('/tmp/phase26-rotation.json'))
assert s['last_key_id'] == 'phase26-key-v2', s
assert s['last_previous_key_id'] == 'phase26-key-v1', s
assert s['previous_signature_valid'] is True, s
assert s['last_algorithm'] == 'hmac-sha256', s
assert s['signature_failures'] == 0, s
PY
fence_one="$(wait_sql_value "SELECT active_fence FROM ${TEST_DB}.control_plane_siem_delivery_coordinator_state WHERE id=1" '-gt 0')"

phase="start second TaskFlow replica as follower"
main_id="$(docker compose ps -q taskflow)"
network="$(docker inspect "$main_id" --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}')"
[ -n "$network" ]
docker rm -f "$ALT_NAME" >/dev/null 2>&1 || true
docker run -d --name "$ALT_NAME" --network "$network" -p 127.0.0.1:18086:8080 \
  -e HTTP_ADDR=:8080 \
  -e MYSQL_DSN="$TEST_DSN" \
  -e REDIS_ADDR=redis:6379 \
  -e REDIS_QUEUE_KEY="$QUEUE_KEY" \
  -e QUEUE_DLQ_ARCHIVE_DIR=/var/lib/taskflow/dlq-archive \
  -e EXECUTION_BACKEND=grpc \
  -e EXECUTOR_GRPC_TARGET=executor-worker:9091 \
  -e CONTROL_PLANE_AUTH_ENABLED=true \
  -e CONTROL_PLANE_BEARER_TOKEN="$TOKEN" \
  -e CONTROL_PLANE_OPERATOR_ID="$OPERATOR" \
  -e CONTROL_PLANE_AUDIT_ENABLED=true \
  -e CONTROL_PLANE_AUDIT_RETENTION_ENABLED=false \
  -e CONTROL_PLANE_SIEM_DELIVERY_ENABLED=true \
  -e CONTROL_PLANE_SIEM_ENDPOINT=http://siem-sink:8080/events \
  -e CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
  -e CONTROL_PLANE_SIEM_SIGNING_PROVIDER=hmac \
  -e CONTROL_PLANE_SIEM_SIGNING_KEY="$ACTIVE_KEY" \
  -e CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$ACTIVE_KEY_ID" \
  -e CONTROL_PLANE_SIEM_PREVIOUS_SIGNING_KEY="$PREVIOUS_KEY" \
  -e CONTROL_PLANE_SIEM_PREVIOUS_SIGNING_KEY_ID="$PREVIOUS_KEY_ID" \
  -e CONTROL_PLANE_SIEM_DISTRIBUTED_COORDINATION=true \
  -e CONTROL_PLANE_SIEM_COORDINATION_KEY="$COORD_KEY" \
  -e CONTROL_PLANE_SIEM_COORDINATION_LEASE_MS=1500 \
  -e CONTROL_PLANE_SIEM_DELIVERY_INTERVAL_MS=100 \
  -e CONTROL_PLANE_SIEM_DELIVERY_LEASE_MS=1200 \
  -e CONTROL_PLANE_SIEM_GC_ENABLED=false \
  -e ACCESS_LOG=false \
  taskflow-go:local >/dev/null
wait_http "$ALT_URL/health/ready"
wait_metric "$BASE_URL" taskflow_control_plane_siem_leader 1
wait_metric "$ALT_URL" taskflow_control_plane_siem_leader 0
curl -fsS -H "Authorization: Bearer $TOKEN" -H 'X-Request-ID: phase26-follower-write' "$ALT_URL/api/v1/ai/routing-policy" >/tmp/phase26-follower.json
wait_sql_value "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audit_deliveries d JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=d.chain_sequence WHERE a.request_id='phase26-follower-write' AND d.state='delivered' AND d.leader_fence=${fence_one}" '-ge 1' >/dev/null

phase="stop leader and prove fenced follower takeover"
docker compose stop taskflow >/dev/null
wait_metric "$ALT_URL" taskflow_control_plane_siem_leader 1
fence_two="$(wait_sql_value "SELECT active_fence FROM ${TEST_DB}.control_plane_siem_delivery_coordinator_state WHERE id=1" "-gt ${fence_one}")"
curl -fsS -H "Authorization: Bearer $TOKEN" -H 'X-Request-ID: phase26-after-failover' "$ALT_URL/api/v1/ai/routing-policy" >/tmp/phase26-failover.json
wait_sql_value "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audit_deliveries d JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=d.chain_sequence WHERE a.request_id='phase26-after-failover' AND d.state='delivered' AND d.leader_fence=${fence_two}" '-ge 1' >/dev/null
curl -fsS "$SIEM_URL/stats" >/tmp/phase26-ha-stats.json
python3 - <<'PY'
import json
s = json.load(open('/tmp/phase26-ha-stats.json'))
seq = s['accepted_sequences']
assert len(seq) == len(set(seq)), s
assert s['duplicate_batches'] == 0, s
assert s['signature_failures'] == 0, s
PY

phase="switch to digest-only remote KMS signer"
docker rm -f "$ALT_NAME" >/dev/null
# Simulate loss of the Redis election/fence state after a successful takeover.
# The next effective fence must still advance because MySQL is the durable
# monotonic source of truth.
docker compose exec -T redis redis-cli DEL "$COORD_KEY" "${COORD_KEY}:fence" >/dev/null
CONTROL_PLANE_AUTH_ENABLED=true \
  CONTROL_PLANE_BEARER_TOKEN="$TOKEN" \
  CONTROL_PLANE_OPERATOR_ID="$OPERATOR" \
  CONTROL_PLANE_AUDIT_ENABLED=true \
  CONTROL_PLANE_AUDIT_RETENTION_ENABLED=false \
  CONTROL_PLANE_SIEM_DELIVERY_ENABLED=true \
  CONTROL_PLANE_SIEM_ENDPOINT=http://siem-sink:8080/events \
  CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
  CONTROL_PLANE_SIEM_SIGNING_PROVIDER=remote \
  CONTROL_PLANE_SIEM_SIGNING_KEY= \
  CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$KMS_KEY_ID" \
  CONTROL_PLANE_SIEM_REMOTE_SIGNER_ENDPOINT=http://kms-signer:9701/sign \
  CONTROL_PLANE_SIEM_REMOTE_SIGNER_BEARER_TOKEN="$KMS_TOKEN" \
  CONTROL_PLANE_SIEM_REMOTE_SIGNER_ALGORITHM=hmac-sha256-digest \
  CONTROL_PLANE_SIEM_DISTRIBUTED_COORDINATION=true \
  CONTROL_PLANE_SIEM_COORDINATION_KEY="$COORD_KEY" \
  CONTROL_PLANE_SIEM_COORDINATION_LEASE_MS=1500 \
  CONTROL_PLANE_SIEM_DELIVERY_INTERVAL_MS=100 \
  CONTROL_PLANE_SIEM_GC_ENABLED=false \
  MYSQL_DSN="$TEST_DSN" \
  REDIS_QUEUE_KEY="$QUEUE_KEY" \
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null
wait_http "$BASE_URL/health/ready"
wait_metric "$BASE_URL" taskflow_control_plane_siem_leader 1
fence_three="$(wait_sql_value "SELECT active_fence FROM ${TEST_DB}.control_plane_siem_delivery_coordinator_state WHERE id=1" "-gt ${fence_two}")"
printf 'fence_one=%s\nfence_two=%s\nfence_three_after_redis_reset=%s\n' "$fence_one" "$fence_two" "$fence_three" >/tmp/phase26-fences.txt
curl -fsS -H "Authorization: Bearer $TOKEN" -H 'X-Request-ID: phase26-remote-kms' "$BASE_URL/api/v1/ai/routing-policy" >/tmp/phase26-kms.json
wait_sql_value "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audit_deliveries d JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=d.chain_sequence WHERE a.request_id='phase26-remote-kms' AND d.state='delivered' AND d.leader_fence=${fence_three}" '-ge 1' >/dev/null
curl -fsS "$KMS_URL/stats" >/tmp/phase26-kms-stats.json
curl -fsS "$SIEM_URL/stats" >/tmp/phase26-kms-siem-stats.json
python3 - <<'PY'
import json
k = json.load(open('/tmp/phase26-kms-stats.json'))
s = json.load(open('/tmp/phase26-kms-siem-stats.json'))
assert k['requests'] >= 1, k
assert k['auth_failures'] == 0 and k['invalid_requests'] == 0, k
assert k['last_key_id'] == 'phase26-kms-v2', k
assert len(k['last_digest_sha256']) == 64, k
assert s['last_key_id'] == 'phase26-kms-v2', s
assert s['last_algorithm'] == 'hmac-sha256-digest', s
assert s['signature_failures'] == 0, s
PY

phase="prepare delivered and replayed-DLQ history for GC"
curl -fsS -H "Authorization: Bearer $TOKEN" -H 'X-Request-ID: phase26-gc-fresh' "$BASE_URL/api/v1/ai/routing-policy" >/tmp/phase26-gc-fresh.json
fresh_delivery="$(wait_sql_value "SELECT d.id FROM ${TEST_DB}.control_plane_audit_deliveries d JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=d.chain_sequence WHERE a.request_id='phase26-gc-fresh' AND d.state='delivered' LIMIT 1" '-gt 0')"
old_delivery="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse "SELECT id FROM ${TEST_DB}.control_plane_audit_deliveries WHERE state='delivered' AND id<>${fresh_delivery} ORDER BY id ASC LIMIT 1")"
[ -n "$old_delivery" ]
old_sequence="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse "SELECT chain_sequence FROM ${TEST_DB}.control_plane_audit_deliveries WHERE id=${old_delivery}")"
[ -n "$old_sequence" ]
docker compose exec -T mysql mysql -utaskflow -ptaskflow -e "DELETE FROM ${TEST_DB}.control_plane_audit_delivery_receipts WHERE chain_sequence=${old_sequence}; UPDATE ${TEST_DB}.control_plane_audit_deliveries SET delivered_at=DATE_SUB(NOW(), INTERVAL 2 HOUR), updated_at=DATE_SUB(NOW(), INTERVAL 2 HOUR) WHERE state='delivered' AND id<>${fresh_delivery}; INSERT INTO ${TEST_DB}.control_plane_audit_delivery_dlq (delivery_id,audit_id,chain_sequence,record_hash,payload_json,state,failure_count,attempts,last_error,failed_at,replayed_at,created_at,updated_at) SELECT id,audit_id,chain_sequence,record_hash,payload_json,'replayed',1,1,'phase26-gc',DATE_SUB(NOW(), INTERVAL 2 HOUR),DATE_SUB(NOW(), INTERVAL 2 HOUR),DATE_SUB(NOW(), INTERVAL 2 HOUR),DATE_SUB(NOW(), INTERVAL 2 HOUR) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE id=${old_delivery} ON DUPLICATE KEY UPDATE state='replayed', replayed_at=DATE_SUB(NOW(), INTERVAL 2 HOUR), updated_at=DATE_SUB(NOW(), INTERVAL 2 HOUR)" >/dev/null

phase="run bounded delivery history GC"
CONTROL_PLANE_AUTH_ENABLED=true \
  CONTROL_PLANE_BEARER_TOKEN="$TOKEN" \
  CONTROL_PLANE_OPERATOR_ID="$OPERATOR" \
  CONTROL_PLANE_AUDIT_ENABLED=true \
  CONTROL_PLANE_AUDIT_RETENTION_ENABLED=false \
  CONTROL_PLANE_SIEM_DELIVERY_ENABLED=true \
  CONTROL_PLANE_SIEM_ENDPOINT=http://siem-sink:8080/events \
  CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
  CONTROL_PLANE_SIEM_SIGNING_PROVIDER=remote \
  CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$KMS_KEY_ID" \
  CONTROL_PLANE_SIEM_REMOTE_SIGNER_ENDPOINT=http://kms-signer:9701/sign \
  CONTROL_PLANE_SIEM_REMOTE_SIGNER_BEARER_TOKEN="$KMS_TOKEN" \
  CONTROL_PLANE_SIEM_REMOTE_SIGNER_ALGORITHM=hmac-sha256-digest \
  CONTROL_PLANE_SIEM_DISTRIBUTED_COORDINATION=true \
  CONTROL_PLANE_SIEM_COORDINATION_KEY="$COORD_KEY" \
  CONTROL_PLANE_SIEM_COORDINATION_LEASE_MS=1500 \
  CONTROL_PLANE_SIEM_DELIVERY_INTERVAL_MS=100 \
  CONTROL_PLANE_SIEM_GC_ENABLED=true \
  CONTROL_PLANE_SIEM_GC_INTERVAL_MS=250 \
  CONTROL_PLANE_SIEM_DELIVERED_RETENTION_MS=60000 \
  CONTROL_PLANE_SIEM_REPLAYED_DLQ_RETENTION_MS=60000 \
  CONTROL_PLANE_SIEM_GC_BATCH=1000 \
  MYSQL_DSN="$TEST_DSN" \
  REDIS_QUEUE_KEY="$QUEUE_KEY" \
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null
wait_http "$BASE_URL/health/ready"
wait_sql_value "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE state='delivered' AND delivered_at < DATE_SUB(NOW(), INTERVAL 60 SECOND)" '= 0' >/dev/null
wait_sql_value "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audit_delivery_dlq WHERE state='replayed' AND replayed_at < DATE_SUB(NOW(), INTERVAL 60 SECOND)" '= 0' >/dev/null
[ "$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE id=${fresh_delivery} AND state='delivered'")" = 1 ]
[ "$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audit_delivery_receipts WHERE chain_sequence=${old_sequence}")" = 1 ]
[ "$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE chain_sequence=${old_sequence}")" = 0 ]
curl -fsS "$BASE_URL/metrics" >/tmp/phase26-metrics.txt
grep -q 'taskflow_control_plane_siem_leadership_total' /tmp/phase26-metrics.txt
grep -q 'taskflow_control_plane_siem_leader_fence' /tmp/phase26-metrics.txt
grep -q 'taskflow_control_plane_siem_purged_total{class="delivered"}' /tmp/phase26-metrics.txt
grep -q 'taskflow_control_plane_siem_purged_total{class="replayed_dlq"}' /tmp/phase26-metrics.txt

phase="restart after GC and prove delivery receipt suppresses backfill redelivery"
curl -fsS "$SIEM_URL/stats" >/tmp/phase26-before-gc-restart.json
accepted_before_restart="$(python3 -c 'import json; print(json.load(open("/tmp/phase26-before-gc-restart.json"))["accepted_events"])')"
CONTROL_PLANE_AUTH_ENABLED=true \
  CONTROL_PLANE_BEARER_TOKEN="$TOKEN" \
  CONTROL_PLANE_OPERATOR_ID="$OPERATOR" \
  CONTROL_PLANE_AUDIT_ENABLED=true \
  CONTROL_PLANE_AUDIT_RETENTION_ENABLED=false \
  CONTROL_PLANE_SIEM_DELIVERY_ENABLED=true \
  CONTROL_PLANE_SIEM_ENDPOINT=http://siem-sink:8080/events \
  CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
  CONTROL_PLANE_SIEM_SIGNING_PROVIDER=remote \
  CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$KMS_KEY_ID" \
  CONTROL_PLANE_SIEM_REMOTE_SIGNER_ENDPOINT=http://kms-signer:9701/sign \
  CONTROL_PLANE_SIEM_REMOTE_SIGNER_BEARER_TOKEN="$KMS_TOKEN" \
  CONTROL_PLANE_SIEM_REMOTE_SIGNER_ALGORITHM=hmac-sha256-digest \
  CONTROL_PLANE_SIEM_DISTRIBUTED_COORDINATION=true \
  CONTROL_PLANE_SIEM_COORDINATION_KEY="$COORD_KEY" \
  CONTROL_PLANE_SIEM_COORDINATION_LEASE_MS=1500 \
  CONTROL_PLANE_SIEM_DELIVERY_INTERVAL_MS=100 \
  CONTROL_PLANE_SIEM_GC_ENABLED=false \
  MYSQL_DSN="$TEST_DSN" \
  REDIS_QUEUE_KEY="$QUEUE_KEY" \
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null
wait_http "$BASE_URL/health/ready"
wait_metric "$BASE_URL" taskflow_control_plane_siem_leader 1
sleep 1
curl -fsS "$SIEM_URL/stats" >/tmp/phase26-after-gc-restart.json
accepted_after_restart="$(python3 -c 'import json; print(json.load(open("/tmp/phase26-after-gc-restart.json"))["accepted_events"])')"
[ "$accepted_after_restart" = "$accepted_before_restart" ]
[ "$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE chain_sequence=${old_sequence}")" = 0 ]
[ "$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audit_delivery_receipts WHERE chain_sequence=${old_sequence}")" = 1 ]

python3 scripts/phase26-siem-ha-kms-check.py
echo "Phase 26 SIEM key rotation, remote KMS signer, leader fencing and delivery GC: ok"
