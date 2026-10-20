#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
PRIMARY_URL="${PHASE30_PRIMARY_URL:-http://127.0.0.1:18084}"
SECONDARY_URL="${PHASE30_SECONDARY_URL:-http://127.0.0.1:18085}"
TOKEN="${PHASE30_TOKEN:-phase30-control-token}"
OPERATOR="${PHASE30_OPERATOR:-phase30-security-admin}"
SINK_TOKEN="${PHASE30_SINK_TOKEN:-phase30-sink-token}"
SIGN_KEY="${PHASE30_SIGNING_KEY:-phase30-signing-key}"
SIGN_KEY_ID="${PHASE30_SIGNING_KEY_ID:-phase30-sign-v1}"
CHECKPOINT_KEY="${PHASE30_CHECKPOINT_KEY:-phase30-checkpoint-secret}"
CHECKPOINT_KEY_ID="${PHASE30_CHECKPOINT_KEY_ID:-phase30-checkpoint-v1}"
PRIMARY_ID="${PHASE30_PRIMARY_RECEIVER_ID:-receiver-primary}"
SECONDARY_ID="${PHASE30_SECONDARY_RECEIVER_ID:-receiver-secondary}"
TEST_DB="${PHASE30_DB:-taskflow_phase30}"
TEST_DSN="taskflow:taskflow@tcp(mysql:3306)/${TEST_DB}?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
DEFAULT_DSN="taskflow:taskflow@tcp(mysql:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
QUEUE_KEY="${PHASE30_QUEUE_KEY:-taskflow:phase30:queue}"
BUCKET="${PHASE30_BUCKET:-taskflow-audit-phase30}"
PRIMARY_ACCESS="${MINIO_PRIMARY_ROOT_USER:-taskflow-primary}"
PRIMARY_SECRET="${MINIO_PRIMARY_ROOT_PASSWORD:-taskflow-primary-secret}"
REPLICA_ACCESS="${MINIO_REPLICA_ROOT_USER:-taskflow-replica}"
REPLICA_SECRET="${MINIO_REPLICA_ROOT_PASSWORD:-taskflow-replica-secret}"
NETWORK=""
phase="initialization"

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 300 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.2
  done
  echo "timeout waiting for $url" >&2
  return 1
}

sql() {
  docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse "$1" 2>/dev/null | tr -d '\r'
}

wait_sql() {
  query="$1"
  i=0
  value=""
  while [ "$i" -lt 300 ]; do
    value="$(sql "$query" || true)"
    if [ "$value" = "1" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.2
  done
  echo "wait_sql failed value=$value query=$query" >&2
  return 1
}

checkpoint_hwm() {
  curl -fsS -H "Authorization: Bearer $SINK_TOKEN" "$1/checkpoint?after_sequence=0&limit=1" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["high_watermark_sequence"])'
}

checkpoint_term() {
  curl -fsS -H "Authorization: Bearer $SINK_TOKEN" "$1/checkpoint?after_sequence=0&limit=1" |
    python3 -c 'import json,sys; d=json.load(sys.stdin); print("%s:%s" % (d["receiver_term"], d["active_receiver"]))'
}

wait_receivers_equal() {
  minimum="$1"
  i=0
  while [ "$i" -lt 300 ]; do
    p="$(checkpoint_hwm "$PRIMARY_URL" 2>/dev/null || echo 0)"
    s="$(checkpoint_hwm "$SECONDARY_URL" 2>/dev/null || echo 0)"
    if [ "$p" = "$s" ] && [ "$p" -ge "$minimum" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.2
  done
  echo "receivers did not converge: primary=${p:-?} secondary=${s:-?} minimum=$minimum" >&2
  return 1
}

s3_tool() {
  endpoint="$1"
  access="$2"
  secret="$3"
  shift 3
  docker run --rm --network "$NETWORK" \
    -e S3_ENDPOINT="$endpoint" \
    -e S3_ACCESS_KEY="$access" \
    -e S3_SECRET_KEY="$secret" \
    -e S3_BUCKET="$BUCKET" \
    -e S3_REGION=us-east-1 \
    -e S3_SECURE=false \
    taskflow-go:phase30-build /out/release-s3-tool "$@"
}

start_taskflow() {
  reconcile="$1"
  reconcile_endpoint=""
  reconcile_receiver=""
  if [ "$reconcile" = "true" ]; then
    reconcile_endpoint="http://siem-sink:8080/checkpoint"
    reconcile_receiver="$PRIMARY_ID"
  fi
  CONTROL_PLANE_AUTH_ENABLED=true \
    CONTROL_PLANE_BEARER_TOKEN="$TOKEN" \
    CONTROL_PLANE_OPERATOR_ID="$OPERATOR" \
    CONTROL_PLANE_AUDIT_ENABLED=true \
    CONTROL_PLANE_AUDIT_RETENTION_ENABLED=false \
    CONTROL_PLANE_SIEM_DELIVERY_ENABLED=true \
    CONTROL_PLANE_SIEM_ENDPOINT=http://siem-sink:8080/events \
    CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
    CONTROL_PLANE_SIEM_SIGNING_PROVIDER=hmac \
    CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" \
    CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" \
    CONTROL_PLANE_SIEM_DELIVERY_INTERVAL_MS=100 \
    CONTROL_PLANE_SIEM_DELIVERY_LEASE_MS=2000 \
    CONTROL_PLANE_SIEM_MAX_ATTEMPTS=30 \
    CONTROL_PLANE_SIEM_RETRY_BASE_MS=100 \
    CONTROL_PLANE_SIEM_RETRY_MAX_MS=500 \
    CONTROL_PLANE_SIEM_DISTRIBUTED_COORDINATION=false \
    CONTROL_PLANE_SIEM_GC_ENABLED=false \
    CONTROL_PLANE_SIEM_DR_ENABLED=true \
    CONTROL_PLANE_SIEM_DR_MIN_COPIES=2 \
    CONTROL_PLANE_SIEM_DR_PRIMARY_S3_ENDPOINT=minio-primary:9000 \
    CONTROL_PLANE_SIEM_DR_PRIMARY_S3_ACCESS_KEY="$PRIMARY_ACCESS" \
    CONTROL_PLANE_SIEM_DR_PRIMARY_S3_SECRET_KEY="$PRIMARY_SECRET" \
    CONTROL_PLANE_SIEM_DR_PRIMARY_S3_BUCKET="$BUCKET" \
    CONTROL_PLANE_SIEM_DR_PRIMARY_S3_REGION=us-east-1 \
    CONTROL_PLANE_SIEM_DR_PRIMARY_S3_SECURE=false \
    CONTROL_PLANE_SIEM_DR_REPLICA_ENABLED=true \
    CONTROL_PLANE_SIEM_DR_REPLICA_S3_ENDPOINT=minio-replica:9000 \
    CONTROL_PLANE_SIEM_DR_REPLICA_S3_ACCESS_KEY="$REPLICA_ACCESS" \
    CONTROL_PLANE_SIEM_DR_REPLICA_S3_SECRET_KEY="$REPLICA_SECRET" \
    CONTROL_PLANE_SIEM_DR_REPLICA_S3_BUCKET="$BUCKET" \
    CONTROL_PLANE_SIEM_DR_REPLICA_S3_REGION=us-east-1 \
    CONTROL_PLANE_SIEM_DR_REPLICA_S3_SECURE=false \
    CONTROL_PLANE_SIEM_PRIMARY_RECEIVER_ID="$PRIMARY_ID" \
    CONTROL_PLANE_SIEM_PRIMARY_CHECKPOINT_ENDPOINT=http://siem-sink:8080/checkpoint \
    CONTROL_PLANE_SIEM_PRIMARY_TERM_ENDPOINT=http://siem-sink:8080/control/term \
    CONTROL_PLANE_SIEM_SECONDARY_ENDPOINT=http://siem-sink-secondary:8080/events \
    CONTROL_PLANE_SIEM_SECONDARY_RECEIVER_ID="$SECONDARY_ID" \
    CONTROL_PLANE_SIEM_SECONDARY_CHECKPOINT_ENDPOINT=http://siem-sink-secondary:8080/checkpoint \
    CONTROL_PLANE_SIEM_SECONDARY_TERM_ENDPOINT=http://siem-sink-secondary:8080/control/term \
    CONTROL_PLANE_SIEM_FAILOVER_ENABLED=true \
    CONTROL_PLANE_SIEM_SHADOW_SECONDARY=false \
    CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" \
    CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
    CONTROL_PLANE_SIEM_CLUSTER_GUARD_ENABLED=true \
    CONTROL_PLANE_SIEM_TERM_FENCING_ENABLED=true \
    CONTROL_PLANE_SIEM_ANTI_ENTROPY_ENABLED=true \
    CONTROL_PLANE_SIEM_ANTI_ENTROPY_INTERVAL_MS=200 \
    CONTROL_PLANE_SIEM_RECONCILE_ENABLED="$reconcile" \
    CONTROL_PLANE_SIEM_RECONCILE_ENDPOINT="$reconcile_endpoint" \
    CONTROL_PLANE_SIEM_RECONCILE_RECEIVER_ID="$reconcile_receiver" \
    CONTROL_PLANE_SIEM_RECONCILE_MIN_EVIDENCE_COPIES=1 \
    MYSQL_DSN="$TEST_DSN" \
    REDIS_QUEUE_KEY="$QUEUE_KEY" \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null
}

cleanup() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "Phase 30 receiver term/failback DR smoke failed during phase: $phase" >&2
    docker compose --profile phase30-smoke --profile archive-s3 logs --tail 350 taskflow siem-sink siem-sink-secondary minio-primary minio-replica >&2 2>/dev/null || true
  fi
  docker compose --profile archive-s3 start minio-primary minio-replica >/dev/null 2>&1 || true
  docker compose --profile phase30-smoke start siem-sink siem-sink-secondary >/dev/null 2>&1 || true
  wait_http http://127.0.0.1:19000/minio/health/live >/dev/null 2>&1 || true
  wait_http http://127.0.0.1:19002/minio/health/live >/dev/null 2>&1 || true
  if [ -n "$NETWORK" ]; then
    s3_tool minio-primary:9000 "$PRIMARY_ACCESS" "$PRIMARY_SECRET" remove-bucket >/dev/null 2>&1 || true
    s3_tool minio-replica:9000 "$REPLICA_ACCESS" "$REPLICA_SECRET" remove-bucket >/dev/null 2>&1 || true
  fi
  CONTROL_PLANE_AUTH_ENABLED=false \
    CONTROL_PLANE_BEARER_TOKEN= \
    CONTROL_PLANE_OPERATOR_ID=operator \
    CONTROL_PLANE_AUDIT_ENABLED=true \
    CONTROL_PLANE_AUDIT_RETENTION_ENABLED=true \
    CONTROL_PLANE_SIEM_DELIVERY_ENABLED=false \
    CONTROL_PLANE_SIEM_SIGNING_PROVIDER=hmac \
    CONTROL_PLANE_SIEM_FAILOVER_ENABLED=false \
    CONTROL_PLANE_SIEM_SHADOW_SECONDARY=false \
    CONTROL_PLANE_SIEM_CLUSTER_GUARD_ENABLED=false \
    CONTROL_PLANE_SIEM_TERM_FENCING_ENABLED=false \
    CONTROL_PLANE_SIEM_ANTI_ENTROPY_ENABLED=false \
    CONTROL_PLANE_SIEM_RECONCILE_ENABLED=false \
    CONTROL_PLANE_SIEM_DR_ENABLED=false \
    MYSQL_DSN="$DEFAULT_DSN" \
    REDIS_QUEUE_KEY=taskflow:queue \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null 2>&1 || true
  docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}" >/dev/null 2>&1 || true
  docker compose --profile phase30-smoke rm -sf siem-sink siem-sink-secondary >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT

phase="build Phase 30 images and static guardrails"
docker build --target build -t taskflow-go:phase30-build --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
docker build -t taskflow-go:local --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
python3 scripts/phase30-receiver-term-failback-dr-check.py

phase="start two signed-checkpoint receivers and two MinIO evidence regions"
CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" \
CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" \
CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" \
CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
PHASE29_PRIMARY_RECEIVER_ID="$PRIMARY_ID" \
PHASE29_SECONDARY_RECEIVER_ID="$SECONDARY_ID" \
PHASE30_ACTIVE_RECEIVER="$PRIMARY_ID" \
  docker compose --profile phase30-smoke --profile archive-s3 up -d mysql redis executor-worker minio-primary minio-replica >/dev/null
CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" \
CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" \
CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" \
CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
PHASE29_PRIMARY_RECEIVER_ID="$PRIMARY_ID" \
PHASE29_SECONDARY_RECEIVER_ID="$SECONDARY_ID" \
PHASE30_ACTIVE_RECEIVER="$PRIMARY_ID" \
  docker compose --profile phase30-smoke up -d --no-deps --force-recreate siem-sink siem-sink-secondary >/dev/null
wait_http "$PRIMARY_URL/health"
wait_http "$SECONDARY_URL/health"
wait_http http://127.0.0.1:19000/minio/health/live
wait_http http://127.0.0.1:19002/minio/health/live

phase="create isolated Phase 30 database and evidence buckets"
mysql_id="$(docker compose ps -q mysql)"
NETWORK="$(docker inspect "$mysql_id" --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}')"
[ -n "$NETWORK" ]
s3_tool minio-primary:9000 "$PRIMARY_ACCESS" "$PRIMARY_SECRET" remove-bucket >/dev/null 2>&1 || true
s3_tool minio-replica:9000 "$REPLICA_ACCESS" "$REPLICA_SECRET" remove-bucket >/dev/null 2>&1 || true
s3_tool minio-primary:9000 "$PRIMARY_ACCESS" "$PRIMARY_SECRET" ensure-bucket >/dev/null
s3_tool minio-replica:9000 "$REPLICA_ACCESS" "$REPLICA_SECRET" ensure-bucket >/dev/null
docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}; CREATE DATABASE ${TEST_DB}; GRANT ALL PRIVILEGES ON ${TEST_DB}.* TO 'taskflow'@'%'; FLUSH PRIVILEGES" >/dev/null
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null

phase="start term-fenced primary and create standby lag"
start_taskflow false
wait_http "$BASE_URL/health/ready"
[ "$(sql "SELECT CONCAT(active_receiver,':',term) FROM ${TEST_DB}.control_plane_siem_receiver_routing_state WHERE id=1")" = "${PRIMARY_ID}:1" ]
docker compose --profile phase30-smoke stop siem-sink-secondary >/dev/null
for request_id in phase30-lag-1 phase30-lag-2 phase30-lag-3; do
  curl -fsS -H "Authorization: Bearer $TOKEN" -H "X-Request-ID: $request_id" "$BASE_URL/api/v1/ai/routing-policy" >/dev/null
  wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='${request_id}' AND r.receiver_id='${PRIMARY_ID}' AND r.receiver_term=1 AND r.replica_copies=2"
done
[ "$(checkpoint_hwm "$PRIMARY_URL")" -ge 3 ]

phase="prove standby anti-entropy catch-up"
CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" \
CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" \
CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" \
CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
PHASE29_SECONDARY_RECEIVER_ID="$SECONDARY_ID" \
PHASE30_ACTIVE_RECEIVER="$PRIMARY_ID" \
  docker compose --profile phase30-smoke start siem-sink-secondary >/dev/null
wait_http "$SECONDARY_URL/health"
wait_receivers_equal 3
curl -fsS "$BASE_URL/metrics" >/tmp/phase30-anti-entropy-metrics.txt
python3 - <<'PY'
import re
text = open('/tmp/phase30-anti-entropy-metrics.txt').read()
m = re.search(r'^taskflow_control_plane_siem_anti_entropy_replayed_total\s+([0-9.eE+-]+)$', text, re.M)
assert m and float(m.group(1)) >= 3, text
PY

phase="automatically fail over to secondary with receiver term 2"
curl -fsS -X POST -H 'Content-Type: application/json' -d '{"fail_always":true}' "$PRIMARY_URL/admin/mode" >/dev/null
curl -fsS -H "Authorization: Bearer $TOKEN" -H 'X-Request-ID: phase30-auto-failover' "$BASE_URL/api/v1/ai/routing-policy" >/dev/null
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase30-auto-failover' AND r.receiver_id='${SECONDARY_ID}' AND r.receiver_term=2"
[ "$(sql "SELECT CONCAT(active_receiver,':',term) FROM ${TEST_DB}.control_plane_siem_receiver_routing_state WHERE id=1")" = "${SECONDARY_ID}:2" ]
[ "$(checkpoint_term "$PRIMARY_URL")" = "2:${SECONDARY_ID}" ]
[ "$(checkpoint_term "$SECONDARY_URL")" = "2:${SECONDARY_ID}" ]

phase="prove stale term fencing and CAS promotion"
stale_code="$(curl -sS -o /tmp/phase30-stale-term.json -w '%{http_code}' -X POST -H "Authorization: Bearer $SINK_TOKEN" -H 'Content-Type: application/json' -d "{\"term\":1,\"active_receiver\":\"${PRIMARY_ID}\"}" "$PRIMARY_URL/control/term")"
[ "$stale_code" = "409" ]
stale_promote_code="$(curl -sS -o /tmp/phase30-stale-promote.json -w '%{http_code}' -X POST -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -H 'X-Request-ID: phase30-stale-promotion' -d "{\"target_receiver\":\"${PRIMARY_ID}\",\"expected_term\":1}" "$BASE_URL/api/v1/control-plane/siem/receivers/promote")"
[ "$stale_promote_code" = "409" ]
[ "$(curl -fsS "$PRIMARY_URL/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["stale_term_rejections"])')" -ge 1 ]
[ "$(sql "SELECT CONCAT(active_receiver,':',term) FROM ${TEST_DB}.control_plane_siem_receiver_routing_state WHERE id=1")" = "${SECONDARY_ID}:2" ]

phase="recover primary, catch it up and perform controlled failback to term 3"
curl -fsS -X POST -H 'Content-Type: application/json' -d '{"fail_always":false}' "$PRIMARY_URL/admin/mode" >/dev/null
secondary_before_failback="$(checkpoint_hwm "$SECONDARY_URL")"
wait_receivers_equal "$secondary_before_failback"
curl -fsS -X POST -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -H 'X-Request-ID: phase30-controlled-failback' -d "{\"target_receiver\":\"${PRIMARY_ID}\",\"expected_term\":2}" "$BASE_URL/api/v1/control-plane/siem/receivers/promote" >/tmp/phase30-failback.json
python3 - <<'PY'
import json
d = json.load(open('/tmp/phase30-failback.json'))
assert d['active_receiver'] == 'receiver-primary', d
assert d['term'] == 3 and d['previous_term'] == 2, d
PY
[ "$(sql "SELECT CONCAT(active_receiver,':',term) FROM ${TEST_DB}.control_plane_siem_receiver_routing_state WHERE id=1")" = "${PRIMARY_ID}:3" ]
[ "$(checkpoint_term "$PRIMARY_URL")" = "3:${PRIMARY_ID}" ]
[ "$(checkpoint_term "$SECONDARY_URL")" = "3:${PRIMARY_ID}" ]
curl -fsS -H "Authorization: Bearer $TOKEN" -H 'X-Request-ID: phase30-post-failback' "$BASE_URL/api/v1/ai/routing-policy" >/dev/null
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase30-post-failback' AND r.receiver_id='${PRIMARY_ID}' AND r.receiver_term=3 AND r.replica_copies=2"
primary_after_failback="$(checkpoint_hwm "$PRIMARY_URL")"
wait_receivers_equal "$primary_after_failback"

phase="verify measured RPO zero and nonzero switchover RTO"
i=0
while [ "$i" -lt 100 ]; do
  curl -fsS "$BASE_URL/metrics" >/tmp/phase30-rpo-rto-metrics.txt
  if python3 - <<'PY'
import re
text = open('/tmp/phase30-rpo-rto-metrics.txt').read()
def value(name):
    m = re.search(r'^' + re.escape(name) + r'\s+([0-9.eE+-]+)$', text, re.M)
    return float(m.group(1)) if m else None
raise SystemExit(0 if value('taskflow_control_plane_siem_receiver_rpo_sequences') == 0 else 1)
PY
  then
    break
  fi
  i=$((i + 1))
  sleep 0.1
done
python3 - <<'PY'
import re
text = open('/tmp/phase30-rpo-rto-metrics.txt').read()
def value(name):
    m = re.search(r'^' + re.escape(name) + r'\s+([0-9.eE+-]+)$', text, re.M)
    assert m, name
    return float(m.group(1))
assert value('taskflow_control_plane_siem_receiver_rpo_sequences') == 0
assert value('taskflow_control_plane_siem_receiver_term') == 3
assert value('taskflow_control_plane_siem_switchover_rto_seconds') > 0
PY

phase="restore routing term and SIEM hot state from signed checkpoints and surviving evidence"
wait_sql "SELECT IF(COUNT(*)=0,1,0) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE state <> 'delivered'"
final_hwm="$(checkpoint_hwm "$PRIMARY_URL")"
wait_receivers_equal "$final_hwm"
primary_event_requests_before="$(curl -fsS "$PRIMARY_URL/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["requests"])')"
secondary_event_requests_before="$(curl -fsS "$SECONDARY_URL/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["requests"])')"
docker compose stop taskflow >/dev/null
docker compose exec -T mysql mysql -uroot -proot "$TEST_DB" -e "DROP TABLE IF EXISTS control_plane_audit_delivery_dlq, control_plane_audit_delivery_receipts, control_plane_audit_deliveries, control_plane_siem_delivery_coordinator_state, control_plane_siem_receiver_routing_state;" >/dev/null
docker compose --profile archive-s3 stop minio-primary >/dev/null
start_taskflow true
wait_http "$BASE_URL/health/ready"
wait_sql "SELECT IF(COUNT(*)=${final_hwm},1,0) FROM ${TEST_DB}.control_plane_audit_delivery_receipts WHERE recovery_source='receiver-checkpoint' AND receiver_id='${PRIMARY_ID}' AND replica_copies=1"
[ "$(sql "SELECT CONCAT(active_receiver,':',term,':',promoted_by) FROM ${TEST_DB}.control_plane_siem_receiver_routing_state WHERE id=1")" = "${PRIMARY_ID}:3:receiver-checkpoint-dr" ]
primary_event_requests_after="$(curl -fsS "$PRIMARY_URL/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["requests"])')"
secondary_event_requests_after="$(curl -fsS "$SECONDARY_URL/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["requests"])')"
[ "$primary_event_requests_after" = "$primary_event_requests_before" ]
[ "$secondary_event_requests_after" = "$secondary_event_requests_before" ]
curl -fsS "$BASE_URL/metrics" >/tmp/phase30-recovery-metrics.txt
grep -q 'taskflow_control_plane_siem_routing_recovery_total 1' /tmp/phase30-recovery-metrics.txt
grep -q 'taskflow_control_plane_siem_reconcile_total{outcome="success"} 1' /tmp/phase30-recovery-metrics.txt
grep -q 'taskflow_control_plane_siem_recovery_evidence_total{source="siem-dr-replica"}' /tmp/phase30-recovery-metrics.txt

phase="restore primary evidence region and prove post-DR durability"
docker compose --profile archive-s3 start minio-primary >/dev/null
wait_http http://127.0.0.1:19000/minio/health/live
curl -fsS -H "Authorization: Bearer $TOKEN" -H 'X-Request-ID: phase30-after-dr' "$BASE_URL/api/v1/ai/routing-policy" >/dev/null
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase30-after-dr' AND r.receiver_id='${PRIMARY_ID}' AND r.receiver_term=3 AND r.replica_copies=2"

python3 scripts/phase30-receiver-term-failback-dr-check.py
echo "Phase 30 receiver term fencing, anti-entropy, controlled failback and RPO/RTO guardrails: ok"
echo "Phase 30 signed-checkpoint routing-state plus surviving-replica SIEM DR restore without event replay: ok"
