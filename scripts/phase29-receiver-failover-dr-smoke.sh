#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
PRIMARY_URL="${PHASE29_PRIMARY_URL:-http://127.0.0.1:18084}"
SECONDARY_URL="${PHASE29_SECONDARY_URL:-http://127.0.0.1:18085}"
TOKEN="${PHASE29_TOKEN:-phase29-control-token}"
OPERATOR="${PHASE29_OPERATOR:-phase29-security-admin}"
SINK_TOKEN="${PHASE29_SINK_TOKEN:-phase29-sink-token}"
SIGN_KEY="${PHASE29_SIGNING_KEY:-phase29-signing-key}"
SIGN_KEY_ID="${PHASE29_SIGNING_KEY_ID:-phase29-sign-v1}"
CHECKPOINT_KEY="${PHASE29_CHECKPOINT_KEY:-phase29-checkpoint-secret}"
CHECKPOINT_KEY_ID="${PHASE29_CHECKPOINT_KEY_ID:-phase29-checkpoint-v1}"
PRIMARY_ID="${PHASE29_PRIMARY_RECEIVER_ID:-receiver-primary}"
SECONDARY_ID="${PHASE29_SECONDARY_RECEIVER_ID:-receiver-secondary}"
TEST_DB="${PHASE29_DB:-taskflow_phase29}"
TEST_DSN="taskflow:taskflow@tcp(mysql:3306)/${TEST_DB}?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
DEFAULT_DSN="taskflow:taskflow@tcp(mysql:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
QUEUE_KEY="${PHASE29_QUEUE_KEY:-taskflow:phase29:queue}"
BUCKET="${PHASE29_BUCKET:-taskflow-audit-phase29}"
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
    taskflow-go:phase29-build /out/release-s3-tool "$@"
}

start_taskflow() {
  reconcile="$1"
  cluster_guard="$2"
  reconcile_endpoint=""
  reconcile_receiver=""
  if [ "$reconcile" = "true" ]; then
    reconcile_endpoint="http://siem-sink-secondary:8080/checkpoint"
    reconcile_receiver="$SECONDARY_ID"
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
    CONTROL_PLANE_SIEM_MAX_ATTEMPTS=20 \
    CONTROL_PLANE_SIEM_RETRY_BASE_MS=200 \
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
    CONTROL_PLANE_SIEM_SECONDARY_ENDPOINT=http://siem-sink-secondary:8080/events \
    CONTROL_PLANE_SIEM_SECONDARY_RECEIVER_ID="$SECONDARY_ID" \
    CONTROL_PLANE_SIEM_SECONDARY_CHECKPOINT_ENDPOINT=http://siem-sink-secondary:8080/checkpoint \
    CONTROL_PLANE_SIEM_FAILOVER_ENABLED=true \
    CONTROL_PLANE_SIEM_SHADOW_SECONDARY=true \
    CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" \
    CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
    CONTROL_PLANE_SIEM_CLUSTER_GUARD_ENABLED="$cluster_guard" \
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
    echo "Phase 29 receiver failover/DR smoke failed during phase: $phase" >&2
    docker compose --profile phase29-smoke --profile archive-s3 logs --tail 300 taskflow siem-sink siem-sink-secondary minio-primary minio-replica >&2 2>/dev/null || true
  fi
  docker compose --profile archive-s3 start minio-primary minio-replica >/dev/null 2>&1 || true
  docker compose --profile phase29-smoke start siem-sink siem-sink-secondary >/dev/null 2>&1 || true
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
    CONTROL_PLANE_SIEM_RECONCILE_ENABLED=false \
    CONTROL_PLANE_SIEM_DR_ENABLED=false \
    MYSQL_DSN="$DEFAULT_DSN" \
    REDIS_QUEUE_KEY=taskflow:queue \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null 2>&1 || true
  docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}" >/dev/null 2>&1 || true
  docker compose --profile phase29-smoke rm -sf siem-sink siem-sink-secondary >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT

phase="build Phase 29 images and static guardrails"
docker build --target build -t taskflow-go:phase29-build --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
docker build -t taskflow-go:local --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
python3 scripts/phase29-receiver-failover-dr-check.py

phase="start two independent signed-checkpoint receivers and two MinIO evidence regions"
CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" \
CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" \
CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" \
CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
PHASE29_PRIMARY_RECEIVER_ID="$PRIMARY_ID" \
PHASE29_SECONDARY_RECEIVER_ID="$SECONDARY_ID" \
  docker compose --profile phase29-smoke --profile archive-s3 up -d mysql redis executor-worker minio-primary minio-replica >/dev/null
CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" \
CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" \
CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" \
CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
PHASE29_PRIMARY_RECEIVER_ID="$PRIMARY_ID" \
PHASE29_SECONDARY_RECEIVER_ID="$SECONDARY_ID" \
  docker compose --profile phase29-smoke up -d --no-deps --force-recreate siem-sink siem-sink-secondary >/dev/null
wait_http "$PRIMARY_URL/health"
wait_http "$SECONDARY_URL/health"
wait_http http://127.0.0.1:19000/minio/health/live
wait_http http://127.0.0.1:19002/minio/health/live

phase="create isolated database and evidence buckets"
mysql_id="$(docker compose ps -q mysql)"
NETWORK="$(docker inspect "$mysql_id" --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}')"
[ -n "$NETWORK" ]
s3_tool minio-primary:9000 "$PRIMARY_ACCESS" "$PRIMARY_SECRET" remove-bucket >/dev/null 2>&1 || true
s3_tool minio-replica:9000 "$REPLICA_ACCESS" "$REPLICA_SECRET" remove-bucket >/dev/null 2>&1 || true
s3_tool minio-primary:9000 "$PRIMARY_ACCESS" "$PRIMARY_SECRET" ensure-bucket >/dev/null
s3_tool minio-replica:9000 "$REPLICA_ACCESS" "$REPLICA_SECRET" ensure-bucket >/dev/null
docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}; CREATE DATABASE ${TEST_DB}; GRANT ALL PRIVILEGES ON ${TEST_DB}.* TO 'taskflow'@'%'; FLUSH PRIVILEGES" >/dev/null
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null

phase="start guarded primary with warm standby shadowing"
start_taskflow false true
wait_http "$BASE_URL/health/ready"
for request_id in phase29-warm-1 phase29-warm-2 phase29-warm-3; do
  curl -fsS -H "Authorization: Bearer $TOKEN" -H "X-Request-ID: $request_id" "$BASE_URL/api/v1/ai/routing-policy" >/dev/null
  wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='${request_id}' AND r.receiver_id='${PRIMARY_ID}' AND r.receiver_gen=1 AND r.replica_copies=2"
done
python3 - <<'PY'
import json, urllib.request
for url in ('http://127.0.0.1:18084/stats', 'http://127.0.0.1:18085/stats'):
    with urllib.request.urlopen(url) as response:
        stats = json.load(response)
    assert stats['accepted_events'] == 3, stats
    assert stats['accepted_sequences'] == [1, 2, 3], stats
PY

phase="verify immutable evidence seals both receiver proofs"
seq_one="$(sql "SELECT r.chain_sequence FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase29-warm-1' LIMIT 1")"
idem_one="$(sql "SELECT r.idempotency_key FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase29-warm-1' LIMIT 1")"
pad_one="$(printf '%020d' "$seq_one")"
object_one="control-plane-audit/${pad_one}-${pad_one}-${idem_one}.json"
s3_tool minio-replica:9000 "$REPLICA_ACCESS" "$REPLICA_SECRET" get "$object_one" >/tmp/phase29-evidence.json
python3 - <<'PY'
import json
e = json.load(open('/tmp/phase29-evidence.json'))
proofs = e['receiver_receipts']
assert {p['receiver_id'] for p in proofs} == {'receiver-primary', 'receiver-secondary'}, e
assert len(e['receiver_receipts_sha256']) == 64, e
PY

phase="prove signed split-brain checkpoint fences startup"
curl -fsS -X POST -H 'Content-Type: application/json' -d '{"checkpoint_hash_override_sequence":3,"checkpoint_hash_override":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"}' "$SECONDARY_URL/admin/mode" >/dev/null
docker compose stop taskflow >/dev/null
start_taskflow false true
sleep 1
if curl -fsS "$BASE_URL/health/ready" >/dev/null 2>&1; then
  echo "TaskFlow became ready despite signed divergent secondary checkpoint" >&2
  exit 1
fi
docker compose logs --tail 120 taskflow >/tmp/phase29-divergence.log 2>&1 || true
grep -Eq 'receiver cluster divergence|checkpoint head diverges|split-brain' /tmp/phase29-divergence.log
curl -fsS -X POST -H 'Content-Type: application/json' -d '{"checkpoint_hash_override_sequence":0,"checkpoint_hash_override":""}' "$SECONDARY_URL/admin/mode" >/dev/null
start_taskflow false true
wait_http "$BASE_URL/health/ready"

phase="fail primary event delivery and switch only after standby signed checkpoint is current"
curl -fsS -X POST -H 'Content-Type: application/json' -d '{"fail_always":true}' "$PRIMARY_URL/admin/mode" >/dev/null
curl -fsS -H "Authorization: Bearer $TOKEN" -H 'X-Request-ID: phase29-failover' "$BASE_URL/api/v1/ai/routing-policy" >/dev/null
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase29-failover' AND r.receiver_id='${SECONDARY_ID}' AND r.replica_copies=2"
curl -fsS "$BASE_URL/metrics" >/tmp/phase29-failover-metrics.txt
grep -q 'taskflow_control_plane_siem_deliveries_total{outcome="failover_delivered"}' /tmp/phase29-failover-metrics.txt
primary_accepted_after_failover="$(curl -fsS "$PRIMARY_URL/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["accepted_events"])')"
[ "$primary_accepted_after_failover" = 3 ]

phase="prove sticky secondary remains active after primary recovers"
curl -fsS -X POST -H 'Content-Type: application/json' -d '{"fail_always":false}' "$PRIMARY_URL/admin/mode" >/dev/null
curl -fsS -H "Authorization: Bearer $TOKEN" -H 'X-Request-ID: phase29-sticky' "$BASE_URL/api/v1/ai/routing-policy" >/dev/null
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase29-sticky' AND r.receiver_id='${SECONDARY_ID}'"
[ "$(curl -fsS "$PRIMARY_URL/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["accepted_events"])')" = 3 ]
[ "$(curl -fsS "$SECONDARY_URL/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["accepted_events"])')" = 5 ]

phase="restart cluster and select secondary from signed higher checkpoint"
docker compose stop taskflow >/dev/null
start_taskflow false true
wait_http "$BASE_URL/health/ready"
docker compose logs --tail 120 taskflow >/tmp/phase29-restart.log 2>&1 || true
grep -q 'receiver cluster starts on secondary' /tmp/phase29-restart.log
curl -fsS -H "Authorization: Bearer $TOKEN" -H 'X-Request-ID: phase29-post-restart' "$BASE_URL/api/v1/ai/routing-policy" >/dev/null
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase29-post-restart' AND r.receiver_id='${SECONDARY_ID}'"
[ "$(curl -fsS "$PRIMARY_URL/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["accepted_events"])')" = 3 ]
[ "$(curl -fsS "$SECONDARY_URL/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["accepted_events"])')" = 6 ]

phase="restore SIEM hot state from secondary signed checkpoint and replica evidence"
secondary_requests_before="$(curl -fsS "$SECONDARY_URL/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["requests"])')"
docker compose stop taskflow >/dev/null
docker compose exec -T mysql mysql -uroot -proot "$TEST_DB" -e "DROP TABLE IF EXISTS control_plane_audit_delivery_dlq, control_plane_audit_delivery_receipts, control_plane_audit_deliveries, control_plane_siem_delivery_coordinator_state;" >/dev/null
docker compose --profile phase29-smoke stop siem-sink >/dev/null
docker compose --profile archive-s3 stop minio-primary >/dev/null
start_taskflow true false
wait_http "$BASE_URL/health/ready"
wait_sql "SELECT IF(COUNT(*)=6,1,0) FROM ${TEST_DB}.control_plane_audit_delivery_receipts WHERE recovery_source='receiver-checkpoint' AND receiver_id='${SECONDARY_ID}' AND receiver_gen=1 AND replica_copies=1"
[ "$(sql "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE state <> 'delivered'")" = 0 ]
secondary_requests_after="$(curl -fsS "$SECONDARY_URL/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["requests"])')"
[ "$secondary_requests_after" = "$secondary_requests_before" ]
curl -fsS "$BASE_URL/metrics" >/tmp/phase29-recovery-metrics.txt
grep -q 'taskflow_control_plane_siem_reconcile_total{outcome="success"} 1' /tmp/phase29-recovery-metrics.txt
grep -q 'taskflow_control_plane_siem_recovery_evidence_total{source="siem-dr-replica"}' /tmp/phase29-recovery-metrics.txt

phase="restore evidence primary and continue receiver failover after DR restore"
docker compose --profile archive-s3 start minio-primary >/dev/null
wait_http http://127.0.0.1:19000/minio/health/live
curl -fsS -H "Authorization: Bearer $TOKEN" -H 'X-Request-ID: phase29-after-dr' "$BASE_URL/api/v1/ai/routing-policy" >/dev/null
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase29-after-dr' AND r.receiver_id='${SECONDARY_ID}' AND r.replica_copies=2"
[ "$(curl -fsS "$SECONDARY_URL/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["accepted_events"])')" = 7 ]

python3 scripts/phase29-receiver-failover-dr-check.py
echo "Phase 29 signed receiver checkpoints, split-brain fencing and sticky active/passive failover: ok"
echo "Phase 29 secondary-checkpoint plus surviving replica restored SIEM hot state without event replay: ok"
