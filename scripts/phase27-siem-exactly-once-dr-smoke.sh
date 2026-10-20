#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
SIEM_URL="${PHASE27_SIEM_URL:-http://127.0.0.1:18084}"
KMS_URL="${PHASE27_KMS_URL:-http://127.0.0.1:19702}"
TOKEN="${PHASE27_TOKEN:-phase27-control-token}"
OPERATOR="${PHASE27_OPERATOR:-phase27-security-admin}"
SINK_TOKEN="${PHASE27_SINK_TOKEN:-phase27-sink-token}"
AWS_ACCESS="${PHASE27_AWS_ACCESS_KEY_ID:-phase27-access}"
AWS_SECRET="${PHASE27_AWS_SECRET_ACCESS_KEY:-phase27-secret}"
AWS_REGION="${PHASE27_AWS_REGION:-ap-southeast-1}"
KMS_KEY_ID="${PHASE27_KMS_KEY_ID:-phase27-kms-key}"
KMS_HMAC_KEY="${PHASE27_KMS_HMAC_KEY:-phase27-kms-hmac-secret}"
TEST_DB="${PHASE27_DB:-taskflow_phase27}"
TEST_DSN="taskflow:taskflow@tcp(mysql:3306)/${TEST_DB}?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
DEFAULT_DSN="taskflow:taskflow@tcp(mysql:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
QUEUE_KEY="${PHASE27_QUEUE_KEY:-taskflow:phase27:queue}"
BUCKET="${PHASE27_BUCKET:-taskflow-audit-phase27}"
PRIMARY_ACCESS="${MINIO_PRIMARY_ROOT_USER:-taskflow-primary}"
PRIMARY_SECRET="${MINIO_PRIMARY_ROOT_PASSWORD:-taskflow-primary-secret}"
REPLICA_ACCESS="${MINIO_REPLICA_ROOT_USER:-taskflow-replica}"
REPLICA_SECRET="${MINIO_REPLICA_ROOT_PASSWORD:-taskflow-replica-secret}"
phase="initialization"
NETWORK=""

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 100 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.2
  done
  return 1
}

wait_sql() {
  sql="$1"
  i=0
  while [ "$i" -lt 160 ]; do
    value="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse "$sql" 2>/dev/null || true)"
    if [ "$value" = "1" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.2
  done
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
    taskflow-go:phase27-build /out/release-s3-tool "$@"
}

cleanup() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "Phase 27 SIEM exactly-once/DR smoke failed during phase: $phase" >&2
    docker compose --profile phase27-smoke --profile archive-s3 logs --tail 200 taskflow siem-sink aws-kms minio-primary minio-replica >&2 2>/dev/null || true
  fi
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
    CONTROL_PLANE_SIEM_AWS_KMS_ENDPOINT= \
    CONTROL_PLANE_SIEM_AWS_KMS_REGION= \
    CONTROL_PLANE_SIEM_AWS_ACCESS_KEY_ID= \
    CONTROL_PLANE_SIEM_AWS_SECRET_ACCESS_KEY= \
    CONTROL_PLANE_SIEM_AWS_KMS_KEY_ID= \
    CONTROL_PLANE_SIEM_DR_ENABLED=false \
    MYSQL_DSN="$DEFAULT_DSN" \
    REDIS_QUEUE_KEY=taskflow:queue \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null 2>&1 || true
  docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}" >/dev/null 2>&1 || true
  docker compose --profile phase27-smoke rm -sf siem-sink aws-kms >/dev/null 2>&1 || true
  docker compose --profile phase27-smoke --profile archive-s3 stop minio-primary minio-replica >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT

phase="build Phase 27 builder and validate static guardrails"
docker build --target build -t taskflow-go:phase27-build --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
python3 scripts/phase27-siem-exactly-once-dr-check.py

phase="start SIEM KMS and independent MinIO regions"
SIEM_SIGNING_KEYS="${KMS_KEY_ID}=${KMS_HMAC_KEY}" \
CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
CONTROL_PLANE_SIEM_AWS_ACCESS_KEY_ID="$AWS_ACCESS" \
CONTROL_PLANE_SIEM_AWS_SECRET_ACCESS_KEY="$AWS_SECRET" \
CONTROL_PLANE_SIEM_AWS_KMS_REGION="$AWS_REGION" \
CONTROL_PLANE_SIEM_AWS_KMS_KEY_ID="$KMS_KEY_ID" \
PHASE27_KMS_HMAC_KEY="$KMS_HMAC_KEY" \
  docker compose --profile phase27-smoke --profile archive-s3 up -d mysql redis executor-worker siem-sink aws-kms minio-primary minio-replica >/dev/null
wait_http "$SIEM_URL/health"
wait_http "$KMS_URL/health"
wait_http http://127.0.0.1:19000/minio/health/live
wait_http http://127.0.0.1:19002/minio/health/live
mysql_id="$(docker compose ps -q mysql)"
NETWORK="$(docker inspect "$mysql_id" --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}')"
[ -n "$NETWORK" ]
s3_tool minio-primary:9000 "$PRIMARY_ACCESS" "$PRIMARY_SECRET" remove-bucket >/dev/null 2>&1 || true
s3_tool minio-replica:9000 "$REPLICA_ACCESS" "$REPLICA_SECRET" remove-bucket >/dev/null 2>&1 || true
s3_tool minio-primary:9000 "$PRIMARY_ACCESS" "$PRIMARY_SECRET" ensure-bucket >/dev/null
s3_tool minio-replica:9000 "$REPLICA_ACCESS" "$REPLICA_SECRET" ensure-bucket >/dev/null
docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}; CREATE DATABASE ${TEST_DB}; GRANT ALL PRIVILEGES ON ${TEST_DB}.* TO 'taskflow'@'%'; FLUSH PRIVILEGES" >/dev/null
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null

phase="start direct AWS-KMS-compatible signing and two-copy DR"
CONTROL_PLANE_AUTH_ENABLED=true \
  CONTROL_PLANE_BEARER_TOKEN="$TOKEN" \
  CONTROL_PLANE_OPERATOR_ID="$OPERATOR" \
  CONTROL_PLANE_AUDIT_ENABLED=true \
  CONTROL_PLANE_AUDIT_RETENTION_ENABLED=false \
  CONTROL_PLANE_SIEM_DELIVERY_ENABLED=true \
  CONTROL_PLANE_SIEM_ENDPOINT=http://siem-sink:8080/events \
  CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
  CONTROL_PLANE_SIEM_SIGNING_PROVIDER=aws-kms \
  CONTROL_PLANE_SIEM_AWS_KMS_ENDPOINT=http://aws-kms:9702 \
  CONTROL_PLANE_SIEM_AWS_KMS_REGION="$AWS_REGION" \
  CONTROL_PLANE_SIEM_AWS_ACCESS_KEY_ID="$AWS_ACCESS" \
  CONTROL_PLANE_SIEM_AWS_SECRET_ACCESS_KEY="$AWS_SECRET" \
  CONTROL_PLANE_SIEM_AWS_KMS_KEY_ID="$KMS_KEY_ID" \
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
  MYSQL_DSN="$TEST_DSN" \
  REDIS_QUEUE_KEY="$QUEUE_KEY" \
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null
wait_http "$BASE_URL/health/ready"

phase="prove accepted-but-lost ACK converges through duplicate acknowledgement"
curl -fsS -X POST -H 'Content-Type: application/json' -d '{"drop_after_accept_remaining":1}' "$SIEM_URL/admin/mode" >/dev/null
curl -fsS -H "Authorization: Bearer $TOKEN" -H 'X-Request-ID: phase27-ambiguous-ack' "$BASE_URL/api/v1/ai/routing-policy" >/tmp/phase27-ambiguous.json
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase27-ambiguous-ack' AND r.ack_status='duplicate' AND r.receipt_id<>'' AND LENGTH(r.payload_sha256)=64 AND LENGTH(r.ack_hash)=64 AND r.replica_copies=2"
seq_one="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse "SELECT r.chain_sequence FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase27-ambiguous-ack' LIMIT 1")"
idem_one="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse "SELECT r.idempotency_key FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase27-ambiguous-ack' LIMIT 1")"
seq_one_pad="$(printf '%020d' "$seq_one")"
object_one="control-plane-audit/${seq_one_pad}-${seq_one_pad}-${idem_one}.json"
s3_tool minio-primary:9000 "$PRIMARY_ACCESS" "$PRIMARY_SECRET" exists "$object_one" >/dev/null
s3_tool minio-replica:9000 "$REPLICA_ACCESS" "$REPLICA_SECRET" exists "$object_one" >/dev/null
s3_tool minio-primary:9000 "$PRIMARY_ACCESS" "$PRIMARY_SECRET" get "$object_one" >/tmp/phase27-primary-evidence.json
s3_tool minio-replica:9000 "$REPLICA_ACCESS" "$REPLICA_SECRET" get "$object_one" >/tmp/phase27-replica-evidence.json
cmp /tmp/phase27-primary-evidence.json /tmp/phase27-replica-evidence.json
python3 - <<'PY'
import json
e = json.load(open('/tmp/phase27-primary-evidence.json'))
assert e['version'] == 'taskflow-control-plane-siem-evidence-v1', e
assert e['algorithm'] == 'aws-kms-HMAC_SHA_256-digest', e
assert e['receiver_receipt']['receipt_id'], e
assert e['receiver_receipt']['idempotency_key'] == e['idempotency_key'], e
assert len(e['payload_sha256']) == 64, e
assert len(e['receipt_proof_sha256']) == 64, e
PY

phase="prove replica gap blocks settlement until second region recovers"
s3_tool minio-replica:9000 "$REPLICA_ACCESS" "$REPLICA_SECRET" remove-bucket >/dev/null
curl -fsS -H "Authorization: Bearer $TOKEN" -H 'X-Request-ID: phase27-replica-gap' "$BASE_URL/api/v1/ai/routing-policy" >/tmp/phase27-gap.json
wait_sql "SELECT IF(COUNT(*)>=1,1,0) FROM ${TEST_DB}.control_plane_audit_deliveries d JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=d.chain_sequence WHERE a.request_id='phase27-replica-gap' AND d.attempts>=1"
[ "$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase27-replica-gap'")" = 0 ]
s3_tool minio-replica:9000 "$REPLICA_ACCESS" "$REPLICA_SECRET" ensure-bucket >/dev/null
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase27-replica-gap' AND r.ack_status='duplicate' AND r.replica_copies=2"
curl -fsS "$BASE_URL/metrics" >/tmp/phase27-metrics-pre-restart.txt
grep -q 'taskflow_control_plane_siem_ack_total{status="duplicate"}' /tmp/phase27-metrics-pre-restart.txt
grep -q 'taskflow_control_plane_siem_dr_total{outcome="durable"}' /tmp/phase27-metrics-pre-restart.txt
grep -q 'taskflow_control_plane_siem_dr_total{outcome="insufficient_copies"}' /tmp/phase27-metrics-pre-restart.txt
grep -q '^taskflow_control_plane_siem_dr_copies 2' /tmp/phase27-metrics-pre-restart.txt

phase="prove local SIEM state loss rebuilds from retained audit chain"
old_delivery_id="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse "SELECT r.delivery_id FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase27-ambiguous-ack' LIMIT 1")"
old_duplicate_batches="$(curl -fsS "$SIEM_URL/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["duplicate_batches"])')"
docker compose exec -T mysql mysql -utaskflow -ptaskflow -e "DELETE FROM ${TEST_DB}.control_plane_audit_delivery_receipts WHERE chain_sequence=${seq_one}; DELETE FROM ${TEST_DB}.control_plane_audit_deliveries WHERE chain_sequence=${seq_one};" >/dev/null
[ "$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audits WHERE chain_sequence=${seq_one}")" = 1 ]
[ "$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audit_delivery_receipts WHERE chain_sequence=${seq_one}")" = 0 ]
docker compose restart taskflow >/dev/null
wait_http "$BASE_URL/health/ready"
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase27-ambiguous-ack' AND r.ack_status='duplicate' AND r.replica_copies=2"
new_delivery_id="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse "SELECT r.delivery_id FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase27-ambiguous-ack' LIMIT 1")"
[ "$new_delivery_id" != "$old_delivery_id" ]
new_duplicate_batches="$(curl -fsS "$SIEM_URL/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["duplicate_batches"])')"
[ "$new_duplicate_batches" -gt "$old_duplicate_batches" ]
s3_tool minio-primary:9000 "$PRIMARY_ACCESS" "$PRIMARY_SECRET" exists "$object_one" >/dev/null
s3_tool minio-replica:9000 "$REPLICA_ACCESS" "$REPLICA_SECRET" exists "$object_one" >/dev/null

phase="verify receipt cursor direct KMS and protocol evidence"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/api/v1/control-plane/siem/receipts?after_sequence=0&limit=100" >/tmp/phase27-receipts.json
curl -fsS "$SIEM_URL/stats" >/tmp/phase27-siem-stats.json
curl -fsS "$KMS_URL/stats" >/tmp/phase27-kms-stats.json
curl -fsS "$BASE_URL/metrics" >/tmp/phase27-metrics.txt
python3 - <<'PY'
import json
r = json.load(open('/tmp/phase27-receipts.json'))['items']
s = json.load(open('/tmp/phase27-siem-stats.json'))
k = json.load(open('/tmp/phase27-kms-stats.json'))
assert len(r) >= 2, r
for row in r:
    assert row['receipt_id'], row
    assert row['idempotency_key'].startswith('taskflow-audit-'), row
    assert len(row['payload_sha256']) == 64, row
    assert len(row['ack_hash']) == 64, row
    assert row['replica_copies'] == 2, row
assert s['accepted_events'] >= 2, s
assert s['duplicate_batches'] >= 2, s
assert s['idempotency_conflicts'] == 0, s
assert s['signature_failures'] == 0 and s['auth_failures'] == 0, s
assert s['last_algorithm'] == 'aws-kms-HMAC_SHA_256-digest', s
assert len(s['accepted_sequences']) == len(set(s['accepted_sequences'])), s
assert k['generate_mac'] >= 2 and k['auth_failures'] == 0, k
assert k['last_key_id'] == 'phase27-kms-key', k
PY
grep -q 'taskflow_control_plane_siem_ack_total{status="duplicate"}' /tmp/phase27-metrics.txt
grep -q 'taskflow_control_plane_siem_dr_total{outcome="durable"}' /tmp/phase27-metrics.txt
grep -q '^taskflow_control_plane_siem_dr_copies 2' /tmp/phase27-metrics.txt

python3 scripts/phase27-siem-exactly-once-dr-check.py
echo "Phase 27 receiver-acknowledged exactly-once acceptance, direct AWS-KMS protocol and two-region audit DR: ok"
