#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
SIEM_URL="${PHASE28_SIEM_URL:-http://127.0.0.1:18084}"
KMS_URL="${PHASE28_KMS_URL:-http://127.0.0.1:19702}"
CREDS_URL="${PHASE28_CREDS_URL:-http://127.0.0.1:19703}"
VAULT_URL="${PHASE28_VAULT_URL:-http://127.0.0.1:18200}"
TOKEN="${PHASE28_TOKEN:-phase28-control-token}"
OPERATOR="${PHASE28_OPERATOR:-phase28-security-admin}"
SINK_TOKEN="${PHASE28_SINK_TOKEN:-phase28-sink-token}"
VAULT_ROOT="${PHASE28_VAULT_ROOT_TOKEN:-phase28-vault-root}"
VAULT_KEY="${PHASE28_VAULT_KEY:-taskflow-phase28-audit}"
VAULT_HEADER_KEY_ID="${PHASE28_VAULT_HEADER_KEY_ID:-phase28-vault-v1}"
AWS_ACCESS="${PHASE28_AWS_ACCESS_KEY_ID:-phase28-temp-access}"
AWS_SECRET="${PHASE28_AWS_SECRET_ACCESS_KEY:-phase28-temp-secret}"
AWS_SESSION="${PHASE28_AWS_SESSION_TOKEN:-phase28-session-token}"
AWS_REGION="${PHASE28_AWS_REGION:-ap-southeast-1}"
AWS_ROLE_ARN="${PHASE28_AWS_ROLE_ARN:-arn:aws:iam::123456789012:role/taskflow-phase28}"
KMS_KEY_ID="${PHASE28_KMS_KEY_ID:-phase28-kms-key}"
KMS_HMAC_KEY="${PHASE28_KMS_HMAC_KEY:-phase28-kms-hmac-secret}"
TEST_DB="${PHASE28_DB:-taskflow_phase28}"
TEST_DSN="taskflow:taskflow@tcp(mysql:3306)/${TEST_DB}?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
DEFAULT_DSN="taskflow:taskflow@tcp(mysql:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
QUEUE_KEY="${PHASE28_QUEUE_KEY:-taskflow:phase28:queue}"
BUCKET="${PHASE28_BUCKET:-taskflow-audit-phase28}"
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
    taskflow-go:phase28-build /out/release-s3-tool "$@"
}

common_taskflow_env() {
  env \
    CONTROL_PLANE_AUTH_ENABLED=true \
    CONTROL_PLANE_BEARER_TOKEN="$TOKEN" \
    CONTROL_PLANE_OPERATOR_ID="$OPERATOR" \
    CONTROL_PLANE_AUDIT_ENABLED=true \
    CONTROL_PLANE_AUDIT_RETENTION_ENABLED=false \
    CONTROL_PLANE_SIEM_DELIVERY_ENABLED=true \
    CONTROL_PLANE_SIEM_ENDPOINT=http://siem-sink:8080/events \
    CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
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
    "$@"
}

start_vault_taskflow() {
  common_taskflow_env \
    CONTROL_PLANE_SIEM_RECONCILE_ENABLED=false \
    CONTROL_PLANE_SIEM_SIGNING_PROVIDER=vault-transit \
    CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$VAULT_HEADER_KEY_ID" \
    CONTROL_PLANE_SIEM_VAULT_ENDPOINT=http://vault:8200 \
    CONTROL_PLANE_SIEM_VAULT_TOKEN="$VAULT_ROOT" \
    CONTROL_PLANE_SIEM_VAULT_TRANSIT_MOUNT=transit \
    CONTROL_PLANE_SIEM_VAULT_TRANSIT_KEY="$VAULT_KEY" \
    CONTROL_PLANE_SIEM_VAULT_TRANSIT_KEY_VERSION=0 \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null
}

start_web_identity_taskflow() {
  common_taskflow_env \
    CONTROL_PLANE_SIEM_RECONCILE_ENABLED=false \
    CONTROL_PLANE_SIEM_SIGNING_PROVIDER=aws-kms \
    CONTROL_PLANE_SIEM_SIGNING_KEY_ID=phase28-unused-header-id \
    CONTROL_PLANE_SIEM_AWS_KMS_ENDPOINT=http://aws-kms:9702 \
    CONTROL_PLANE_SIEM_AWS_KMS_REGION="$AWS_REGION" \
    CONTROL_PLANE_SIEM_AWS_KMS_KEY_ID="$KMS_KEY_ID" \
    CONTROL_PLANE_SIEM_AWS_CREDENTIALS_SOURCE=web-identity \
    CONTROL_PLANE_SIEM_AWS_ACCESS_KEY_ID= \
    CONTROL_PLANE_SIEM_AWS_SECRET_ACCESS_KEY= \
    CONTROL_PLANE_SIEM_AWS_SESSION_TOKEN= \
    AWS_WEB_IDENTITY_TOKEN_FILE=/var/run/secrets/taskflow/phase28-web-identity-token \
    AWS_ROLE_ARN="$AWS_ROLE_ARN" \
    AWS_ROLE_SESSION_NAME=taskflow-phase28 \
    CONTROL_PLANE_SIEM_AWS_STS_ENDPOINT=http://aws-credentials:9703/sts \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null
}

start_auto_imds_taskflow() {
  reconcile_enabled="$1"
  common_taskflow_env \
    CONTROL_PLANE_SIEM_RECONCILE_ENABLED="$reconcile_enabled" \
    CONTROL_PLANE_SIEM_RECONCILE_ENDPOINT=http://siem-sink:8080/checkpoint \
    CONTROL_PLANE_SIEM_RECONCILE_MIN_EVIDENCE_COPIES=1 \
    CONTROL_PLANE_SIEM_SIGNING_PROVIDER=aws-kms \
    CONTROL_PLANE_SIEM_SIGNING_KEY_ID=phase28-unused-header-id \
    CONTROL_PLANE_SIEM_AWS_KMS_ENDPOINT=http://aws-kms:9702 \
    CONTROL_PLANE_SIEM_AWS_KMS_REGION="$AWS_REGION" \
    CONTROL_PLANE_SIEM_AWS_KMS_KEY_ID="$KMS_KEY_ID" \
    CONTROL_PLANE_SIEM_AWS_CREDENTIALS_SOURCE=auto \
    CONTROL_PLANE_SIEM_AWS_ACCESS_KEY_ID= \
    CONTROL_PLANE_SIEM_AWS_SECRET_ACCESS_KEY= \
    CONTROL_PLANE_SIEM_AWS_SESSION_TOKEN= \
    AWS_WEB_IDENTITY_TOKEN_FILE= \
    AWS_ROLE_ARN= \
    CONTROL_PLANE_SIEM_AWS_IMDS_ENDPOINT=http://aws-credentials:9703 \
    CONTROL_PLANE_SIEM_AWS_IMDS_TIMEOUT_MS=2000 \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null
}

cleanup() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "Phase 28 Vault/workload-identity/reconciliation smoke failed during phase: $phase" >&2
    docker compose --profile phase28-smoke --profile archive-s3 logs --tail 250 taskflow siem-sink aws-kms aws-credentials vault minio-primary minio-replica >&2 2>/dev/null || true
  fi
  docker compose --profile archive-s3 start minio-primary minio-replica >/dev/null 2>&1 || true
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
    CONTROL_PLANE_SIEM_AWS_CREDENTIALS_SOURCE=static \
    CONTROL_PLANE_SIEM_DR_ENABLED=false \
    CONTROL_PLANE_SIEM_RECONCILE_ENABLED=false \
    MYSQL_DSN="$DEFAULT_DSN" \
    REDIS_QUEUE_KEY=taskflow:queue \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null 2>&1 || true
  docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}" >/dev/null 2>&1 || true
  docker compose --profile phase28-smoke rm -sf siem-sink aws-kms aws-credentials vault >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT

phase="build Phase 28 helper and static guardrails"
docker build --target build -t taskflow-go:phase28-build --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
docker build -t taskflow-go:local --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
python3 scripts/phase28-vault-credentials-reconcile-check.py

phase="start Vault receiver AWS credential services and two MinIO regions"
SIEM_SIGNING_KEYS="${KMS_KEY_ID}=${KMS_HMAC_KEY}" \
CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
SIEM_VAULT_VERIFY_ENDPOINT="http://vault:8200/v1/transit/verify/${VAULT_KEY}/sha2-256" \
SIEM_VAULT_TOKEN="$VAULT_ROOT" \
SIEM_VAULT_KEY_ID="$VAULT_HEADER_KEY_ID" \
CONTROL_PLANE_SIEM_AWS_ACCESS_KEY_ID="$AWS_ACCESS" \
CONTROL_PLANE_SIEM_AWS_SECRET_ACCESS_KEY="$AWS_SECRET" \
CONTROL_PLANE_SIEM_AWS_KMS_REGION="$AWS_REGION" \
CONTROL_PLANE_SIEM_AWS_KMS_KEY_ID="$KMS_KEY_ID" \
PHASE28_AWS_ACCESS_KEY_ID="$AWS_ACCESS" \
PHASE28_AWS_SECRET_ACCESS_KEY="$AWS_SECRET" \
PHASE28_AWS_SESSION_TOKEN="$AWS_SESSION" \
PHASE28_AWS_ROLE_ARN="$AWS_ROLE_ARN" \
PHASE28_KMS_HMAC_KEY="$KMS_HMAC_KEY" \
PHASE27_KMS_HMAC_KEY="$KMS_HMAC_KEY" \
PHASE28_VAULT_ROOT_TOKEN="$VAULT_ROOT" \
  docker compose --profile phase28-smoke --profile archive-s3 up -d mysql redis executor-worker siem-sink aws-kms aws-credentials vault minio-primary minio-replica >/dev/null
wait_http "$SIEM_URL/health"
wait_http "$KMS_URL/health"
wait_http "$CREDS_URL/health"
wait_http "$VAULT_URL/v1/sys/health"
wait_http http://127.0.0.1:19000/minio/health/live
wait_http http://127.0.0.1:19002/minio/health/live

phase="configure real Vault Transit RSA key"
if ! docker compose exec -T -e VAULT_ADDR=http://127.0.0.1:8200 -e VAULT_TOKEN="$VAULT_ROOT" vault vault secrets list -format=json | grep -q '"transit/"'; then
  docker compose exec -T -e VAULT_ADDR=http://127.0.0.1:8200 -e VAULT_TOKEN="$VAULT_ROOT" vault vault secrets enable transit >/dev/null
fi
if ! docker compose exec -T -e VAULT_ADDR=http://127.0.0.1:8200 -e VAULT_TOKEN="$VAULT_ROOT" vault vault read transit/keys/"$VAULT_KEY" >/dev/null 2>&1; then
  docker compose exec -T -e VAULT_ADDR=http://127.0.0.1:8200 -e VAULT_TOKEN="$VAULT_ROOT" vault vault write transit/keys/"$VAULT_KEY" type=rsa-2048 >/dev/null
fi
docker compose exec -T -e VAULT_ADDR=http://127.0.0.1:8200 -e VAULT_TOKEN="$VAULT_ROOT" vault vault read transit/keys/"$VAULT_KEY" | grep -q 'rsa-2048'

phase="create isolated database and DR buckets"
mysql_id="$(docker compose ps -q mysql)"
NETWORK="$(docker inspect "$mysql_id" --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}')"
[ -n "$NETWORK" ]
s3_tool minio-primary:9000 "$PRIMARY_ACCESS" "$PRIMARY_SECRET" remove-bucket >/dev/null 2>&1 || true
s3_tool minio-replica:9000 "$REPLICA_ACCESS" "$REPLICA_SECRET" remove-bucket >/dev/null 2>&1 || true
s3_tool minio-primary:9000 "$PRIMARY_ACCESS" "$PRIMARY_SECRET" ensure-bucket >/dev/null
s3_tool minio-replica:9000 "$REPLICA_ACCESS" "$REPLICA_SECRET" ensure-bucket >/dev/null
docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}; CREATE DATABASE ${TEST_DB}; GRANT ALL PRIVILEGES ON ${TEST_DB}.* TO 'taskflow'@'%'; FLUSH PRIVILEGES" >/dev/null
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null

phase="real Vault Transit signing and receiver verification"
start_vault_taskflow
wait_http "$BASE_URL/health/ready"
curl -fsS -H "Authorization: Bearer $TOKEN" -H 'X-Request-ID: phase28-vault' "$BASE_URL/api/v1/ai/routing-policy" >/tmp/phase28-vault.json
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase28-vault' AND r.replica_copies=2 AND r.ack_status='accepted'"
curl -fsS "$SIEM_URL/stats" >/tmp/phase28-vault-siem.json
python3 - <<'PY'
import json
s = json.load(open('/tmp/phase28-vault-siem.json'))
assert s['signature_failures'] == 0 and s['auth_failures'] == 0, s
assert s['last_algorithm'] == 'vault-transit-rsa-pkcs1v15-sha2-256-prehashed', s
assert s['last_key_id'] == 'phase28-vault-v1', s
PY

phase="AWS WebIdentity STS credential provider signs KMS requests with session credentials"
start_web_identity_taskflow
wait_http "$BASE_URL/health/ready"
curl -fsS -H "Authorization: Bearer $TOKEN" -H 'X-Request-ID: phase28-web-identity' "$BASE_URL/api/v1/ai/routing-policy" >/tmp/phase28-web-identity.json
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase28-web-identity' AND r.replica_copies=2"
curl -fsS "$CREDS_URL/stats" >/tmp/phase28-creds-web.json
curl -fsS "$KMS_URL/stats" >/tmp/phase28-kms-web.json
python3 - <<'PY'
import json
c = json.load(open('/tmp/phase28-creds-web.json'))
k = json.load(open('/tmp/phase28-kms-web.json'))
assert c['sts_requests'] >= 1 and c['sts_failures'] == 0, c
assert k['generate_mac'] >= 1 and k['auth_failures'] == 0, k
PY

phase="AWS auto credential chain falls through to IMDSv2"
start_auto_imds_taskflow false
wait_http "$BASE_URL/health/ready"
curl -fsS -H "Authorization: Bearer $TOKEN" -H 'X-Request-ID: phase28-imdsv2' "$BASE_URL/api/v1/ai/routing-policy" >/tmp/phase28-imds.json
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase28-imdsv2' AND r.replica_copies=2"
curl -fsS "$CREDS_URL/stats" >/tmp/phase28-creds-imds.json
python3 - <<'PY'
import json
c = json.load(open('/tmp/phase28-creds-imds.json'))
assert c['imds_token_requests'] >= 1, c
assert c['imds_role_requests'] >= 1, c
assert c['imds_credential_requests'] >= 1, c
assert c['imds_failures'] == 0, c
PY

phase="capture receiver checkpoint before SIEM hot-state database loss"
curl -fsS -H "Authorization: Bearer $SINK_TOKEN" "$SIEM_URL/checkpoint?after_sequence=0&limit=100" >/tmp/phase28-checkpoint-before.json
curl -fsS "$SIEM_URL/stats" >/tmp/phase28-siem-before-recovery.json
accepted_before="$(python3 -c 'import json; print(json.load(open("/tmp/phase28-siem-before-recovery.json"))["accepted_events"])')"
requests_before="$(python3 -c 'import json; print(json.load(open("/tmp/phase28-siem-before-recovery.json"))["requests"])')"
python3 - <<'PY'
import json
c = json.load(open('/tmp/phase28-checkpoint-before.json'))
assert c['version'] == 'taskflow-control-plane-siem-checkpoint-v1', c
assert c['high_watermark_sequence'] >= 3, c
assert len(c['items']) >= 3, c
PY

phase="drop SIEM hot-state tables and make primary evidence region unavailable"
docker compose stop taskflow >/dev/null
docker compose exec -T mysql mysql -uroot -proot "$TEST_DB" -e "DROP TABLE IF EXISTS control_plane_audit_delivery_dlq, control_plane_audit_delivery_receipts, control_plane_audit_deliveries, control_plane_siem_delivery_coordinator_state;" >/dev/null
docker compose --profile archive-s3 stop minio-primary >/dev/null
[ "$(sql "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audits")" -ge 3 ]

phase="receiver checkpoint plus surviving replica reconstructs local receipts without event replay"
start_auto_imds_taskflow true
wait_http "$BASE_URL/health/ready"
wait_sql "SELECT IF(COUNT(*)=${accepted_before},1,0) FROM ${TEST_DB}.control_plane_audit_delivery_receipts WHERE recovery_source='receiver-checkpoint' AND ack_status='reconciled' AND replica_copies=1"
[ "$(sql "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE state <> 'delivered'")" = 0 ]
curl -fsS "$SIEM_URL/stats" >/tmp/phase28-siem-after-recovery.json
requests_after="$(python3 -c 'import json; print(json.load(open("/tmp/phase28-siem-after-recovery.json"))["requests"])')"
[ "$requests_after" = "$requests_before" ]
curl -fsS "$BASE_URL/metrics" >/tmp/phase28-recovery-metrics.txt
grep -q 'taskflow_control_plane_siem_reconcile_total{outcome="success"} 1' /tmp/phase28-recovery-metrics.txt
grep -q "taskflow_control_plane_siem_recovered_receipts_total ${accepted_before}" /tmp/phase28-recovery-metrics.txt
grep -q 'taskflow_control_plane_siem_recovery_evidence_total{source="siem-dr-replica"}' /tmp/phase28-recovery-metrics.txt

phase="restore primary region and return new deliveries to two-copy durability"
docker compose --profile archive-s3 start minio-primary >/dev/null
wait_http http://127.0.0.1:19000/minio/health/live
curl -fsS -H "Authorization: Bearer $TOKEN" -H 'X-Request-ID: phase28-post-recovery' "$BASE_URL/api/v1/ai/routing-policy" >/tmp/phase28-post-recovery.json
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase28-post-recovery' AND r.replica_copies=2 AND r.recovery_source=''"
post_seq="$(sql "SELECT r.chain_sequence FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase28-post-recovery' LIMIT 1")"
post_idem="$(sql "SELECT r.idempotency_key FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase28-post-recovery' LIMIT 1")"
post_pad="$(printf '%020d' "$post_seq")"
post_object="control-plane-audit/${post_pad}-${post_pad}-${post_idem}.json"
s3_tool minio-primary:9000 "$PRIMARY_ACCESS" "$PRIMARY_SECRET" exists "$post_object" >/dev/null
s3_tool minio-replica:9000 "$REPLICA_ACCESS" "$REPLICA_SECRET" exists "$post_object" >/dev/null

phase="final protocol and recovery assertions"
curl -fsS "$SIEM_URL/stats" >/tmp/phase28-siem-final.json
curl -fsS "$KMS_URL/stats" >/tmp/phase28-kms-final.json
curl -fsS "$CREDS_URL/stats" >/tmp/phase28-creds-final.json
python3 - <<'PY'
import json
s = json.load(open('/tmp/phase28-siem-final.json'))
k = json.load(open('/tmp/phase28-kms-final.json'))
c = json.load(open('/tmp/phase28-creds-final.json'))
assert s['signature_failures'] == 0 and s['auth_failures'] == 0, s
assert s['idempotency_conflicts'] == 0, s
assert len(s['accepted_sequences']) == len(set(s['accepted_sequences'])), s
assert k['generate_mac'] >= 3 and k['auth_failures'] == 0, k
assert c['sts_requests'] >= 1 and c['sts_failures'] == 0, c
assert c['imds_credential_requests'] >= 2 and c['imds_failures'] == 0, c
PY

python3 scripts/phase28-vault-credentials-reconcile-check.py
echo "Phase 28 real Vault Transit, AWS WebIdentity/IMDSv2 credential chain and receiver-checkpoint DR: ok"
echo "Phase 28 SIEM hot-state DB loss with primary-region outage recovered from surviving replica without event replay: ok"
