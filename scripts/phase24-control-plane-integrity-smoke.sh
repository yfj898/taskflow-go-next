#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
TOKEN="${PHASE24_TOKEN:-phase24-integrity-token}"
OPERATOR="${PHASE24_OPERATOR:-security-admin}"
TEST_DB="${PHASE24_DB:-taskflow_phase24}"
TEST_DSN="taskflow:taskflow@tcp(mysql:3306)/${TEST_DB}?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
DEFAULT_DSN="taskflow:taskflow@tcp(mysql:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
phase="initialization"

wait_ready() {
  i=0
  while [ "$i" -lt 40 ]; do
    if curl -fsS "$BASE_URL/health/ready" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

wait_mysql() {
  i=0
  while [ "$i" -lt 40 ]; do
    if docker compose exec -T mysql mysqladmin ping -h 127.0.0.1 -proot >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

cleanup() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "Phase 24 control-plane integrity smoke failed during phase: $phase" >&2
    docker compose logs --tail 200 taskflow >&2 2>/dev/null || true
  fi
  CONTROL_PLANE_AUTH_ENABLED=false \
    CONTROL_PLANE_BEARER_TOKEN= \
    CONTROL_PLANE_PREVIOUS_BEARER_TOKEN= \
    CONTROL_PLANE_OPERATOR_ID=operator \
    CONTROL_PLANE_PREVIOUS_OPERATOR_ID= \
    CONTROL_PLANE_AUDIT_ENABLED=true \
    CONTROL_PLANE_AUDIT_RETENTION_ENABLED=true \
    CONTROL_PLANE_AUDIT_LIFECYCLE_INTERVAL_MS=3600000 \
    CONTROL_PLANE_AUDIT_RETENTION_MS=7776000000 \
    CONTROL_PLANE_AUDIT_RETENTION_BATCH=1000 \
    MYSQL_DSN="$DEFAULT_DSN" \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}" >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT

phase="create isolated Phase 24 schema"
docker compose up -d mysql redis executor-worker >/dev/null
wait_mysql
docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}; CREATE DATABASE ${TEST_DB}; GRANT ALL PRIVILEGES ON ${TEST_DB}.* TO 'taskflow'@'%'; FLUSH PRIVILEGES" >/dev/null

phase="migrate isolated schema"
CONTROL_PLANE_AUTH_ENABLED=true \
  CONTROL_PLANE_BEARER_TOKEN="$TOKEN" \
  CONTROL_PLANE_OPERATOR_ID="$OPERATOR" \
  CONTROL_PLANE_AUDIT_ENABLED=true \
  CONTROL_PLANE_AUDIT_RETENTION_ENABLED=false \
  MYSQL_DSN="$TEST_DSN" \
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null
wait_ready

phase="seed legacy completed audit without chain metadata"
docker compose exec -T mysql mysql -utaskflow -ptaskflow -e \
  "INSERT INTO ${TEST_DB}.control_plane_audits (operator_id,credential_slot,method,route,target,request_id,status_code,outcome,created_at,completed_at) VALUES ('legacy-admin','active','PUT','/api/v1/ai/routing-policy','/api/v1/ai/routing-policy','phase23-legacy-row',200,'success',NOW(3),NOW(3))" >/dev/null

phase="restart and seal legacy row"
CONTROL_PLANE_AUTH_ENABLED=true \
  CONTROL_PLANE_BEARER_TOKEN="$TOKEN" \
  CONTROL_PLANE_OPERATOR_ID="$OPERATOR" \
  CONTROL_PLANE_AUDIT_ENABLED=true \
  CONTROL_PLANE_AUDIT_RETENTION_ENABLED=false \
  MYSQL_DSN="$TEST_DSN" \
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null
wait_ready

phase="legacy completed rows are sealed"
unsealed="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse \
  "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audits WHERE outcome <> 'started' AND completed_at IS NOT NULL AND chain_sequence IS NULL")"
[ "$unsealed" = 0 ]

phase="create completed chained records"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/api/v1/ai/routing-policy" >/tmp/phase24-policy.json
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/api/v1/queue/dead-letters?limit=1" >/tmp/phase24-dlq.json

phase="verify valid chain"
verify_code="$(curl -sS -o /tmp/phase24-verify.json -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/api/v1/control-plane/audits/verify")"
[ "$verify_code" = 200 ]
grep -q '"valid":true' /tmp/phase24-verify.json

phase="SIEM NDJSON export contains hashes and checkpoint"
export_code="$(curl -sS -D /tmp/phase24-export.headers -o /tmp/phase24-export.ndjson -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/api/v1/control-plane/audits/export?after_sequence=0&limit=500")"
[ "$export_code" = 200 ]
grep -qi '^Content-Type: application/x-ndjson' /tmp/phase24-export.headers
grep -qi '^X-TaskFlow-Audit-Head-Hash:' /tmp/phase24-export.headers
grep -q '"chain_sequence":' /tmp/phase24-export.ndjson
grep -q '"record_hash":"' /tmp/phase24-export.ndjson
grep -q '"type":"checkpoint"' /tmp/phase24-export.ndjson
if grep -Fq "$TOKEN" /tmp/phase24-export.ndjson; then
  echo "raw control-plane credential leaked into SIEM export" >&2
  exit 1
fi

phase="tamper with one finalized row"
tamper_id="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse \
  "SELECT id FROM ${TEST_DB}.control_plane_audits WHERE chain_sequence IS NOT NULL AND operator_id='$OPERATOR' ORDER BY chain_sequence ASC LIMIT 1")"
[ -n "$tamper_id" ]
docker compose exec -T mysql mysql -utaskflow -ptaskflow -e \
  "UPDATE ${TEST_DB}.control_plane_audits SET operator_id='tampered-admin' WHERE id=$tamper_id" >/dev/null

phase="tamper is detected"
tampered_code="$(curl -sS -o /tmp/phase24-tampered.json -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/api/v1/control-plane/audits/verify")"
[ "$tampered_code" = 409 ]
grep -q '"valid":false' /tmp/phase24-tampered.json
grep -q 'record hash mismatch' /tmp/phase24-tampered.json

phase="restore tampered row and verify recovery"
docker compose exec -T mysql mysql -utaskflow -ptaskflow -e \
  "UPDATE ${TEST_DB}.control_plane_audits SET operator_id='$OPERATOR' WHERE id=$tamper_id" >/dev/null
recovered_code="$(curl -sS -o /tmp/phase24-recovered.json -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/api/v1/control-plane/audits/verify")"
[ "$recovered_code" = 200 ]
grep -q '"valid":true' /tmp/phase24-recovered.json

phase="run short retention lifecycle"
CONTROL_PLANE_AUTH_ENABLED=true \
  CONTROL_PLANE_BEARER_TOKEN="$TOKEN" \
  CONTROL_PLANE_OPERATOR_ID="$OPERATOR" \
  CONTROL_PLANE_AUDIT_ENABLED=true \
  CONTROL_PLANE_AUDIT_RETENTION_ENABLED=true \
  CONTROL_PLANE_AUDIT_LIFECYCLE_INTERVAL_MS=1000 \
  CONTROL_PLANE_AUDIT_RETENTION_MS=1000 \
  CONTROL_PLANE_AUDIT_RETENTION_BATCH=1000 \
  MYSQL_DSN="$TEST_DSN" \
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null
wait_ready
sleep 3

phase="retention anchor advances while chain remains valid"
anchor="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse \
  "SELECT retention_anchor_sequence FROM ${TEST_DB}.control_plane_audit_chain_state WHERE id=1")"
[ "${anchor:-0}" -gt 0 ]
retention_verify="$(curl -sS -o /tmp/phase24-retention-verify.json -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/api/v1/control-plane/audits/verify")"
[ "$retention_verify" = 200 ]
grep -q '"valid":true' /tmp/phase24-retention-verify.json

phase="SIEM cursor detects retention gap"
gap_code="$(curl -sS -o /tmp/phase24-gap.json -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/api/v1/control-plane/audits/export?after_sequence=0&limit=100")"
[ "$gap_code" = 410 ]
grep -q 'retention_anchor_sequence' /tmp/phase24-gap.json

phase="metrics expose integrity retention and export"
curl -fsS "$BASE_URL/metrics" >/tmp/phase24-metrics.txt
grep -q 'taskflow_control_plane_audit_integrity' /tmp/phase24-metrics.txt
grep -q 'taskflow_control_plane_audit_lifecycle_total' /tmp/phase24-metrics.txt
grep -q 'taskflow_control_plane_audit_exports_total' /tmp/phase24-metrics.txt

python3 scripts/phase24-control-plane-integrity-check.py
echo "Phase 24 tamper-evident control-plane audit, anchored retention and SIEM export: ok"
