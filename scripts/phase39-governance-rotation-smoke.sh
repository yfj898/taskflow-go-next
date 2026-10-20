#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
PRIMARY_URL="${PHASE39_PRIMARY_URL:-http://127.0.0.1:18084}"
SECONDARY_URL="${PHASE39_SECONDARY_URL:-http://127.0.0.1:18085}"
GOV_PRIMARY_URL="${PHASE39_GOVERNANCE_PRIMARY_URL:-http://127.0.0.1:19704}"
GOV_SECONDARY_URL="${PHASE39_GOVERNANCE_SECONDARY_URL:-http://127.0.0.1:19705}"

COMMANDER="phase39-commander"
COMMANDER_TOKEN="${PHASE39_COMMANDER_TOKEN:-phase39-commander-token}"
REQUESTER="phase39-requester"
REQUESTER_TOKEN="${PHASE39_REQUESTER_TOKEN:-phase39-requester-token}"
APPROVER="phase39-approver"
APPROVER_TOKEN="${PHASE39_APPROVER_TOKEN:-phase39-approver-token}"
ADDITIONAL_CREDENTIALS="${APPROVER}=${APPROVER_TOKEN}"
ROLE_POLICY="${REQUESTER}=requester;${APPROVER}=approver;${COMMANDER}=incident-commander"

SINK_TOKEN="${PHASE39_SINK_TOKEN:-phase39-sink-token}"
SIGN_KEY="${PHASE39_SIGNING_KEY:-phase39-signing-key}"
SIGN_KEY_ID="${PHASE39_SIGNING_KEY_ID:-phase39-sign-v1}"
CHECKPOINT_KEY="${PHASE39_CHECKPOINT_KEY:-phase39-checkpoint-secret}"
CHECKPOINT_KEY_ID="${PHASE39_CHECKPOINT_KEY_ID:-phase39-checkpoint-v1}"
GOV_PRIMARY_TOKEN="${PHASE39_GOVERNANCE_PRIMARY_TOKEN:-phase39-governance-primary-token}"
GOV_SECONDARY_TOKEN="${PHASE39_GOVERNANCE_SECONDARY_TOKEN:-phase39-governance-secondary-token}"
GOV_SIGN_KEY="${PHASE39_GOVERNANCE_SIGNING_KEY:-phase39-governance-signing-key}"
GOV_SIGN_KEY_ID="${PHASE39_GOVERNANCE_SIGNING_KEY_ID:-phase39-governance-v1}"
GOV_PRIMARY_REPLICA="governance-primary"
GOV_SECONDARY_REPLICA="governance-secondary"

PRIMARY_OLD_KEY="${PHASE39_PRIMARY_OLD_RECEIPT_KEY:-phase39-primary-old-secret}"
PRIMARY_OLD_KEY_ID="${PHASE39_PRIMARY_OLD_RECEIPT_KEY_ID:-phase39-primary-old-v1}"
SECONDARY_OLD_KEY="${PHASE39_SECONDARY_OLD_RECEIPT_KEY:-phase39-secondary-old-secret}"
SECONDARY_OLD_KEY_ID="${PHASE39_SECONDARY_OLD_RECEIPT_KEY_ID:-phase39-secondary-old-v1}"
PRIMARY_NEW_KEY="${PHASE39_PRIMARY_NEW_RECEIPT_KEY:-phase39-primary-new-secret}"
PRIMARY_NEW_KEY_ID="${PHASE39_PRIMARY_NEW_RECEIPT_KEY_ID:-phase39-primary-new-v2}"
SECONDARY_NEW_KEY="${PHASE39_SECONDARY_NEW_RECEIPT_KEY:-phase39-secondary-new-secret}"
SECONDARY_NEW_KEY_ID="${PHASE39_SECONDARY_NEW_RECEIPT_KEY_ID:-phase39-secondary-new-v2}"
OLD_KEY_GENERATION=1
NEW_KEY_GENERATION=2
PREVIOUS_VALID_UNTIL_MS="${PHASE39_PREVIOUS_VALID_UNTIL_MS:-$(python3 -c 'import time; print(int(time.time()*1000)+3600000)')}"
MEMBERSHIP_EPOCH="${PHASE39_MEMBERSHIP_EPOCH:-7}"
STALE_EPOCH="${PHASE39_STALE_MEMBERSHIP_EPOCH:-6}"
export PHASE39_MEMBERSHIP_EPOCH="$MEMBERSHIP_EPOCH"

PRIMARY_ID="receiver-primary"
SECONDARY_ID="receiver-secondary"
TEST_DB="${PHASE39_DB:-taskflow_phase39}"
TEST_DSN="taskflow:taskflow@tcp(mysql:3306)/${TEST_DB}?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
DEFAULT_DSN="taskflow:taskflow@tcp(mysql:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
QUEUE_KEY="${PHASE39_QUEUE_KEY:-taskflow:phase39:queue}"
phase="initialization"

wait_http() {
  url="$1"; i=0
  while [ "$i" -lt 300 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 0.2
  done
  echo "timeout waiting for $url" >&2
  return 1
}

wait_mysql() {
  i=0
  while [ "$i" -lt 300 ]; do
    if docker compose exec -T mysql mysqladmin ping -uroot -proot --silent >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 0.2
  done
  echo "timeout waiting for mysql" >&2
  return 1
}

sql() {
  docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse "$1" 2>/dev/null | tr -d '\r'
}

wait_sql() {
  query="$1"; i=0; value=""
  while [ "$i" -lt 400 ]; do
    value="$(sql "$query" || true)"
    if [ "$value" = "1" ]; then return 0; fi
    i=$((i + 1)); sleep 0.2
  done
  echo "wait_sql failed value=$value query=$query" >&2
  return 1
}

governance_status() {
  curl -fsS -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/api/v1/control-plane/siem/governance/status"
}

start_governance_replicas() {
  primary_key="$1"; primary_key_id="$2"; secondary_key="$3"; secondary_key_id="$4"; primary_epoch="$5"; secondary_epoch="$6"; primary_generation="$7"; secondary_generation="$8"
  CONTROL_PLANE_SIEM_GOVERNANCE_BEARER_TOKEN="$GOV_PRIMARY_TOKEN" \
  CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_BEARER_TOKEN="$GOV_SECONDARY_TOKEN" \
  CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY="$GOV_SIGN_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY_ID="$GOV_SIGN_KEY_ID" \
  CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_REPLICA_ID="$GOV_PRIMARY_REPLICA" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_RECEIPT_HMAC_KEY="$primary_key" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_RECEIPT_HMAC_KEY_ID="$primary_key_id" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_RECEIPT_KEY_GENERATION="$primary_generation" \
  CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_REPLICA_ID="$GOV_SECONDARY_REPLICA" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_HMAC_KEY="$secondary_key" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_HMAC_KEY_ID="$secondary_key_id" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_KEY_GENERATION="$secondary_generation" \
  PHASE39_GOVERNANCE_PRIMARY_MEMBERSHIP_EPOCH="$primary_epoch" PHASE39_GOVERNANCE_SECONDARY_MEMBERSHIP_EPOCH="$secondary_epoch" \
    docker compose --profile phase39-smoke up -d --force-recreate siem-governance siem-governance-secondary >/dev/null
  wait_http "$GOV_PRIMARY_URL/health"
  wait_http "$GOV_SECONDARY_URL/health"
}

restart_secondary_governance() {
  key="$1"; key_id="$2"; epoch="$3"; generation="$4"
  CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_BEARER_TOKEN="$GOV_SECONDARY_TOKEN" \
  CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY="$GOV_SIGN_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY_ID="$GOV_SIGN_KEY_ID" \
  CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_REPLICA_ID="$GOV_SECONDARY_REPLICA" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_HMAC_KEY="$key" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_HMAC_KEY_ID="$key_id" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_KEY_GENERATION="$generation" \
  PHASE39_GOVERNANCE_SECONDARY_MEMBERSHIP_EPOCH="$epoch" \
    docker compose --profile phase39-smoke up -d --force-recreate siem-governance-secondary >/dev/null
  wait_http "$GOV_SECONDARY_URL/health"
}

start_taskflow() {
  CONTROL_PLANE_AUTH_ENABLED=true \
    CONTROL_PLANE_BEARER_TOKEN="$COMMANDER_TOKEN" CONTROL_PLANE_PREVIOUS_BEARER_TOKEN="$REQUESTER_TOKEN" \
    CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS="$ADDITIONAL_CREDENTIALS" CONTROL_PLANE_OPERATOR_ID="$COMMANDER" CONTROL_PLANE_PREVIOUS_OPERATOR_ID="$REQUESTER" \
    CONTROL_PLANE_AUDIT_ENABLED=true CONTROL_PLANE_AUDIT_RETENTION_ENABLED=false \
    CONTROL_PLANE_SIEM_DELIVERY_ENABLED=true CONTROL_PLANE_SIEM_ENDPOINT=http://siem-sink:8080/events \
    CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" CONTROL_PLANE_SIEM_SIGNING_PROVIDER=hmac CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" \
    CONTROL_PLANE_SIEM_DELIVERY_INTERVAL_MS=100 CONTROL_PLANE_SIEM_DELIVERY_LEASE_MS=1500 CONTROL_PLANE_SIEM_MAX_ATTEMPTS=10 CONTROL_PLANE_SIEM_RETRY_BASE_MS=100 CONTROL_PLANE_SIEM_RETRY_MAX_MS=300 \
    CONTROL_PLANE_SIEM_DISTRIBUTED_COORDINATION=false CONTROL_PLANE_SIEM_GC_ENABLED=false CONTROL_PLANE_SIEM_DR_ENABLED=false \
    CONTROL_PLANE_SIEM_PRIMARY_RECEIVER_ID="$PRIMARY_ID" CONTROL_PLANE_SIEM_PRIMARY_CHECKPOINT_ENDPOINT=http://siem-sink:8080/checkpoint CONTROL_PLANE_SIEM_PRIMARY_TERM_ENDPOINT=http://siem-sink:8080/control/term \
    CONTROL_PLANE_SIEM_SECONDARY_ENDPOINT=http://siem-sink-secondary:8080/events CONTROL_PLANE_SIEM_SECONDARY_RECEIVER_ID="$SECONDARY_ID" CONTROL_PLANE_SIEM_SECONDARY_CHECKPOINT_ENDPOINT=http://siem-sink-secondary:8080/checkpoint CONTROL_PLANE_SIEM_SECONDARY_TERM_ENDPOINT=http://siem-sink-secondary:8080/control/term \
    CONTROL_PLANE_SIEM_FAILOVER_ENABLED=false CONTROL_PLANE_SIEM_SHADOW_SECONDARY=false CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
    CONTROL_PLANE_SIEM_CLUSTER_GUARD_ENABLED=true CONTROL_PLANE_SIEM_TERM_FENCING_ENABLED=true CONTROL_PLANE_SIEM_ANTI_ENTROPY_ENABLED=true CONTROL_PLANE_SIEM_ANTI_ENTROPY_INTERVAL_MS=100 \
    CONTROL_PLANE_SIEM_PROMOTION_WORKFLOW_ENABLED=true CONTROL_PLANE_SIEM_PROMOTION_INTENT_TTL_MS=60000 CONTROL_PLANE_SIEM_PROMOTION_LEASE_MS=5000 \
    CONTROL_PLANE_SIEM_PROMOTION_APPROVAL_QUORUM=1 CONTROL_PLANE_SIEM_PROMOTION_ROLE_POLICY="$ROLE_POLICY" CONTROL_PLANE_SIEM_BREAK_GLASS_ENABLED=true \
    CONTROL_PLANE_SIEM_GOVERNANCE_ENDPOINT=http://siem-governance:8080 CONTROL_PLANE_SIEM_GOVERNANCE_BEARER_TOKEN="$GOV_PRIMARY_TOKEN" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY="$GOV_SIGN_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY_ID="$GOV_SIGN_KEY_ID" CONTROL_PLANE_SIEM_GOVERNANCE_TIMEOUT_MS=500 \
    CONTROL_PLANE_SIEM_GOVERNANCE_HA_ENABLED=true CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_REPLICA_ID="$GOV_PRIMARY_REPLICA" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_RECEIPT_HMAC_KEY="$PRIMARY_NEW_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_RECEIPT_HMAC_KEY_ID="$PRIMARY_NEW_KEY_ID" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_RECEIPT_KEY_GENERATION="$NEW_KEY_GENERATION" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_PREVIOUS_RECEIPT_HMAC_KEY="$PRIMARY_OLD_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_PREVIOUS_RECEIPT_HMAC_KEY_ID="$PRIMARY_OLD_KEY_ID" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_PREVIOUS_RECEIPT_KEY_GENERATION="$OLD_KEY_GENERATION" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_PREVIOUS_RECEIPT_VALID_UNTIL_UNIX_MS="$PREVIOUS_VALID_UNTIL_MS" \
    CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_ENDPOINT=http://siem-governance-secondary:8080 CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_BEARER_TOKEN="$GOV_SECONDARY_TOKEN" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_REPLICA_ID="$GOV_SECONDARY_REPLICA" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_HMAC_KEY="$SECONDARY_NEW_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_HMAC_KEY_ID="$SECONDARY_NEW_KEY_ID" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_KEY_GENERATION="$NEW_KEY_GENERATION" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_PREVIOUS_RECEIPT_HMAC_KEY="$SECONDARY_OLD_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_PREVIOUS_RECEIPT_HMAC_KEY_ID="$SECONDARY_OLD_KEY_ID" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_PREVIOUS_RECEIPT_KEY_GENERATION="$OLD_KEY_GENERATION" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_PREVIOUS_RECEIPT_VALID_UNTIL_UNIX_MS="$PREVIOUS_VALID_UNTIL_MS" CONTROL_PLANE_SIEM_GOVERNANCE_RECEIPT_QUORUM=2 CONTROL_PLANE_SIEM_GOVERNANCE_MEMBERSHIP_EPOCH="$MEMBERSHIP_EPOCH" \
    CONTROL_PLANE_SIEM_GOVERNANCE_DEBT_POLICY_ENABLED=true CONTROL_PLANE_SIEM_GOVERNANCE_MAX_OUTSTANDING=1000 CONTROL_PLANE_SIEM_GOVERNANCE_MAX_AGE_MS=600000 \
    CONTROL_PLANE_SIEM_GOVERNANCE_REMEDIATION_APPROVAL_REQUIRED=false CONTROL_PLANE_SIEM_RTO_ALERT_THRESHOLD_MS=30000 CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED=false \
    MYSQL_DSN="$TEST_DSN" REDIS_QUEUE_KEY="$QUEUE_KEY" \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null
}

cleanup() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "Phase 39 governance rotation smoke failed during phase: $phase" >&2
    docker compose --profile phase39-smoke logs --tail 500 taskflow siem-sink siem-sink-secondary siem-governance siem-governance-secondary >&2 2>/dev/null || true
  fi
  CONTROL_PLANE_AUTH_ENABLED=false CONTROL_PLANE_BEARER_TOKEN= CONTROL_PLANE_PREVIOUS_BEARER_TOKEN= CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS= CONTROL_PLANE_OPERATOR_ID=operator CONTROL_PLANE_PREVIOUS_OPERATOR_ID= \
    CONTROL_PLANE_AUDIT_ENABLED=true CONTROL_PLANE_AUDIT_RETENTION_ENABLED=true CONTROL_PLANE_SIEM_DELIVERY_ENABLED=false CONTROL_PLANE_SIEM_SIGNING_PROVIDER=hmac \
    CONTROL_PLANE_SIEM_FAILOVER_ENABLED=false CONTROL_PLANE_SIEM_SHADOW_SECONDARY=false CONTROL_PLANE_SIEM_CLUSTER_GUARD_ENABLED=false CONTROL_PLANE_SIEM_TERM_FENCING_ENABLED=false \
    CONTROL_PLANE_SIEM_ANTI_ENTROPY_ENABLED=false CONTROL_PLANE_SIEM_PROMOTION_WORKFLOW_ENABLED=false CONTROL_PLANE_SIEM_BREAK_GLASS_ENABLED=false CONTROL_PLANE_SIEM_GOVERNANCE_ENDPOINT= \
    CONTROL_PLANE_SIEM_GOVERNANCE_HA_ENABLED=false CONTROL_PLANE_SIEM_GOVERNANCE_RECEIPT_QUORUM=1 CONTROL_PLANE_SIEM_GOVERNANCE_MEMBERSHIP_EPOCH=1 CONTROL_PLANE_SIEM_GOVERNANCE_DEBT_POLICY_ENABLED=false CONTROL_PLANE_SIEM_GOVERNANCE_REMEDIATION_APPROVAL_REQUIRED=false \
    CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED=false CONTROL_PLANE_SIEM_RECONCILE_ENABLED=false CONTROL_PLANE_SIEM_DR_ENABLED=false MYSQL_DSN="$DEFAULT_DSN" REDIS_QUEUE_KEY=taskflow:queue \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null 2>&1 || true
  docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}" >/dev/null 2>&1 || true
  docker compose --profile phase39-smoke rm -sf siem-sink siem-sink-secondary siem-governance siem-governance-secondary >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT

phase="build Phase 39 image and static guardrails"
docker build --target build -t taskflow-go:phase39-build --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
docker build -t taskflow-go:local --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
python3 scripts/phase39-governance-rotation-check.py

phase="start isolated receivers and old-key governance replicas at membership epoch"
docker compose --profile phase39-smoke rm -sf siem-sink siem-sink-secondary siem-governance siem-governance-secondary >/dev/null 2>&1 || true
docker compose up -d mysql redis executor-worker >/dev/null
wait_mysql
CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
PHASE29_PRIMARY_RECEIVER_ID="$PRIMARY_ID" PHASE29_SECONDARY_RECEIVER_ID="$SECONDARY_ID" PHASE30_ACTIVE_RECEIVER="$PRIMARY_ID" \
  docker compose --profile phase39-smoke up -d --force-recreate siem-sink siem-sink-secondary >/dev/null
wait_http "$PRIMARY_URL/health"; wait_http "$SECONDARY_URL/health"
start_governance_replicas "$PRIMARY_OLD_KEY" "$PRIMARY_OLD_KEY_ID" "$SECONDARY_OLD_KEY" "$SECONDARY_OLD_KEY_ID" "$MEMBERSHIP_EPOCH" "$MEMBERSHIP_EPOCH" "$OLD_KEY_GENERATION" "$OLD_KEY_GENERATION"

phase="create isolated database and prove previous receipt keys preserve 2-of-2 quorum"
docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}; CREATE DATABASE ${TEST_DB}; GRANT ALL PRIVILEGES ON ${TEST_DB}.* TO 'taskflow'@'%'; FLUSH PRIVILEGES" >/dev/null
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null
start_taskflow
wait_http "$BASE_URL/health/ready"
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_siem_term_ledger_anchors WHERE term=1"
term1_id="$(sql "SELECT id FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=1")"
wait_sql "SELECT IF(COUNT(*)=2 AND COUNT(DISTINCT receipt_key_id)=2 AND MIN(membership_epoch)=${MEMBERSHIP_EPOCH} AND MAX(membership_epoch)=${MEMBERSHIP_EPOCH},1,0) FROM ${TEST_DB}.control_plane_siem_governance_replica_receipts WHERE kind='anchor' AND target_id=${term1_id} AND receipt_key_id IN ('${PRIMARY_OLD_KEY_ID}','${SECONDARY_OLD_KEY_ID}')"
governance_status >/tmp/phase39-status-old.json
python3 - <<'PY'
import json, os
d=json.load(open('/tmp/phase39-status-old.json'))
assert d['ha_enabled'] is True and d['replica_quorum']==2, d
assert d['membership_epoch']==int(os.environ.get('PHASE39_MEMBERSHIP_EPOCH','7')), d
assert d['fenced'] is False and d['replica_receipt_debt']==0, d
PY

phase="rotate both governance replicas to active receipt keys without restarting TaskFlow"
start_governance_replicas "$PRIMARY_NEW_KEY" "$PRIMARY_NEW_KEY_ID" "$SECONDARY_NEW_KEY" "$SECONDARY_NEW_KEY_ID" "$MEMBERSHIP_EPOCH" "$MEMBERSHIP_EPOCH" "$NEW_KEY_GENERATION" "$NEW_KEY_GENERATION"
curl -fsS --max-time 15 -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"target_receiver\":\"${SECONDARY_ID}\",\"expected_term\":1,\"incident_id\":\"INC-39-ROTATE\",\"reason\":\"prove previous receipt keys preserved quorum during rolling rotation\"}" \
  "$BASE_URL/api/v1/control-plane/siem/receivers/break-glass" >/tmp/phase39-break2.json
term2_id="$(sql "SELECT id FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=2")"
wait_sql "SELECT IF(anchor_state='delivered',1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE id=${term2_id}"
wait_sql "SELECT IF(COUNT(*)=2 AND MIN(membership_epoch)=${MEMBERSHIP_EPOCH} AND MAX(membership_epoch)=${MEMBERSHIP_EPOCH},1,0) FROM ${TEST_DB}.control_plane_siem_governance_replica_receipts WHERE kind='anchor' AND target_id=${term2_id} AND receipt_key_id IN ('${PRIMARY_NEW_KEY_ID}','${SECONDARY_NEW_KEY_ID}')"
governance_status >/tmp/phase39-status-new.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase39-status-new.json'))
assert d['fenced'] is False and d['dead']==0 and d['replica_receipt_debt']==0, d
PY

phase="restart secondary with valid active key but stale membership epoch"
restart_secondary_governance "$SECONDARY_NEW_KEY" "$SECONDARY_NEW_KEY_ID" "$STALE_EPOCH" "$NEW_KEY_GENERATION"
curl -fsS --max-time 15 -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"target_receiver\":\"${PRIMARY_ID}\",\"expected_term\":2,\"incident_id\":\"INC-39-EPOCH\",\"reason\":\"prove stale membership epoch is cryptographically fenced\"}" \
  "$BASE_URL/api/v1/control-plane/siem/receivers/break-glass" >/tmp/phase39-break3.json
term3_id="$(sql "SELECT id FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=3")"
wait_sql "SELECT IF(anchor_state='dead' AND anchor_last_error LIKE '%membership epoch mismatch%',1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE id=${term3_id}"
wait_sql "SELECT IF(COUNT(*)=1 AND MIN(membership_epoch)=${MEMBERSHIP_EPOCH},1,0) FROM ${TEST_DB}.control_plane_siem_governance_replica_receipts WHERE kind='anchor' AND target_id=${term3_id} AND replica_id='${GOV_PRIMARY_REPLICA}'"
[ "$(sql "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_siem_governance_replica_receipts WHERE kind='anchor' AND target_id=${term3_id} AND replica_id='${GOV_SECONDARY_REPLICA}'")" = "0" ]
governance_status >/tmp/phase39-status-stale.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase39-status-stale.json'))
assert d['fenced'] is True and d['dead'] >= 1, d
assert d['membership_epoch']==7, d
assert 'dead governance evidence' in d['fence_reason'], d
PY

phase="prove ordinary promotion cannot bypass stale membership epoch evidence"
curl -fsS -X POST -H "Authorization: Bearer $REQUESTER_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"target_receiver\":\"${SECONDARY_ID}\",\"expected_term\":3,\"reason\":\"stale governance membership must keep promotion fenced\"}" \
  "$BASE_URL/api/v1/control-plane/siem/receivers/intents" >/tmp/phase39-intent.json
intent_id="$(python3 -c 'import json; print(json.load(open("/tmp/phase39-intent.json"))["id"])')"
curl -fsS -X POST -H "Authorization: Bearer $APPROVER_TOKEN" -H 'Content-Type: application/json' -d '{"note":"approval cannot override membership epoch fence"}' \
  "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_id}/approve" >/dev/null
fenced_code="$(curl -sS -o /tmp/phase39-fenced.json -w '%{http_code}' -X POST -H "Authorization: Bearer $APPROVER_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_id}/execute")"
[ "$fenced_code" = "409" ]
grep -q 'dead governance evidence' /tmp/phase39-fenced.json

echo "Phase 39 previous receipt keys preserved quorum during rolling rotation: ok"
echo "Phase 39 active receipt keys took over without TaskFlow restart or quorum interruption: ok"
echo "Phase 39 stale membership epoch with a valid signature became permanent dead evidence and fenced ordinary promotion: ok"
