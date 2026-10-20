#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
PRIMARY_URL="${PHASE40_PRIMARY_URL:-http://127.0.0.1:18084}"
SECONDARY_URL="${PHASE40_SECONDARY_URL:-http://127.0.0.1:18085}"
GOV_PRIMARY_URL="${PHASE40_GOVERNANCE_PRIMARY_URL:-http://127.0.0.1:19704}"
GOV_SECONDARY_URL="${PHASE40_GOVERNANCE_SECONDARY_URL:-http://127.0.0.1:19705}"

COMMANDER="phase40-commander"
COMMANDER_TOKEN="${PHASE40_COMMANDER_TOKEN:-phase40-commander-token}"
REQUESTER="phase40-requester"
REQUESTER_TOKEN="${PHASE40_REQUESTER_TOKEN:-phase40-requester-token}"
APPROVER="phase40-approver"
APPROVER_TOKEN="${PHASE40_APPROVER_TOKEN:-phase40-approver-token}"
ADDITIONAL_CREDENTIALS="${APPROVER}=${APPROVER_TOKEN}"
ROLE_POLICY="${REQUESTER}=requester;${APPROVER}=approver;${COMMANDER}=incident-commander"

SINK_TOKEN="${PHASE40_SINK_TOKEN:-phase40-sink-token}"
SIGN_KEY="${PHASE40_SIGNING_KEY:-phase40-signing-key}"
SIGN_KEY_ID="${PHASE40_SIGNING_KEY_ID:-phase40-sign-v1}"
CHECKPOINT_KEY="${PHASE40_CHECKPOINT_KEY:-phase40-checkpoint-secret}"
CHECKPOINT_KEY_ID="${PHASE40_CHECKPOINT_KEY_ID:-phase40-checkpoint-v1}"
GOV_PRIMARY_TOKEN="${PHASE40_GOVERNANCE_PRIMARY_TOKEN:-phase40-governance-primary-token}"
GOV_SECONDARY_TOKEN="${PHASE40_GOVERNANCE_SECONDARY_TOKEN:-phase40-governance-secondary-token}"
GOV_SIGN_KEY="${PHASE40_GOVERNANCE_SIGNING_KEY:-phase40-governance-signing-key}"
GOV_SIGN_KEY_ID="${PHASE40_GOVERNANCE_SIGNING_KEY_ID:-phase40-governance-v1}"
GOV_PRIMARY_REPLICA="governance-primary"
GOV_SECONDARY_REPLICA="governance-secondary"

PRIMARY_OLD_KEY="${PHASE40_PRIMARY_OLD_RECEIPT_KEY:-phase40-primary-old-secret}"
PRIMARY_OLD_KEY_ID="${PHASE40_PRIMARY_OLD_RECEIPT_KEY_ID:-phase40-primary-v1}"
PRIMARY_NEW_KEY="${PHASE40_PRIMARY_NEW_RECEIPT_KEY:-phase40-primary-new-secret}"
PRIMARY_NEW_KEY_ID="${PHASE40_PRIMARY_NEW_RECEIPT_KEY_ID:-phase40-primary-v2}"
SECONDARY_OLD_KEY="${PHASE40_SECONDARY_OLD_RECEIPT_KEY:-phase40-secondary-old-secret}"
SECONDARY_OLD_KEY_ID="${PHASE40_SECONDARY_OLD_RECEIPT_KEY_ID:-phase40-secondary-v1}"
SECONDARY_NEW_KEY="${PHASE40_SECONDARY_NEW_RECEIPT_KEY:-phase40-secondary-new-secret}"
SECONDARY_NEW_KEY_ID="${PHASE40_SECONDARY_NEW_RECEIPT_KEY_ID:-phase40-secondary-v2}"
OLD_GENERATION=1
NEW_GENERATION=2
MEMBERSHIP_EPOCH="${PHASE40_MEMBERSHIP_EPOCH:-11}"
FUTURE_VALID_UNTIL_MS="${PHASE40_FUTURE_VALID_UNTIL_MS:-$(python3 -c 'import time; print(int(time.time()*1000)+3600000)')}"
PAST_VALID_UNTIL_MS="${PHASE40_PAST_VALID_UNTIL_MS:-$(python3 -c 'import time; print(int(time.time()*1000)-60000)')}"

PRIMARY_ID="receiver-primary"
SECONDARY_ID="receiver-secondary"
ROLLBACK_DB="${PHASE40_ROLLBACK_DB:-taskflow_phase40}"
EXPIRY_DB="${PHASE40_EXPIRY_DB:-taskflow_phase40_expiry}"
DEFAULT_DSN="taskflow:taskflow@tcp(mysql:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
QUEUE_KEY="${PHASE40_QUEUE_KEY:-taskflow:phase40:queue}"
CURRENT_DSN=""

TF_PRIMARY_ACTIVE_KEY=""
TF_PRIMARY_ACTIVE_KEY_ID=""
TF_PRIMARY_ACTIVE_GENERATION=1
TF_PRIMARY_PREVIOUS_KEY=""
TF_PRIMARY_PREVIOUS_KEY_ID=""
TF_PRIMARY_PREVIOUS_GENERATION=0
TF_PRIMARY_PREVIOUS_VALID_UNTIL_MS=0
TF_SECONDARY_ACTIVE_KEY=""
TF_SECONDARY_ACTIVE_KEY_ID=""
TF_SECONDARY_ACTIVE_GENERATION=1
TF_SECONDARY_PREVIOUS_KEY=""
TF_SECONDARY_PREVIOUS_KEY_ID=""
TF_SECONDARY_PREVIOUS_GENERATION=0
TF_SECONDARY_PREVIOUS_VALID_UNTIL_MS=0
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

checkpoint_hwm() {
  curl -fsS -H "Authorization: Bearer $SINK_TOKEN" "$1/checkpoint?after_sequence=0&limit=1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["high_watermark_sequence"])'
}

wait_receivers_stable_equal() {
  i=0; stable=0; last=""; p=0; s=0
  while [ "$i" -lt 300 ]; do
    p="$(checkpoint_hwm "$PRIMARY_URL" 2>/dev/null || echo 0)"
    s="$(checkpoint_hwm "$SECONDARY_URL" 2>/dev/null || echo 0)"
    if [ "$p" = "$s" ] && [ "$p" -gt 0 ]; then
      if [ "$p" = "$last" ]; then stable=$((stable + 1)); else stable=1; last="$p"; fi
      if [ "$stable" -ge 4 ]; then return 0; fi
    else
      stable=0; last=""
    fi
    i=$((i + 1)); sleep 0.1
  done
  echo "receivers did not reach stable equality primary=$p secondary=$s" >&2
  return 1
}

create_db() {
  name="$1"
  docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${name}; CREATE DATABASE ${name}; GRANT ALL PRIVILEGES ON ${name}.* TO 'taskflow'@'%'; FLUSH PRIVILEGES" >/dev/null
  CURRENT_DSN="taskflow:taskflow@tcp(mysql:3306)/${name}?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
  docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null
}

start_receivers() {
  docker compose --profile phase40-smoke rm -sf siem-sink siem-sink-secondary >/dev/null 2>&1 || true
  CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
  CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
  PHASE29_PRIMARY_RECEIVER_ID="$PRIMARY_ID" PHASE29_SECONDARY_RECEIVER_ID="$SECONDARY_ID" PHASE30_ACTIVE_RECEIVER="$PRIMARY_ID" \
    docker compose --profile phase40-smoke up -d --force-recreate siem-sink siem-sink-secondary >/dev/null
  wait_http "$PRIMARY_URL/health"
  wait_http "$SECONDARY_URL/health"
}

start_governance_replicas() {
  primary_key="$1"; primary_key_id="$2"; primary_generation="$3"
  secondary_key="$4"; secondary_key_id="$5"; secondary_generation="$6"
  docker compose --profile phase40-smoke rm -sf siem-governance siem-governance-secondary >/dev/null 2>&1 || true
  CONTROL_PLANE_SIEM_GOVERNANCE_BEARER_TOKEN="$GOV_PRIMARY_TOKEN" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_BEARER_TOKEN="$GOV_SECONDARY_TOKEN" \
  CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY="$GOV_SIGN_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY_ID="$GOV_SIGN_KEY_ID" \
  CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_REPLICA_ID="$GOV_PRIMARY_REPLICA" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_RECEIPT_HMAC_KEY="$primary_key" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_RECEIPT_HMAC_KEY_ID="$primary_key_id" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_RECEIPT_KEY_GENERATION="$primary_generation" \
  CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_REPLICA_ID="$GOV_SECONDARY_REPLICA" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_HMAC_KEY="$secondary_key" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_HMAC_KEY_ID="$secondary_key_id" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_KEY_GENERATION="$secondary_generation" \
  PHASE39_GOVERNANCE_PRIMARY_MEMBERSHIP_EPOCH="$MEMBERSHIP_EPOCH" PHASE39_GOVERNANCE_SECONDARY_MEMBERSHIP_EPOCH="$MEMBERSHIP_EPOCH" \
    docker compose --profile phase40-smoke up -d --force-recreate siem-governance siem-governance-secondary >/dev/null
  wait_http "$GOV_PRIMARY_URL/health"
  wait_http "$GOV_SECONDARY_URL/health"
}

configure_rotation_trust() {
  valid_until="$1"
  TF_PRIMARY_ACTIVE_KEY="$PRIMARY_NEW_KEY"; TF_PRIMARY_ACTIVE_KEY_ID="$PRIMARY_NEW_KEY_ID"; TF_PRIMARY_ACTIVE_GENERATION=$NEW_GENERATION
  TF_PRIMARY_PREVIOUS_KEY="$PRIMARY_OLD_KEY"; TF_PRIMARY_PREVIOUS_KEY_ID="$PRIMARY_OLD_KEY_ID"; TF_PRIMARY_PREVIOUS_GENERATION=$OLD_GENERATION; TF_PRIMARY_PREVIOUS_VALID_UNTIL_MS="$valid_until"
  TF_SECONDARY_ACTIVE_KEY="$SECONDARY_NEW_KEY"; TF_SECONDARY_ACTIVE_KEY_ID="$SECONDARY_NEW_KEY_ID"; TF_SECONDARY_ACTIVE_GENERATION=$NEW_GENERATION
  TF_SECONDARY_PREVIOUS_KEY="$SECONDARY_OLD_KEY"; TF_SECONDARY_PREVIOUS_KEY_ID="$SECONDARY_OLD_KEY_ID"; TF_SECONDARY_PREVIOUS_GENERATION=$OLD_GENERATION; TF_SECONDARY_PREVIOUS_VALID_UNTIL_MS="$valid_until"
}

configure_rollback_trust() {
  TF_PRIMARY_ACTIVE_KEY="$PRIMARY_OLD_KEY"; TF_PRIMARY_ACTIVE_KEY_ID="$PRIMARY_OLD_KEY_ID"; TF_PRIMARY_ACTIVE_GENERATION=$OLD_GENERATION
  TF_PRIMARY_PREVIOUS_KEY=""; TF_PRIMARY_PREVIOUS_KEY_ID=""; TF_PRIMARY_PREVIOUS_GENERATION=0; TF_PRIMARY_PREVIOUS_VALID_UNTIL_MS=0
  TF_SECONDARY_ACTIVE_KEY="$SECONDARY_OLD_KEY"; TF_SECONDARY_ACTIVE_KEY_ID="$SECONDARY_OLD_KEY_ID"; TF_SECONDARY_ACTIVE_GENERATION=$OLD_GENERATION
  TF_SECONDARY_PREVIOUS_KEY=""; TF_SECONDARY_PREVIOUS_KEY_ID=""; TF_SECONDARY_PREVIOUS_GENERATION=0; TF_SECONDARY_PREVIOUS_VALID_UNTIL_MS=0
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
    CONTROL_PLANE_SIEM_GOVERNANCE_HA_ENABLED=true CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_REPLICA_ID="$GOV_PRIMARY_REPLICA" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_RECEIPT_HMAC_KEY="$TF_PRIMARY_ACTIVE_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_RECEIPT_HMAC_KEY_ID="$TF_PRIMARY_ACTIVE_KEY_ID" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_RECEIPT_KEY_GENERATION="$TF_PRIMARY_ACTIVE_GENERATION" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_PREVIOUS_RECEIPT_HMAC_KEY="$TF_PRIMARY_PREVIOUS_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_PREVIOUS_RECEIPT_HMAC_KEY_ID="$TF_PRIMARY_PREVIOUS_KEY_ID" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_PREVIOUS_RECEIPT_KEY_GENERATION="$TF_PRIMARY_PREVIOUS_GENERATION" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_PREVIOUS_RECEIPT_VALID_UNTIL_UNIX_MS="$TF_PRIMARY_PREVIOUS_VALID_UNTIL_MS" \
    CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_ENDPOINT=http://siem-governance-secondary:8080 CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_BEARER_TOKEN="$GOV_SECONDARY_TOKEN" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_REPLICA_ID="$GOV_SECONDARY_REPLICA" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_HMAC_KEY="$TF_SECONDARY_ACTIVE_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_HMAC_KEY_ID="$TF_SECONDARY_ACTIVE_KEY_ID" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_KEY_GENERATION="$TF_SECONDARY_ACTIVE_GENERATION" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_PREVIOUS_RECEIPT_HMAC_KEY="$TF_SECONDARY_PREVIOUS_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_PREVIOUS_RECEIPT_HMAC_KEY_ID="$TF_SECONDARY_PREVIOUS_KEY_ID" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_PREVIOUS_RECEIPT_KEY_GENERATION="$TF_SECONDARY_PREVIOUS_GENERATION" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_PREVIOUS_RECEIPT_VALID_UNTIL_UNIX_MS="$TF_SECONDARY_PREVIOUS_VALID_UNTIL_MS" CONTROL_PLANE_SIEM_GOVERNANCE_RECEIPT_QUORUM=2 CONTROL_PLANE_SIEM_GOVERNANCE_MEMBERSHIP_EPOCH="$MEMBERSHIP_EPOCH" \
    CONTROL_PLANE_SIEM_GOVERNANCE_DEBT_POLICY_ENABLED=true CONTROL_PLANE_SIEM_GOVERNANCE_MAX_OUTSTANDING=1000 CONTROL_PLANE_SIEM_GOVERNANCE_MAX_AGE_MS=600000 \
    CONTROL_PLANE_SIEM_GOVERNANCE_REMEDIATION_APPROVAL_REQUIRED=false CONTROL_PLANE_SIEM_RTO_ALERT_THRESHOLD_MS=30000 CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED=false \
    MYSQL_DSN="$CURRENT_DSN" REDIS_QUEUE_KEY="$QUEUE_KEY" \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null
}

cleanup() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "Phase 40 governance key retirement smoke failed during phase: $phase" >&2
    docker compose --profile phase40-smoke logs --tail 500 taskflow siem-sink siem-sink-secondary siem-governance siem-governance-secondary >&2 2>/dev/null || true
  fi
  CONTROL_PLANE_AUTH_ENABLED=false CONTROL_PLANE_BEARER_TOKEN= CONTROL_PLANE_PREVIOUS_BEARER_TOKEN= CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS= CONTROL_PLANE_OPERATOR_ID=operator CONTROL_PLANE_PREVIOUS_OPERATOR_ID= \
    CONTROL_PLANE_AUDIT_ENABLED=true CONTROL_PLANE_AUDIT_RETENTION_ENABLED=true CONTROL_PLANE_SIEM_DELIVERY_ENABLED=false CONTROL_PLANE_SIEM_SIGNING_PROVIDER=hmac \
    CONTROL_PLANE_SIEM_FAILOVER_ENABLED=false CONTROL_PLANE_SIEM_SHADOW_SECONDARY=false CONTROL_PLANE_SIEM_CLUSTER_GUARD_ENABLED=false CONTROL_PLANE_SIEM_TERM_FENCING_ENABLED=false \
    CONTROL_PLANE_SIEM_ANTI_ENTROPY_ENABLED=false CONTROL_PLANE_SIEM_PROMOTION_WORKFLOW_ENABLED=false CONTROL_PLANE_SIEM_BREAK_GLASS_ENABLED=false CONTROL_PLANE_SIEM_GOVERNANCE_ENDPOINT= \
    CONTROL_PLANE_SIEM_GOVERNANCE_HA_ENABLED=false CONTROL_PLANE_SIEM_GOVERNANCE_RECEIPT_QUORUM=1 CONTROL_PLANE_SIEM_GOVERNANCE_MEMBERSHIP_EPOCH=1 CONTROL_PLANE_SIEM_GOVERNANCE_DEBT_POLICY_ENABLED=false CONTROL_PLANE_SIEM_GOVERNANCE_REMEDIATION_APPROVAL_REQUIRED=false \
    CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED=false CONTROL_PLANE_SIEM_RECONCILE_ENABLED=false CONTROL_PLANE_SIEM_DR_ENABLED=false MYSQL_DSN="$DEFAULT_DSN" REDIS_QUEUE_KEY=taskflow:queue \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null 2>&1 || true
  docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${ROLLBACK_DB}; DROP DATABASE IF EXISTS ${EXPIRY_DB};" >/dev/null 2>&1 || true
  docker compose --profile phase40-smoke rm -sf siem-sink siem-sink-secondary siem-governance siem-governance-secondary >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT

phase="build Phase 40 image and static guardrails"
docker build --target build -t taskflow-go:phase40-build --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
docker build -t taskflow-go:local --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
python3 scripts/phase40-governance-key-retirement-check.py

phase="start isolated infrastructure for durable generation rollback proof"
docker compose --profile phase40-smoke rm -sf siem-sink siem-sink-secondary siem-governance siem-governance-secondary >/dev/null 2>&1 || true
docker compose up -d mysql redis executor-worker >/dev/null
wait_mysql
start_receivers
start_governance_replicas "$PRIMARY_OLD_KEY" "$PRIMARY_OLD_KEY_ID" "$OLD_GENERATION" "$SECONDARY_OLD_KEY" "$SECONDARY_OLD_KEY_ID" "$OLD_GENERATION"
create_db "$ROLLBACK_DB"
configure_rotation_trust "$FUTURE_VALID_UNTIL_MS"
start_taskflow
wait_http "$BASE_URL/health/ready"

phase="prove previous generation 1 receipts are accepted only inside the configured grace"
wait_sql "SELECT IF(anchor_state='delivered',1,0) FROM ${ROLLBACK_DB}.control_plane_siem_receiver_term_history WHERE term=1"
term1_id="$(sql "SELECT id FROM ${ROLLBACK_DB}.control_plane_siem_receiver_term_history WHERE term=1")"
wait_sql "SELECT IF(COUNT(*)=2 AND MIN(receipt_key_generation)=1 AND MAX(receipt_key_generation)=1 AND MIN(receipt_key_state)='previous' AND MAX(receipt_key_state)='previous',1,0) FROM ${ROLLBACK_DB}.control_plane_siem_governance_replica_receipts WHERE kind='anchor' AND target_id=${term1_id}"
governance_status >/tmp/phase40-status-grace.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase40-status-grace.json'))
assert d['ha_enabled'] is True and d['previous_key_grace_replicas']==2, d
assert d['fenced'] is False and d['replica_receipt_debt']==0, d
PY
curl -fsS -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/metrics" >/tmp/phase40-metrics-grace.txt
grep -q 'taskflow_control_plane_siem_governance_previous_key_grace_replicas 2' /tmp/phase40-metrics-grace.txt

phase="rotate governance replicas to active generation 2 and persist monotonic floors"
start_governance_replicas "$PRIMARY_NEW_KEY" "$PRIMARY_NEW_KEY_ID" "$NEW_GENERATION" "$SECONDARY_NEW_KEY" "$SECONDARY_NEW_KEY_ID" "$NEW_GENERATION"
curl -fsS --max-time 15 -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"target_receiver\":\"${SECONDARY_ID}\",\"expected_term\":1,\"incident_id\":\"INC-40-GEN2\",\"reason\":\"advance durable governance receipt key generation\"}" \
  "$BASE_URL/api/v1/control-plane/siem/receivers/break-glass" >/tmp/phase40-term2.json
wait_sql "SELECT IF(anchor_state='delivered',1,0) FROM ${ROLLBACK_DB}.control_plane_siem_receiver_term_history WHERE term=2"
term2_id="$(sql "SELECT id FROM ${ROLLBACK_DB}.control_plane_siem_receiver_term_history WHERE term=2")"
wait_sql "SELECT IF(COUNT(*)=2 AND MIN(receipt_key_generation)=2 AND MAX(receipt_key_generation)=2 AND MIN(receipt_key_state)='active' AND MAX(receipt_key_state)='active',1,0) FROM ${ROLLBACK_DB}.control_plane_siem_governance_replica_receipts WHERE kind='anchor' AND target_id=${term2_id}"
wait_sql "SELECT IF(COUNT(*)=2 AND MIN(highest_active_generation)=2 AND MAX(highest_active_generation)=2,1,0) FROM ${ROLLBACK_DB}.control_plane_siem_governance_replica_key_states"

phase="restart with generation 1 as active and prove durable anti-rollback survives process restart"
start_governance_replicas "$PRIMARY_OLD_KEY" "$PRIMARY_OLD_KEY_ID" "$OLD_GENERATION" "$SECONDARY_OLD_KEY" "$SECONDARY_OLD_KEY_ID" "$OLD_GENERATION"
configure_rollback_trust
start_taskflow
wait_http "$BASE_URL/health/ready"
curl -fsS --max-time 15 -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"target_receiver\":\"${PRIMARY_ID}\",\"expected_term\":2,\"incident_id\":\"INC-40-ROLLBACK\",\"reason\":\"prove retired key generation cannot become active again\"}" \
  "$BASE_URL/api/v1/control-plane/siem/receivers/break-glass" >/tmp/phase40-term3.json
wait_sql "SELECT IF(anchor_state='dead' AND anchor_last_error LIKE '%key generation rollback%',1,0) FROM ${ROLLBACK_DB}.control_plane_siem_receiver_term_history WHERE term=3"
wait_sql "SELECT IF(COUNT(*)=2 AND MIN(highest_active_generation)=2 AND MAX(highest_active_generation)=2,1,0) FROM ${ROLLBACK_DB}.control_plane_siem_governance_replica_key_states"
governance_status >/tmp/phase40-status-rollback.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase40-status-rollback.json'))
assert d['fenced'] is True and d['dead'] >= 1, d
assert 'dead governance evidence' in d['fence_reason'], d
PY

phase="prove ordinary promotion cannot bypass persistent generation rollback fence"
wait_sql "SELECT IF(COUNT(*)=0,1,0) FROM ${ROLLBACK_DB}.control_plane_audit_deliveries WHERE state <> 'delivered'"
curl -fsS -X POST -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/sync" >/dev/null
wait_receivers_stable_equal
curl -fsS -X POST -H "Authorization: Bearer $REQUESTER_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"target_receiver\":\"${SECONDARY_ID}\",\"expected_term\":3,\"reason\":\"rollback fence must block ordinary promotion\"}" \
  "$BASE_URL/api/v1/control-plane/siem/receivers/intents" >/tmp/phase40-intent.json
intent_id="$(python3 -c 'import json; print(json.load(open("/tmp/phase40-intent.json"))["id"])')"
curl -fsS -X POST -H "Authorization: Bearer $APPROVER_TOKEN" -H 'Content-Type: application/json' -d '{"note":"approval cannot override retired key generation"}' \
  "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_id}/approve" >/dev/null
fenced_code="$(curl -sS -o /tmp/phase40-fenced.json -w '%{http_code}' -X POST -H "Authorization: Bearer $APPROVER_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_id}/execute")"
[ "$fenced_code" = "409" ]
grep -q 'dead governance evidence' /tmp/phase40-fenced.json

phase="recreate isolated receivers/database and prove expired previous-key grace fails closed"
start_receivers
start_governance_replicas "$PRIMARY_OLD_KEY" "$PRIMARY_OLD_KEY_ID" "$OLD_GENERATION" "$SECONDARY_OLD_KEY" "$SECONDARY_OLD_KEY_ID" "$OLD_GENERATION"
create_db "$EXPIRY_DB"
configure_rotation_trust "$PAST_VALID_UNTIL_MS"
start_taskflow
wait_http "$BASE_URL/health/ready"
wait_sql "SELECT IF(anchor_state='dead' AND anchor_last_error LIKE '%previous receipt key grace expired%',1,0) FROM ${EXPIRY_DB}.control_plane_siem_receiver_term_history WHERE term=1"
governance_status >/tmp/phase40-status-expired.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase40-status-expired.json'))
assert d['previous_key_grace_replicas']==0, d
assert d['fenced'] is True and d['dead'] >= 1, d
PY

echo "Phase 40 bounded previous-key grace accepted generation 1 only inside the configured retirement window: ok"
echo "Phase 40 durable replica key-state preserved generation 2 and rejected active-generation rollback after TaskFlow restart: ok"
echo "Phase 40 expired previous receipt key was rejected despite valid HMAC and ordinary promotion remained fail-closed: ok"
