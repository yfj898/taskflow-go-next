#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
PRIMARY_URL="${PHASE38_PRIMARY_URL:-http://127.0.0.1:18084}"
SECONDARY_URL="${PHASE38_SECONDARY_URL:-http://127.0.0.1:18085}"
GOV_PRIMARY_URL="${PHASE38_GOVERNANCE_PRIMARY_URL:-http://127.0.0.1:19704}"
GOV_SECONDARY_URL="${PHASE38_GOVERNANCE_SECONDARY_URL:-http://127.0.0.1:19705}"

COMMANDER="phase38-commander"
COMMANDER_TOKEN="${PHASE38_COMMANDER_TOKEN:-phase38-commander-token}"
REQUESTER="phase38-requester"
REQUESTER_TOKEN="${PHASE38_REQUESTER_TOKEN:-phase38-requester-token}"
APPROVER="phase38-approver"
APPROVER_TOKEN="${PHASE38_APPROVER_TOKEN:-phase38-approver-token}"
ADDITIONAL_CREDENTIALS="${APPROVER}=${APPROVER_TOKEN}"
ROLE_POLICY="${REQUESTER}=requester;${APPROVER}=approver;${COMMANDER}=incident-commander"

SINK_TOKEN="${PHASE38_SINK_TOKEN:-phase38-sink-token}"
SIGN_KEY="${PHASE38_SIGNING_KEY:-phase38-signing-key}"
SIGN_KEY_ID="${PHASE38_SIGNING_KEY_ID:-phase38-sign-v1}"
CHECKPOINT_KEY="${PHASE38_CHECKPOINT_KEY:-phase38-checkpoint-secret}"
CHECKPOINT_KEY_ID="${PHASE38_CHECKPOINT_KEY_ID:-phase38-checkpoint-v1}"
GOV_PRIMARY_TOKEN="${PHASE38_GOVERNANCE_PRIMARY_TOKEN:-phase38-governance-primary-token}"
GOV_SECONDARY_TOKEN="${PHASE38_GOVERNANCE_SECONDARY_TOKEN:-phase38-governance-secondary-token}"
GOV_SIGN_KEY="${PHASE38_GOVERNANCE_SIGNING_KEY:-phase38-governance-signing-key}"
GOV_SIGN_KEY_ID="${PHASE38_GOVERNANCE_SIGNING_KEY_ID:-phase38-governance-v1}"
GOV_PRIMARY_REPLICA="governance-primary"
GOV_SECONDARY_REPLICA="governance-secondary"
GOV_PRIMARY_RECEIPT_KEY="${PHASE38_GOVERNANCE_PRIMARY_RECEIPT_KEY:-phase38-primary-receipt-key}"
GOV_PRIMARY_RECEIPT_KEY_ID="${PHASE38_GOVERNANCE_PRIMARY_RECEIPT_KEY_ID:-phase38-primary-receipt-v1}"
GOV_SECONDARY_RECEIPT_KEY="${PHASE38_GOVERNANCE_SECONDARY_RECEIPT_KEY:-phase38-secondary-receipt-key}"
GOV_SECONDARY_RECEIPT_KEY_ID="${PHASE38_GOVERNANCE_SECONDARY_RECEIPT_KEY_ID:-phase38-secondary-receipt-v1}"
FAULT_HASH="ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

PRIMARY_ID="receiver-primary"
SECONDARY_ID="receiver-secondary"
TEST_DB="${PHASE38_DB:-taskflow_phase38}"
TEST_DSN="taskflow:taskflow@tcp(mysql:3306)/${TEST_DB}?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
DEFAULT_DSN="taskflow:taskflow@tcp(mysql:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
QUEUE_KEY="${PHASE38_QUEUE_KEY:-taskflow:phase38:queue}"
PRIMARY_GOV_CONTAINER=""
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
    docker compose up -d mysql >/dev/null 2>&1 || true
    if docker compose exec -T mysql mysqladmin ping -uroot -proot --silent >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 0.2
  done
  echo "timeout waiting for mysql compose service" >&2
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

start_taskflow() {
  CONTROL_PLANE_AUTH_ENABLED=true \
    CONTROL_PLANE_BEARER_TOKEN="$COMMANDER_TOKEN" CONTROL_PLANE_PREVIOUS_BEARER_TOKEN="$REQUESTER_TOKEN" \
    CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS="$ADDITIONAL_CREDENTIALS" CONTROL_PLANE_OPERATOR_ID="$COMMANDER" CONTROL_PLANE_PREVIOUS_OPERATOR_ID="$REQUESTER" \
    CONTROL_PLANE_AUDIT_ENABLED=true CONTROL_PLANE_AUDIT_RETENTION_ENABLED=false \
    CONTROL_PLANE_SIEM_DELIVERY_ENABLED=true CONTROL_PLANE_SIEM_ENDPOINT=http://siem-sink:8080/events \
    CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" CONTROL_PLANE_SIEM_SIGNING_PROVIDER=hmac CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" \
    CONTROL_PLANE_SIEM_DELIVERY_INTERVAL_MS=100 CONTROL_PLANE_SIEM_DELIVERY_LEASE_MS=1500 CONTROL_PLANE_SIEM_MAX_ATTEMPTS=50 CONTROL_PLANE_SIEM_RETRY_BASE_MS=100 CONTROL_PLANE_SIEM_RETRY_MAX_MS=500 \
    CONTROL_PLANE_SIEM_DISTRIBUTED_COORDINATION=false CONTROL_PLANE_SIEM_GC_ENABLED=false CONTROL_PLANE_SIEM_DR_ENABLED=false \
    CONTROL_PLANE_SIEM_PRIMARY_RECEIVER_ID="$PRIMARY_ID" CONTROL_PLANE_SIEM_PRIMARY_CHECKPOINT_ENDPOINT=http://siem-sink:8080/checkpoint CONTROL_PLANE_SIEM_PRIMARY_TERM_ENDPOINT=http://siem-sink:8080/control/term \
    CONTROL_PLANE_SIEM_SECONDARY_ENDPOINT=http://siem-sink-secondary:8080/events CONTROL_PLANE_SIEM_SECONDARY_RECEIVER_ID="$SECONDARY_ID" CONTROL_PLANE_SIEM_SECONDARY_CHECKPOINT_ENDPOINT=http://siem-sink-secondary:8080/checkpoint CONTROL_PLANE_SIEM_SECONDARY_TERM_ENDPOINT=http://siem-sink-secondary:8080/control/term \
    CONTROL_PLANE_SIEM_FAILOVER_ENABLED=false CONTROL_PLANE_SIEM_SHADOW_SECONDARY=false CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
    CONTROL_PLANE_SIEM_CLUSTER_GUARD_ENABLED=true CONTROL_PLANE_SIEM_TERM_FENCING_ENABLED=true CONTROL_PLANE_SIEM_ANTI_ENTROPY_ENABLED=true CONTROL_PLANE_SIEM_ANTI_ENTROPY_INTERVAL_MS=100 \
    CONTROL_PLANE_SIEM_PROMOTION_WORKFLOW_ENABLED=true CONTROL_PLANE_SIEM_PROMOTION_INTENT_TTL_MS=60000 CONTROL_PLANE_SIEM_PROMOTION_LEASE_MS=5000 \
    CONTROL_PLANE_SIEM_PROMOTION_APPROVAL_QUORUM=1 CONTROL_PLANE_SIEM_PROMOTION_ROLE_POLICY="$ROLE_POLICY" CONTROL_PLANE_SIEM_BREAK_GLASS_ENABLED=true \
    CONTROL_PLANE_SIEM_GOVERNANCE_ENDPOINT=http://siem-governance:8080 CONTROL_PLANE_SIEM_GOVERNANCE_BEARER_TOKEN="$GOV_PRIMARY_TOKEN" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY="$GOV_SIGN_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY_ID="$GOV_SIGN_KEY_ID" CONTROL_PLANE_SIEM_GOVERNANCE_TIMEOUT_MS=250 \
    CONTROL_PLANE_SIEM_GOVERNANCE_HA_ENABLED=true CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_REPLICA_ID="$GOV_PRIMARY_REPLICA" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_RECEIPT_HMAC_KEY="$GOV_PRIMARY_RECEIPT_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_RECEIPT_HMAC_KEY_ID="$GOV_PRIMARY_RECEIPT_KEY_ID" \
    CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_ENDPOINT=http://siem-governance-secondary:8080 CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_BEARER_TOKEN="$GOV_SECONDARY_TOKEN" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_REPLICA_ID="$GOV_SECONDARY_REPLICA" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_HMAC_KEY="$GOV_SECONDARY_RECEIPT_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_HMAC_KEY_ID="$GOV_SECONDARY_RECEIPT_KEY_ID" CONTROL_PLANE_SIEM_GOVERNANCE_RECEIPT_QUORUM=2 \
    CONTROL_PLANE_SIEM_GOVERNANCE_DEBT_POLICY_ENABLED=true CONTROL_PLANE_SIEM_GOVERNANCE_MAX_OUTSTANDING=1000 CONTROL_PLANE_SIEM_GOVERNANCE_MAX_AGE_MS=600000 \
    CONTROL_PLANE_SIEM_GOVERNANCE_REMEDIATION_APPROVAL_REQUIRED=false CONTROL_PLANE_SIEM_GOVERNANCE_REMEDIATION_STALE_TARGET_FENCING_ENABLED=false \
    CONTROL_PLANE_SIEM_RTO_ALERT_THRESHOLD_MS=30000 CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED=false MYSQL_DSN="$TEST_DSN" REDIS_QUEUE_KEY="$QUEUE_KEY" \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null
}

cleanup() {
  rc=$?
  if [ -n "$PRIMARY_GOV_CONTAINER" ]; then docker unpause "$PRIMARY_GOV_CONTAINER" >/dev/null 2>&1 || true; fi
  if [ "$rc" -ne 0 ]; then
    echo "Phase 38 governance HA smoke failed during phase: $phase" >&2
    docker compose --profile phase38-smoke logs --tail 500 taskflow siem-sink siem-sink-secondary siem-governance siem-governance-secondary >&2 2>/dev/null || true
  fi
  CONTROL_PLANE_AUTH_ENABLED=false CONTROL_PLANE_BEARER_TOKEN= CONTROL_PLANE_PREVIOUS_BEARER_TOKEN= CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS= CONTROL_PLANE_OPERATOR_ID=operator CONTROL_PLANE_PREVIOUS_OPERATOR_ID= \
    CONTROL_PLANE_AUDIT_ENABLED=true CONTROL_PLANE_AUDIT_RETENTION_ENABLED=true CONTROL_PLANE_SIEM_DELIVERY_ENABLED=false CONTROL_PLANE_SIEM_SIGNING_PROVIDER=hmac \
    CONTROL_PLANE_SIEM_FAILOVER_ENABLED=false CONTROL_PLANE_SIEM_SHADOW_SECONDARY=false CONTROL_PLANE_SIEM_CLUSTER_GUARD_ENABLED=false CONTROL_PLANE_SIEM_TERM_FENCING_ENABLED=false \
    CONTROL_PLANE_SIEM_ANTI_ENTROPY_ENABLED=false CONTROL_PLANE_SIEM_PROMOTION_WORKFLOW_ENABLED=false CONTROL_PLANE_SIEM_BREAK_GLASS_ENABLED=false CONTROL_PLANE_SIEM_GOVERNANCE_ENDPOINT= \
    CONTROL_PLANE_SIEM_GOVERNANCE_HA_ENABLED=false CONTROL_PLANE_SIEM_GOVERNANCE_RECEIPT_QUORUM=1 CONTROL_PLANE_SIEM_GOVERNANCE_DEBT_POLICY_ENABLED=false CONTROL_PLANE_SIEM_GOVERNANCE_REMEDIATION_APPROVAL_REQUIRED=false \
    CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED=false CONTROL_PLANE_SIEM_RECONCILE_ENABLED=false CONTROL_PLANE_SIEM_DR_ENABLED=false MYSQL_DSN="$DEFAULT_DSN" REDIS_QUEUE_KEY=taskflow:queue \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null 2>&1 || true
  docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}" >/dev/null 2>&1 || true
  docker compose --profile phase38-smoke rm -sf siem-sink siem-sink-secondary siem-governance siem-governance-secondary >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT

phase="build Phase 38 image and static guardrails"
docker build --target build -t taskflow-go:phase38-build --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
docker build -t taskflow-go:local --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
python3 scripts/phase38-governance-ha-check.py

phase="start isolated SIEM receivers and signed governance replicas"
docker compose --profile phase38-smoke rm -sf siem-sink siem-sink-secondary siem-governance siem-governance-secondary >/dev/null 2>&1 || true
CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
CONTROL_PLANE_SIEM_GOVERNANCE_BEARER_TOKEN="$GOV_PRIMARY_TOKEN" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_BEARER_TOKEN="$GOV_SECONDARY_TOKEN" \
CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY="$GOV_SIGN_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY_ID="$GOV_SIGN_KEY_ID" \
CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_REPLICA_ID="$GOV_PRIMARY_REPLICA" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_RECEIPT_HMAC_KEY="$GOV_PRIMARY_RECEIPT_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_RECEIPT_HMAC_KEY_ID="$GOV_PRIMARY_RECEIPT_KEY_ID" \
CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_REPLICA_ID="$GOV_SECONDARY_REPLICA" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_HMAC_KEY="$GOV_SECONDARY_RECEIPT_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_HMAC_KEY_ID="$GOV_SECONDARY_RECEIPT_KEY_ID" \
PHASE38_GOVERNANCE_SECONDARY_FAULT_TERM=3 PHASE38_GOVERNANCE_SECONDARY_FAULT_RECORD_HASH="$FAULT_HASH" \
PHASE29_PRIMARY_RECEIVER_ID="$PRIMARY_ID" PHASE29_SECONDARY_RECEIVER_ID="$SECONDARY_ID" PHASE30_ACTIVE_RECEIVER="$PRIMARY_ID" \
  docker compose up -d mysql redis executor-worker >/dev/null
CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
CONTROL_PLANE_SIEM_GOVERNANCE_BEARER_TOKEN="$GOV_PRIMARY_TOKEN" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_BEARER_TOKEN="$GOV_SECONDARY_TOKEN" \
CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY="$GOV_SIGN_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY_ID="$GOV_SIGN_KEY_ID" \
CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_REPLICA_ID="$GOV_PRIMARY_REPLICA" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_RECEIPT_HMAC_KEY="$GOV_PRIMARY_RECEIPT_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_RECEIPT_HMAC_KEY_ID="$GOV_PRIMARY_RECEIPT_KEY_ID" \
CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_REPLICA_ID="$GOV_SECONDARY_REPLICA" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_HMAC_KEY="$GOV_SECONDARY_RECEIPT_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_HMAC_KEY_ID="$GOV_SECONDARY_RECEIPT_KEY_ID" \
PHASE38_GOVERNANCE_SECONDARY_FAULT_TERM=3 PHASE38_GOVERNANCE_SECONDARY_FAULT_RECORD_HASH="$FAULT_HASH" \
PHASE29_PRIMARY_RECEIVER_ID="$PRIMARY_ID" PHASE29_SECONDARY_RECEIVER_ID="$SECONDARY_ID" PHASE30_ACTIVE_RECEIVER="$PRIMARY_ID" \
  docker compose --profile phase38-smoke up -d --force-recreate siem-sink siem-sink-secondary siem-governance siem-governance-secondary >/dev/null
wait_http "$PRIMARY_URL/health"; wait_http "$SECONDARY_URL/health"; wait_http "$GOV_PRIMARY_URL/health"; wait_http "$GOV_SECONDARY_URL/health"
wait_mysql

phase="create isolated database and establish healthy 2-of-2 term 1 quorum"
docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}; CREATE DATABASE ${TEST_DB}; GRANT ALL PRIVILEGES ON ${TEST_DB}.* TO 'taskflow'@'%'; FLUSH PRIVILEGES" >/dev/null
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null
start_taskflow
wait_http "$BASE_URL/health/ready"
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_siem_term_ledger_anchors WHERE term=1"
term1_id="$(sql "SELECT id FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=1")"
wait_sql "SELECT IF(COUNT(*)=2 AND COUNT(DISTINCT replica_id)=2 AND COUNT(DISTINCT receipt_hash)=1,1,0) FROM ${TEST_DB}.control_plane_siem_governance_replica_receipts WHERE kind='anchor' AND target_id=${term1_id}"
governance_status >/tmp/phase38-status-term1.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase38-status-term1.json'))
assert d['ha_enabled'] is True and d['replica_count']==2 and d['replica_quorum']==2, d
assert d['fenced'] is False and d['replica_receipt_debt']==0, d
assert d['verified_replica_receipts'] >= 2, d
PY

phase="pause primary governance and prove durable secondary receipt plus immediate replica quorum incomplete fence"
PRIMARY_GOV_CONTAINER="$(docker compose ps -q siem-governance)"; [ -n "$PRIMARY_GOV_CONTAINER" ]
docker pause "$PRIMARY_GOV_CONTAINER" >/dev/null
curl -fsS --max-time 15 -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"target_receiver\":\"${SECONDARY_ID}\",\"expected_term\":1,\"incident_id\":\"INC-38-OUTAGE\",\"reason\":\"prove signed governance replica outage recovery\"}" \
  "$BASE_URL/api/v1/control-plane/siem/receivers/break-glass" >/tmp/phase38-break.json
term2_id="$(sql "SELECT id FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=2")"
[ -n "$term2_id" ]
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_siem_governance_replica_receipts WHERE kind='anchor' AND target_id=${term2_id} AND replica_id='${GOV_SECONDARY_REPLICA}'"
[ "$(sql "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_siem_governance_replica_receipts WHERE kind='anchor' AND target_id=${term2_id} AND replica_id='${GOV_PRIMARY_REPLICA}'")" = "0" ]
governance_status >/tmp/phase38-status-outage.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase38-status-outage.json'))
assert d['fenced'] is True and d['replica_receipt_debt'] >= 1, d
assert 'replica quorum incomplete' in d['fence_reason'], d
PY
curl -fsS -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/api/v1/control-plane/siem/governance/replica-receipts?kind=anchor&target_id=${term2_id}" >/tmp/phase38-partial-receipts.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase38-partial-receipts.json'))['items']
assert len(d)==1 and d[0]['replica_id']=='governance-secondary', d
assert d[0]['receipt_signature'].startswith('sha256=') and len(d[0]['receipt_hash'])==64, d
PY

phase="restore primary governance and reconcile the same immutable term to signed 2-of-2 quorum"
docker unpause "$PRIMARY_GOV_CONTAINER" >/dev/null
PRIMARY_GOV_CONTAINER=""
wait_http "$GOV_PRIMARY_URL/health"
wait_sql "SELECT IF(anchor_state='delivered',1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=2"
wait_sql "SELECT IF(COUNT(*)=2 AND COUNT(DISTINCT replica_id)=2 AND COUNT(DISTINCT receipt_hash)=1,1,0) FROM ${TEST_DB}.control_plane_siem_governance_replica_receipts WHERE kind='anchor' AND target_id=${term2_id}"
wait_sql "SELECT IF(COUNT(*)=0,1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE anchor_state <> 'delivered'"
wait_sql "SELECT IF(COUNT(*)=0,1,0) FROM ${TEST_DB}.control_plane_siem_incident_evidence WHERE alert_state <> 'delivered'"
governance_status >/tmp/phase38-status-recovered.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase38-status-recovered.json'))
assert d['fenced'] is False and d['dead']==0 and d['replica_receipt_debt']==0, d
PY
curl -fsS -H "Authorization: Bearer $GOV_PRIMARY_TOKEN" "$GOV_PRIMARY_URL/anchors" >/tmp/phase38-primary-anchors.json
curl -fsS -H "Authorization: Bearer $GOV_SECONDARY_TOKEN" "$GOV_SECONDARY_URL/anchors" >/tmp/phase38-secondary-anchors.json
python3 - <<'PY'
import json
p={x['term']:x for x in json.load(open('/tmp/phase38-primary-anchors.json'))['items']}
s={x['term']:x for x in json.load(open('/tmp/phase38-secondary-anchors.json'))['items']}
assert p[2]['record_hash']==s[2]['record_hash'] and p[2]['receipt_hash']==s[2]['receipt_hash'], (p[2],s[2])
assert p[2]['replica_id']=='governance-primary' and s[2]['replica_id']=='governance-secondary'
assert p[2]['receipt_signature'].startswith('sha256=') and s[2]['receipt_signature'].startswith('sha256=')
PY

phase="promote normally to term 3 and prove signed replica divergence becomes permanent dead evidence"
wait_sql "SELECT IF(COUNT(*)=0,1,0) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE state <> 'delivered'"
curl -fsS -X POST -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/sync" >/dev/null
wait_receivers_stable_equal
curl -fsS -X POST -H "Authorization: Bearer $REQUESTER_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"target_receiver\":\"${PRIMARY_ID}\",\"expected_term\":2,\"reason\":\"create term three for signed replica divergence\"}" \
  "$BASE_URL/api/v1/control-plane/siem/receivers/intents" >/tmp/phase38-intent3.json
intent3_id="$(python3 -c 'import json; print(json.load(open("/tmp/phase38-intent3.json"))["id"])')"
curl -fsS -X POST -H "Authorization: Bearer $APPROVER_TOKEN" -H 'Content-Type: application/json' -d '{"note":"approve term three consistency test"}' \
  "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent3_id}/approve" >/dev/null
wait_sql "SELECT IF(COUNT(*)=0,1,0) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE state <> 'delivered'"
curl -fsS -X POST -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/sync" >/dev/null
wait_receivers_stable_equal
curl -fsS -X POST -H "Authorization: Bearer $APPROVER_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent3_id}/execute" >/tmp/phase38-execute3.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase38-execute3.json'))
assert d['active_receiver']=='receiver-primary' and d['term']==3, d
PY
term3_id="$(sql "SELECT id FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=3")"
wait_sql "SELECT IF(anchor_state='dead' AND (anchor_last_error LIKE '%replica consistency failure%' OR anchor_last_error LIKE '%HTTP 409%'),1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE id=${term3_id}"
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_siem_governance_replica_receipts WHERE kind='anchor' AND target_id=${term3_id} AND replica_id='${GOV_PRIMARY_REPLICA}'"
[ "$(sql "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_siem_governance_replica_receipts WHERE kind='anchor' AND target_id=${term3_id} AND replica_id='${GOV_SECONDARY_REPLICA}'")" = "0" ]
curl -fsS -H "Authorization: Bearer $GOV_SECONDARY_TOKEN" "$GOV_SECONDARY_URL/anchors" >/tmp/phase38-divergent-secondary.json
python3 - <<'PY'
import json
items={x['term']:x for x in json.load(open('/tmp/phase38-divergent-secondary.json'))['items']}
x=items[3]
assert x['record_hash']=='f'*64, x
assert x['replica_id']=='governance-secondary' and x['receipt_signature'].startswith('sha256='), x
PY
governance_status >/tmp/phase38-status-conflict.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase38-status-conflict.json'))
assert d['fenced'] is True and d['dead'] >= 1, d
assert 'dead governance evidence' in d['fence_reason'], d
PY

phase="prove ordinary promotion remains fenced after signed governance equivocation"
curl -fsS -X POST -H "Authorization: Bearer $REQUESTER_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"target_receiver\":\"${SECONDARY_ID}\",\"expected_term\":3,\"reason\":\"must remain fenced after governance equivocation\"}" \
  "$BASE_URL/api/v1/control-plane/siem/receivers/intents" >/tmp/phase38-intent4.json
intent4_id="$(python3 -c 'import json; print(json.load(open("/tmp/phase38-intent4.json"))["id"])')"
curl -fsS -X POST -H "Authorization: Bearer $APPROVER_TOKEN" -H 'Content-Type: application/json' -d '{"note":"approval cannot bypass governance fence"}' \
  "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent4_id}/approve" >/dev/null
fenced_code="$(curl -sS -o /tmp/phase38-fenced-execute.json -w '%{http_code}' -X POST -H "Authorization: Bearer $APPROVER_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent4_id}/execute")"
[ "$fenced_code" = "409" ]
grep -q 'dead governance evidence' /tmp/phase38-fenced-execute.json

phase="verify replica quorum observability"
curl -fsS -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/metrics" >/tmp/phase38-metrics.txt
grep -q 'taskflow_control_plane_siem_governance_quorum_total{class="anchor",outcome="incomplete"}' /tmp/phase38-metrics.txt
grep -q 'taskflow_control_plane_siem_governance_quorum_total{class="anchor",outcome="satisfied"}' /tmp/phase38-metrics.txt
grep -q 'taskflow_control_plane_siem_governance_quorum_total{class="anchor",outcome="conflict"}' /tmp/phase38-metrics.txt
grep -q 'taskflow_control_plane_siem_governance_replica_receipts_total{class="anchor",outcome="verified",replica="governance-primary"}' /tmp/phase38-metrics.txt
grep -q 'taskflow_control_plane_siem_governance_replica_receipts_total{class="anchor",outcome="verified",replica="governance-secondary"}' /tmp/phase38-metrics.txt
grep -q 'taskflow_control_plane_siem_governance_replica_receipt_debt 0' /tmp/phase38-metrics.txt

echo "Phase 38 signed governance replica outage produced durable partial receipt and replica quorum incomplete fence: ok"
echo "Phase 38 restored primary reconciled the immutable term to verified 2-of-2 signed receipt quorum: ok"
echo "Phase 38 signed replica divergence was detected as permanent dead governance evidence and fenced ordinary promotion: ok"
