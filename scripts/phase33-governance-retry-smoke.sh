#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
PRIMARY_URL="${PHASE33_PRIMARY_URL:-http://127.0.0.1:18084}"
SECONDARY_URL="${PHASE33_SECONDARY_URL:-http://127.0.0.1:18085}"
GOV_URL="${PHASE33_GOVERNANCE_URL:-http://127.0.0.1:19704}"

COMMANDER="phase33-commander"
COMMANDER_TOKEN="${PHASE33_COMMANDER_TOKEN:-phase33-commander-token}"
REQUESTER="phase33-requester"
REQUESTER_TOKEN="${PHASE33_REQUESTER_TOKEN:-phase33-requester-token}"
ROLE_POLICY="${REQUESTER}=requester|approver;${COMMANDER}=incident-commander"

SINK_TOKEN="${PHASE33_SINK_TOKEN:-phase33-sink-token}"
SIGN_KEY="${PHASE33_SIGNING_KEY:-phase33-signing-key}"
SIGN_KEY_ID="${PHASE33_SIGNING_KEY_ID:-phase33-sign-v1}"
CHECKPOINT_KEY="${PHASE33_CHECKPOINT_KEY:-phase33-checkpoint-secret}"
CHECKPOINT_KEY_ID="${PHASE33_CHECKPOINT_KEY_ID:-phase33-checkpoint-v1}"
GOV_TOKEN="${PHASE33_GOVERNANCE_TOKEN:-phase33-governance-token}"
GOV_SIGN_KEY="${PHASE33_GOVERNANCE_SIGNING_KEY:-phase33-governance-signing-key}"
GOV_SIGN_KEY_ID="${PHASE33_GOVERNANCE_SIGNING_KEY_ID:-phase33-governance-v1}"

PRIMARY_ID="receiver-primary"
SECONDARY_ID="receiver-secondary"
TEST_DB="${PHASE33_DB:-taskflow_phase33}"
TEST_DSN="taskflow:taskflow@tcp(mysql:3306)/${TEST_DB}?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
DEFAULT_DSN="taskflow:taskflow@tcp(mysql:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
QUEUE_KEY="${PHASE33_QUEUE_KEY:-taskflow:phase33:queue}"
GOV_CONTAINER=""
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

sql() {
  docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse "$1" 2>/dev/null | tr -d '\r'
}

wait_sql() {
  query="$1"; i=0; value=""
  while [ "$i" -lt 300 ]; do
    value="$(sql "$query" || true)"
    if [ "$value" = "1" ]; then return 0; fi
    i=$((i + 1)); sleep 0.2
  done
  echo "wait_sql failed value=$value query=$query" >&2
  return 1
}

gov_json() {
  curl -fsS -H "Authorization: Bearer $GOV_TOKEN" "$GOV_URL$1"
}

start_taskflow() {
  CONTROL_PLANE_AUTH_ENABLED=true \
    CONTROL_PLANE_BEARER_TOKEN="$COMMANDER_TOKEN" \
    CONTROL_PLANE_PREVIOUS_BEARER_TOKEN="$REQUESTER_TOKEN" \
    CONTROL_PLANE_OPERATOR_ID="$COMMANDER" \
    CONTROL_PLANE_PREVIOUS_OPERATOR_ID="$REQUESTER" \
    CONTROL_PLANE_AUDIT_ENABLED=true CONTROL_PLANE_AUDIT_RETENTION_ENABLED=false \
    CONTROL_PLANE_SIEM_DELIVERY_ENABLED=true \
    CONTROL_PLANE_SIEM_ENDPOINT=http://siem-sink:8080/events \
    CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
    CONTROL_PLANE_SIEM_SIGNING_PROVIDER=hmac \
    CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" \
    CONTROL_PLANE_SIEM_DELIVERY_INTERVAL_MS=100 CONTROL_PLANE_SIEM_DELIVERY_LEASE_MS=1500 \
    CONTROL_PLANE_SIEM_MAX_ATTEMPTS=5 CONTROL_PLANE_SIEM_RETRY_BASE_MS=100 CONTROL_PLANE_SIEM_RETRY_MAX_MS=300 \
    CONTROL_PLANE_SIEM_DISTRIBUTED_COORDINATION=false CONTROL_PLANE_SIEM_GC_ENABLED=false CONTROL_PLANE_SIEM_DR_ENABLED=false \
    CONTROL_PLANE_SIEM_PRIMARY_RECEIVER_ID="$PRIMARY_ID" \
    CONTROL_PLANE_SIEM_PRIMARY_CHECKPOINT_ENDPOINT=http://siem-sink:8080/checkpoint \
    CONTROL_PLANE_SIEM_PRIMARY_TERM_ENDPOINT=http://siem-sink:8080/control/term \
    CONTROL_PLANE_SIEM_SECONDARY_ENDPOINT=http://siem-sink-secondary:8080/events \
    CONTROL_PLANE_SIEM_SECONDARY_RECEIVER_ID="$SECONDARY_ID" \
    CONTROL_PLANE_SIEM_SECONDARY_CHECKPOINT_ENDPOINT=http://siem-sink-secondary:8080/checkpoint \
    CONTROL_PLANE_SIEM_SECONDARY_TERM_ENDPOINT=http://siem-sink-secondary:8080/control/term \
    CONTROL_PLANE_SIEM_FAILOVER_ENABLED=false CONTROL_PLANE_SIEM_SHADOW_SECONDARY=false \
    CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
    CONTROL_PLANE_SIEM_CLUSTER_GUARD_ENABLED=true CONTROL_PLANE_SIEM_TERM_FENCING_ENABLED=true \
    CONTROL_PLANE_SIEM_ANTI_ENTROPY_ENABLED=true CONTROL_PLANE_SIEM_ANTI_ENTROPY_INTERVAL_MS=100 \
    CONTROL_PLANE_SIEM_PROMOTION_WORKFLOW_ENABLED=true CONTROL_PLANE_SIEM_PROMOTION_INTENT_TTL_MS=60000 CONTROL_PLANE_SIEM_PROMOTION_LEASE_MS=5000 \
    CONTROL_PLANE_SIEM_PROMOTION_APPROVAL_QUORUM=1 CONTROL_PLANE_SIEM_PROMOTION_ROLE_POLICY="$ROLE_POLICY" \
    CONTROL_PLANE_SIEM_BREAK_GLASS_ENABLED=true \
    CONTROL_PLANE_SIEM_GOVERNANCE_ENDPOINT=http://siem-governance:8080 \
    CONTROL_PLANE_SIEM_GOVERNANCE_BEARER_TOKEN="$GOV_TOKEN" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY="$GOV_SIGN_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY_ID="$GOV_SIGN_KEY_ID" \
    CONTROL_PLANE_SIEM_GOVERNANCE_TIMEOUT_MS=500 CONTROL_PLANE_SIEM_RTO_ALERT_THRESHOLD_MS=30000 \
    CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED=false \
    MYSQL_DSN="$TEST_DSN" REDIS_QUEUE_KEY="$QUEUE_KEY" \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null
}

cleanup() {
  rc=$?
  if [ -n "$GOV_CONTAINER" ]; then
    docker unpause "$GOV_CONTAINER" >/dev/null 2>&1 || true
  fi
  if [ "$rc" -ne 0 ]; then
    echo "Phase 33 governance retry smoke failed during phase: $phase" >&2
    docker compose --profile phase33-smoke logs --tail 400 taskflow siem-sink siem-sink-secondary siem-governance >&2 2>/dev/null || true
  fi
  CONTROL_PLANE_AUTH_ENABLED=false CONTROL_PLANE_BEARER_TOKEN= CONTROL_PLANE_PREVIOUS_BEARER_TOKEN= CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS= CONTROL_PLANE_OPERATOR_ID=operator CONTROL_PLANE_PREVIOUS_OPERATOR_ID= \
    CONTROL_PLANE_AUDIT_ENABLED=true CONTROL_PLANE_AUDIT_RETENTION_ENABLED=true CONTROL_PLANE_SIEM_DELIVERY_ENABLED=false CONTROL_PLANE_SIEM_SIGNING_PROVIDER=hmac \
    CONTROL_PLANE_SIEM_FAILOVER_ENABLED=false CONTROL_PLANE_SIEM_SHADOW_SECONDARY=false CONTROL_PLANE_SIEM_CLUSTER_GUARD_ENABLED=false CONTROL_PLANE_SIEM_TERM_FENCING_ENABLED=false \
    CONTROL_PLANE_SIEM_ANTI_ENTROPY_ENABLED=false CONTROL_PLANE_SIEM_PROMOTION_WORKFLOW_ENABLED=false CONTROL_PLANE_SIEM_BREAK_GLASS_ENABLED=false CONTROL_PLANE_SIEM_GOVERNANCE_ENDPOINT= \
    CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED=false CONTROL_PLANE_SIEM_RECONCILE_ENABLED=false CONTROL_PLANE_SIEM_DR_ENABLED=false \
    MYSQL_DSN="$DEFAULT_DSN" REDIS_QUEUE_KEY=taskflow:queue docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null 2>&1 || true
  docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}" >/dev/null 2>&1 || true
  docker compose --profile phase33-smoke rm -sf siem-sink siem-sink-secondary siem-governance >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT

phase="build Phase 33 image and static guardrails"
docker build --target build -t taskflow-go:phase33-build --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
docker build -t taskflow-go:local --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
python3 scripts/phase33-governance-retry-check.py

phase="start receiver pair and governance service"
CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
CONTROL_PLANE_SIEM_GOVERNANCE_BEARER_TOKEN="$GOV_TOKEN" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY="$GOV_SIGN_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY_ID="$GOV_SIGN_KEY_ID" \
PHASE29_PRIMARY_RECEIVER_ID="$PRIMARY_ID" PHASE29_SECONDARY_RECEIVER_ID="$SECONDARY_ID" PHASE30_ACTIVE_RECEIVER="$PRIMARY_ID" \
  docker compose --profile phase33-smoke up -d mysql redis executor-worker siem-sink siem-sink-secondary siem-governance >/dev/null
wait_http "$PRIMARY_URL/health"; wait_http "$SECONDARY_URL/health"; wait_http "$GOV_URL/health"

phase="create isolated Phase 33 database and establish initial anchor"
docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}; CREATE DATABASE ${TEST_DB}; GRANT ALL PRIVILEGES ON ${TEST_DB}.* TO 'taskflow'@'%'; FLUSH PRIVILEGES" >/dev/null
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null
start_taskflow
wait_http "$BASE_URL/health/ready"
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_siem_term_ledger_anchors WHERE term=1"
wait_sql "SELECT IF(anchor_state='delivered',1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=1"

phase="pause governance and prove break-glass survives external outage"
GOV_CONTAINER="$(docker compose ps -q siem-governance)"
[ -n "$GOV_CONTAINER" ]
docker pause "$GOV_CONTAINER" >/dev/null
curl -fsS --max-time 12 -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"target_receiver\":\"${SECONDARY_ID}\",\"expected_term\":1,\"incident_id\":\"INC-33-001\",\"reason\":\"governance outage recovery proof\"}" \
  "$BASE_URL/api/v1/control-plane/siem/receivers/break-glass" >/tmp/phase33-break.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase33-break.json'))
assert d['active_receiver']=='receiver-secondary' and d['term']==2, d
assert d['emergency'] is True and d['incident_id']=='INC-33-001', d
assert d.get('anchored', False) is False, d
PY
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=2 AND emergency=1 AND incident_id='INC-33-001'"
wait_sql "SELECT IF(anchor_state IN ('pending','delivering') AND anchor_attempts>=1,1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=2"
wait_sql "SELECT IF(alert_state IN ('pending','delivering') AND alert_attempts>=1,1,0) FROM ${TEST_DB}.control_plane_siem_incident_evidence WHERE incident_id='INC-33-001' AND kind='break_glass'"
[ "$(sql "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_siem_term_ledger_anchors WHERE term=2")" = "0" ]

phase="restore governance and prove automatic durable reconciliation"
docker unpause "$GOV_CONTAINER" >/dev/null
wait_http "$GOV_URL/health"
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_siem_term_ledger_anchors WHERE term=2"
wait_sql "SELECT IF(anchor_state='delivered' AND anchor_attempts>=1 AND anchor_last_error='',1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=2"
wait_sql "SELECT IF(alert_state='delivered' AND alert_attempts>=1 AND alert_last_error='' AND alerted_at IS NOT NULL,1,0) FROM ${TEST_DB}.control_plane_siem_incident_evidence WHERE incident_id='INC-33-001' AND kind='break_glass'"
gov_json /stats >/tmp/phase33-gov-before-restart.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase33-gov-before-restart.json'))
assert d['anchors']==2, d
assert d['alerts']>=1 and d['signature_failures']==0 and d['auth_failures']==0, d
PY

phase="restart TaskFlow and prove delivered governance work is not replayed"
start_taskflow
wait_http "$BASE_URL/health/ready"
sleep 1
gov_json /stats >/tmp/phase33-gov-after-restart.json
python3 - <<'PY'
import json
before=json.load(open('/tmp/phase33-gov-before-restart.json'))
after=json.load(open('/tmp/phase33-gov-after-restart.json'))
assert after['anchor_requests']==before['anchor_requests'], (before, after)
assert after['alert_requests']==before['alert_requests'], (before, after)
assert after['anchors']==2 and after['alerts']==before['alerts'], (before, after)
PY

echo "Phase 33 governance outage did not block break-glass receiver promotion: ok"
echo "Phase 33 term anchors and incident alerts were durably retried and reconciled: ok"
echo "Phase 33 restart preserved delivered state without duplicate governance replay: ok"
