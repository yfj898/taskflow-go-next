#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
PRIMARY_URL="${PHASE34_PRIMARY_URL:-http://127.0.0.1:18084}"
SECONDARY_URL="${PHASE34_SECONDARY_URL:-http://127.0.0.1:18085}"
GOV_URL="${PHASE34_GOVERNANCE_URL:-http://127.0.0.1:19704}"

COMMANDER="phase34-commander"
COMMANDER_TOKEN="${PHASE34_COMMANDER_TOKEN:-phase34-commander-token}"
REQUESTER="phase34-requester"
REQUESTER_TOKEN="${PHASE34_REQUESTER_TOKEN:-phase34-requester-token}"
APPROVER="phase34-approver"
APPROVER_TOKEN="${PHASE34_APPROVER_TOKEN:-phase34-approver-token}"
ADDITIONAL_CREDENTIALS="${APPROVER}=${APPROVER_TOKEN}"
ROLE_POLICY="${REQUESTER}=requester;${APPROVER}=approver;${COMMANDER}=incident-commander"

SINK_TOKEN="${PHASE34_SINK_TOKEN:-phase34-sink-token}"
SIGN_KEY="${PHASE34_SIGNING_KEY:-phase34-signing-key}"
SIGN_KEY_ID="${PHASE34_SIGNING_KEY_ID:-phase34-sign-v1}"
CHECKPOINT_KEY="${PHASE34_CHECKPOINT_KEY:-phase34-checkpoint-secret}"
CHECKPOINT_KEY_ID="${PHASE34_CHECKPOINT_KEY_ID:-phase34-checkpoint-v1}"
GOV_TOKEN="${PHASE34_GOVERNANCE_TOKEN:-phase34-governance-token}"
GOV_SIGN_KEY="${PHASE34_GOVERNANCE_SIGNING_KEY:-phase34-governance-signing-key}"
GOV_SIGN_KEY_ID="${PHASE34_GOVERNANCE_SIGNING_KEY_ID:-phase34-governance-v1}"

PRIMARY_ID="receiver-primary"
SECONDARY_ID="receiver-secondary"
TEST_DB="${PHASE34_DB:-taskflow_phase34}"
TEST_DSN="taskflow:taskflow@tcp(mysql:3306)/${TEST_DB}?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
DEFAULT_DSN="taskflow:taskflow@tcp(mysql:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
QUEUE_KEY="${PHASE34_QUEUE_KEY:-taskflow:phase34:queue}"
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

governance_status() {
  curl -fsS -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/api/v1/control-plane/siem/governance/status"
}

wait_governance_fence() {
  expected="$1"; i=0
  while [ "$i" -lt 150 ]; do
    if governance_status >/tmp/phase34-governance-status.json 2>/dev/null; then
      got="$(python3 -c 'import json; print(str(json.load(open("/tmp/phase34-governance-status.json"))["fenced"]).lower())')"
      if [ "$got" = "$expected" ]; then return 0; fi
    fi
    i=$((i + 1)); sleep 0.2
  done
  echo "governance fence did not reach expected=$expected" >&2
  cat /tmp/phase34-governance-status.json >&2 2>/dev/null || true
  return 1
}

start_taskflow() {
  CONTROL_PLANE_AUTH_ENABLED=true \
    CONTROL_PLANE_BEARER_TOKEN="$COMMANDER_TOKEN" \
    CONTROL_PLANE_PREVIOUS_BEARER_TOKEN="$REQUESTER_TOKEN" \
    CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS="$ADDITIONAL_CREDENTIALS" \
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
    CONTROL_PLANE_SIEM_GOVERNANCE_TIMEOUT_MS=300 \
    CONTROL_PLANE_SIEM_GOVERNANCE_DEBT_POLICY_ENABLED=true CONTROL_PLANE_SIEM_GOVERNANCE_MAX_OUTSTANDING=100 CONTROL_PLANE_SIEM_GOVERNANCE_MAX_AGE_MS=500 \
    CONTROL_PLANE_SIEM_RTO_ALERT_THRESHOLD_MS=30000 CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED=false \
    MYSQL_DSN="$TEST_DSN" REDIS_QUEUE_KEY="$QUEUE_KEY" \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null
}

cleanup() {
  rc=$?
  if [ -n "$GOV_CONTAINER" ]; then
    docker unpause "$GOV_CONTAINER" >/dev/null 2>&1 || true
  fi
  if [ "$rc" -ne 0 ]; then
    echo "Phase 34 governance debt smoke failed during phase: $phase" >&2
    docker compose --profile phase34-smoke logs --tail 400 taskflow siem-sink siem-sink-secondary siem-governance >&2 2>/dev/null || true
  fi
  CONTROL_PLANE_AUTH_ENABLED=false CONTROL_PLANE_BEARER_TOKEN= CONTROL_PLANE_PREVIOUS_BEARER_TOKEN= CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS= CONTROL_PLANE_OPERATOR_ID=operator CONTROL_PLANE_PREVIOUS_OPERATOR_ID= \
    CONTROL_PLANE_AUDIT_ENABLED=true CONTROL_PLANE_AUDIT_RETENTION_ENABLED=true CONTROL_PLANE_SIEM_DELIVERY_ENABLED=false CONTROL_PLANE_SIEM_SIGNING_PROVIDER=hmac \
    CONTROL_PLANE_SIEM_FAILOVER_ENABLED=false CONTROL_PLANE_SIEM_SHADOW_SECONDARY=false CONTROL_PLANE_SIEM_CLUSTER_GUARD_ENABLED=false CONTROL_PLANE_SIEM_TERM_FENCING_ENABLED=false \
    CONTROL_PLANE_SIEM_ANTI_ENTROPY_ENABLED=false CONTROL_PLANE_SIEM_PROMOTION_WORKFLOW_ENABLED=false CONTROL_PLANE_SIEM_BREAK_GLASS_ENABLED=false CONTROL_PLANE_SIEM_GOVERNANCE_ENDPOINT= \
    CONTROL_PLANE_SIEM_GOVERNANCE_DEBT_POLICY_ENABLED=false CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED=false CONTROL_PLANE_SIEM_RECONCILE_ENABLED=false CONTROL_PLANE_SIEM_DR_ENABLED=false \
    MYSQL_DSN="$DEFAULT_DSN" REDIS_QUEUE_KEY=taskflow:queue docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null 2>&1 || true
  docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}" >/dev/null 2>&1 || true
  docker compose --profile phase34-smoke rm -sf siem-sink siem-sink-secondary siem-governance >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT

phase="build Phase 34 image and static guardrails"
docker build --target build -t taskflow-go:phase34-build --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
docker build -t taskflow-go:local --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
python3 scripts/phase34-governance-debt-check.py

phase="start receiver pair and independent governance service"
CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
CONTROL_PLANE_SIEM_GOVERNANCE_BEARER_TOKEN="$GOV_TOKEN" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY="$GOV_SIGN_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY_ID="$GOV_SIGN_KEY_ID" \
PHASE29_PRIMARY_RECEIVER_ID="$PRIMARY_ID" PHASE29_SECONDARY_RECEIVER_ID="$SECONDARY_ID" PHASE30_ACTIVE_RECEIVER="$PRIMARY_ID" \
  docker compose --profile phase34-smoke up -d mysql redis executor-worker siem-sink siem-sink-secondary siem-governance >/dev/null
wait_http "$PRIMARY_URL/health"; wait_http "$SECONDARY_URL/health"; wait_http "$GOV_URL/health"

phase="create isolated Phase 34 database and verify healthy governance debt state"
docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}; CREATE DATABASE ${TEST_DB}; GRANT ALL PRIVILEGES ON ${TEST_DB}.* TO 'taskflow'@'%'; FLUSH PRIVILEGES" >/dev/null
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null
start_taskflow
wait_http "$BASE_URL/health/ready"
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_siem_term_ledger_anchors WHERE term=1"
governance_status >/tmp/phase34-governance-healthy.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase34-governance-healthy.json'))
assert d['enabled'] is True and d['policy_enabled'] is True, d
assert d['fenced'] is False and d['outstanding']==0 and d['dead']==0, d
assert d['max_outstanding']==100 and d['max_age_seconds']==0.5, d
PY

phase="pause governance, create durable debt and wait for age-based safety fence"
GOV_CONTAINER="$(docker compose ps -q siem-governance)"
[ -n "$GOV_CONTAINER" ]
docker pause "$GOV_CONTAINER" >/dev/null
curl -fsS --max-time 12 -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"target_receiver\":\"${SECONDARY_ID}\",\"expected_term\":1,\"incident_id\":\"INC-34-001\",\"reason\":\"create governance debt\"}" \
  "$BASE_URL/api/v1/control-plane/siem/receivers/break-glass" >/tmp/phase34-break-one.json
wait_sql "SELECT IF(anchor_state IN ('pending','delivering') AND anchor_attempts>=1,1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=2"
wait_sql "SELECT IF(alert_state IN ('pending','delivering') AND alert_attempts>=1,1,0) FROM ${TEST_DB}.control_plane_siem_incident_evidence WHERE incident_id='INC-34-001' AND kind='break_glass'"
wait_governance_fence true
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase34-governance-status.json'))
assert d['fenced'] is True and d['outstanding'] >= 2 and d['dead']==0, d
assert d['oldest_outstanding_seconds'] >= 0.5, d
assert 'oldest governance debt' in d['fence_reason'], d
assert d['anchors']['outstanding'] >= 1 and d['alerts']['outstanding'] >= 1, d
PY
curl -fsS -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/metrics" >/tmp/phase34-metrics.txt
grep -q 'taskflow_control_plane_siem_governance_debt_fenced 1' /tmp/phase34-metrics.txt
grep -q 'taskflow_control_plane_siem_governance_backlog{class="anchor",state="pending"}' /tmp/phase34-metrics.txt

phase="prove ordinary approved promotion is fenced by governance debt policy"
curl -fsS -X POST -H "Authorization: Bearer $REQUESTER_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"target_receiver\":\"${PRIMARY_ID}\",\"expected_term\":2,\"reason\":\"must be governance fenced\"}" \
  "$BASE_URL/api/v1/control-plane/siem/receivers/intents" >/tmp/phase34-intent.json
intent_id="$(python3 -c 'import json; print(json.load(open("/tmp/phase34-intent.json"))["id"])')"
curl -fsS -X POST -H "Authorization: Bearer $APPROVER_TOKEN" -H 'Content-Type: application/json' -d '{"note":"approved but debt fenced"}' \
  "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_id}/approve" >/tmp/phase34-approve.json
execute_code="$(curl -sS -o /tmp/phase34-execute-fenced.json -w '%{http_code}' -X POST -H "Authorization: Bearer $APPROVER_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_id}/execute")"
[ "$execute_code" = "409" ]
grep -qi 'governance debt policy' /tmp/phase34-execute-fenced.json
wait_sql "SELECT IF(term=2 AND active_receiver='${SECONDARY_ID}',1,0) FROM ${TEST_DB}.control_plane_siem_receiver_routing_state WHERE id=1"

phase="prove break-glass bypasses governance debt fence and remains auditable"
curl -fsS --max-time 12 -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"target_receiver\":\"${PRIMARY_ID}\",\"expected_term\":2,\"incident_id\":\"INC-34-002\",\"reason\":\"break-glass governance debt bypass\"}" \
  "$BASE_URL/api/v1/control-plane/siem/receivers/break-glass" >/tmp/phase34-break-two.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase34-break-two.json'))
assert d['active_receiver']=='receiver-primary' and d['term']==3 and d['emergency'] is True, d
assert d['incident_id']=='INC-34-002', d
PY
wait_sql "SELECT IF(term=3 AND active_receiver='${PRIMARY_ID}',1,0) FROM ${TEST_DB}.control_plane_siem_receiver_routing_state WHERE id=1"
curl -fsS -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/metrics" >/tmp/phase34-metrics-bypass.txt
python3 - <<'PY'
import re
text=open('/tmp/phase34-metrics-bypass.txt').read()
m=re.search(r'taskflow_control_plane_siem_break_glass_total\{outcome="governance_debt_bypass"\} ([0-9.eE+-]+)', text)
assert m and float(m.group(1)) >= 1, m.group(0) if m else text[-2000:]
PY

phase="restore governance and prove debt fence clears automatically"
docker unpause "$GOV_CONTAINER" >/dev/null
wait_http "$GOV_URL/health"
wait_sql "SELECT IF(COUNT(*)=3,1,0) FROM ${TEST_DB}.control_plane_siem_term_ledger_anchors WHERE term IN (1,2,3)"
wait_sql "SELECT IF(COUNT(*)=0,1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE anchor_state <> 'delivered'"
wait_sql "SELECT IF(COUNT(*)=0,1,0) FROM ${TEST_DB}.control_plane_siem_incident_evidence WHERE alert_state <> 'delivered'"
wait_governance_fence false
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase34-governance-status.json'))
assert d['fenced'] is False and d['outstanding']==0 and d['dead']==0, d
assert d['oldest_outstanding_seconds']==0, d
assert d['anchors']['delivered'] >= 3 and d['alerts']['delivered'] >= 2, d
PY
curl -fsS -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/metrics" >/tmp/phase34-metrics-recovered.txt
grep -q 'taskflow_control_plane_siem_governance_debt_fenced 0' /tmp/phase34-metrics-recovered.txt

echo "Phase 34 governance debt status and Prometheus backlog observability: ok"
echo "Phase 34 ordinary promotion governance debt policy fence: ok"
echo "Phase 34 break-glass bypass and automatic fence recovery: ok"
