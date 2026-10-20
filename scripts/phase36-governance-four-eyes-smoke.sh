#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
PRIMARY_URL="${PHASE36_PRIMARY_URL:-http://127.0.0.1:18084}"
SECONDARY_URL="${PHASE36_SECONDARY_URL:-http://127.0.0.1:18085}"
GOV_URL="${PHASE36_GOVERNANCE_URL:-http://127.0.0.1:19704}"

COMMANDER="phase36-commander"
COMMANDER_TOKEN="${PHASE36_COMMANDER_TOKEN:-phase36-commander-token}"
REQUESTER="phase36-requester"
REQUESTER_TOKEN="${PHASE36_REQUESTER_TOKEN:-phase36-requester-token}"
APPROVER="phase36-approver"
APPROVER_TOKEN="${PHASE36_APPROVER_TOKEN:-phase36-approver-token}"
REVIEWER="phase36-reviewer"
REVIEWER_TOKEN="${PHASE36_REVIEWER_TOKEN:-phase36-reviewer-token}"
ADDITIONAL_CREDENTIALS="${APPROVER}=${APPROVER_TOKEN};${REVIEWER}=${REVIEWER_TOKEN}"
ROLE_POLICY="${REQUESTER}=requester;${APPROVER}=approver;${COMMANDER}=incident-commander|governance-reviewer;${REVIEWER}=governance-reviewer"

SINK_TOKEN="${PHASE36_SINK_TOKEN:-phase36-sink-token}"
SIGN_KEY="${PHASE36_SIGNING_KEY:-phase36-signing-key}"
SIGN_KEY_ID="${PHASE36_SIGNING_KEY_ID:-phase36-sign-v1}"
CHECKPOINT_KEY="${PHASE36_CHECKPOINT_KEY:-phase36-checkpoint-secret}"
CHECKPOINT_KEY_ID="${PHASE36_CHECKPOINT_KEY_ID:-phase36-checkpoint-v1}"
GOV_TOKEN="${PHASE36_GOVERNANCE_TOKEN:-phase36-governance-token}"
GOV_SIGN_KEY="${PHASE36_GOVERNANCE_SIGNING_KEY:-phase36-governance-signing-key}"
GOV_SIGN_KEY_ID="${PHASE36_GOVERNANCE_SIGNING_KEY_ID:-phase36-governance-v1}"

PRIMARY_ID="receiver-primary"
SECONDARY_ID="receiver-secondary"
TEST_DB="${PHASE36_DB:-taskflow_phase36}"
TEST_DSN="taskflow:taskflow@tcp(mysql:3306)/${TEST_DB}?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
DEFAULT_DSN="taskflow:taskflow@tcp(mysql:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
QUEUE_KEY="${PHASE36_QUEUE_KEY:-taskflow:phase36:queue}"
OPERATION_KEY="phase36-term2-anchor-remediation"
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
    CONTROL_PLANE_BEARER_TOKEN="$COMMANDER_TOKEN" \
    CONTROL_PLANE_PREVIOUS_BEARER_TOKEN="$REQUESTER_TOKEN" \
    CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS="$ADDITIONAL_CREDENTIALS" \
    CONTROL_PLANE_OPERATOR_ID="$COMMANDER" CONTROL_PLANE_PREVIOUS_OPERATOR_ID="$REQUESTER" \
    CONTROL_PLANE_AUDIT_ENABLED=true CONTROL_PLANE_AUDIT_RETENTION_ENABLED=false \
    CONTROL_PLANE_SIEM_DELIVERY_ENABLED=true CONTROL_PLANE_SIEM_ENDPOINT=http://siem-sink:8080/events \
    CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" CONTROL_PLANE_SIEM_SIGNING_PROVIDER=hmac \
    CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" \
    CONTROL_PLANE_SIEM_DELIVERY_INTERVAL_MS=100 CONTROL_PLANE_SIEM_DELIVERY_LEASE_MS=1500 \
    CONTROL_PLANE_SIEM_MAX_ATTEMPTS=5 CONTROL_PLANE_SIEM_RETRY_BASE_MS=100 CONTROL_PLANE_SIEM_RETRY_MAX_MS=300 \
    CONTROL_PLANE_SIEM_DISTRIBUTED_COORDINATION=false CONTROL_PLANE_SIEM_GC_ENABLED=false CONTROL_PLANE_SIEM_DR_ENABLED=false \
    CONTROL_PLANE_SIEM_PRIMARY_RECEIVER_ID="$PRIMARY_ID" CONTROL_PLANE_SIEM_PRIMARY_CHECKPOINT_ENDPOINT=http://siem-sink:8080/checkpoint \
    CONTROL_PLANE_SIEM_PRIMARY_TERM_ENDPOINT=http://siem-sink:8080/control/term \
    CONTROL_PLANE_SIEM_SECONDARY_ENDPOINT=http://siem-sink-secondary:8080/events CONTROL_PLANE_SIEM_SECONDARY_RECEIVER_ID="$SECONDARY_ID" \
    CONTROL_PLANE_SIEM_SECONDARY_CHECKPOINT_ENDPOINT=http://siem-sink-secondary:8080/checkpoint CONTROL_PLANE_SIEM_SECONDARY_TERM_ENDPOINT=http://siem-sink-secondary:8080/control/term \
    CONTROL_PLANE_SIEM_FAILOVER_ENABLED=false CONTROL_PLANE_SIEM_SHADOW_SECONDARY=false \
    CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
    CONTROL_PLANE_SIEM_CLUSTER_GUARD_ENABLED=true CONTROL_PLANE_SIEM_TERM_FENCING_ENABLED=true \
    CONTROL_PLANE_SIEM_ANTI_ENTROPY_ENABLED=true CONTROL_PLANE_SIEM_ANTI_ENTROPY_INTERVAL_MS=100 \
    CONTROL_PLANE_SIEM_PROMOTION_WORKFLOW_ENABLED=true CONTROL_PLANE_SIEM_PROMOTION_INTENT_TTL_MS=60000 CONTROL_PLANE_SIEM_PROMOTION_LEASE_MS=5000 \
    CONTROL_PLANE_SIEM_PROMOTION_APPROVAL_QUORUM=1 CONTROL_PLANE_SIEM_PROMOTION_ROLE_POLICY="$ROLE_POLICY" CONTROL_PLANE_SIEM_BREAK_GLASS_ENABLED=true \
    CONTROL_PLANE_SIEM_GOVERNANCE_ENDPOINT=http://siem-governance:8080 CONTROL_PLANE_SIEM_GOVERNANCE_BEARER_TOKEN="$GOV_TOKEN" \
    CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY="$GOV_SIGN_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY_ID="$GOV_SIGN_KEY_ID" CONTROL_PLANE_SIEM_GOVERNANCE_TIMEOUT_MS=300 \
    CONTROL_PLANE_SIEM_GOVERNANCE_DEBT_POLICY_ENABLED=true CONTROL_PLANE_SIEM_GOVERNANCE_MAX_OUTSTANDING=1000 CONTROL_PLANE_SIEM_GOVERNANCE_MAX_AGE_MS=600000 \
    CONTROL_PLANE_SIEM_GOVERNANCE_REMEDIATION_APPROVAL_REQUIRED=true \
    CONTROL_PLANE_SIEM_RTO_ALERT_THRESHOLD_MS=30000 CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED=false \
    MYSQL_DSN="$TEST_DSN" REDIS_QUEUE_KEY="$QUEUE_KEY" \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null
}

cleanup() {
  rc=$?
  if [ -n "$GOV_CONTAINER" ]; then docker unpause "$GOV_CONTAINER" >/dev/null 2>&1 || true; fi
  if [ "$rc" -ne 0 ]; then
    echo "Phase 36 governance four-eyes smoke failed during phase: $phase" >&2
    docker compose --profile phase36-smoke logs --tail 400 taskflow siem-sink siem-sink-secondary siem-governance >&2 2>/dev/null || true
  fi
  CONTROL_PLANE_AUTH_ENABLED=false CONTROL_PLANE_BEARER_TOKEN= CONTROL_PLANE_PREVIOUS_BEARER_TOKEN= CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS= CONTROL_PLANE_OPERATOR_ID=operator CONTROL_PLANE_PREVIOUS_OPERATOR_ID= \
    CONTROL_PLANE_AUDIT_ENABLED=true CONTROL_PLANE_AUDIT_RETENTION_ENABLED=true CONTROL_PLANE_SIEM_DELIVERY_ENABLED=false CONTROL_PLANE_SIEM_SIGNING_PROVIDER=hmac \
    CONTROL_PLANE_SIEM_FAILOVER_ENABLED=false CONTROL_PLANE_SIEM_SHADOW_SECONDARY=false CONTROL_PLANE_SIEM_CLUSTER_GUARD_ENABLED=false CONTROL_PLANE_SIEM_TERM_FENCING_ENABLED=false \
    CONTROL_PLANE_SIEM_ANTI_ENTROPY_ENABLED=false CONTROL_PLANE_SIEM_PROMOTION_WORKFLOW_ENABLED=false CONTROL_PLANE_SIEM_BREAK_GLASS_ENABLED=false CONTROL_PLANE_SIEM_GOVERNANCE_ENDPOINT= \
    CONTROL_PLANE_SIEM_GOVERNANCE_DEBT_POLICY_ENABLED=false CONTROL_PLANE_SIEM_GOVERNANCE_REMEDIATION_APPROVAL_REQUIRED=false CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED=false \
    CONTROL_PLANE_SIEM_RECONCILE_ENABLED=false CONTROL_PLANE_SIEM_DR_ENABLED=false MYSQL_DSN="$DEFAULT_DSN" REDIS_QUEUE_KEY=taskflow:queue \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null 2>&1 || true
  docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}" >/dev/null 2>&1 || true
  docker compose --profile phase36-smoke rm -sf siem-sink siem-sink-secondary siem-governance >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT

phase="build Phase 36 image and static guardrails"
docker build --target build -t taskflow-go:phase36-build --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
docker build -t taskflow-go:local --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
python3 scripts/phase36-governance-four-eyes-check.py

phase="start isolated receivers and governance service"
CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
CONTROL_PLANE_SIEM_GOVERNANCE_BEARER_TOKEN="$GOV_TOKEN" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY="$GOV_SIGN_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY_ID="$GOV_SIGN_KEY_ID" \
PHASE29_PRIMARY_RECEIVER_ID="$PRIMARY_ID" PHASE29_SECONDARY_RECEIVER_ID="$SECONDARY_ID" PHASE30_ACTIVE_RECEIVER="$PRIMARY_ID" \
  docker compose --profile phase36-smoke up -d mysql redis executor-worker siem-sink siem-sink-secondary siem-governance >/dev/null
wait_http "$PRIMARY_URL/health"; wait_http "$SECONDARY_URL/health"; wait_http "$GOV_URL/health"

phase="create isolated database and establish term 1 anchor"
docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}; CREATE DATABASE ${TEST_DB}; GRANT ALL PRIVILEGES ON ${TEST_DB}.* TO 'taskflow'@'%'; FLUSH PRIVILEGES" >/dev/null
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null
start_taskflow
wait_http "$BASE_URL/health/ready"
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_siem_term_ledger_anchors WHERE term=1"

phase="create dead term 2 governance evidence"
GOV_CONTAINER="$(docker compose ps -q siem-governance)"; [ -n "$GOV_CONTAINER" ]
docker pause "$GOV_CONTAINER" >/dev/null
curl -fsS --max-time 12 -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"target_receiver\":\"${SECONDARY_ID}\",\"expected_term\":1,\"incident_id\":\"INC-36-001\",\"reason\":\"create four-eyes remediation evidence\"}" \
  "$BASE_URL/api/v1/control-plane/siem/receivers/break-glass" >/tmp/phase36-break.json
wait_sql "SELECT IF(anchor_state IN ('pending','delivering') AND anchor_attempts>=1,1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=2"
original_hash="$(sql "SELECT record_hash FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=2")"
[ "${#original_hash}" -eq 64 ]
docker compose exec -T mysql mysql -uroot -proot -e "UPDATE ${TEST_DB}.control_plane_siem_receiver_term_history SET record_hash=REPEAT('0',64) WHERE term=2" >/dev/null
docker unpause "$GOV_CONTAINER" >/dev/null
wait_http "$GOV_URL/health"
wait_sql "SELECT IF(anchor_state='dead' AND anchor_last_error LIKE '%term history hash mismatch%',1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=2"
curl -fsS -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/api/v1/control-plane/siem/governance/dead?kind=anchor" >/tmp/phase36-dead.json
dead_id="$(python3 -c 'import json; d=json.load(open("/tmp/phase36-dead.json")); x=d["items"][0]; assert x["term"]==2; print(x["id"])')"

phase="prove direct requeue is disabled while four-eyes approval is required"
direct_code="$(curl -sS -o /tmp/phase36-direct.json -w '%{http_code}' -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' \
  -d '{"reason":"direct mutation must be disabled"}' "$BASE_URL/api/v1/control-plane/siem/governance/dead/anchor/${dead_id}/requeue")"
[ "$direct_code" = "409" ]
grep -q 'approval workflow is required' /tmp/phase36-direct.json

phase="create idempotent remediation request without mutating dead evidence"
request_body="{\"operation_key\":\"${OPERATION_KEY}\",\"reason\":\"repair validated term hash under four-eyes control\"}"
curl -fsS -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' -d "$request_body" \
  "$BASE_URL/api/v1/control-plane/siem/governance/dead/anchor/${dead_id}/remediations" >/tmp/phase36-request-one.json
remediation_id="$(python3 -c 'import json; d=json.load(open("/tmp/phase36-request-one.json")); assert d["state"]=="pending_approval"; print(d["id"])')"
curl -fsS -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' -d "$request_body" \
  "$BASE_URL/api/v1/control-plane/siem/governance/dead/anchor/${dead_id}/remediations" >/tmp/phase36-request-retry.json
retry_id="$(python3 -c 'import json; print(json.load(open("/tmp/phase36-request-retry.json"))["id"])')"
[ "$retry_id" = "$remediation_id" ]
[ "$(sql "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_siem_governance_remediations WHERE operation_key='${OPERATION_KEY}'")" = "1" ]
wait_sql "SELECT IF(anchor_state='dead',1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE id=${dead_id}"
governance_status >/tmp/phase36-status-pending.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase36-status-pending.json'))
assert d['fenced'] is True and d['dead'] >= 1, d
assert d['pending_remediation_approvals']==1 and d['open_remediations']==0, d
PY

phase="prove operation-key conflicts and maker-checker separation are fail-closed"
conflict_code="$(curl -sS -o /tmp/phase36-operation-conflict.json -w '%{http_code}' -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"operation_key\":\"${OPERATION_KEY}\",\"reason\":\"different payload must conflict\"}" \
  "$BASE_URL/api/v1/control-plane/siem/governance/dead/anchor/${dead_id}/remediations")"
[ "$conflict_code" = "409" ]
grep -q 'conflicts with an existing request' /tmp/phase36-operation-conflict.json
self_code="$(curl -sS -o /tmp/phase36-self-approve.json -w '%{http_code}' -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' \
  -d '{"note":"requester must not approve own remediation"}' "$BASE_URL/api/v1/control-plane/siem/governance/remediations/${remediation_id}/approve")"
[ "$self_code" = "409" ]
grep -q 'must be different operators' /tmp/phase36-self-approve.json
wrong_role_code="$(curl -sS -o /tmp/phase36-wrong-reviewer.json -w '%{http_code}' -X POST -H "Authorization: Bearer $APPROVER_TOKEN" -H 'Content-Type: application/json' \
  -d '{"note":"promotion approver is not governance reviewer"}' "$BASE_URL/api/v1/control-plane/siem/governance/remediations/${remediation_id}/approve")"
[ "$wrong_role_code" = "409" ]
grep -q 'governance-reviewer' /tmp/phase36-wrong-reviewer.json
wait_sql "SELECT IF(state='pending_approval' AND approved_by='',1,0) FROM ${TEST_DB}.control_plane_siem_governance_remediations WHERE id=${remediation_id}"

phase="repair root cause and let an independent reviewer authorize requeue"
docker compose exec -T mysql mysql -uroot -proot -e "UPDATE ${TEST_DB}.control_plane_siem_receiver_term_history SET record_hash='${original_hash}' WHERE id=${dead_id}" >/dev/null
approval_body='{"note":"independent review confirms immutable term hash repair"}'
curl -fsS -X POST -H "Authorization: Bearer $REVIEWER_TOKEN" -H 'Content-Type: application/json' -d "$approval_body" \
  "$BASE_URL/api/v1/control-plane/siem/governance/remediations/${remediation_id}/approve" >/tmp/phase36-approved.json
wait_sql "SELECT IF(anchor_state='delivered',1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE id=${dead_id}"
wait_sql "SELECT IF(state='resolved' AND approved_by='${REVIEWER}' AND approved_at IS NOT NULL,1,0) FROM ${TEST_DB}.control_plane_siem_governance_remediations WHERE id=${remediation_id}"
curl -fsS -X POST -H "Authorization: Bearer $REVIEWER_TOKEN" -H 'Content-Type: application/json' -d "$approval_body" \
  "$BASE_URL/api/v1/control-plane/siem/governance/remediations/${remediation_id}/approve" >/tmp/phase36-approve-retry.json
retry_approval_id="$(python3 -c 'import json; print(json.load(open("/tmp/phase36-approve-retry.json"))["id"])')"
[ "$retry_approval_id" = "$remediation_id" ]
governance_status >/tmp/phase36-status-resolved.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase36-status-resolved.json'))
assert d['fenced'] is False and d['dead']==0, d
assert d['pending_remediation_approvals']==0 and d['open_remediations']==0, d
PY

phase="verify four-eyes metrics and restore ordinary promotion"
curl -fsS -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/metrics" >/tmp/phase36-metrics.txt
grep -q 'taskflow_control_plane_siem_governance_pending_remediation_approvals 0' /tmp/phase36-metrics.txt
grep -q 'taskflow_control_plane_siem_governance_remediations_total{outcome="requested"} 1' /tmp/phase36-metrics.txt
grep -q 'taskflow_control_plane_siem_governance_remediations_total{outcome="request_idempotent"} 1' /tmp/phase36-metrics.txt
grep -q 'taskflow_control_plane_siem_governance_remediations_total{outcome="approved"} 1' /tmp/phase36-metrics.txt
grep -q 'taskflow_control_plane_siem_governance_remediations_total{outcome="approval_idempotent"} 1' /tmp/phase36-metrics.txt
wait_sql "SELECT IF(COUNT(*)=0,1,0) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE state <> 'delivered'"
curl -fsS -X POST -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/sync" >/dev/null
wait_sql "SELECT IF(COUNT(*)=0,1,0) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE state <> 'delivered'"
wait_receivers_stable_equal
curl -fsS -X POST -H "Authorization: Bearer $REQUESTER_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"target_receiver\":\"${PRIMARY_ID}\",\"expected_term\":2,\"reason\":\"post-four-eyes remediation promotion\"}" \
  "$BASE_URL/api/v1/control-plane/siem/receivers/intents" >/tmp/phase36-intent.json
intent_id="$(python3 -c 'import json; print(json.load(open("/tmp/phase36-intent.json"))["id"])')"
curl -fsS -X POST -H "Authorization: Bearer $APPROVER_TOKEN" -H 'Content-Type: application/json' -d '{"note":"post-remediation promotion approval"}' \
  "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_id}/approve" >/dev/null
wait_sql "SELECT IF(COUNT(*)=0,1,0) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE state <> 'delivered'"
curl -fsS -X POST -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/sync" >/dev/null
wait_sql "SELECT IF(COUNT(*)=0,1,0) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE state <> 'delivered'"
wait_receivers_stable_equal
curl -fsS -X POST -H "Authorization: Bearer $APPROVER_TOKEN" \
  "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_id}/execute" >/tmp/phase36-execute.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase36-execute.json'))
assert d['active_receiver']=='receiver-primary' and d['term']==3, d
PY

echo "Phase 36 four-eyes governance remediation and maker-checker separation: ok"
echo "Phase 36 operation-key request/approval retries were idempotent: ok"
echo "Phase 36 independent approval resolved dead evidence and restored ordinary promotion: ok"
