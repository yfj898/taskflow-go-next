#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
PRIMARY_URL="${PHASE37_PRIMARY_URL:-http://127.0.0.1:18084}"
SECONDARY_URL="${PHASE37_SECONDARY_URL:-http://127.0.0.1:18085}"
GOV_URL="${PHASE37_GOVERNANCE_URL:-http://127.0.0.1:19704}"

COMMANDER="phase37-commander"
COMMANDER_TOKEN="${PHASE37_COMMANDER_TOKEN:-phase37-commander-token}"
REQUESTER="phase37-requester"
REQUESTER_TOKEN="${PHASE37_REQUESTER_TOKEN:-phase37-requester-token}"
APPROVER="phase37-approver"
APPROVER_TOKEN="${PHASE37_APPROVER_TOKEN:-phase37-approver-token}"
REVIEWER="phase37-reviewer"
REVIEWER_TOKEN="${PHASE37_REVIEWER_TOKEN:-phase37-reviewer-token}"
ADDITIONAL_CREDENTIALS="${APPROVER}=${APPROVER_TOKEN};${REVIEWER}=${REVIEWER_TOKEN}"
ROLE_POLICY="${REQUESTER}=requester;${APPROVER}=approver;${COMMANDER}=incident-commander;${REVIEWER}=governance-reviewer"

SINK_TOKEN="${PHASE37_SINK_TOKEN:-phase37-sink-token}"
SIGN_KEY="${PHASE37_SIGNING_KEY:-phase37-signing-key}"
SIGN_KEY_ID="${PHASE37_SIGNING_KEY_ID:-phase37-sign-v1}"
CHECKPOINT_KEY="${PHASE37_CHECKPOINT_KEY:-phase37-checkpoint-secret}"
CHECKPOINT_KEY_ID="${PHASE37_CHECKPOINT_KEY_ID:-phase37-checkpoint-v1}"
GOV_TOKEN="${PHASE37_GOVERNANCE_TOKEN:-phase37-governance-token}"
GOV_SIGN_KEY="${PHASE37_GOVERNANCE_SIGNING_KEY:-phase37-governance-signing-key}"
GOV_SIGN_KEY_ID="${PHASE37_GOVERNANCE_SIGNING_KEY_ID:-phase37-governance-v1}"

PRIMARY_ID="receiver-primary"
SECONDARY_ID="receiver-secondary"
TEST_DB="${PHASE37_DB:-taskflow_phase37}"
TEST_DSN="taskflow:taskflow@tcp(mysql:3306)/${TEST_DB}?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
DEFAULT_DSN="taskflow:taskflow@tcp(mysql:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
QUEUE_KEY="${PHASE37_QUEUE_KEY:-taskflow:phase37:queue}"
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
    CONTROL_PLANE_BEARER_TOKEN="$COMMANDER_TOKEN" CONTROL_PLANE_PREVIOUS_BEARER_TOKEN="$REQUESTER_TOKEN" \
    CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS="$ADDITIONAL_CREDENTIALS" CONTROL_PLANE_OPERATOR_ID="$COMMANDER" CONTROL_PLANE_PREVIOUS_OPERATOR_ID="$REQUESTER" \
    CONTROL_PLANE_AUDIT_ENABLED=true CONTROL_PLANE_AUDIT_RETENTION_ENABLED=false \
    CONTROL_PLANE_SIEM_DELIVERY_ENABLED=true CONTROL_PLANE_SIEM_ENDPOINT=http://siem-sink:8080/events \
    CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" CONTROL_PLANE_SIEM_SIGNING_PROVIDER=hmac CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" \
    CONTROL_PLANE_SIEM_DELIVERY_INTERVAL_MS=100 CONTROL_PLANE_SIEM_DELIVERY_LEASE_MS=1500 CONTROL_PLANE_SIEM_MAX_ATTEMPTS=5 CONTROL_PLANE_SIEM_RETRY_BASE_MS=100 CONTROL_PLANE_SIEM_RETRY_MAX_MS=300 \
    CONTROL_PLANE_SIEM_DISTRIBUTED_COORDINATION=false CONTROL_PLANE_SIEM_GC_ENABLED=false CONTROL_PLANE_SIEM_DR_ENABLED=false \
    CONTROL_PLANE_SIEM_PRIMARY_RECEIVER_ID="$PRIMARY_ID" CONTROL_PLANE_SIEM_PRIMARY_CHECKPOINT_ENDPOINT=http://siem-sink:8080/checkpoint CONTROL_PLANE_SIEM_PRIMARY_TERM_ENDPOINT=http://siem-sink:8080/control/term \
    CONTROL_PLANE_SIEM_SECONDARY_ENDPOINT=http://siem-sink-secondary:8080/events CONTROL_PLANE_SIEM_SECONDARY_RECEIVER_ID="$SECONDARY_ID" CONTROL_PLANE_SIEM_SECONDARY_CHECKPOINT_ENDPOINT=http://siem-sink-secondary:8080/checkpoint CONTROL_PLANE_SIEM_SECONDARY_TERM_ENDPOINT=http://siem-sink-secondary:8080/control/term \
    CONTROL_PLANE_SIEM_FAILOVER_ENABLED=false CONTROL_PLANE_SIEM_SHADOW_SECONDARY=false CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
    CONTROL_PLANE_SIEM_CLUSTER_GUARD_ENABLED=true CONTROL_PLANE_SIEM_TERM_FENCING_ENABLED=true CONTROL_PLANE_SIEM_ANTI_ENTROPY_ENABLED=true CONTROL_PLANE_SIEM_ANTI_ENTROPY_INTERVAL_MS=100 \
    CONTROL_PLANE_SIEM_PROMOTION_WORKFLOW_ENABLED=true CONTROL_PLANE_SIEM_PROMOTION_INTENT_TTL_MS=60000 CONTROL_PLANE_SIEM_PROMOTION_LEASE_MS=5000 \
    CONTROL_PLANE_SIEM_PROMOTION_APPROVAL_QUORUM=1 CONTROL_PLANE_SIEM_PROMOTION_ROLE_POLICY="$ROLE_POLICY" CONTROL_PLANE_SIEM_BREAK_GLASS_ENABLED=true \
    CONTROL_PLANE_SIEM_GOVERNANCE_ENDPOINT=http://siem-governance:8080 CONTROL_PLANE_SIEM_GOVERNANCE_BEARER_TOKEN="$GOV_TOKEN" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY="$GOV_SIGN_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY_ID="$GOV_SIGN_KEY_ID" CONTROL_PLANE_SIEM_GOVERNANCE_TIMEOUT_MS=300 \
    CONTROL_PLANE_SIEM_GOVERNANCE_DEBT_POLICY_ENABLED=true CONTROL_PLANE_SIEM_GOVERNANCE_MAX_OUTSTANDING=1000 CONTROL_PLANE_SIEM_GOVERNANCE_MAX_AGE_MS=600000 \
    CONTROL_PLANE_SIEM_GOVERNANCE_REMEDIATION_APPROVAL_REQUIRED=true CONTROL_PLANE_SIEM_GOVERNANCE_REMEDIATION_APPROVAL_TTL_MS=3000 CONTROL_PLANE_SIEM_GOVERNANCE_REMEDIATION_STALE_TARGET_FENCING_ENABLED=true \
    CONTROL_PLANE_SIEM_RTO_ALERT_THRESHOLD_MS=30000 CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED=false MYSQL_DSN="$TEST_DSN" REDIS_QUEUE_KEY="$QUEUE_KEY" \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null
}

cleanup() {
  rc=$?
  if [ -n "$GOV_CONTAINER" ]; then docker unpause "$GOV_CONTAINER" >/dev/null 2>&1 || true; fi
  if [ "$rc" -ne 0 ]; then
    echo "Phase 37 governance approval lifecycle smoke failed during phase: $phase" >&2
    docker compose --profile phase37-smoke logs --tail 400 taskflow siem-sink siem-sink-secondary siem-governance >&2 2>/dev/null || true
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
  docker compose --profile phase37-smoke rm -sf siem-sink siem-sink-secondary siem-governance >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT

phase="build Phase 37 image and static guardrails"
docker build --target build -t taskflow-go:phase37-build --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
docker build -t taskflow-go:local --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
python3 scripts/phase37-governance-approval-lifecycle-check.py

phase="start isolated receivers and governance service"
CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
CONTROL_PLANE_SIEM_GOVERNANCE_BEARER_TOKEN="$GOV_TOKEN" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY="$GOV_SIGN_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY_ID="$GOV_SIGN_KEY_ID" \
PHASE29_PRIMARY_RECEIVER_ID="$PRIMARY_ID" PHASE29_SECONDARY_RECEIVER_ID="$SECONDARY_ID" PHASE30_ACTIVE_RECEIVER="$PRIMARY_ID" \
  docker compose --profile phase37-smoke up -d mysql redis executor-worker siem-sink siem-sink-secondary siem-governance >/dev/null
wait_http "$PRIMARY_URL/health"; wait_http "$SECONDARY_URL/health"; wait_http "$GOV_URL/health"

phase="create isolated database and dead term 2 evidence"
docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}; CREATE DATABASE ${TEST_DB}; GRANT ALL PRIVILEGES ON ${TEST_DB}.* TO 'taskflow'@'%'; FLUSH PRIVILEGES" >/dev/null
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null
start_taskflow
wait_http "$BASE_URL/health/ready"
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_siem_term_ledger_anchors WHERE term=1"
GOV_CONTAINER="$(docker compose ps -q siem-governance)"; [ -n "$GOV_CONTAINER" ]
docker pause "$GOV_CONTAINER" >/dev/null
curl -fsS --max-time 12 -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"target_receiver\":\"${SECONDARY_ID}\",\"expected_term\":1,\"incident_id\":\"INC-37-001\",\"reason\":\"create lifecycle remediation evidence\"}" \
  "$BASE_URL/api/v1/control-plane/siem/receivers/break-glass" >/tmp/phase37-break.json
wait_sql "SELECT IF(anchor_state IN ('pending','delivering') AND anchor_attempts>=1,1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=2"
original_hash="$(sql "SELECT record_hash FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=2")"
[ "${#original_hash}" -eq 64 ]
docker compose exec -T mysql mysql -uroot -proot -e "UPDATE ${TEST_DB}.control_plane_siem_receiver_term_history SET record_hash=REPEAT('0',64) WHERE term=2" >/dev/null
docker unpause "$GOV_CONTAINER" >/dev/null
wait_http "$GOV_URL/health"
wait_sql "SELECT IF(anchor_state='dead' AND anchor_last_error LIKE '%term history hash mismatch%',1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=2"
curl -fsS -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/api/v1/control-plane/siem/governance/dead?kind=anchor" >/tmp/phase37-dead.json
dead_id="$(python3 -c 'import json; d=json.load(open("/tmp/phase37-dead.json")); x=d["items"][0]; assert x["term"]==2; print(x["id"])')"

phase="reviewer rejects one pending remediation without mutating dead target"
curl -fsS -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' \
  -d '{"operation_key":"phase37-reject","reason":"reviewer must be able to reject unsafe repair"}' \
  "$BASE_URL/api/v1/control-plane/siem/governance/dead/anchor/${dead_id}/remediations" >/tmp/phase37-reject-request.json
reject_id="$(python3 -c 'import json; d=json.load(open("/tmp/phase37-reject-request.json")); assert d["state"]=="pending_approval" and d["target_identity"]=="0"*64 and d["expires_at"]; print(d["id"])')"
reject_body='{"note":"repair evidence is insufficient"}'
curl -fsS -X POST -H "Authorization: Bearer $REVIEWER_TOKEN" -H 'Content-Type: application/json' -d "$reject_body" \
  "$BASE_URL/api/v1/control-plane/siem/governance/remediations/${reject_id}/reject" >/tmp/phase37-rejected.json
curl -fsS -X POST -H "Authorization: Bearer $REVIEWER_TOKEN" -H 'Content-Type: application/json' -d "$reject_body" \
  "$BASE_URL/api/v1/control-plane/siem/governance/remediations/${reject_id}/reject" >/tmp/phase37-rejected-retry.json
wait_sql "SELECT IF(state='rejected' AND rejected_by='${REVIEWER}' AND rejected_at IS NOT NULL,1,0) FROM ${TEST_DB}.control_plane_siem_governance_remediations WHERE id=${reject_id}"
wait_sql "SELECT IF(anchor_state='dead',1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE id=${dead_id}"

phase="requester cancels its own pending remediation"
curl -fsS -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' \
  -d '{"operation_key":"phase37-cancel","reason":"requester should be able to withdraw stale plan"}' \
  "$BASE_URL/api/v1/control-plane/siem/governance/dead/anchor/${dead_id}/remediations" >/tmp/phase37-cancel-request.json
cancel_id="$(python3 -c 'import json; print(json.load(open("/tmp/phase37-cancel-request.json"))["id"])')"
cancel_body='{"note":"withdraw remediation plan before review"}'
curl -fsS -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' -d "$cancel_body" \
  "$BASE_URL/api/v1/control-plane/siem/governance/remediations/${cancel_id}/cancel" >/tmp/phase37-cancelled.json
curl -fsS -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' -d "$cancel_body" \
  "$BASE_URL/api/v1/control-plane/siem/governance/remediations/${cancel_id}/cancel" >/tmp/phase37-cancelled-retry.json
wait_sql "SELECT IF(state='cancelled' AND cancelled_by='${COMMANDER}' AND cancelled_at IS NOT NULL,1,0) FROM ${TEST_DB}.control_plane_siem_governance_remediations WHERE id=${cancel_id}"

phase="pending remediation expires and cannot be approved later"
curl -fsS -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' \
  -d '{"operation_key":"phase37-expire","reason":"approval window must be bounded"}' \
  "$BASE_URL/api/v1/control-plane/siem/governance/dead/anchor/${dead_id}/remediations" >/tmp/phase37-expire-request.json
expire_id="$(python3 -c 'import json; print(json.load(open("/tmp/phase37-expire-request.json"))["id"])')"
sleep 4
governance_status >/tmp/phase37-status-expired.json
wait_sql "SELECT IF(state='expired' AND resolved_at IS NOT NULL,1,0) FROM ${TEST_DB}.control_plane_siem_governance_remediations WHERE id=${expire_id}"
late_code="$(curl -sS -o /tmp/phase37-late-approve.json -w '%{http_code}' -X POST -H "Authorization: Bearer $REVIEWER_TOKEN" -H 'Content-Type: application/json' \
  -d '{"note":"late approval must fail"}' "$BASE_URL/api/v1/control-plane/siem/governance/remediations/${expire_id}/approve")"
[ "$late_code" = "409" ]
grep -q 'not pending approval' /tmp/phase37-late-approve.json

phase="target identity changes after request and stale approval is fenced"
curl -fsS -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' \
  -d '{"operation_key":"phase37-stale","reason":"bind approval to exact dead evidence identity"}' \
  "$BASE_URL/api/v1/control-plane/siem/governance/dead/anchor/${dead_id}/remediations" >/tmp/phase37-stale-request.json
stale_id="$(python3 -c 'import json; d=json.load(open("/tmp/phase37-stale-request.json")); assert d["target_identity"]=="0"*64; print(d["id"])')"
docker compose exec -T mysql mysql -uroot -proot -e "UPDATE ${TEST_DB}.control_plane_siem_receiver_term_history SET record_hash='${original_hash}' WHERE id=${dead_id}" >/dev/null
stale_code="$(curl -sS -o /tmp/phase37-stale-approve.json -w '%{http_code}' -X POST -H "Authorization: Bearer $REVIEWER_TOKEN" -H 'Content-Type: application/json' \
  -d '{"note":"approval must fail because target identity changed"}' "$BASE_URL/api/v1/control-plane/siem/governance/remediations/${stale_id}/approve")"
[ "$stale_code" = "409" ]
grep -q 'target changed after request' /tmp/phase37-stale-approve.json
wait_sql "SELECT IF(state='stale' AND result LIKE '%stale governance anchor target%',1,0) FROM ${TEST_DB}.control_plane_siem_governance_remediations WHERE id=${stale_id}"
wait_sql "SELECT IF(anchor_state='dead' AND record_hash='${original_hash}',1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE id=${dead_id}"

phase="fresh request against repaired target can be approved and resolved"
curl -fsS -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' \
  -d '{"operation_key":"phase37-fresh","reason":"fresh request after target repair"}' \
  "$BASE_URL/api/v1/control-plane/siem/governance/dead/anchor/${dead_id}/remediations" >/tmp/phase37-fresh-request.json
fresh_id="$(python3 -c 'import json; d=json.load(open("/tmp/phase37-fresh-request.json")); assert len(d["target_identity"])==64; print(d["id"])')"
curl -fsS -X POST -H "Authorization: Bearer $REVIEWER_TOKEN" -H 'Content-Type: application/json' \
  -d '{"note":"fresh identity matches repaired target"}' "$BASE_URL/api/v1/control-plane/siem/governance/remediations/${fresh_id}/approve" >/tmp/phase37-fresh-approved.json
wait_sql "SELECT IF(anchor_state='delivered',1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE id=${dead_id}"
wait_sql "SELECT IF(state='resolved' AND approved_by='${REVIEWER}',1,0) FROM ${TEST_DB}.control_plane_siem_governance_remediations WHERE id=${fresh_id}"
governance_status >/tmp/phase37-status-recovered.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase37-status-recovered.json'))
assert d['fenced'] is False and d['dead']==0, d
assert d['pending_remediation_approvals']==0 and d['open_remediations']==0, d
PY

phase="verify lifecycle metrics and ordinary promotion recovery"
curl -fsS -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/metrics" >/tmp/phase37-metrics.txt
grep -q 'taskflow_control_plane_siem_governance_remediations_total{outcome="rejected"} 1' /tmp/phase37-metrics.txt
grep -q 'taskflow_control_plane_siem_governance_remediations_total{outcome="rejection_idempotent"} 1' /tmp/phase37-metrics.txt
grep -q 'taskflow_control_plane_siem_governance_remediations_total{outcome="cancelled"} 1' /tmp/phase37-metrics.txt
grep -q 'taskflow_control_plane_siem_governance_remediations_total{outcome="cancellation_idempotent"} 1' /tmp/phase37-metrics.txt
grep -q 'taskflow_control_plane_siem_governance_remediations_total{outcome="expired"}' /tmp/phase37-metrics.txt
grep -q 'taskflow_control_plane_siem_governance_remediations_total{outcome="stale"} 1' /tmp/phase37-metrics.txt
wait_sql "SELECT IF(COUNT(*)=0,1,0) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE state <> 'delivered'"
curl -fsS -X POST -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/sync" >/dev/null
wait_sql "SELECT IF(COUNT(*)=0,1,0) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE state <> 'delivered'"
wait_receivers_stable_equal
curl -fsS -X POST -H "Authorization: Bearer $REQUESTER_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"target_receiver\":\"${PRIMARY_ID}\",\"expected_term\":2,\"reason\":\"post-phase37 promotion\"}" \
  "$BASE_URL/api/v1/control-plane/siem/receivers/intents" >/tmp/phase37-intent.json
intent_id="$(python3 -c 'import json; print(json.load(open("/tmp/phase37-intent.json"))["id"])')"
curl -fsS -X POST -H "Authorization: Bearer $APPROVER_TOKEN" -H 'Content-Type: application/json' -d '{"note":"post-phase37 approval"}' \
  "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_id}/approve" >/dev/null
wait_sql "SELECT IF(COUNT(*)=0,1,0) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE state <> 'delivered'"
curl -fsS -X POST -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/sync" >/dev/null
wait_sql "SELECT IF(COUNT(*)=0,1,0) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE state <> 'delivered'"
wait_receivers_stable_equal
curl -fsS -X POST -H "Authorization: Bearer $APPROVER_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_id}/execute" >/tmp/phase37-execute.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase37-execute.json'))
assert d['active_receiver']=='receiver-primary' and d['term']==3, d
PY

echo "Phase 37 reviewer rejection, requester cancellation and bounded approval TTL: ok"
echo "Phase 37 stale-target identity fencing prevented obsolete approval from mutating evidence: ok"
echo "Phase 37 fresh post-repair request resolved governance debt and restored ordinary promotion: ok"
