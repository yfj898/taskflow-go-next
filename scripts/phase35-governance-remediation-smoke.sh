#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
PRIMARY_URL="${PHASE35_PRIMARY_URL:-http://127.0.0.1:18084}"
SECONDARY_URL="${PHASE35_SECONDARY_URL:-http://127.0.0.1:18085}"
GOV_URL="${PHASE35_GOVERNANCE_URL:-http://127.0.0.1:19704}"

COMMANDER="phase35-commander"
COMMANDER_TOKEN="${PHASE35_COMMANDER_TOKEN:-phase35-commander-token}"
REQUESTER="phase35-requester"
REQUESTER_TOKEN="${PHASE35_REQUESTER_TOKEN:-phase35-requester-token}"
APPROVER="phase35-approver"
APPROVER_TOKEN="${PHASE35_APPROVER_TOKEN:-phase35-approver-token}"
ADDITIONAL_CREDENTIALS="${APPROVER}=${APPROVER_TOKEN}"
ROLE_POLICY="${REQUESTER}=requester;${APPROVER}=approver;${COMMANDER}=incident-commander"

SINK_TOKEN="${PHASE35_SINK_TOKEN:-phase35-sink-token}"
SIGN_KEY="${PHASE35_SIGNING_KEY:-phase35-signing-key}"
SIGN_KEY_ID="${PHASE35_SIGNING_KEY_ID:-phase35-sign-v1}"
CHECKPOINT_KEY="${PHASE35_CHECKPOINT_KEY:-phase35-checkpoint-secret}"
CHECKPOINT_KEY_ID="${PHASE35_CHECKPOINT_KEY_ID:-phase35-checkpoint-v1}"
GOV_TOKEN="${PHASE35_GOVERNANCE_TOKEN:-phase35-governance-token}"
GOV_SIGN_KEY="${PHASE35_GOVERNANCE_SIGNING_KEY:-phase35-governance-signing-key}"
GOV_SIGN_KEY_ID="${PHASE35_GOVERNANCE_SIGNING_KEY_ID:-phase35-governance-v1}"

PRIMARY_ID="receiver-primary"
SECONDARY_ID="receiver-secondary"
TEST_DB="${PHASE35_DB:-taskflow_phase35}"
TEST_DSN="taskflow:taskflow@tcp(mysql:3306)/${TEST_DB}?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
DEFAULT_DSN="taskflow:taskflow@tcp(mysql:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
QUEUE_KEY="${PHASE35_QUEUE_KEY:-taskflow:phase35:queue}"
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
    CONTROL_PLANE_SIEM_RTO_ALERT_THRESHOLD_MS=30000 CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED=false \
    MYSQL_DSN="$TEST_DSN" REDIS_QUEUE_KEY="$QUEUE_KEY" \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null
}

cleanup() {
  rc=$?
  if [ -n "$GOV_CONTAINER" ]; then docker unpause "$GOV_CONTAINER" >/dev/null 2>&1 || true; fi
  if [ "$rc" -ne 0 ]; then
    echo "Phase 35 governance remediation smoke failed during phase: $phase" >&2
    docker compose --profile phase35-smoke logs --tail 400 taskflow siem-sink siem-sink-secondary siem-governance >&2 2>/dev/null || true
  fi
  CONTROL_PLANE_AUTH_ENABLED=false CONTROL_PLANE_BEARER_TOKEN= CONTROL_PLANE_PREVIOUS_BEARER_TOKEN= CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS= CONTROL_PLANE_OPERATOR_ID=operator CONTROL_PLANE_PREVIOUS_OPERATOR_ID= \
    CONTROL_PLANE_AUDIT_ENABLED=true CONTROL_PLANE_AUDIT_RETENTION_ENABLED=true CONTROL_PLANE_SIEM_DELIVERY_ENABLED=false CONTROL_PLANE_SIEM_SIGNING_PROVIDER=hmac \
    CONTROL_PLANE_SIEM_FAILOVER_ENABLED=false CONTROL_PLANE_SIEM_SHADOW_SECONDARY=false CONTROL_PLANE_SIEM_CLUSTER_GUARD_ENABLED=false CONTROL_PLANE_SIEM_TERM_FENCING_ENABLED=false \
    CONTROL_PLANE_SIEM_ANTI_ENTROPY_ENABLED=false CONTROL_PLANE_SIEM_PROMOTION_WORKFLOW_ENABLED=false CONTROL_PLANE_SIEM_BREAK_GLASS_ENABLED=false CONTROL_PLANE_SIEM_GOVERNANCE_ENDPOINT= \
    CONTROL_PLANE_SIEM_GOVERNANCE_DEBT_POLICY_ENABLED=false CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED=false CONTROL_PLANE_SIEM_RECONCILE_ENABLED=false CONTROL_PLANE_SIEM_DR_ENABLED=false \
    MYSQL_DSN="$DEFAULT_DSN" REDIS_QUEUE_KEY=taskflow:queue docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null 2>&1 || true
  docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}" >/dev/null 2>&1 || true
  docker compose --profile phase35-smoke rm -sf siem-sink siem-sink-secondary siem-governance >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT

phase="build Phase 35 image and static guardrails"
docker build --target build -t taskflow-go:phase35-build --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
docker build -t taskflow-go:local --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
python3 scripts/phase35-governance-remediation-check.py

phase="start isolated receivers and governance service"
CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
CONTROL_PLANE_SIEM_GOVERNANCE_BEARER_TOKEN="$GOV_TOKEN" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY="$GOV_SIGN_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY_ID="$GOV_SIGN_KEY_ID" \
PHASE29_PRIMARY_RECEIVER_ID="$PRIMARY_ID" PHASE29_SECONDARY_RECEIVER_ID="$SECONDARY_ID" PHASE30_ACTIVE_RECEIVER="$PRIMARY_ID" \
  docker compose --profile phase35-smoke up -d mysql redis executor-worker siem-sink siem-sink-secondary siem-governance >/dev/null
wait_http "$PRIMARY_URL/health"; wait_http "$SECONDARY_URL/health"; wait_http "$GOV_URL/health"

phase="create isolated database and establish term 1 anchor"
docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}; CREATE DATABASE ${TEST_DB}; GRANT ALL PRIVILEGES ON ${TEST_DB}.* TO 'taskflow'@'%'; FLUSH PRIVILEGES" >/dev/null
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null
start_taskflow
wait_http "$BASE_URL/health/ready"
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_siem_term_ledger_anchors WHERE term=1"

phase="create term 2 while governance is unavailable and corrupt its immutable hash"
GOV_CONTAINER="$(docker compose ps -q siem-governance)"; [ -n "$GOV_CONTAINER" ]
docker pause "$GOV_CONTAINER" >/dev/null
curl -fsS --max-time 12 -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"target_receiver\":\"${SECONDARY_ID}\",\"expected_term\":1,\"incident_id\":\"INC-35-001\",\"reason\":\"create dead governance evidence\"}" \
  "$BASE_URL/api/v1/control-plane/siem/receivers/break-glass" >/tmp/phase35-break.json
wait_sql "SELECT IF(anchor_state IN ('pending','delivering') AND anchor_attempts>=1,1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=2"
original_hash="$(sql "SELECT record_hash FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=2")"
[ "${#original_hash}" -eq 64 ]
docker compose exec -T mysql mysql -uroot -proot -e "UPDATE ${TEST_DB}.control_plane_siem_receiver_term_history SET record_hash=REPEAT('0',64) WHERE term=2" >/dev/null
docker unpause "$GOV_CONTAINER" >/dev/null
wait_http "$GOV_URL/health"
wait_sql "SELECT IF(anchor_state='dead' AND anchor_last_error LIKE '%term history hash mismatch%',1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=2"

phase="verify dead-evidence listing and debt fence"
governance_status >/tmp/phase35-status-dead.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase35-status-dead.json'))
assert d['fenced'] is True and d['dead'] >= 1, d
assert 'dead governance evidence' in d['fence_reason'], d
PY
curl -fsS -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/api/v1/control-plane/siem/governance/dead?kind=anchor" >/tmp/phase35-dead.json
dead_id="$(python3 -c 'import json; d=json.load(open("/tmp/phase35-dead.json")); x=d["items"][0]; assert x["term"]==2 and "term history hash mismatch" in x["last_error"]; print(x["id"])')"

phase="prove non-commander remediation is denied"
deny_code="$(curl -sS -o /tmp/phase35-denied.json -w '%{http_code}' -X POST -H "Authorization: Bearer $REQUESTER_TOKEN" -H 'Content-Type: application/json' \
  -d '{"reason":"requester must not requeue integrity evidence"}' "$BASE_URL/api/v1/control-plane/siem/governance/dead/anchor/${dead_id}/requeue")"
[ "$deny_code" = "409" ]
grep -q 'incident-commander' /tmp/phase35-denied.json
[ "$(sql "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_siem_governance_remediations")" = "0" ]

phase="requeue without fixing root cause and prove fail-closed settlement"
curl -fsS -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' \
  -d '{"reason":"first remediation attempt before root cause repair"}' \
  "$BASE_URL/api/v1/control-plane/siem/governance/dead/anchor/${dead_id}/requeue" >/tmp/phase35-requeue-one.json
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_siem_governance_remediations WHERE kind='anchor' AND target_id=${dead_id} AND state='failed' AND operator_id='${COMMANDER}' AND result LIKE '%term history hash mismatch%'"
wait_sql "SELECT IF(anchor_state='dead',1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE id=${dead_id}"
governance_status >/tmp/phase35-status-still-fenced.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase35-status-still-fenced.json'))
assert d['fenced'] is True and d['dead'] >= 1 and d['open_remediations']==0, d
PY

phase="repair root cause, requeue again and prove resolved delivery"
docker compose exec -T mysql mysql -uroot -proot -e "UPDATE ${TEST_DB}.control_plane_siem_receiver_term_history SET record_hash='${original_hash}' WHERE id=${dead_id}" >/dev/null
curl -fsS -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' \
  -d '{"reason":"root cause repaired; retry immutable anchor"}' \
  "$BASE_URL/api/v1/control-plane/siem/governance/dead/anchor/${dead_id}/requeue" >/tmp/phase35-requeue-two.json
wait_sql "SELECT IF(anchor_state='delivered',1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE id=${dead_id}"
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_siem_term_ledger_anchors WHERE term=2 AND record_hash='${original_hash}'"
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_siem_governance_remediations WHERE kind='anchor' AND target_id=${dead_id} AND state='resolved' AND operator_id='${COMMANDER}'"
governance_status >/tmp/phase35-status-recovered.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase35-status-recovered.json'))
assert d['fenced'] is False and d['dead']==0 and d['open_remediations']==0, d
PY

phase="verify append-only remediation evidence and metrics"
curl -fsS -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/api/v1/control-plane/siem/governance/remediations?limit=10" >/tmp/phase35-remediations.json
python3 - <<'PY'
import json
items=json.load(open('/tmp/phase35-remediations.json'))['items']
mine=[x for x in items if x['kind']=='anchor' and x['operator_id']=='phase35-commander']
assert len(mine)==2, mine
assert {x['state'] for x in mine}=={'failed','resolved'}, mine
assert all(x['reason'] and x['previous_state']=='dead' for x in mine), mine
assert any('term history hash mismatch' in x.get('previous_error','') for x in mine), mine
PY
curl -fsS -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/metrics" >/tmp/phase35-metrics.txt
grep -q 'taskflow_control_plane_siem_governance_open_remediations 0' /tmp/phase35-metrics.txt
grep -q 'taskflow_control_plane_siem_governance_remediations_total{outcome="requeued"} 2' /tmp/phase35-metrics.txt
grep -q 'taskflow_control_plane_siem_governance_remediations_total{outcome="failed"} 1' /tmp/phase35-metrics.txt
grep -q 'taskflow_control_plane_siem_governance_remediations_total{outcome="resolved"} 1' /tmp/phase35-metrics.txt

phase="prove ordinary promotion is available only after remediation resolves"
wait_sql "SELECT IF(COUNT(*)=0,1,0) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE state <> 'delivered'"
curl -fsS -X POST -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/sync" >/dev/null
wait_receivers_stable_equal
curl -fsS -X POST -H "Authorization: Bearer $REQUESTER_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"target_receiver\":\"${PRIMARY_ID}\",\"expected_term\":2,\"reason\":\"post-remediation promotion\"}" \
  "$BASE_URL/api/v1/control-plane/siem/receivers/intents" >/tmp/phase35-intent.json
intent_id="$(python3 -c 'import json; print(json.load(open("/tmp/phase35-intent.json"))["id"])')"
curl -fsS -X POST -H "Authorization: Bearer $APPROVER_TOKEN" -H 'Content-Type: application/json' -d '{"note":"post-remediation approval"}' \
  "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_id}/approve" >/dev/null
wait_sql "SELECT IF(COUNT(*)=0,1,0) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE state <> 'delivered'"
curl -fsS -X POST -H "Authorization: Bearer $REQUESTER_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/sync" >/dev/null
wait_sql "SELECT IF(COUNT(*)=0,1,0) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE state <> 'delivered'"
wait_receivers_stable_equal
execute_code="$(curl -sS -o /tmp/phase35-execute.json -w '%{http_code}' -X POST -H "Authorization: Bearer $APPROVER_TOKEN" \
  "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_id}/execute")"
if [ "$execute_code" != "200" ]; then
  echo "post-remediation promotion returned HTTP $execute_code" >&2
  cat /tmp/phase35-execute.json >&2
  exit 1
fi
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase35-execute.json'))
assert d['active_receiver']=='receiver-primary' and d['term']==3, d
PY

echo "Phase 35 dead governance evidence listing and role-restricted remediation: ok"
echo "Phase 35 unresolved integrity fault stayed fail-closed with durable failed remediation evidence: ok"
echo "Phase 35 repaired evidence requeue resolved, cleared fence and restored ordinary promotion: ok"
