#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
ALT_URL="${PHASE32_ALT_URL:-http://127.0.0.1:18082}"
PRIMARY_URL="${PHASE32_PRIMARY_URL:-http://127.0.0.1:18084}"
SECONDARY_URL="${PHASE32_SECONDARY_URL:-http://127.0.0.1:18085}"
GOV_URL="${PHASE32_GOVERNANCE_URL:-http://127.0.0.1:19704}"

REQUEST_TOKEN="${PHASE32_REQUEST_TOKEN:-phase32-request-token}"
APPROVER_A_TOKEN="${PHASE32_APPROVER_A_TOKEN:-phase32-approve-a-token}"
APPROVER_B_TOKEN="${PHASE32_APPROVER_B_TOKEN:-phase32-approve-b-token}"
COMMANDER_TOKEN="${PHASE32_COMMANDER_TOKEN:-phase32-commander-token}"
REQUESTER="phase32-requester"
APPROVER_A="phase32-approver-a"
APPROVER_B="phase32-approver-b"
COMMANDER="phase32-commander"
EXTRA_CREDENTIALS="${APPROVER_B}=${APPROVER_B_TOKEN};${COMMANDER}=${COMMANDER_TOKEN}"
ROLE_POLICY="${REQUESTER}=requester|approver;${APPROVER_A}=approver;${APPROVER_B}=approver;${COMMANDER}=incident-commander"

SINK_TOKEN="${PHASE32_SINK_TOKEN:-phase32-sink-token}"
SIGN_KEY="${PHASE32_SIGNING_KEY:-phase32-signing-key}"
SIGN_KEY_ID="${PHASE32_SIGNING_KEY_ID:-phase32-sign-v1}"
CHECKPOINT_KEY="${PHASE32_CHECKPOINT_KEY:-phase32-checkpoint-secret}"
CHECKPOINT_KEY_ID="${PHASE32_CHECKPOINT_KEY_ID:-phase32-checkpoint-v1}"
GOV_TOKEN="${PHASE32_GOVERNANCE_TOKEN:-phase32-governance-token}"
GOV_SIGN_KEY="${PHASE32_GOVERNANCE_SIGNING_KEY:-phase32-governance-signing-key}"
GOV_SIGN_KEY_ID="${PHASE32_GOVERNANCE_SIGNING_KEY_ID:-phase32-governance-v1}"

PRIMARY_ID="receiver-primary"
SECONDARY_ID="receiver-secondary"
TEST_DB="${PHASE32_DB:-taskflow_phase32}"
TEST_DSN="taskflow:taskflow@tcp(mysql:3306)/${TEST_DB}?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
DEFAULT_DSN="taskflow:taskflow@tcp(mysql:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
QUEUE_KEY="${PHASE32_QUEUE_KEY:-taskflow:phase32:queue}"
ALT_CONTAINER="taskflow-phase32-alt"
NETWORK=""
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

checkpoint_hwm() {
  curl -fsS -H "Authorization: Bearer $SINK_TOKEN" "$1/checkpoint?after_sequence=0&limit=1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["high_watermark_sequence"])'
}

checkpoint_term() {
  curl -fsS -H "Authorization: Bearer $SINK_TOKEN" "$1/checkpoint?after_sequence=0&limit=1" | python3 -c 'import json,sys; d=json.load(sys.stdin); print("%s:%s" % (d["receiver_term"], d["active_receiver"]))'
}

wait_receivers_equal() {
  minimum="$1"; i=0; p=0; s=0
  while [ "$i" -lt 300 ]; do
    p="$(checkpoint_hwm "$PRIMARY_URL" 2>/dev/null || echo 0)"
    s="$(checkpoint_hwm "$SECONDARY_URL" 2>/dev/null || echo 0)"
    if [ "$p" = "$s" ] && [ "$p" -ge "$minimum" ]; then return 0; fi
    i=$((i + 1)); sleep 0.2
  done
  echo "receivers did not converge primary=$p secondary=$s minimum=$minimum" >&2
  return 1
}

wait_receivers_stable_equal() {
  i=0; stable=0; last=""; p=0; s=0
  while [ "$i" -lt 300 ]; do
    p="$(checkpoint_hwm "$PRIMARY_URL" 2>/dev/null || echo 0)"
    s="$(checkpoint_hwm "$SECONDARY_URL" 2>/dev/null || echo 0)"
    if [ "$p" = "$s" ] && [ "$p" -gt 0 ]; then
      if [ "$p" = "$last" ]; then
        stable=$((stable + 1))
      else
        stable=1
        last="$p"
      fi
      if [ "$stable" -ge 4 ]; then return 0; fi
    else
      stable=0
      last=""
    fi
    i=$((i + 1)); sleep 0.1
  done
  echo "receivers did not reach a stable equal high-watermark primary=$p secondary=$s" >&2
  return 1
}

wait_rpo_fence() {
  expected="$1"; i=0
  while [ "$i" -lt 150 ]; do
    if curl -fsS -H "Authorization: Bearer $REQUEST_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/status" >/tmp/phase32-status.json 2>/dev/null; then
      got="$(python3 -c 'import json; print(str(json.load(open("/tmp/phase32-status.json"))["rpo_fenced"]).lower())')"
      if [ "$got" = "$expected" ]; then return 0; fi
    fi
    i=$((i + 1)); sleep 0.2
  done
  echo "RPO fence did not reach expected=$expected" >&2
  cat /tmp/phase32-status.json >&2 2>/dev/null || true
  return 1
}

gov_json() {
  path="$1"
  curl -fsS -H "Authorization: Bearer $GOV_TOKEN" "$GOV_URL$path"
}

start_primary() {
  CONTROL_PLANE_AUTH_ENABLED=true \
    CONTROL_PLANE_BEARER_TOKEN="$REQUEST_TOKEN" \
    CONTROL_PLANE_PREVIOUS_BEARER_TOKEN="$APPROVER_A_TOKEN" \
    CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS="$EXTRA_CREDENTIALS" \
    CONTROL_PLANE_OPERATOR_ID="$REQUESTER" \
    CONTROL_PLANE_PREVIOUS_OPERATOR_ID="$APPROVER_A" \
    CONTROL_PLANE_AUDIT_ENABLED=true CONTROL_PLANE_AUDIT_RETENTION_ENABLED=false \
    CONTROL_PLANE_SIEM_DELIVERY_ENABLED=true \
    CONTROL_PLANE_SIEM_ENDPOINT=http://siem-sink:8080/events \
    CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
    CONTROL_PLANE_SIEM_SIGNING_PROVIDER=hmac \
    CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" \
    CONTROL_PLANE_SIEM_DELIVERY_INTERVAL_MS=100 CONTROL_PLANE_SIEM_DELIVERY_LEASE_MS=2000 \
    CONTROL_PLANE_SIEM_MAX_ATTEMPTS=30 CONTROL_PLANE_SIEM_RETRY_BASE_MS=100 CONTROL_PLANE_SIEM_RETRY_MAX_MS=500 \
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
    CONTROL_PLANE_SIEM_PROMOTION_APPROVAL_QUORUM=2 CONTROL_PLANE_SIEM_PROMOTION_ROLE_POLICY="$ROLE_POLICY" \
    CONTROL_PLANE_SIEM_BREAK_GLASS_ENABLED=true \
    CONTROL_PLANE_SIEM_GOVERNANCE_ENDPOINT=http://siem-governance:8080 \
    CONTROL_PLANE_SIEM_GOVERNANCE_BEARER_TOKEN="$GOV_TOKEN" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY="$GOV_SIGN_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY_ID="$GOV_SIGN_KEY_ID" \
    CONTROL_PLANE_SIEM_GOVERNANCE_TIMEOUT_MS=3000 CONTROL_PLANE_SIEM_RTO_ALERT_THRESHOLD_MS=1 \
    CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED=true CONTROL_PLANE_SIEM_RPO_MAX_SEQUENCES=1 CONTROL_PLANE_SIEM_RPO_GRACE_MS=300 \
    MYSQL_DSN="$TEST_DSN" REDIS_QUEUE_KEY="$QUEUE_KEY" \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null
}

start_alt() {
  docker rm -f "$ALT_CONTAINER" >/dev/null 2>&1 || true
  docker run -d --name "$ALT_CONTAINER" --network "$NETWORK" -p 127.0.0.1:18082:8080 \
    -e HTTP_ADDR=:8080 -e PPROF_ADDR=:6060 -e MYSQL_DSN="$TEST_DSN" -e REDIS_ADDR=redis:6379 -e REDIS_QUEUE_KEY="$QUEUE_KEY" \
    -e EXECUTION_BACKEND=grpc -e EXECUTOR_GRPC_TARGET=executor-worker:9091 \
    -e CONTROL_PLANE_AUTH_ENABLED=true -e CONTROL_PLANE_BEARER_TOKEN="$REQUEST_TOKEN" -e CONTROL_PLANE_PREVIOUS_BEARER_TOKEN="$APPROVER_A_TOKEN" \
    -e CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS="$EXTRA_CREDENTIALS" -e CONTROL_PLANE_OPERATOR_ID="$REQUESTER" -e CONTROL_PLANE_PREVIOUS_OPERATOR_ID="$APPROVER_A" \
    -e CONTROL_PLANE_AUDIT_ENABLED=true -e CONTROL_PLANE_AUDIT_RETENTION_ENABLED=false \
    -e CONTROL_PLANE_SIEM_DELIVERY_ENABLED=true -e CONTROL_PLANE_SIEM_ENDPOINT=http://siem-sink:8080/events -e CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
    -e CONTROL_PLANE_SIEM_SIGNING_PROVIDER=hmac -e CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" -e CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" \
    -e CONTROL_PLANE_SIEM_DELIVERY_INTERVAL_MS=600000 -e CONTROL_PLANE_SIEM_GC_ENABLED=false \
    -e CONTROL_PLANE_SIEM_PRIMARY_RECEIVER_ID="$PRIMARY_ID" -e CONTROL_PLANE_SIEM_PRIMARY_CHECKPOINT_ENDPOINT=http://siem-sink:8080/checkpoint -e CONTROL_PLANE_SIEM_PRIMARY_TERM_ENDPOINT=http://siem-sink:8080/control/term \
    -e CONTROL_PLANE_SIEM_SECONDARY_ENDPOINT=http://siem-sink-secondary:8080/events -e CONTROL_PLANE_SIEM_SECONDARY_RECEIVER_ID="$SECONDARY_ID" -e CONTROL_PLANE_SIEM_SECONDARY_CHECKPOINT_ENDPOINT=http://siem-sink-secondary:8080/checkpoint -e CONTROL_PLANE_SIEM_SECONDARY_TERM_ENDPOINT=http://siem-sink-secondary:8080/control/term \
    -e CONTROL_PLANE_SIEM_FAILOVER_ENABLED=false -e CONTROL_PLANE_SIEM_CLUSTER_GUARD_ENABLED=true -e CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" -e CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
    -e CONTROL_PLANE_SIEM_TERM_FENCING_ENABLED=true -e CONTROL_PLANE_SIEM_ANTI_ENTROPY_ENABLED=false \
    -e CONTROL_PLANE_SIEM_PROMOTION_WORKFLOW_ENABLED=true -e CONTROL_PLANE_SIEM_PROMOTION_APPROVAL_QUORUM=2 -e CONTROL_PLANE_SIEM_PROMOTION_ROLE_POLICY="$ROLE_POLICY" \
    -e CONTROL_PLANE_SIEM_BREAK_GLASS_ENABLED=true \
    -e CONTROL_PLANE_SIEM_GOVERNANCE_ENDPOINT=http://siem-governance:8080 -e CONTROL_PLANE_SIEM_GOVERNANCE_BEARER_TOKEN="$GOV_TOKEN" -e CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY="$GOV_SIGN_KEY" -e CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY_ID="$GOV_SIGN_KEY_ID" \
    -e CONTROL_PLANE_SIEM_RTO_ALERT_THRESHOLD_MS=1 -e CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED=false \
    -e QUEUE_DLQ_ARCHIVE_ENABLED=false -e ACCESS_LOG=false taskflow-go:local >/dev/null
  wait_http "$ALT_URL/health/ready"
}

cleanup() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "Phase 32 governance smoke failed during phase: $phase" >&2
    docker compose --profile phase32-smoke logs --tail 400 taskflow siem-sink siem-sink-secondary siem-governance >&2 2>/dev/null || true
    docker logs --tail 250 "$ALT_CONTAINER" >&2 2>/dev/null || true
  fi
  docker rm -f "$ALT_CONTAINER" >/dev/null 2>&1 || true
  CONTROL_PLANE_AUTH_ENABLED=false CONTROL_PLANE_BEARER_TOKEN= CONTROL_PLANE_PREVIOUS_BEARER_TOKEN= CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS= CONTROL_PLANE_OPERATOR_ID=operator CONTROL_PLANE_PREVIOUS_OPERATOR_ID= \
    CONTROL_PLANE_AUDIT_ENABLED=true CONTROL_PLANE_AUDIT_RETENTION_ENABLED=true CONTROL_PLANE_SIEM_DELIVERY_ENABLED=false CONTROL_PLANE_SIEM_SIGNING_PROVIDER=hmac \
    CONTROL_PLANE_SIEM_FAILOVER_ENABLED=false CONTROL_PLANE_SIEM_SHADOW_SECONDARY=false CONTROL_PLANE_SIEM_CLUSTER_GUARD_ENABLED=false CONTROL_PLANE_SIEM_TERM_FENCING_ENABLED=false \
    CONTROL_PLANE_SIEM_ANTI_ENTROPY_ENABLED=false CONTROL_PLANE_SIEM_PROMOTION_WORKFLOW_ENABLED=false CONTROL_PLANE_SIEM_BREAK_GLASS_ENABLED=false CONTROL_PLANE_SIEM_GOVERNANCE_ENDPOINT= \
    CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED=false CONTROL_PLANE_SIEM_RECONCILE_ENABLED=false CONTROL_PLANE_SIEM_DR_ENABLED=false \
    MYSQL_DSN="$DEFAULT_DSN" REDIS_QUEUE_KEY=taskflow:queue docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null 2>&1 || true
  docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}" >/dev/null 2>&1 || true
  docker compose --profile phase32-smoke rm -sf siem-sink siem-sink-secondary siem-governance >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT

phase="build Phase 32 image and static guardrails"
docker build --target build -t taskflow-go:phase32-build --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
docker build -t taskflow-go:local --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
python3 scripts/phase32-governance-check.py

phase="start receiver pair and independent governance anchor service"
CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
CONTROL_PLANE_SIEM_GOVERNANCE_BEARER_TOKEN="$GOV_TOKEN" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY="$GOV_SIGN_KEY" CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY_ID="$GOV_SIGN_KEY_ID" \
PHASE29_PRIMARY_RECEIVER_ID="$PRIMARY_ID" PHASE29_SECONDARY_RECEIVER_ID="$SECONDARY_ID" PHASE30_ACTIVE_RECEIVER="$PRIMARY_ID" \
  docker compose --profile phase32-smoke up -d mysql redis executor-worker siem-governance >/dev/null
CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
PHASE29_PRIMARY_RECEIVER_ID="$PRIMARY_ID" PHASE29_SECONDARY_RECEIVER_ID="$SECONDARY_ID" PHASE30_ACTIVE_RECEIVER="$PRIMARY_ID" \
  docker compose --profile phase32-smoke up -d --no-deps --force-recreate siem-sink siem-sink-secondary >/dev/null
wait_http "$PRIMARY_URL/health"; wait_http "$SECONDARY_URL/health"; wait_http "$GOV_URL/health"

phase="create isolated Phase 32 database and start governed control plane"
docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}; CREATE DATABASE ${TEST_DB}; GRANT ALL PRIVILEGES ON ${TEST_DB}.* TO 'taskflow'@'%'; FLUSH PRIVILEGES" >/dev/null
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null
mysql_id="$(docker compose ps -q mysql)"
NETWORK="$(docker inspect "$mysql_id" --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}')"
[ -n "$NETWORK" ]
start_primary; wait_http "$BASE_URL/health/ready"
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_siem_receiver_routing_state WHERE id=1 AND active_receiver='${PRIMARY_ID}' AND term=1"
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_siem_term_ledger_anchors WHERE term=1 AND LENGTH(record_hash)=64 AND LENGTH(receipt_hash)=64"
gov_json /stats >/tmp/phase32-gov-start.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase32-gov-start.json'))
assert d['anchors']==1 and d['signature_failures']==0 and d['auth_failures']==0, d
PY

phase="prove role policy and two-approver quorum across two TaskFlow instances"
start_alt
curl -fsS -X POST -H "Authorization: Bearer $REQUEST_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"target_receiver\":\"${SECONDARY_ID}\",\"expected_term\":1,\"reason\":\"phase32-quorum\"}" "$BASE_URL/api/v1/control-plane/siem/receivers/intents" >/tmp/phase32-intent1.json
intent_one="$(python3 -c 'import json; print(json.load(open("/tmp/phase32-intent1.json"))["id"])')"
self_code="$(curl -sS -o /tmp/phase32-self.json -w '%{http_code}' -X POST -H "Authorization: Bearer $REQUEST_TOKEN" -H 'Content-Type: application/json' -d '{"note":"self"}' "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_one}/approve")"
[ "$self_code" = "409" ]
commander_approve_code="$(curl -sS -o /tmp/phase32-commander-approve.json -w '%{http_code}' -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' -d '{"note":"wrong role"}' "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_one}/approve")"
[ "$commander_approve_code" = "409" ]; grep -qi 'lacks approver role' /tmp/phase32-commander-approve.json

curl -fsS -X POST -H "Authorization: Bearer $APPROVER_A_TOKEN" -H 'Content-Type: application/json' -d '{"note":"approval-a"}' "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_one}/approve" >/tmp/phase32-approve-a.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase32-approve-a.json'))
assert d['state']=='pending' and d['approval_count']==1 and d['required_approvals']==2, d
PY
early_execute_code="$(curl -sS -o /tmp/phase32-early-exec.json -w '%{http_code}' -X POST -H "Authorization: Bearer $APPROVER_A_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_one}/execute")"
[ "$early_execute_code" = "409" ]
duplicate_approve_code="$(curl -sS -o /tmp/phase32-duplicate-approve.json -w '%{http_code}' -X POST -H "Authorization: Bearer $APPROVER_A_TOKEN" -H 'Content-Type: application/json' -d '{"note":"duplicate"}' "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_one}/approve")"
[ "$duplicate_approve_code" = "409" ]

curl -fsS -X POST -H "Authorization: Bearer $APPROVER_B_TOKEN" -H 'Content-Type: application/json' -d '{"note":"approval-b"}' "$ALT_URL/api/v1/control-plane/siem/receivers/intents/${intent_one}/approve" >/tmp/phase32-approve-b.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase32-approve-b.json'))
assert d['state']=='approved' and d['approval_count']==2 and d['required_approvals']==2, d
assert set(d['approved_by'].split(','))=={'phase32-approver-a','phase32-approver-b'}, d
PY
curl -fsS -H "Authorization: Bearer $REQUEST_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_one}/approvals" >/tmp/phase32-approvals.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase32-approvals.json'))
assert len(d['items'])==2 and {x['operator_id'] for x in d['items']}=={'phase32-approver-a','phase32-approver-b'}, d
PY

# The second approval is itself a control-plane mutation and therefore advances
# the audit chain. Converge the standby on that final governance audit before
# attempting the safety-gated switchover.
wait_sql "SELECT IF(COUNT(*)=0,1,0) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE state <> 'delivered'"
curl -fsS -X POST -H "Authorization: Bearer $REQUEST_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/sync" >/tmp/phase32-pre-exec-sync.json
wait_sql "SELECT IF(COUNT(*)=0,1,0) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE state <> 'delivered'"
wait_receivers_stable_equal

phase="execute quorum-approved switchover and externally anchor term 2"
exec_code="$(curl -sS -o /tmp/phase32-exec1.json -w '%{http_code}' -X POST -H "Authorization: Bearer $APPROVER_B_TOKEN" "$ALT_URL/api/v1/control-plane/siem/receivers/intents/${intent_one}/execute")"
if [ "$exec_code" != "200" ]; then
  echo "promotion execute returned HTTP $exec_code" >&2
  cat /tmp/phase32-exec1.json >&2
  exit 1
fi
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase32-exec1.json'))
assert d['active_receiver']=='receiver-secondary' and d['term']==2 and d['anchored'] is True and d['anchor_id'], d
PY
wait_sql "SELECT IF(active_receiver='${SECONDARY_ID}' AND term=2,1,0) FROM ${TEST_DB}.control_plane_siem_receiver_routing_state WHERE id=1"
[ "$(checkpoint_term "$PRIMARY_URL")" = "2:${SECONDARY_ID}" ]; [ "$(checkpoint_term "$SECONDARY_URL")" = "2:${SECONDARY_ID}" ]
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=2 AND hash_version=2 AND action='operator-approved' AND approved_by LIKE '%${APPROVER_A}%' AND approved_by LIKE '%${APPROVER_B}%'"
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_siem_term_ledger_anchors WHERE term=2"
wait_sql "SELECT IF(COUNT(*)>=1,1,0) FROM ${TEST_DB}.control_plane_siem_incident_evidence WHERE kind='rto_breach' AND term=2"

phase="prove automated RPO SLO incident alerting and recovery evidence"
curl -fsS -X POST -H 'Content-Type: application/json' -d '{"fail_always":true}' "$PRIMARY_URL/admin/mode" >/dev/null
for request_id in phase32-rpo-1 phase32-rpo-2 phase32-rpo-3; do
  curl -fsS -H "Authorization: Bearer $REQUEST_TOKEN" -H "X-Request-ID: $request_id" "$BASE_URL/api/v1/ai/routing-policy" >/dev/null
  wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='${request_id}' AND r.receiver_id='${SECONDARY_ID}' AND r.receiver_term=2"
done
sleep 0.5
wait_rpo_fence true
wait_sql "SELECT IF(COUNT(*)>=1,1,0) FROM ${TEST_DB}.control_plane_siem_incident_evidence WHERE kind='rpo_breach' AND term=2 AND rpo_sequences>1"
curl -fsS -X POST -H 'Content-Type: application/json' -d '{"fail_always":false}' "$PRIMARY_URL/admin/mode" >/dev/null
i=0; sync_code=000
while [ "$i" -lt 100 ]; do
  sync_code="$(curl -sS -o /tmp/phase32-sync.json -w '%{http_code}' -X POST -H "Authorization: Bearer $REQUEST_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/sync")"
  if [ "$sync_code" = "200" ]; then break; fi
  i=$((i + 1)); sleep 0.2
done
[ "$sync_code" = "200" ]
secondary_hwm="$(checkpoint_hwm "$SECONDARY_URL")"; wait_receivers_equal "$secondary_hwm"; wait_rpo_fence false
wait_sql "SELECT IF(COUNT(*)>=1,1,0) FROM ${TEST_DB}.control_plane_siem_incident_evidence WHERE kind='rpo_recovered' AND term=2"

phase="prove break-glass role isolation and emergency promotion without quorum"
curl -fsS -X POST -H "Authorization: Bearer $REQUEST_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"target_receiver\":\"${PRIMARY_ID}\",\"expected_term\":2,\"reason\":\"pending-normal-failback\"}" "$BASE_URL/api/v1/control-plane/siem/receivers/intents" >/tmp/phase32-intent2.json
intent_two="$(python3 -c 'import json; print(json.load(open("/tmp/phase32-intent2.json"))["id"])')"
bad_break_code="$(curl -sS -o /tmp/phase32-bad-break.json -w '%{http_code}' -X POST -H "Authorization: Bearer $REQUEST_TOKEN" -H 'Content-Type: application/json' -d "{\"target_receiver\":\"${PRIMARY_ID}\",\"expected_term\":2,\"incident_id\":\"INC-32-001\",\"reason\":\"emergency failback\"}" "$BASE_URL/api/v1/control-plane/siem/receivers/break-glass")"
[ "$bad_break_code" = "409" ]; grep -qi 'incident-commander' /tmp/phase32-bad-break.json
break_code="$(curl -sS -o /tmp/phase32-break.json -w '%{http_code}' -X POST -H "Authorization: Bearer $COMMANDER_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"target_receiver\":\"${PRIMARY_ID}\",\"expected_term\":2,\"incident_id\":\"INC-32-001\",\"reason\":\"emergency failback\"}" "$BASE_URL/api/v1/control-plane/siem/receivers/break-glass")"
if [ "$break_code" != "200" ]; then
  echo "break-glass returned HTTP $break_code" >&2
  cat /tmp/phase32-break.json >&2
  exit 1
fi
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase32-break.json'))
assert d['active_receiver']=='receiver-primary' and d['term']==3, d
assert d['emergency'] is True and d['incident_id']=='INC-32-001' and d['anchored'] is True, d
PY
wait_sql "SELECT IF(state='cancelled',1,0) FROM ${TEST_DB}.control_plane_siem_promotion_intents WHERE id=${intent_two}"
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=3 AND action='break-glass' AND emergency=1 AND incident_id='INC-32-001' AND hash_version=2"
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_siem_term_ledger_anchors WHERE term=3"
wait_sql "SELECT IF(COUNT(*)>=1,1,0) FROM ${TEST_DB}.control_plane_siem_incident_evidence WHERE incident_id='INC-32-001' AND kind='break_glass' AND term=3"
[ "$(checkpoint_term "$PRIMARY_URL")" = "3:${PRIMARY_ID}" ]; [ "$(checkpoint_term "$SECONDARY_URL")" = "3:${PRIMARY_ID}" ]

phase="prove external anchor survives and exposes local term-ledger tampering"
curl -fsS -H "Authorization: Bearer $REQUEST_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/term-history" >/tmp/phase32-history-before.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase32-history-before.json'))
assert d['verification']['valid'] is True and d['verification']['last_term']==3, d
PY
gov_json /anchors >/tmp/phase32-external-anchors.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase32-external-anchors.json'))['items']
assert [x['term'] for x in d]==[1,2,3], d
assert all(len(x['record_hash'])==64 and len(x['receipt_hash'])==64 for x in d), d
PY
term3_hash="$(sql "SELECT record_hash FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=3")"
external_term3_hash="$(python3 -c 'import json; d=json.load(open("/tmp/phase32-external-anchors.json"))["items"]; print([x for x in d if x["term"]==3][0]["record_hash"])')"
[ "$term3_hash" = "$external_term3_hash" ]
docker compose exec -T mysql mysql -uroot -proot "$TEST_DB" -e "UPDATE control_plane_siem_receiver_term_history SET reason='tampered-phase32' WHERE term=3" >/dev/null
history_code="$(curl -sS -o /tmp/phase32-history-tampered.json -w '%{http_code}' -H "Authorization: Bearer $REQUEST_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/term-history")"
[ "$history_code" = "409" ]
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase32-history-tampered.json'))
assert d['verification']['valid'] is False and d['verification']['failed_term']==3, d
PY
anchor_code="$(curl -sS -o /tmp/phase32-anchor-tampered.json -w '%{http_code}' -X POST -H "Authorization: Bearer $APPROVER_A_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/term-history/anchor")"
[ "$anchor_code" = "503" ]; grep -qi 'tampered' /tmp/phase32-anchor-tampered.json
gov_json /anchors >/tmp/phase32-external-anchors-after.json
external_term3_after="$(python3 -c 'import json; d=json.load(open("/tmp/phase32-external-anchors-after.json"))["items"]; print([x for x in d if x["term"]==3][0]["record_hash"])')"
[ "$external_term3_after" = "$external_term3_hash" ]

phase="verify governance signatures, incident alerts and bounded metrics"
gov_json /stats >/tmp/phase32-gov-final.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase32-gov-final.json'))
assert d['anchors']==3 and d['anchor_conflicts']==0, d
assert d['alerts']>=3 and d['alert_requests']>=3, d
assert d['signature_failures']==0 and d['auth_failures']==0, d
PY
curl -fsS "$BASE_URL/metrics" >/tmp/phase32-metrics.txt
curl -fsS "$ALT_URL/metrics" >/tmp/phase32-alt-metrics.txt
python3 - <<'PY'
import re
primary=open('/tmp/phase32-metrics.txt').read()
alternate=open('/tmp/phase32-alt-metrics.txt').read()
import json
governance=json.load(open('/tmp/phase32-gov-final.json'))
def total(text, prefix):
    values=[]
    for line in text.splitlines():
        if line.startswith(prefix):
            try: values.append(float(line.rsplit(' ',1)[1]))
            except Exception: pass
    return sum(values)
cluster=primary+'\n'+alternate
assert total(primary, 'taskflow_control_plane_siem_break_glass_total{outcome="executed"}') >= 1
assert total(cluster, 'taskflow_control_plane_siem_term_anchors_total{outcome="anchored"}') >= 3
delivered=total(cluster, 'taskflow_control_plane_siem_incident_alerts_total{outcome="delivered"}')
assert governance['alert_requests'] >= 4 and governance['alerts'] == governance['alert_requests'], governance
assert delivered == governance['alert_requests'], (delivered, governance)
PY

echo "Phase 32 break-glass, approval quorum, signed external anchors and SLO incident guardrails: ok"
echo "Phase 32 four-operator role governance and cross-instance approval quorum: ok"
echo "Phase 32 external term anchor exposed local ledger tampering and incident alerts were receiver-independent: ok"
