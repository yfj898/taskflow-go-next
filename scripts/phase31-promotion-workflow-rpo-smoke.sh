#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
ALT_URL="${PHASE31_ALT_URL:-http://127.0.0.1:18081}"
PRIMARY_URL="${PHASE31_PRIMARY_URL:-http://127.0.0.1:18084}"
SECONDARY_URL="${PHASE31_SECONDARY_URL:-http://127.0.0.1:18085}"
REQUEST_TOKEN="${PHASE31_REQUEST_TOKEN:-phase31-request-token}"
APPROVE_TOKEN="${PHASE31_APPROVE_TOKEN:-phase31-approve-token}"
REQUESTER="${PHASE31_REQUESTER:-phase31-requester}"
APPROVER="${PHASE31_APPROVER:-phase31-approver}"
SINK_TOKEN="${PHASE31_SINK_TOKEN:-phase31-sink-token}"
SIGN_KEY="${PHASE31_SIGNING_KEY:-phase31-signing-key}"
SIGN_KEY_ID="${PHASE31_SIGNING_KEY_ID:-phase31-sign-v1}"
CHECKPOINT_KEY="${PHASE31_CHECKPOINT_KEY:-phase31-checkpoint-secret}"
CHECKPOINT_KEY_ID="${PHASE31_CHECKPOINT_KEY_ID:-phase31-checkpoint-v1}"
PRIMARY_ID="${PHASE31_PRIMARY_RECEIVER_ID:-receiver-primary}"
SECONDARY_ID="${PHASE31_SECONDARY_RECEIVER_ID:-receiver-secondary}"
TEST_DB="${PHASE31_DB:-taskflow_phase31}"
TEST_DSN="taskflow:taskflow@tcp(mysql:3306)/${TEST_DB}?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
DEFAULT_DSN="taskflow:taskflow@tcp(mysql:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
QUEUE_KEY="${PHASE31_QUEUE_KEY:-taskflow:phase31:queue}"
ALT_CONTAINER="taskflow-phase31-alt"
NETWORK=""
phase="initialization"

wait_http() {
  url="$1"
  i=0
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

wait_rpo_fence() {
  expected="$1"; token="$2"; i=0
  while [ "$i" -lt 100 ]; do
    if curl -fsS -H "Authorization: Bearer $token" "$BASE_URL/api/v1/control-plane/siem/receivers/status" >/tmp/phase31-status.json 2>/dev/null; then
      got="$(python3 -c 'import json; print(str(json.load(open("/tmp/phase31-status.json"))["rpo_fenced"]).lower())')"
      if [ "$got" = "$expected" ]; then return 0; fi
    fi
    i=$((i + 1)); sleep 0.2
  done
  echo "RPO fence did not reach expected=$expected" >&2
  cat /tmp/phase31-status.json >&2 2>/dev/null || true
  return 1
}

start_primary() {
  CONTROL_PLANE_AUTH_ENABLED=true \
    CONTROL_PLANE_BEARER_TOKEN="$REQUEST_TOKEN" \
    CONTROL_PLANE_PREVIOUS_BEARER_TOKEN="$APPROVE_TOKEN" \
    CONTROL_PLANE_OPERATOR_ID="$REQUESTER" \
    CONTROL_PLANE_PREVIOUS_OPERATOR_ID="$APPROVER" \
    CONTROL_PLANE_AUDIT_ENABLED=true \
    CONTROL_PLANE_AUDIT_RETENTION_ENABLED=false \
    CONTROL_PLANE_SIEM_DELIVERY_ENABLED=true \
    CONTROL_PLANE_SIEM_ENDPOINT=http://siem-sink:8080/events \
    CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
    CONTROL_PLANE_SIEM_SIGNING_PROVIDER=hmac \
    CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" \
    CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" \
    CONTROL_PLANE_SIEM_DELIVERY_INTERVAL_MS=100 \
    CONTROL_PLANE_SIEM_DELIVERY_LEASE_MS=2000 \
    CONTROL_PLANE_SIEM_MAX_ATTEMPTS=30 \
    CONTROL_PLANE_SIEM_RETRY_BASE_MS=100 \
    CONTROL_PLANE_SIEM_RETRY_MAX_MS=500 \
    CONTROL_PLANE_SIEM_DISTRIBUTED_COORDINATION=false \
    CONTROL_PLANE_SIEM_GC_ENABLED=false \
    CONTROL_PLANE_SIEM_DR_ENABLED=false \
    CONTROL_PLANE_SIEM_PRIMARY_RECEIVER_ID="$PRIMARY_ID" \
    CONTROL_PLANE_SIEM_PRIMARY_CHECKPOINT_ENDPOINT=http://siem-sink:8080/checkpoint \
    CONTROL_PLANE_SIEM_PRIMARY_TERM_ENDPOINT=http://siem-sink:8080/control/term \
    CONTROL_PLANE_SIEM_SECONDARY_ENDPOINT=http://siem-sink-secondary:8080/events \
    CONTROL_PLANE_SIEM_SECONDARY_RECEIVER_ID="$SECONDARY_ID" \
    CONTROL_PLANE_SIEM_SECONDARY_CHECKPOINT_ENDPOINT=http://siem-sink-secondary:8080/checkpoint \
    CONTROL_PLANE_SIEM_SECONDARY_TERM_ENDPOINT=http://siem-sink-secondary:8080/control/term \
    CONTROL_PLANE_SIEM_FAILOVER_ENABLED=true \
    CONTROL_PLANE_SIEM_SHADOW_SECONDARY=false \
    CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" \
    CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
    CONTROL_PLANE_SIEM_CLUSTER_GUARD_ENABLED=true \
    CONTROL_PLANE_SIEM_TERM_FENCING_ENABLED=true \
    CONTROL_PLANE_SIEM_ANTI_ENTROPY_ENABLED=true \
    CONTROL_PLANE_SIEM_ANTI_ENTROPY_INTERVAL_MS=100 \
    CONTROL_PLANE_SIEM_PROMOTION_WORKFLOW_ENABLED=true \
    CONTROL_PLANE_SIEM_PROMOTION_INTENT_TTL_MS=60000 \
    CONTROL_PLANE_SIEM_PROMOTION_LEASE_MS=5000 \
    CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED=true \
    CONTROL_PLANE_SIEM_RPO_MAX_SEQUENCES=1 \
    CONTROL_PLANE_SIEM_RPO_GRACE_MS=300 \
    MYSQL_DSN="$TEST_DSN" REDIS_QUEUE_KEY="$QUEUE_KEY" \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null
}

start_alt() {
  docker rm -f "$ALT_CONTAINER" >/dev/null 2>&1 || true
  docker run -d --name "$ALT_CONTAINER" --network "$NETWORK" -p 127.0.0.1:18081:8080 \
    -e HTTP_ADDR=:8080 -e PPROF_ADDR=:6060 -e MYSQL_DSN="$TEST_DSN" -e REDIS_ADDR=redis:6379 -e REDIS_QUEUE_KEY="$QUEUE_KEY" \
    -e EXECUTION_BACKEND=grpc -e EXECUTOR_GRPC_TARGET=executor-worker:9091 \
    -e CONTROL_PLANE_AUTH_ENABLED=true -e CONTROL_PLANE_BEARER_TOKEN="$REQUEST_TOKEN" -e CONTROL_PLANE_PREVIOUS_BEARER_TOKEN="$APPROVE_TOKEN" \
    -e CONTROL_PLANE_OPERATOR_ID="$REQUESTER" -e CONTROL_PLANE_PREVIOUS_OPERATOR_ID="$APPROVER" \
    -e CONTROL_PLANE_AUDIT_ENABLED=true -e CONTROL_PLANE_AUDIT_RETENTION_ENABLED=false \
    -e CONTROL_PLANE_SIEM_DELIVERY_ENABLED=true -e CONTROL_PLANE_SIEM_ENDPOINT=http://siem-sink:8080/events -e CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
    -e CONTROL_PLANE_SIEM_SIGNING_PROVIDER=hmac -e CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" -e CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" \
    -e CONTROL_PLANE_SIEM_DELIVERY_INTERVAL_MS=600000 -e CONTROL_PLANE_SIEM_DELIVERY_LEASE_MS=2000 -e CONTROL_PLANE_SIEM_MAX_ATTEMPTS=30 -e CONTROL_PLANE_SIEM_GC_ENABLED=false \
    -e CONTROL_PLANE_SIEM_PRIMARY_RECEIVER_ID="$PRIMARY_ID" -e CONTROL_PLANE_SIEM_PRIMARY_CHECKPOINT_ENDPOINT=http://siem-sink:8080/checkpoint -e CONTROL_PLANE_SIEM_PRIMARY_TERM_ENDPOINT=http://siem-sink:8080/control/term \
    -e CONTROL_PLANE_SIEM_SECONDARY_ENDPOINT=http://siem-sink-secondary:8080/events -e CONTROL_PLANE_SIEM_SECONDARY_RECEIVER_ID="$SECONDARY_ID" -e CONTROL_PLANE_SIEM_SECONDARY_CHECKPOINT_ENDPOINT=http://siem-sink-secondary:8080/checkpoint -e CONTROL_PLANE_SIEM_SECONDARY_TERM_ENDPOINT=http://siem-sink-secondary:8080/control/term \
    -e CONTROL_PLANE_SIEM_FAILOVER_ENABLED=false -e CONTROL_PLANE_SIEM_SHADOW_SECONDARY=false -e CONTROL_PLANE_SIEM_CLUSTER_GUARD_ENABLED=true \
    -e CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" -e CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
    -e CONTROL_PLANE_SIEM_TERM_FENCING_ENABLED=true -e CONTROL_PLANE_SIEM_ANTI_ENTROPY_ENABLED=false \
    -e CONTROL_PLANE_SIEM_PROMOTION_WORKFLOW_ENABLED=true -e CONTROL_PLANE_SIEM_PROMOTION_INTENT_TTL_MS=60000 -e CONTROL_PLANE_SIEM_PROMOTION_LEASE_MS=5000 \
    -e CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED=false -e QUEUE_DLQ_ARCHIVE_ENABLED=false -e ACCESS_LOG=false taskflow-go:local >/dev/null
  wait_http "$ALT_URL/health/ready"
}

cleanup() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "Phase 31 promotion workflow/RPO smoke failed during phase: $phase" >&2
    docker compose --profile phase31-smoke logs --tail 350 taskflow siem-sink siem-sink-secondary >&2 2>/dev/null || true
    docker logs --tail 250 "$ALT_CONTAINER" >&2 2>/dev/null || true
  fi
  docker rm -f "$ALT_CONTAINER" >/dev/null 2>&1 || true
  CONTROL_PLANE_AUTH_ENABLED=false CONTROL_PLANE_BEARER_TOKEN= CONTROL_PLANE_PREVIOUS_BEARER_TOKEN= CONTROL_PLANE_OPERATOR_ID=operator CONTROL_PLANE_PREVIOUS_OPERATOR_ID= \
    CONTROL_PLANE_AUDIT_ENABLED=true CONTROL_PLANE_AUDIT_RETENTION_ENABLED=true CONTROL_PLANE_SIEM_DELIVERY_ENABLED=false CONTROL_PLANE_SIEM_SIGNING_PROVIDER=hmac \
    CONTROL_PLANE_SIEM_FAILOVER_ENABLED=false CONTROL_PLANE_SIEM_SHADOW_SECONDARY=false CONTROL_PLANE_SIEM_CLUSTER_GUARD_ENABLED=false CONTROL_PLANE_SIEM_TERM_FENCING_ENABLED=false \
    CONTROL_PLANE_SIEM_ANTI_ENTROPY_ENABLED=false CONTROL_PLANE_SIEM_PROMOTION_WORKFLOW_ENABLED=false CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED=false CONTROL_PLANE_SIEM_RECONCILE_ENABLED=false CONTROL_PLANE_SIEM_DR_ENABLED=false \
    MYSQL_DSN="$DEFAULT_DSN" REDIS_QUEUE_KEY=taskflow:queue docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null 2>&1 || true
  docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}" >/dev/null 2>&1 || true
  docker compose --profile phase31-smoke rm -sf siem-sink siem-sink-secondary >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT

phase="build Phase 31 image and static guardrails"
docker build --target build -t taskflow-go:phase31-build --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
docker build -t taskflow-go:local --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
python3 scripts/phase31-promotion-workflow-rpo-check.py

phase="start independent signed-checkpoint receivers"
CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
PHASE29_PRIMARY_RECEIVER_ID="$PRIMARY_ID" PHASE29_SECONDARY_RECEIVER_ID="$SECONDARY_ID" PHASE30_ACTIVE_RECEIVER="$PRIMARY_ID" \
  docker compose --profile phase31-smoke up -d mysql redis executor-worker >/dev/null
CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGN_KEY" CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGN_KEY_ID" CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY="$CHECKPOINT_KEY" CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID="$CHECKPOINT_KEY_ID" \
PHASE29_PRIMARY_RECEIVER_ID="$PRIMARY_ID" PHASE29_SECONDARY_RECEIVER_ID="$SECONDARY_ID" PHASE30_ACTIVE_RECEIVER="$PRIMARY_ID" \
  docker compose --profile phase31-smoke up -d --no-deps --force-recreate siem-sink siem-sink-secondary >/dev/null
wait_http "$PRIMARY_URL/health"; wait_http "$SECONDARY_URL/health"

phase="create isolated Phase 31 database"
docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}; CREATE DATABASE ${TEST_DB}; GRANT ALL PRIVILEGES ON ${TEST_DB}.* TO 'taskflow'@'%'; FLUSH PRIVILEGES" >/dev/null
docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null
mysql_id="$(docker compose ps -q mysql)"
NETWORK="$(docker inspect "$mysql_id" --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}')"
[ -n "$NETWORK" ]

phase="start Phase 31 primary control-plane instance"
start_primary; wait_http "$BASE_URL/health/ready"
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_siem_receiver_routing_state WHERE id=1 AND active_receiver='${PRIMARY_ID}' AND term=1"
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE term=1 AND LENGTH(record_hash)=64"

phase="prove durable RPO SLO fencing"
curl -fsS -X POST -H 'Content-Type: application/json' -d '{"fail_always":true}' "$SECONDARY_URL/admin/mode" >/dev/null
for request_id in phase31-rpo-1 phase31-rpo-2 phase31-rpo-3; do
  curl -fsS -H "Authorization: Bearer $REQUEST_TOKEN" -H "X-Request-ID: $request_id" "$BASE_URL/api/v1/ai/routing-policy" >/dev/null
  wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='${request_id}' AND r.receiver_id='${PRIMARY_ID}' AND r.receiver_term=1"
done
sleep 0.5
wait_rpo_fence true "$REQUEST_TOKEN"
wait_sql "SELECT IF(rpo_fenced=1,1,0) FROM ${TEST_DB}.control_plane_siem_receiver_routing_state WHERE id=1"

phase="prove two-person approval and multi-instance execution lease"
curl -fsS -X POST -H "Authorization: Bearer $REQUEST_TOKEN" -H 'Content-Type: application/json' -H 'X-Request-ID: phase31-intent-request' \
  -d "{\"target_receiver\":\"${SECONDARY_ID}\",\"expected_term\":1,\"reason\":\"phase31-rpo-recovery\"}" "$BASE_URL/api/v1/control-plane/siem/receivers/intents" >/tmp/phase31-intent1.json
intent_one="$(python3 -c 'import json; print(json.load(open("/tmp/phase31-intent1.json"))["id"])')"
self_approve_code="$(curl -sS -o /tmp/phase31-self-approve.json -w '%{http_code}' -X POST -H "Authorization: Bearer $REQUEST_TOKEN" -H 'Content-Type: application/json' -d '{"note":"self approval must fail"}' "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_one}/approve")"
[ "$self_approve_code" = "409" ]
competing_code="$(curl -sS -o /tmp/phase31-competing.json -w '%{http_code}' -X POST -H "Authorization: Bearer $REQUEST_TOKEN" -H 'Content-Type: application/json' -d "{\"target_receiver\":\"${SECONDARY_ID}\",\"expected_term\":1,\"reason\":\"competing\"}" "$BASE_URL/api/v1/control-plane/siem/receivers/intents")"
[ "$competing_code" = "409" ]
curl -fsS -X POST -H "Authorization: Bearer $APPROVE_TOKEN" -H 'Content-Type: application/json' -d '{"note":"independent approval"}' "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_one}/approve" >/tmp/phase31-approved1.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase31-approved1.json'))
assert d['state']=='approved' and d['requested_by']=='phase31-requester' and d['approved_by']=='phase31-approver', d
PY

fenced_execute_code="$(curl -sS -o /tmp/phase31-fenced-execute.json -w '%{http_code}' -X POST -H "Authorization: Bearer $APPROVE_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_one}/execute")"
[ "$fenced_execute_code" = "409" ]; grep -qi 'RPO policy' /tmp/phase31-fenced-execute.json
direct_promote_code="$(curl -sS -o /tmp/phase31-direct-promote.json -w '%{http_code}' -X POST -H "Authorization: Bearer $APPROVE_TOKEN" -H 'Content-Type: application/json' -d "{\"target_receiver\":\"${SECONDARY_ID}\",\"expected_term\":1}" "$BASE_URL/api/v1/control-plane/siem/receivers/promote")"
[ "$direct_promote_code" = "409" ]; grep -qi 'approved promotion intent' /tmp/phase31-direct-promote.json

phase="recover standby and clear durable RPO fence"
curl -fsS -X POST -H 'Content-Type: application/json' -d '{"fail_always":false}' "$SECONDARY_URL/admin/mode" >/dev/null
i=0; sync_code=000
while [ "$i" -lt 100 ]; do
  sync_code="$(curl -sS -o /tmp/phase31-sync.json -w '%{http_code}' -X POST -H "Authorization: Bearer $REQUEST_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/sync")"
  if [ "$sync_code" = "200" ]; then break; fi
  i=$((i + 1)); sleep 0.2
done
[ "$sync_code" = "200" ]
primary_hwm="$(checkpoint_hwm "$PRIMARY_URL")"; wait_receivers_equal "$primary_hwm"; wait_rpo_fence false "$REQUEST_TOKEN"

phase="start second TaskFlow instance sharing promotion coordination state"
start_alt

phase="race approved intent execution across two TaskFlow instances"
(curl -sS -o /tmp/phase31-exec-a.json -w '%{http_code}' -X POST -H "Authorization: Bearer $APPROVE_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_one}/execute" >/tmp/phase31-exec-a.code) & pid_a=$!
(curl -sS -o /tmp/phase31-exec-b.json -w '%{http_code}' -X POST -H "Authorization: Bearer $APPROVE_TOKEN" "$ALT_URL/api/v1/control-plane/siem/receivers/intents/${intent_one}/execute" >/tmp/phase31-exec-b.code) & pid_b=$!
wait "$pid_a"; wait "$pid_b"
python3 - <<'PY'
codes=sorted([open('/tmp/phase31-exec-a.code').read().strip(),open('/tmp/phase31-exec-b.code').read().strip()])
assert codes==['200','409'], codes
PY
wait_sql "SELECT IF(active_receiver='${SECONDARY_ID}' AND term=2 AND transition_token='',1,0) FROM ${TEST_DB}.control_plane_siem_receiver_routing_state WHERE id=1"
wait_sql "SELECT IF(state='executed' AND result_term=2 AND requested_by='${REQUESTER}' AND approved_by='${APPROVER}',1,0) FROM ${TEST_DB}.control_plane_siem_promotion_intents WHERE id=${intent_one}"
[ "$(checkpoint_term "$PRIMARY_URL")" = "2:${SECONDARY_ID}" ]; [ "$(checkpoint_term "$SECONDARY_URL")" = "2:${SECONDARY_ID}" ]
[ "$(curl -fsS "$PRIMARY_URL/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["term_updates"])')" = 1 ]
[ "$(curl -fsS "$SECONDARY_URL/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["term_updates"])')" = 1 ]

phase="prove receiver rejects same-term conflicting active assignment"
term_conflict_code="$(curl -sS -o /tmp/phase31-term-conflict.json -w '%{http_code}' -X POST -H "Authorization: Bearer $SINK_TOKEN" -H 'Content-Type: application/json' -d "{\"term\":2,\"active_receiver\":\"${PRIMARY_ID}\"}" "$PRIMARY_URL/control/term")"
[ "$term_conflict_code" = "409" ]
[ "$(curl -fsS "$PRIMARY_URL/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["term_conflict_rejections"])')" -ge 1 ]

phase="perform approved controlled failback at term 3"
current_hwm="$(checkpoint_hwm "$SECONDARY_URL")"; wait_receivers_equal "$current_hwm"
curl -fsS -X POST -H "Authorization: Bearer $REQUEST_TOKEN" -H 'Content-Type: application/json' -H 'X-Request-ID: phase31-failback-request' \
  -d "{\"target_receiver\":\"${PRIMARY_ID}\",\"expected_term\":2,\"reason\":\"phase31-controlled-failback\"}" "$BASE_URL/api/v1/control-plane/siem/receivers/intents" >/tmp/phase31-intent2.json
intent_two="$(python3 -c 'import json; print(json.load(open("/tmp/phase31-intent2.json"))["id"])')"
curl -fsS -X POST -H "Authorization: Bearer $APPROVE_TOKEN" -H 'Content-Type: application/json' -d '{"note":"approve failback"}' "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_two}/approve" >/dev/null
wait_sql "SELECT IF(COUNT(*)=0,1,0) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE state <> 'delivered'"
curl -fsS -X POST -H "Authorization: Bearer $REQUEST_TOKEN" -H 'X-Request-ID: phase31-pre-failback-sync' "$BASE_URL/api/v1/control-plane/siem/receivers/sync" >/tmp/phase31-pre-failback-sync.json
wait_sql "SELECT IF(COUNT(*)=1,1,0) FROM ${TEST_DB}.control_plane_audit_delivery_receipts r JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=r.chain_sequence WHERE a.request_id='phase31-pre-failback-sync' AND r.receiver_id='${SECONDARY_ID}' AND r.receiver_term=2"
pre_failback_sync_sequence="$(sql "SELECT chain_sequence FROM ${TEST_DB}.control_plane_audits WHERE request_id='phase31-pre-failback-sync' ORDER BY chain_sequence DESC LIMIT 1")"
[ -n "$pre_failback_sync_sequence" ]
wait_receivers_equal "$pre_failback_sync_sequence"
curl -fsS -X POST -H "Authorization: Bearer $APPROVE_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/intents/${intent_two}/execute" >/tmp/phase31-failback.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase31-failback.json'))
assert d['active_receiver']=='receiver-primary', d
assert d['previous_term']==2 and d['term']==3, d
assert d['promoted_by']=='phase31-approver', d
PY
wait_sql "SELECT IF(active_receiver='${PRIMARY_ID}' AND term=3 AND transition_token='',1,0) FROM ${TEST_DB}.control_plane_siem_receiver_routing_state WHERE id=1"
[ "$(checkpoint_term "$PRIMARY_URL")" = "3:${PRIMARY_ID}" ]; [ "$(checkpoint_term "$SECONDARY_URL")" = "3:${PRIMARY_ID}" ]
[ "$(curl -fsS "$PRIMARY_URL/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["term_updates"])')" = 2 ]
[ "$(curl -fsS "$SECONDARY_URL/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["term_updates"])')" = 2 ]

phase="verify append-only hash-chained term ledger"
wait_sql "SELECT IF(COUNT(*)=3,1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history"
wait_sql "SELECT IF(COUNT(*)=2,1,0) FROM ${TEST_DB}.control_plane_siem_receiver_term_history WHERE action='operator-approved' AND LENGTH(record_hash)=64"
curl -fsS -H "Authorization: Bearer $REQUEST_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/term-history?limit=100" >/tmp/phase31-history.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase31-history.json'))
assert d['verification']['valid'] is True, d
items={int(row['term']):row for row in d['items']}
assert items[2]['previous_term']==1 and items[2]['previous_receiver']=='receiver-primary', items[2]
assert items[2]['active_receiver']=='receiver-secondary' and items[2]['action']=='operator-approved', items[2]
assert items[2]['requested_by']=='phase31-requester' and items[2]['approved_by']=='phase31-approver', items[2]
assert items[3]['previous_term']==2 and items[3]['previous_receiver']=='receiver-secondary', items[3]
assert items[3]['active_receiver']=='receiver-primary', items[3]
PY

phase="prove term-history tamper detection"
docker compose exec -T mysql mysql -utaskflow -ptaskflow "$TEST_DB" -e "UPDATE control_plane_siem_receiver_term_history SET reason='tampered' WHERE term=2" >/dev/null
tamper_code="$(curl -sS -o /tmp/phase31-history-tampered.json -w '%{http_code}' -H "Authorization: Bearer $REQUEST_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/term-history?limit=100")"
[ "$tamper_code" = "409" ]
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase31-history-tampered.json'))
assert d['verification']['valid'] is False and d['verification']['failed_term']==2, d
PY
docker compose exec -T mysql mysql -utaskflow -ptaskflow "$TEST_DB" -e "UPDATE control_plane_siem_receiver_term_history SET reason='phase31-rpo-recovery' WHERE term=2" >/dev/null
curl -fsS -H "Authorization: Bearer $REQUEST_TOKEN" "$BASE_URL/api/v1/control-plane/siem/receivers/term-history?limit=100" >/tmp/phase31-history-restored.json
python3 - <<'PY'
import json
assert json.load(open('/tmp/phase31-history-restored.json'))['verification']['valid'] is True
PY

phase="verify Phase 31 bounded metrics and final RPO convergence"
final_hwm="$(checkpoint_hwm "$PRIMARY_URL")"; wait_receivers_equal "$final_hwm"; wait_rpo_fence false "$REQUEST_TOKEN"
curl -fsS "$BASE_URL/metrics" >/tmp/phase31-metrics.txt
python3 - <<'PY'
import re
text=open('/tmp/phase31-metrics.txt').read()
def scalar(name):
    m=re.search(r'^'+re.escape(name)+r'\s+([0-9.eE+-]+)$',text,re.M)
    assert m,name
    return float(m.group(1))
assert scalar('taskflow_control_plane_siem_rpo_fenced')==0
assert scalar('taskflow_control_plane_siem_rpo_breaches_total')>=1
assert scalar('taskflow_control_plane_siem_receiver_term')==3
assert scalar('taskflow_control_plane_siem_receiver_rpo_sequences')<=1
assert scalar('taskflow_control_plane_siem_term_history_integrity')==1
assert 'taskflow_control_plane_siem_promotion_intents_total{outcome="requested"}' in text
assert 'taskflow_control_plane_siem_promotion_intents_total{outcome="approved"}' in text
assert 'taskflow_control_plane_siem_promotion_execution_total{outcome="rpo_fenced"}' in text
assert 'taskflow_control_plane_siem_promotion_execution_total{outcome="executed"}' in text
PY

python3 scripts/phase31-promotion-workflow-rpo-check.py
echo "Phase 31 durable RPO SLO fencing and recovery convergence: ok"
echo "Phase 31 two-person promotion approval and multi-instance execution lease: ok"
echo "Phase 31 immutable hash-chained receiver term ledger and tamper detection: ok"
