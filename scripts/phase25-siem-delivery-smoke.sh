#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
SIEM_URL="${PHASE25_SIEM_URL:-http://127.0.0.1:18084}"
TOKEN="${PHASE25_TOKEN:-phase25-control-token}"
OPERATOR="${PHASE25_OPERATOR:-security-delivery-admin}"
SINK_TOKEN="${PHASE25_SINK_TOKEN:-phase25-sink-token}"
SIGNING_KEY="${PHASE25_SIGNING_KEY:-phase25-signing-key}"
SIGNING_KEY_ID="${PHASE25_SIGNING_KEY_ID:-phase25-key-v1}"
TEST_DB="${PHASE25_DB:-taskflow_phase25}"
TEST_DSN="taskflow:taskflow@tcp(mysql:3306)/${TEST_DB}?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
DEFAULT_DSN="taskflow:taskflow@tcp(mysql:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
phase="initialization"

wait_ready() {
  i=0
  while [ "$i" -lt 50 ]; do
    if curl -fsS "$BASE_URL/health/ready" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

wait_siem() {
  i=0
  while [ "$i" -lt 50 ]; do
    if curl -fsS "$SIEM_URL/health" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

wait_sql_value() {
  sql="$1"
  predicate="$2"
  i=0
  while [ "$i" -lt 80 ]; do
    value="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse "$sql" 2>/dev/null || true)"
    if [ -n "$value" ] && sh -c "[ \"$value\" $predicate ]"; then
      printf '%s' "$value"
      return 0
    fi
    i=$((i + 1))
    sleep 0.25
  done
  return 1
}

cleanup() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "Phase 25 SIEM delivery smoke failed during phase: $phase" >&2
    docker compose logs --tail 200 taskflow siem-sink >&2 2>/dev/null || true
  fi
  CONTROL_PLANE_AUTH_ENABLED=false \
    CONTROL_PLANE_BEARER_TOKEN= \
    CONTROL_PLANE_PREVIOUS_BEARER_TOKEN= \
    CONTROL_PLANE_OPERATOR_ID=operator \
    CONTROL_PLANE_PREVIOUS_OPERATOR_ID= \
    CONTROL_PLANE_AUDIT_ENABLED=true \
    CONTROL_PLANE_AUDIT_RETENTION_ENABLED=true \
    CONTROL_PLANE_SIEM_DELIVERY_ENABLED=false \
    CONTROL_PLANE_SIEM_ENDPOINT= \
    CONTROL_PLANE_SIEM_BEARER_TOKEN= \
    CONTROL_PLANE_SIEM_SIGNING_KEY= \
    MYSQL_DSN="$DEFAULT_DSN" \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}" >/dev/null 2>&1 || true
  docker compose --profile phase25-smoke rm -sf siem-sink >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT

phase="validate fail-closed SIEM signing configuration"
if docker run --rm \
  -e CONTROL_PLANE_SIEM_DELIVERY_ENABLED=true \
  -e CONTROL_PLANE_SIEM_ENDPOINT=http://siem-sink:8080/events \
  -e CONTROL_PLANE_SIEM_SIGNING_KEY= \
  taskflow-go:local >/tmp/phase25-invalid-config.log 2>&1; then
  echo "SIEM delivery unexpectedly started without signing key" >&2
  exit 1
fi
grep -q 'requires CONTROL_PLANE_SIEM_SIGNING_KEY' /tmp/phase25-invalid-config.log

phase="create isolated Phase 25 schema and SIEM sink"
CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGNING_KEY" \
CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGNING_KEY_ID" \
CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
  docker compose --profile phase25-smoke up -d mysql redis executor-worker siem-sink >/dev/null
wait_siem
docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${TEST_DB}; CREATE DATABASE ${TEST_DB}; GRANT ALL PRIVILEGES ON ${TEST_DB}.* TO 'taskflow'@'%'; FLUSH PRIVILEGES" >/dev/null

phase="create legacy sealed audit before SIEM delivery is enabled"
CONTROL_PLANE_AUTH_ENABLED=true \
  CONTROL_PLANE_BEARER_TOKEN="$TOKEN" \
  CONTROL_PLANE_OPERATOR_ID="$OPERATOR" \
  CONTROL_PLANE_AUDIT_ENABLED=true \
  CONTROL_PLANE_AUDIT_RETENTION_ENABLED=false \
  CONTROL_PLANE_SIEM_DELIVERY_ENABLED=false \
  MYSQL_DSN="$TEST_DSN" \
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null
wait_ready
curl -fsS \
  -H "Authorization: Bearer $TOKEN" \
  -H 'X-Request-ID: phase25-legacy-audit' \
  "$BASE_URL/api/v1/ai/routing-policy" >/tmp/phase25-legacy.json
legacy_sequence="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse \
  "SELECT chain_sequence FROM ${TEST_DB}.control_plane_audits WHERE request_id='phase25-legacy-audit' LIMIT 1")"
[ -n "$legacy_sequence" ]
legacy_delivery_count="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse \
  "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE chain_sequence=${legacy_sequence}")"
[ "$legacy_delivery_count" = 0 ]

phase="enable SIEM delivery and backfill pre-Phase25 audit"
CONTROL_PLANE_AUTH_ENABLED=true \
  CONTROL_PLANE_BEARER_TOKEN="$TOKEN" \
  CONTROL_PLANE_OPERATOR_ID="$OPERATOR" \
  CONTROL_PLANE_AUDIT_ENABLED=true \
  CONTROL_PLANE_AUDIT_RETENTION_ENABLED=false \
  CONTROL_PLANE_SIEM_DELIVERY_ENABLED=true \
  CONTROL_PLANE_SIEM_ENDPOINT=http://siem-sink:8080/events \
  CONTROL_PLANE_SIEM_BEARER_TOKEN="$SINK_TOKEN" \
  CONTROL_PLANE_SIEM_SIGNING_KEY="$SIGNING_KEY" \
  CONTROL_PLANE_SIEM_SIGNING_KEY_ID="$SIGNING_KEY_ID" \
  CONTROL_PLANE_SIEM_DELIVERY_INTERVAL_MS=100 \
  CONTROL_PLANE_SIEM_DELIVERY_BATCH=20 \
  CONTROL_PLANE_SIEM_DELIVERY_LEASE_MS=2000 \
  CONTROL_PLANE_SIEM_MAX_ATTEMPTS=3 \
  CONTROL_PLANE_SIEM_RETRY_BASE_MS=200 \
  CONTROL_PLANE_SIEM_RETRY_MAX_MS=400 \
  CONTROL_PLANE_SIEM_HTTP_TIMEOUT_MS=1000 \
  MYSQL_DSN="$TEST_DSN" \
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null
wait_ready
wait_sql_value "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audit_deliveries WHERE chain_sequence=${legacy_sequence} AND state='delivered'" '-ge 1' >/dev/null

phase="verify receiver accepted authenticated HMAC-signed delivery"
curl -fsS "$SIEM_URL/stats" >/tmp/phase25-siem-stats.json
python3 - <<'PY'
import json
s = json.load(open('/tmp/phase25-siem-stats.json'))
assert s['accepted_events'] >= 1, s
assert s['signature_failures'] == 0, s
assert s['auth_failures'] == 0, s
assert s['last_key_id'] == 'phase25-key-v1', s
assert s['last_idempotency_key'].startswith('taskflow-audit-'), s
PY

phase="prove audit completion and delivery outbox are transactionally paired"
curl -fsS \
  -X PUT \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'X-Request-ID: phase25-transactional-outbox' \
  -d '{"strategy":"balanced","default_output_tokens":256}' \
  "$BASE_URL/api/v1/ai/routing-policy" >/tmp/phase25-policy-update.json
paired="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse \
  "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audits a JOIN ${TEST_DB}.control_plane_audit_deliveries d ON d.chain_sequence=a.chain_sequence AND d.record_hash=a.record_hash WHERE a.request_id='phase25-transactional-outbox'")"
[ "$paired" = 1 ]

phase="force delivery retries into durable DLQ"
curl -fsS -X POST -H 'Content-Type: application/json' -d '{"fail_always":true}' "$SIEM_URL/admin/mode" >/dev/null
curl -fsS \
  -H "Authorization: Bearer $TOKEN" \
  -H 'X-Request-ID: phase25-delivery-dlq' \
  "$BASE_URL/api/v1/ai/routing-policy" >/tmp/phase25-dlq-trigger.json
dlq_id="$(wait_sql_value \
  "SELECT q.id FROM ${TEST_DB}.control_plane_audit_delivery_dlq q JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=q.chain_sequence WHERE a.request_id='phase25-delivery-dlq' AND q.state='dead' LIMIT 1" \
  '-gt 0')"
[ -n "$dlq_id" ]
attempts="$(docker compose exec -T mysql mysql -utaskflow -ptaskflow -Nse \
  "SELECT d.attempts FROM ${TEST_DB}.control_plane_audit_deliveries d JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=d.chain_sequence WHERE a.request_id='phase25-delivery-dlq' LIMIT 1")"
[ "$attempts" -ge 3 ]

phase="operator replays delivery DLQ after SIEM recovers"
curl -fsS -X POST -H 'Content-Type: application/json' -d '{"fail_always":false,"fail_remaining":0}' "$SIEM_URL/admin/mode" >/dev/null
replay_code="$(curl -sS -o /tmp/phase25-replay.json -w '%{http_code}' \
  -X POST -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/api/v1/control-plane/siem/dlq/${dlq_id}/replay")"
[ "$replay_code" = 202 ]
wait_sql_value "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audit_delivery_dlq WHERE id=${dlq_id} AND state='replayed'" '-ge 1' >/dev/null
wait_sql_value "SELECT COUNT(*) FROM ${TEST_DB}.control_plane_audit_deliveries d JOIN ${TEST_DB}.control_plane_audits a ON a.chain_sequence=d.chain_sequence WHERE a.request_id='phase25-delivery-dlq' AND d.state='delivered'" '-ge 1' >/dev/null

phase="protected delivery status and metrics"
[ "$(curl -sS -o /tmp/phase25-status.json -w '%{http_code}' -H "Authorization: Bearer $TOKEN" "$BASE_URL/api/v1/control-plane/siem/status")" = 200 ]
[ "$(curl -sS -o /tmp/phase25-deliveries.json -w '%{http_code}' -H "Authorization: Bearer $TOKEN" "$BASE_URL/api/v1/control-plane/siem/deliveries?limit=20")" = 200 ]
[ "$(curl -sS -o /tmp/phase25-dlq-list.json -w '%{http_code}' -H "Authorization: Bearer $TOKEN" "$BASE_URL/api/v1/control-plane/siem/dlq?limit=20")" = 200 ]
curl -fsS "$BASE_URL/metrics" >/tmp/phase25-metrics.txt
grep -q 'taskflow_control_plane_siem_deliveries_total' /tmp/phase25-metrics.txt
grep -q 'taskflow_control_plane_siem_delivery_backlog' /tmp/phase25-metrics.txt
grep -q 'taskflow_control_plane_siem_delivery_dlq' /tmp/phase25-metrics.txt

phase="final SIEM receiver evidence"
curl -fsS "$SIEM_URL/stats" >/tmp/phase25-siem-final.json
python3 - <<'PY'
import json
s = json.load(open('/tmp/phase25-siem-final.json'))
assert s['accepted_events'] >= 3, s
assert s['forced_failures'] >= 3, s
assert s['signature_failures'] == 0, s
assert s['auth_failures'] == 0, s
PY

python3 scripts/phase25-siem-delivery-check.py
echo "Phase 25 signed SIEM delivery outbox, retry, DLQ and replay: ok"
