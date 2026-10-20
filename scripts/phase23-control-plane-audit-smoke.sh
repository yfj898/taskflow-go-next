#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
ACTIVE_TOKEN="${PHASE23_ACTIVE_TOKEN:-phase23-active-token}"
PREVIOUS_TOKEN="${PHASE23_PREVIOUS_TOKEN:-phase23-previous-token}"
ACTIVE_OPERATOR="${PHASE23_ACTIVE_OPERATOR:-platform-admin-v2}"
PREVIOUS_OPERATOR="${PHASE23_PREVIOUS_OPERATOR:-platform-admin-v1}"
phase="initialization"

wait_ready() {
  i=0
  while [ "$i" -lt 40 ]; do
    if curl -fsS "$BASE_URL/health/ready" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

cleanup() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "Phase 23 control-plane audit smoke failed during phase: $phase" >&2
    docker compose logs --tail 200 taskflow >&2 2>/dev/null || true
  fi
  CONTROL_PLANE_AUTH_ENABLED=false \
    CONTROL_PLANE_BEARER_TOKEN= \
    CONTROL_PLANE_PREVIOUS_BEARER_TOKEN= \
    CONTROL_PLANE_OPERATOR_ID=operator \
    CONTROL_PLANE_PREVIOUS_OPERATOR_ID= \
    CONTROL_PLANE_AUDIT_ENABLED=true \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT

phase="rotation config rejects duplicate credentials"
if docker run --rm \
  -e CONTROL_PLANE_AUTH_ENABLED=true \
  -e CONTROL_PLANE_BEARER_TOKEN=same-token \
  -e CONTROL_PLANE_PREVIOUS_BEARER_TOKEN=same-token \
  -e CONTROL_PLANE_OPERATOR_ID=admin \
  taskflow-go:local >/tmp/phase23-duplicate-token.log 2>&1; then
  echo "duplicate active/previous tokens unexpectedly started" >&2
  exit 1
fi
grep -q 'CONTROL_PLANE_PREVIOUS_BEARER_TOKEN must differ' /tmp/phase23-duplicate-token.log

phase="start overlapping active and previous credentials"
CONTROL_PLANE_AUTH_ENABLED=true \
  CONTROL_PLANE_BEARER_TOKEN="$ACTIVE_TOKEN" \
  CONTROL_PLANE_PREVIOUS_BEARER_TOKEN="$PREVIOUS_TOKEN" \
  CONTROL_PLANE_OPERATOR_ID="$ACTIVE_OPERATOR" \
  CONTROL_PLANE_PREVIOUS_OPERATOR_ID="$PREVIOUS_OPERATOR" \
  CONTROL_PLANE_AUDIT_ENABLED=true \
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null
wait_ready

phase="previous credential accepted during overlap"
previous_code="$(curl -sS -o /tmp/phase23-previous.json -w '%{http_code}' \
  -H "Authorization: Bearer $PREVIOUS_TOKEN" \
  "$BASE_URL/api/v1/ai/routing-policy")"
[ "$previous_code" = 200 ]

phase="active credential accepted during overlap"
active_code="$(curl -sS -o /tmp/phase23-active.json -w '%{http_code}' \
  -H "Authorization: Bearer $ACTIVE_TOKEN" \
  "$BASE_URL/api/v1/ai/routing-policy")"
[ "$active_code" = 200 ]

phase="control-plane mutation is durably audited"
strategy="$(python3 -c 'import json; print(json.load(open("/tmp/phase23-active.json"))["strategy"])')"
tokens="$(python3 -c 'import json; print(json.load(open("/tmp/phase23-active.json"))["default_output_tokens"])')"
update_code="$(curl -sS -o /tmp/phase23-update.json -w '%{http_code}' \
  -X PUT \
  -H "Authorization: Bearer $ACTIVE_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'X-Request-ID: phase23-active-mutation' \
  -d "{\"strategy\":\"$strategy\",\"default_output_tokens\":$tokens}" \
  "$BASE_URL/api/v1/ai/routing-policy")"
[ "$update_code" = 200 ]

phase="audit API exposes mapped identities without secrets"
audit_code="$(curl -sS -o /tmp/phase23-audits.json -w '%{http_code}' \
  -H "Authorization: Bearer $ACTIVE_TOKEN" \
  "$BASE_URL/api/v1/control-plane/audits?limit=50")"
[ "$audit_code" = 200 ]
grep -q "\"operator_id\":\"$PREVIOUS_OPERATOR\"" /tmp/phase23-audits.json
grep -q '"credential_slot":"previous"' /tmp/phase23-audits.json
grep -q "\"operator_id\":\"$ACTIVE_OPERATOR\"" /tmp/phase23-audits.json
grep -q '"credential_slot":"active"' /tmp/phase23-audits.json
grep -q '"request_id":"phase23-active-mutation"' /tmp/phase23-audits.json
grep -q '"outcome":"success"' /tmp/phase23-audits.json
if grep -Fq "$ACTIVE_TOKEN" /tmp/phase23-audits.json || grep -Fq "$PREVIOUS_TOKEN" /tmp/phase23-audits.json; then
  echo "raw control-plane credential leaked into audit response" >&2
  exit 1
fi

phase="audit metric exposed"
curl -fsS "$BASE_URL/metrics" >/tmp/phase23-metrics.txt
grep -q 'taskflow_control_plane_audits_total' /tmp/phase23-metrics.txt

phase="retire previous credential"
CONTROL_PLANE_AUTH_ENABLED=true \
  CONTROL_PLANE_BEARER_TOKEN="$ACTIVE_TOKEN" \
  CONTROL_PLANE_PREVIOUS_BEARER_TOKEN= \
  CONTROL_PLANE_OPERATOR_ID="$ACTIVE_OPERATOR" \
  CONTROL_PLANE_PREVIOUS_OPERATOR_ID= \
  CONTROL_PLANE_AUDIT_ENABLED=true \
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null
wait_ready

retired_code="$(curl -sS -o /tmp/phase23-retired.json -w '%{http_code}' \
  -H "Authorization: Bearer $PREVIOUS_TOKEN" \
  "$BASE_URL/api/v1/ai/routing-policy")"
[ "$retired_code" = 401 ]
active_after_code="$(curl -sS -o /tmp/phase23-active-after.json -w '%{http_code}' \
  -H "Authorization: Bearer $ACTIVE_TOKEN" \
  "$BASE_URL/api/v1/ai/routing-policy")"
[ "$active_after_code" = 200 ]

echo "Phase 23 durable control-plane audit and zero-downtime token rotation: ok"
