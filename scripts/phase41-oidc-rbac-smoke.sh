#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
OIDC_URL="${PHASE41_OIDC_URL:-http://127.0.0.1:19706}"
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

token() {
  path="$1"
  curl -fsS "$OIDC_URL/token?$path" | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])'
}

cleanup() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "Phase 41 OIDC/RBAC smoke failed during phase: $phase" >&2
    docker compose --profile phase41-smoke logs --tail 300 taskflow oidc-mock >&2 2>/dev/null || true
  fi
  CONTROL_PLANE_AUTH_ENABLED=false CONTROL_PLANE_AUTH_MODE=bearer CONTROL_PLANE_BEARER_TOKEN= CONTROL_PLANE_PREVIOUS_BEARER_TOKEN= \
    CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS= CONTROL_PLANE_OPERATOR_ID=operator CONTROL_PLANE_OIDC_ISSUER= CONTROL_PLANE_OIDC_AUDIENCE= \
    CONTROL_PLANE_OIDC_JWKS_URL= CONTROL_PLANE_OIDC_ROLE_MAPPINGS= CONTROL_PLANE_OIDC_RBAC_ENABLED=false \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  docker compose --profile phase41-smoke rm -sf oidc-mock >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT

phase="build Phase 41 image"
docker build -t taskflow-go:local --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null

phase="start OIDC issuer and TaskFlow"
docker compose up -d mysql redis executor-worker >/dev/null
docker compose --profile phase41-smoke up -d --force-recreate oidc-mock >/dev/null
wait_http "$OIDC_URL/health"
CONTROL_PLANE_AUTH_ENABLED=true CONTROL_PLANE_AUTH_MODE=oidc CONTROL_PLANE_BEARER_TOKEN= CONTROL_PLANE_PREVIOUS_BEARER_TOKEN= \
  CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS= CONTROL_PLANE_OPERATOR_ID=operator \
  CONTROL_PLANE_OIDC_ISSUER=http://oidc-mock:8080 CONTROL_PLANE_OIDC_AUDIENCE=taskflow-control-plane CONTROL_PLANE_OIDC_JWKS_URL=http://oidc-mock:8080/jwks \
  CONTROL_PLANE_OIDC_GROUPS_CLAIM=groups CONTROL_PLANE_OIDC_ROLE_MAPPINGS='readers=control-plane-reader;operators=control-plane-reader|control-plane-operator;commanders=incident-commander' \
  CONTROL_PLANE_OIDC_JWKS_CACHE_TTL_MS=600000 CONTROL_PLANE_OIDC_CLOCK_SKEW_MS=0 CONTROL_PLANE_OIDC_RBAC_ENABLED=true \
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null
wait_http "$BASE_URL/health/ready"

phase="prove OIDC authentication and RBAC"
reader="$(token 'sub=reader&groups=readers')"
operator="$(token 'sub=operator&groups=operators')"
expired="$(token 'sub=expired&groups=operators&ttl=-60')"
wrong_aud="$(token 'sub=wrong-aud&groups=operators&aud=other-service')"

[ "$(curl -sS -o /tmp/phase41-noauth.json -w '%{http_code}' "$BASE_URL/api/v1/control-plane/audits?limit=1")" = 401 ]
[ "$(curl -sS -o /tmp/phase41-read.json -w '%{http_code}' -H "Authorization: Bearer $reader" "$BASE_URL/api/v1/control-plane/audits?limit=1")" = 200 ]
[ "$(curl -sS -o /tmp/phase41-reader-mutation.json -w '%{http_code}' -X DELETE -H "Authorization: Bearer $reader" "$BASE_URL/api/v1/queue/dead-letters/999999")" = 403 ]
operator_code="$(curl -sS -o /tmp/phase41-operator-mutation.json -w '%{http_code}' -X DELETE -H "Authorization: Bearer $operator" "$BASE_URL/api/v1/queue/dead-letters/999999")"
[ "$operator_code" != 401 ] && [ "$operator_code" != 403 ]
[ "$(curl -sS -o /tmp/phase41-expired.json -w '%{http_code}' -H "Authorization: Bearer $expired" "$BASE_URL/api/v1/control-plane/audits?limit=1")" = 401 ]
[ "$(curl -sS -o /tmp/phase41-aud.json -w '%{http_code}' -H "Authorization: Bearer $wrong_aud" "$BASE_URL/api/v1/control-plane/audits?limit=1")" = 401 ]

phase="rotate JWKS and prove unknown kid refreshes without TaskFlow restart"
curl -fsS -X POST "$OIDC_URL/rotate" >/tmp/phase41-rotate.json
rotated="$(token 'sub=operator-v2&groups=operators')"
[ "$(curl -sS -o /tmp/phase41-rotated.json -w '%{http_code}' -H "Authorization: Bearer $rotated" "$BASE_URL/api/v1/control-plane/audits?limit=1")" = 200 ]

echo "Phase 41 OIDC RS256 issuer/audience/expiry validation and group RBAC: ok"
echo "Phase 41 JWKS kid rotation refreshed trust without TaskFlow restart: ok"
