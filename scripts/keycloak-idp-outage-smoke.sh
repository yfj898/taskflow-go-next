#!/bin/sh
# Lab-only controlled Keycloak interruption and recovery drill.
# Requires a healthy independent TaskFlow stage with JWKS cache.
# Never run against production IdP. Always tries to restart Keycloak on exit.
set -eu
BASE=http://127.0.0.1:18090
API=http://127.0.0.1:18088/api/v1/cloud/tenants/team-a/quota
CONTAINER=taskflow-keycloak
stopped=0

restart_if_stopped() {
  if [ "$stopped" = 1 ]; then
    docker start "$CONTAINER" >/dev/null 2>&1 || true
  fi
}
trap restart_if_stopped EXIT INT TERM

for name in "$CONTAINER" taskflow-keycloak-stage; do
  if ! docker ps --format '{{.Names}}' | grep -qx "$name"; then
    echo "Missing Keycloak lab dependency $name" >&2
    exit 2
  fi
done
if ! curl -fsS --max-time 3 http://127.0.0.1:18088/health/ready >/dev/null; then
  echo 'TaskFlow test API must be ready' >&2
  exit 2
fi
password="$(docker inspect "$CONTAINER" --format '{{range .Config.Env}}{{println .}}{{end}}' | sed -n 's/^KC_BOOTSTRAP_ADMIN_PASSWORD=//p')"
admin="$(curl -fsS --max-time 6 -d grant_type=password -d client_id=admin-cli \
  --data-urlencode username=taskflow-lab-admin --data-urlencode "password=$password" \
  "$BASE/realms/master/protocol/openid-connect/token" | jq -r '.access_token // empty')"
unset password
test -n "$admin"
uuid="$(curl -fsS -H "Authorization: Bearer $admin" -G \
  --data-urlencode clientId=taskflow-tenant-a \
  "$BASE/admin/realms/taskflow-lab/clients" | jq -r '.[0].id // empty')"
test -n "$uuid"
secret="$(curl -fsS -H "Authorization: Bearer $admin" \
  "$BASE/admin/realms/taskflow-lab/clients/$uuid/client-secret" | jq -r '.value // empty')"
unset admin
test -n "$secret"
issue_token() {
  response="$(curl -fsS --max-time 6 -d grant_type=client_credentials \
    --data-urlencode client_id=taskflow-tenant-a --data-urlencode "client_secret=$secret" \
    "$BASE/realms/taskflow-lab/protocol/openid-connect/token")" || return 1
  jwt="$(printf '%s' "$response" | jq -er '.access_token // empty')" || return 1
  test -n "$jwt" || return 1
  printf '%s' "$jwt"
}
status() {
  curl -sS --max-time 4 -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $1" "$API"
}

token="$(issue_token)"
test -n "$token"
test "$(status "$token")" = 200
echo 'PASS cached-JWKS-primed authorized request 200'

stopped=1
docker stop --time 8 "$CONTAINER" >/dev/null
if curl -fsS --max-time 2 "$BASE/realms/taskflow-lab/.well-known/openid-configuration" >/dev/null 2>&1; then
  echo 'Keycloak is still serving discovery after attempted stop' >&2
  exit 1
fi
echo 'PASS real Keycloak discovery unavailable during planned outage'

# This token remains valid for a short outage because TaskFlow's verifier
# already has its signer in the bounded in-memory JWKS cache. This is NOT
# proof of token revocation or of availability past cache/expiry deadlines.
cached_code="$(status "$token")"
if [ "$cached_code" != 200 ]; then
  echo "Previously verified token failed within JWKS cache TTL: HTTP $cached_code" >&2
  exit 1
fi
echo 'PASS TaskFlow accepts still-valid cached JWT while IdP is briefly down'

if issue_token >/dev/null 2>&1; then
  echo 'IdP unexpectedly issued a fresh token while stopped' >&2
  exit 1
fi
echo 'PASS new OAuth token exchange unavailable during IdP outage'

docker start "$CONTAINER" >/dev/null
stopped=0
i=0
until curl -fsS --max-time 2 "$BASE/realms/taskflow-lab/.well-known/openid-configuration" >/dev/null 2>&1; do
  i=$((i+1))
  if [ "$i" -ge 45 ]; then
    echo 'Keycloak did not recover within 90 seconds' >&2
    exit 1
  fi
  sleep 2
done
fresh="$(issue_token)"
test -n "$fresh"
test "$(status "$fresh")" = 200
echo 'PASS Keycloak recovered; freshly issued signed JWT accepted by TaskFlow'
echo 'KEYCLOAK_IDP_OUTAGE_RECOVERY_PASS'
