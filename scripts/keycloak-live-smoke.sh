#!/bin/sh
# Tests a separate running TaskFlow API at 127.0.0.1:18088 using tokens signed
# by a real, local Keycloak server. Lab only. It does not mutate production.
set -eu
BASE=http://127.0.0.1:18090
API=http://127.0.0.1:18088/api/v1/cloud
ADMIN_PASSWORD="$(docker inspect taskflow-keycloak --format '{{range .Config.Env}}{{println .}}{{end}}' | sed -n 's/^KC_BOOTSTRAP_ADMIN_PASSWORD=//p')"
if [ -z "$ADMIN_PASSWORD" ] || ! docker ps --format '{{.Names}}' | grep -qx taskflow-keycloak-stage; then
  echo 'Keycloak lab and a separate TaskFlow staging instance are required' >&2
  exit 2
fi
ADMIN_TOKEN="$(curl -fsS --max-time 8 -d grant_type=password -d client_id=admin-cli \
  --data-urlencode 'username=taskflow-lab-admin' --data-urlencode "password=$ADMIN_PASSWORD" \
  "$BASE/realms/master/protocol/openid-connect/token" | jq -r '.access_token // empty')"
unset ADMIN_PASSWORD
test -n "$ADMIN_TOKEN"

fetch_secret() {
  id="$(curl -fsS --max-time 8 -H "Authorization: Bearer $ADMIN_TOKEN" \
    -G --data-urlencode "clientId=$1" "$BASE/admin/realms/taskflow-lab/clients" | jq -r '.[0].id // empty')"
  test -n "$id"
  curl -fsS --max-time 8 -H "Authorization: Bearer $ADMIN_TOKEN" \
    "$BASE/admin/realms/taskflow-lab/clients/$id/client-secret" | jq -r '.value // empty'
}
issue_token() {
  secret="$(fetch_secret "$1")"
  test -n "$secret"
  curl -fsS --max-time 8 -d grant_type=client_credentials \
    --data-urlencode "client_id=$1" --data-urlencode "client_secret=$secret" \
    "$BASE/realms/taskflow-lab/protocol/openid-connect/token" | jq -r '.access_token // empty'
}

A="$(issue_token taskflow-tenant-a)"
B="$(issue_token taskflow-tenant-b)"
READER="$(issue_token taskflow-tenant-a-reader)"
NO_GRANT="$(issue_token taskflow-tenant-a-no-grant)"
unset ADMIN_TOKEN
test -n "$A" && test -n "$B" && test -n "$READER" && test -n "$NO_GRANT"

check() {
  name="$1"; method="$2"; uri="$3"; jwt="$4"; expected="$5"; body="$6"
  if [ -n "$jwt" ]; then
    code="$(curl -sS --max-time 7 -X "$method" -o /dev/null -w '%{http_code}' \
      -H "Authorization: Bearer $jwt" -H 'Content-Type: application/json' \
      -H 'X-Tenant-ID: team-b' --data-raw "$body" "$API$uri")"
  else
    code="$(curl -sS --max-time 7 -X "$method" -o /dev/null -w '%{http_code}' \
      -H 'Content-Type: application/json' --data-raw "$body" "$API$uri")"
  fi
  if [ "$code" != "$expected" ]; then
    echo "FAIL $name: got HTTP $code expected $expected" >&2
    exit 1
  fi
  echo "PASS $name HTTP $code"
}

QUOTA='{"cpu_milli_limit":4000,"memory_mib_limit":4096,"replica_limit":8}'
check missing_token GET /resources '' 401 ''
check operator_a_set_own_quota PUT /tenants/team-a/quota "$A" 200 "$QUOTA"
check operator_b_set_own_quota PUT /tenants/team-b/quota "$B" 200 "$QUOTA"
check operator_a_read_own_quota GET /tenants/team-a/quota "$A" 200 ''
check operator_b_read_own_quota GET /tenants/team-b/quota "$B" 200 ''
check operator_a_denied_other_tenant GET /tenants/team-b/quota "$A" 403 ''
check operator_b_denied_other_tenant GET /tenants/team-a/quota "$B" 403 ''
check operator_a_cross_tenant_quota_write PUT /tenants/team-b/quota "$A" 403 "$QUOTA"
check reader_a_can_read GET /tenants/team-a/quota "$READER" 200 ''
check reader_a_cannot_write PUT /tenants/team-a/quota "$READER" 403 "$QUOTA"
check no_tenant_grant_denied GET /tenants/team-a/quota "$NO_GRANT" 403 ''
check tampered_signature_rejected GET /tenants/team-a/quota "${A}invalid" 401 ''
check scoped_resource_listing GET /resources "$A" 200 ''

echo 'KEYCLOAK_LIVE_STAGE_SMOKE_PASS (13 checks; staging only, no Kubernetes provider)'
