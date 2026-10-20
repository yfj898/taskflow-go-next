#!/bin/sh
# Exercise real RS256 realm signing-key rotation against the separate running
# TaskFlow OIDC staging API. This changes only the taskflow-lab realm.
# Keeps the previous signing key to preserve the token overlap window.
set -eu

BASE=http://127.0.0.1:18090
API=http://127.0.0.1:18088/api/v1/cloud/tenants/team-a/quota
REALM=taskflow-lab
ROTATION_NAME=taskflow-lab-signing-rotation-20261009

if ! docker ps --format '{{.Names}}' | grep -qx taskflow-keycloak; then
  echo 'Isolated Keycloak lab must be running' >&2
  exit 2
fi
if ! curl -fsS --max-time 3 http://127.0.0.1:18088/health/ready >/dev/null; then
  echo 'Independent TaskFlow OIDC staging service is not ready' >&2
  exit 2
fi

PASSWORD="$(docker inspect taskflow-keycloak --format '{{range .Config.Env}}{{println .}}{{end}}' | sed -n 's/^KC_BOOTSTRAP_ADMIN_PASSWORD=//p')"
ADMIN_TOKEN="$(curl -fsS --max-time 8 -d grant_type=password -d client_id=admin-cli \
  --data-urlencode username=taskflow-lab-admin --data-urlencode "password=$PASSWORD" \
  "$BASE/realms/master/protocol/openid-connect/token" | jq -r '.access_token // empty')"
unset PASSWORD
test -n "$ADMIN_TOKEN"

get_admin() {
  curl -fsS --max-time 8 -H "Authorization: Bearer $ADMIN_TOKEN" "$BASE/admin/realms/$REALM/$1"
}

CLIENT_UUID="$(get_admin 'clients?clientId=taskflow-tenant-a' | jq -r '.[0].id // empty')"
test -n "$CLIENT_UUID"
CLIENT_SECRET="$(get_admin "clients/$CLIENT_UUID/client-secret" | jq -r '.value // empty')"
test -n "$CLIENT_SECRET"
token() {
  curl -fsS --max-time 8 -d grant_type=client_credentials \
    --data-urlencode client_id=taskflow-tenant-a \
    --data-urlencode "client_secret=$CLIENT_SECRET" \
    "$BASE/realms/$REALM/protocol/openid-connect/token" | jq -r '.access_token // empty'
}
get_kid() {
  # Decode only the JWT header (no secret or JWT printed).
  printf '%s' "$1" | cut -d. -f1 | tr '_-' '/+' | awk '{printf "%s",$0}' | base64 -d 2>/dev/null | jq -r '.kid // empty'
}
expect_status() {
  name="$1"; jwt="$2"; want="$3"
  code="$(curl -sS --max-time 6 -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $jwt" "$API")"
  if [ "$code" != "$want" ]; then
    echo "FAIL $name: HTTP=$code expected=$want" >&2
    exit 1
  fi
  echo "PASS $name HTTP=$code"
}

old_jwt="$(token)"
old_kid="$(get_kid "$old_jwt")"
test -n "$old_kid"
expect_status pre_rotation_old_token "$old_jwt" 200

existing="$(get_admin 'components?type=org.keycloak.keys.KeyProvider' | jq -r --arg name "$ROTATION_NAME" '.[]|select(.name==$name)|.id')"
if [ -z "$existing" ]; then
  # A KeyProvider must be attached to the realm *UUID*, not the realm name.
  realm_uuid="$(get_admin '' | jq -r '.id // empty')"
  test -n "$realm_uuid"
  result="$(curl -sS --max-time 15 -o /dev/null -w '%{http_code}' \
    -X POST -H "Authorization: Bearer $ADMIN_TOKEN" -H 'Content-Type: application/json' \
    --data "{\"name\":\"$ROTATION_NAME\",\"providerId\":\"rsa-generated\",\"providerType\":\"org.keycloak.keys.KeyProvider\",\"parentId\":\"$realm_uuid\",\"config\":{\"priority\":[\"200\"],\"algorithm\":[\"RS256\"],\"keySize\":[\"2048\"],\"enabled\":[\"true\"]}}" \
    "$BASE/admin/realms/$REALM/components")"
  if [ "$result" != 201 ] && [ "$result" != 204 ]; then
    echo "Keycloak signing-key component creation failed HTTP $result" >&2
    exit 1
  fi
  echo 'Created higher-priority RSA signing key in isolated Keycloak realm'
else
  echo 'Signing-key rotation component already present; verifying idempotently'
fi

new_jwt="$(token)"
new_kid="$(get_kid "$new_jwt")"
test -n "$new_kid"
if [ "$new_kid" = "$old_kid" ] && [ -z "$existing" ]; then
  echo 'Rotation did not result in a new signing key' >&2
  exit 1
fi
jwks="$(curl -fsS --max-time 7 "$BASE/realms/$REALM/protocol/openid-connect/certs")"
printf '%s' "$jwks" | jq -e --arg kid "$old_kid" 'any(.keys[]; .kid==$kid)' >/dev/null
printf '%s' "$jwks" | jq -e --arg kid "$new_kid" 'any(.keys[]; .kid==$kid)' >/dev/null
echo 'PASS old_and_new_signing_keys_published_in_JWKS'
expect_status post_rotation_new_token "$new_jwt" 200
expect_status post_rotation_existing_old_token "$old_jwt" 200
expect_status post_rotation_tampered_token "${new_jwt}invalid" 401
unset old_jwt new_jwt CLIENT_SECRET ADMIN_TOKEN jwks
echo 'KEYCLOAK_REAL_SIGNING_KEY_ROTATION_PASS'
