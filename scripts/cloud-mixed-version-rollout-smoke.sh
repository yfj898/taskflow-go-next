#!/bin/sh
# Real process-level expand -> mixed old/new writers -> old drain -> contract
# -> new OIDC-only server acceptance. Exclusive to the local disposable lab.
# Requires a preexisting historical TaskFlow image provided separately.
# Does not modify the normal taskflow-go-mysql-1 or port 8080 installation.
set -eu

NETWORK=taskflow-keycloak-lab-net
DB=taskflow-keycloak-mysql-ci
REDIS=taskflow-rollout-redis
OLD=taskflow-rollout-old
NEW=taskflow-rollout-new
DB_NAME=taskflow_rolling_ci
OLD_IMAGE="${TASKFLOW_LEGACY_IMAGE:-taskflow-go:local}"
NEW_IMAGE="${TASKFLOW_NEW_IMAGE:-taskflow-go:enterprise-acceptance-20261009}"
OLD_API=http://127.0.0.1:18086
NEW_API=http://127.0.0.1:18087
KEYCLOAK=http://127.0.0.1:18090
phase=preflight

for name in "$OLD" "$NEW" "$REDIS"; do
  if docker ps -a --format '{{.Names}}' | grep -qx "$name"; then
    echo "Refusing to replace existing container $name" >&2
    exit 2
  fi
done
for image in "$OLD_IMAGE" "$NEW_IMAGE"; do
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    echo "Missing image $image; supply a genuine old release image" >&2
    exit 2
  fi
done
if ! docker ps --format '{{.Names}}' | grep -qx "$DB"; then
  echo 'Dedicated Keycloak CI MySQL container is required' >&2
  exit 2
fi
if ! docker ps --format '{{.Names}}' | grep -qx taskflow-keycloak; then
  echo 'Keycloak lab is required for final OIDC contract check' >&2
  exit 2
fi
if [ "$(docker image inspect "$OLD_IMAGE" --format '{{.Id}}')" = "$(docker image inspect "$NEW_IMAGE" --format '{{.Id}}')" ]; then
  echo 'Old and new images must be different digests' >&2
  exit 2
fi

pw="$(docker inspect "$DB" --format '{{range .Config.Env}}{{println .}}{{end}}' | sed -n 's/^MYSQL_ROOT_PASSWORD=//p')"
test -n "$pw"
DSN="root:$pw@tcp($DB:3306)/$DB_NAME?charset=utf8mb4&parseTime=True&loc=Local"
SCHEMA_DSN="root:$pw@tcp($DB:3306)/$DB_NAME?parseTime=true&charset=utf8mb4"
cleanup() {
  result=$?
  docker rm -f "$OLD" "$NEW" "$REDIS" >/dev/null 2>&1 || true
  # This schema is exclusive to the test and is *never* the existing TaskFlow DB.
  docker exec "$DB" mysql -uroot "-p$pw" -e "DROP DATABASE IF EXISTS $DB_NAME" >/dev/null 2>&1 || true
  if [ "$result" -ne 0 ]; then
    echo "MIXED_VERSION_ROLLOUT_FAIL phase=$phase status=$result" >&2
  fi
}
trap cleanup EXIT INT TERM

phase=initialize_disposable_db
docker exec "$DB" mysql -uroot "-p$pw" -e "CREATE DATABASE $DB_NAME" 2>/dev/null
docker run -d --rm --name "$REDIS" --network "$NETWORK" redis:7.4-alpine redis-server --save '' --appendonly no >/dev/null

common_env() {
  # Print nothing; application args are passed below explicitly.
  :
}
start_api() {
  name="$1"; port="$2"; image="$3"; auth="$4"
  if [ "$auth" = oidc ]; then
    docker run -d --name "$name" --network "$NETWORK" -p "127.0.0.1:$port:8080" \
      -e "MYSQL_DSN=$DSN" -e REDIS_ADDR="$REDIS:6379" -e QUEUE_BACKEND=redis \
      -e QUEUE_DLQ_ARCHIVE_DIR=/var/lib/taskflow/dlq-archive \
      -e QUEUE_DLQ_ARCHIVE_REPLICA_DIR=/var/lib/taskflow/dlq-archive-replica \
      -e CONTROL_PLANE_AUTH_ENABLED=true -e CONTROL_PLANE_AUTH_MODE=oidc \
      -e CONTROL_PLANE_OIDC_RBAC_ENABLED=true \
      -e CONTROL_PLANE_OIDC_ISSUER="$KEYCLOAK/realms/taskflow-lab" \
      -e CONTROL_PLANE_OIDC_AUDIENCE=taskflow-control-plane \
      -e CONTROL_PLANE_OIDC_JWKS_URL=http://taskflow-keycloak:8080/realms/taskflow-lab/protocol/openid-connect/certs \
      -e CONTROL_PLANE_OIDC_GROUPS_CLAIM=groups \
      -e 'CONTROL_PLANE_OIDC_ROLE_MAPPINGS=operators=control-plane-operator;readers=control-plane-reader' \
      -e CLOUD_TENANT_AUTHZ_ENABLED=true -e CLOUD_CONTROL_ENABLED=false \
      "$image" >/dev/null
  else
    docker run -d --name "$name" --network "$NETWORK" -p "127.0.0.1:$port:8080" \
      -e "MYSQL_DSN=$DSN" -e REDIS_ADDR="$REDIS:6379" -e QUEUE_BACKEND=redis \
      -e QUEUE_DLQ_ARCHIVE_DIR=/var/lib/taskflow/dlq-archive \
      -e QUEUE_DLQ_ARCHIVE_REPLICA_DIR=/var/lib/taskflow/dlq-archive-replica \
      -e CONTROL_PLANE_AUTH_ENABLED=false -e CLOUD_TENANT_AUTHZ_ENABLED=false \
      -e CLOUD_CONTROL_ENABLED=false "$image" >/dev/null
  fi
  i=0
  until curl -fsS --max-time 2 "http://127.0.0.1:$port/health/ready" >/dev/null 2>&1; do
    i=$((i+1))
    if [ "$i" -gt 45 ] || ! docker ps --format '{{.Names}}' | grep -qx "$name"; then
      echo "$name failed startup" >&2
      docker logs --tail 12 "$name" >&2 || true
      return 1
    fi
    sleep 1
  done
}
schema() {
  docker run --rm --network "$NETWORK" -e "TASKFLOW_SCHEMA_DSN=$SCHEMA_DSN" \
    --entrypoint /schema-migrate "$NEW_IMAGE" "$@"
}

phase=old_binary_bootstrap
start_api "$OLD" 18086 "$OLD_IMAGE" legacy
echo 'PASS old binary starts with historical global idempotency schema'

phase=legacy_schema_verification
status="$(schema --phase=status)"
printf '%s' "$status" | jq -e '.CompositeReady == false and (.LegacyUnique|length) >= 1' >/dev/null
echo 'PASS legacy global unique index identified'

phase=expand_with_old_running
status="$(schema --phase=expand)"
printf '%s' "$status" | jq -e '.CompositeReady == true and (.LegacyUnique|length) >= 1' >/dev/null
echo 'PASS expand created composite unique index while preserving old index'

phase=start_new_binary_during_overlap
start_api "$NEW" 18087 "$NEW_IMAGE" legacy
curl -fsS --max-time 4 "$OLD_API/health/ready" >/dev/null
curl -fsS --max-time 4 "$NEW_API/health/ready" >/dev/null
status="$(schema --phase=status)"
printf '%s' "$status" | jq -e '.CompositeReady == true and (.LegacyUnique|length) >= 1' >/dev/null
echo 'PASS old and new writers healthy during expand; legacy index intact'

phase=drain_old_writer
docker rm -f "$OLD" >/dev/null
if docker ps --format '{{.Names}}' | grep -qx "$OLD"; then
  echo 'Old writer did not drain' >&2
  exit 1
fi
echo 'PASS old writer stopped before contract'

phase=contract
status="$(schema --phase=contract --ack-old-writers-stopped)"
printf '%s' "$status" | jq -e '.CompositeReady == true and (.LegacyUnique|length) == 0' >/dev/null
echo 'PASS contract removed global key uniqueness after old writer drained'

phase=restart_new_with_oidc
docker rm -f "$NEW" >/dev/null
start_api "$NEW" 18087 "$NEW_IMAGE" oidc
code="$(curl -sS --max-time 5 -o /dev/null -w '%{http_code}' "$NEW_API/api/v1/cloud/resources")"
test "$code" = 401
echo 'PASS OIDC-only new writer starts after contract and rejects anonymous requests'

phase=idp_signed_token_verification
admin_password="$(docker inspect taskflow-keycloak --format '{{range .Config.Env}}{{println .}}{{end}}' | sed -n 's/^KC_BOOTSTRAP_ADMIN_PASSWORD=//p')"
admin="$(curl -fsS -d grant_type=password -d client_id=admin-cli --data-urlencode username=taskflow-lab-admin --data-urlencode "password=$admin_password" "$KEYCLOAK/realms/master/protocol/openid-connect/token" | jq -r '.access_token')"
unset admin_password
uuid="$(curl -fsS -H "Authorization: Bearer $admin" -G --data-urlencode clientId=taskflow-tenant-a "$KEYCLOAK/admin/realms/taskflow-lab/clients" | jq -r '.[0].id')"
secret="$(curl -fsS -H "Authorization: Bearer $admin" "$KEYCLOAK/admin/realms/taskflow-lab/clients/$uuid/client-secret" | jq -r '.value')"
token="$(curl -fsS -d grant_type=client_credentials --data-urlencode client_id=taskflow-tenant-a --data-urlencode "client_secret=$secret" "$KEYCLOAK/realms/taskflow-lab/protocol/openid-connect/token" | jq -r '.access_token')"
code="$(curl -sS --max-time 5 -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $token" "$NEW_API/api/v1/cloud/resources")"
if [ "$code" != 200 ]; then
  echo "OIDC-authorized new binary failed after contract: HTTP $code" >&2
  exit 1
fi
unset secret token admin
echo 'PASS actual Keycloak signed tenant JWT accepted after contract'
echo 'MIXED_VERSION_EXPAND_CONTRACT_OIDC_PASS'
