#!/bin/sh
# Integration lab ONLY: fetches bootstrap admin credentials from the local
# Keycloak Docker container, obtains actual service-client secrets via the
# Keycloak Admin API, and verifies OIDC against a disposable MySQL database.
# Never use this script with production Keycloak or production databases.
set -eu

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
BASE="${TASKFLOW_KEYCLOAK_LAB_URL:-http://127.0.0.1:18090}"
DB_CONTAINER=taskflow-keycloak-mysql-ci
NEW_DB=0

if [ "$BASE" != 'http://127.0.0.1:18090' ]; then
  echo 'Refusing non-loopback or non-lab Keycloak URL' >&2
  exit 2
fi
for command in docker curl jq openssl; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "Missing prerequisite: $command" >&2
    exit 2
  fi
done
if ! docker ps --format '{{.Names}}' | grep -qx taskflow-keycloak; then
  echo 'Missing running isolated taskflow-keycloak container' >&2
  exit 2
fi

if ! docker ps --format '{{.Names}}' | grep -qx "$DB_CONTAINER"; then
  if docker ps -a --format '{{.Names}}' | grep -qx "$DB_CONTAINER"; then
    echo 'An existing, stopped test-MySQL container needs manual inspection' >&2
    exit 2
  fi
  docker run -d --rm --name "$DB_CONTAINER" -p 127.0.0.1:13479:3306 \
    -e MYSQL_ROOT_PASSWORD="$(openssl rand -hex 24)" \
    -e MYSQL_DATABASE=taskflow_keycloak_ci mysql:8.4 --max_connections=120 >/dev/null
  NEW_DB=1
fi
cleanup() {
  if [ "$NEW_DB" = 1 ]; then
    docker stop "$DB_CONTAINER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

i=0
until docker exec "$DB_CONTAINER" mysqladmin ping --silent >/dev/null 2>&1; do
  i=$((i+1))
  if [ "$i" -ge 45 ]; then
    echo 'Temporary MySQL did not become healthy' >&2
    exit 1
  fi
  sleep 1
done

# The bootstrap password is readable by Docker administrators already; this
# lab-only convenience must not be copied into a production credential flow.
ADMIN_PASSWORD="$(docker inspect taskflow-keycloak --format '{{range .Config.Env}}{{println .}}{{end}}' | sed -n 's/^KC_BOOTSTRAP_ADMIN_PASSWORD=//p')"
if [ -z "$ADMIN_PASSWORD" ]; then
  echo 'Keycloak lab bootstrap password is not present' >&2
  exit 2
fi
ADMIN_TOKEN="$(curl -fsS --max-time 8 -d grant_type=password -d client_id=admin-cli \
  --data-urlencode 'username=taskflow-lab-admin' --data-urlencode "password=$ADMIN_PASSWORD" \
  "$BASE/realms/master/protocol/openid-connect/token" | jq -r '.access_token // empty')"
unset ADMIN_PASSWORD
if [ -z "$ADMIN_TOKEN" ]; then
  echo 'Could not authenticate to the Keycloak lab Admin API' >&2
  exit 1
fi

fetch_secret() {
  id="$(curl -fsS --max-time 6 -H "Authorization: Bearer $ADMIN_TOKEN" \
    -G --data-urlencode "clientId=$1" "$BASE/admin/realms/taskflow-lab/clients" | jq -r '.[0].id // empty')"
  if [ -z "$id" ]; then
    echo "Missing Keycloak client: $1" >&2
    exit 1
  fi
  curl -fsS --max-time 6 -H "Authorization: Bearer $ADMIN_TOKEN" \
    "$BASE/admin/realms/taskflow-lab/clients/$id/client-secret" | jq -r '.value // empty'
}

export TASKFLOW_KEYCLOAK_TENANT_A_SECRET="$(fetch_secret taskflow-tenant-a)"
export TASKFLOW_KEYCLOAK_TENANT_B_SECRET="$(fetch_secret taskflow-tenant-b)"
export TASKFLOW_KEYCLOAK_READER_SECRET="$(fetch_secret taskflow-tenant-a-reader)"
export TASKFLOW_KEYCLOAK_NO_GRANT_SECRET="$(fetch_secret taskflow-tenant-a-no-grant)"
unset ADMIN_TOKEN
if [ -z "$TASKFLOW_KEYCLOAK_TENANT_A_SECRET" ] || [ -z "$TASKFLOW_KEYCLOAK_TENANT_B_SECRET" ] || \
  [ -z "$TASKFLOW_KEYCLOAK_READER_SECRET" ] || [ -z "$TASKFLOW_KEYCLOAK_NO_GRANT_SECRET" ]; then
  echo 'One or more Keycloak client credentials are missing' >&2
  exit 1
fi

export TASKFLOW_KEYCLOAK_ISSUER="$BASE/realms/taskflow-lab"
DB_PASSWORD="$(docker inspect "$DB_CONTAINER" --format '{{range .Config.Env}}{{println .}}{{end}}' | sed -n 's/^MYSQL_ROOT_PASSWORD=//p')"
export TASKFLOW_KEYCLOAK_MYSQL_DSN="root:$DB_PASSWORD@tcp(127.0.0.1:13479)/taskflow_keycloak_ci?parseTime=true&charset=utf8mb4"
unset DB_PASSWORD

echo 'Verifying actual Keycloak client_credentials JWT -> TaskFlow Gin/RBAC -> disposable MySQL'
docker run --rm --network host \
  -v taskflow-go-mod-cache:/go/pkg/mod \
  -v taskflow-go-build-cache:/root/.cache/go-build \
  -v "$ROOT:/src:ro" -w /src \
  -e TASKFLOW_KEYCLOAK_TENANT_A_SECRET \
  -e TASKFLOW_KEYCLOAK_TENANT_B_SECRET \
  -e TASKFLOW_KEYCLOAK_READER_SECRET \
  -e TASKFLOW_KEYCLOAK_NO_GRANT_SECRET \
  -e TASKFLOW_KEYCLOAK_ISSUER \
  -e TASKFLOW_KEYCLOAK_MYSQL_DSN \
  -e GOPROXY=https://goproxy.cn,direct \
  golang:1.26.8-alpine /usr/local/go/bin/go test -count=1 -v ./internal/handler \
    -run '^TestKeycloakRealTokenTenantIsolation$'
