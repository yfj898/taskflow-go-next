#!/bin/sh
# Start a separate, loopback-only TaskFlow staging API using a real Keycloak
# lab realm and an isolated MySQL/Redis instance. No existing production
# TaskFlow or business schema is modified.
set -eu

KEYCLOAK=taskflow-keycloak
DB=taskflow-keycloak-mysql-ci
REDIS=taskflow-keycloak-redis-stage
STAGE=taskflow-keycloak-stage
NETWORK=taskflow-keycloak-lab-net
RELEASE_IMAGE="${TASKFLOW_STAGE_IMAGE:-taskflow-go:enterprise-acceptance-20261009}"

if ! docker ps --format '{{.Names}}' | grep -qx "$KEYCLOAK"; then
  echo 'Keycloak lab is not running; start the isolated identity provider first' >&2
  exit 2
fi
if ! docker image inspect "$RELEASE_IMAGE" >/dev/null 2>&1; then
  echo "Missing TaskFlow release image: $RELEASE_IMAGE" >&2
  exit 2
fi
if docker ps --format '{{.Names}}' | grep -qx "$STAGE"; then
  if ! curl -fsS --max-time 3 http://127.0.0.1:18088/health/ready >/dev/null 2>&1; then
    echo 'TaskFlow staging container is running but not ready; inspect logs' >&2
    exit 1
  fi
  echo 'TaskFlow OIDC staging already running on http://127.0.0.1:18088'
  exit 0
fi
if docker ps -a --format '{{.Names}}' | grep -qx "$STAGE"; then
  echo 'Previous OIDC staging container exists but is stopped; inspect its logs before restarting' >&2
  exit 2
fi

if ! docker ps --format '{{.Names}}' | grep -qx "$DB"; then
  if docker ps -a --format '{{.Names}}' | grep -qx "$DB"; then
    echo 'Stopped MySQL test container requires manual review' >&2
    exit 2
  fi
  docker run -d --rm --name "$DB" -p 127.0.0.1:13479:3306 \
    -e MYSQL_ROOT_PASSWORD="$(openssl rand -hex 24)" \
    -e MYSQL_DATABASE=taskflow_keycloak_ci mysql:8.4 --max_connections=120 >/dev/null
fi
if ! docker network inspect "$NETWORK" --format '{{range .Containers}}{{.Name}}{{" "}}{{end}}' | grep -qw "$DB"; then
  docker network connect "$NETWORK" "$DB"
fi
if ! docker ps --format '{{.Names}}' | grep -qx "$REDIS"; then
  docker run -d --rm --name "$REDIS" --network "$NETWORK" \
    redis:7.4-alpine redis-server --save '' --appendonly no >/dev/null
fi

mysql_password="$(docker inspect "$DB" --format '{{range .Config.Env}}{{println .}}{{end}}' | sed -n 's/^MYSQL_ROOT_PASSWORD=//p')"
if [ -z "$mysql_password" ]; then
  echo 'Disposable MySQL root credential is unavailable' >&2
  exit 1
fi
i=0
until docker exec "$DB" mysqladmin ping --silent >/dev/null 2>&1; do
  i=$((i+1))
  if [ "$i" -gt 40 ]; then
    echo 'Test MySQL did not become ready' >&2
    exit 1
  fi
  sleep 1
done
docker exec "$DB" mysql -uroot "-p$mysql_password" \
  -e 'CREATE DATABASE IF NOT EXISTS taskflow_keycloak_stage_ci' 2>/dev/null

docker run -d --name "$STAGE" --network "$NETWORK" \
  -p 127.0.0.1:18088:8080 \
  -e "MYSQL_DSN=root:$mysql_password@tcp($DB:3306)/taskflow_keycloak_stage_ci?charset=utf8mb4&parseTime=True&loc=Local" \
  -e REDIS_ADDR="$REDIS:6379" \
  -e QUEUE_BACKEND=redis \
  -e QUEUE_DLQ_ARCHIVE_DIR=/var/lib/taskflow/dlq-archive \
  -e QUEUE_DLQ_ARCHIVE_REPLICA_DIR=/var/lib/taskflow/dlq-archive-replica \
  -e CONTROL_PLANE_AUTH_ENABLED=true \
  -e CONTROL_PLANE_AUTH_MODE=oidc \
  -e CONTROL_PLANE_OIDC_RBAC_ENABLED=true \
  -e CONTROL_PLANE_OIDC_ISSUER=http://127.0.0.1:18090/realms/taskflow-lab \
  -e CONTROL_PLANE_OIDC_AUDIENCE=taskflow-control-plane \
  -e CONTROL_PLANE_OIDC_JWKS_URL=http://taskflow-keycloak:8080/realms/taskflow-lab/protocol/openid-connect/certs \
  -e CONTROL_PLANE_OIDC_GROUPS_CLAIM=groups \
  -e 'CONTROL_PLANE_OIDC_ROLE_MAPPINGS=operators=control-plane-operator;readers=control-plane-reader' \
  -e CLOUD_TENANT_AUTHZ_ENABLED=true \
  -e CLOUD_CONTROL_ENABLED=false \
  "$RELEASE_IMAGE" >/dev/null
unset mysql_password

i=0
until curl -fsS --max-time 2 http://127.0.0.1:18088/health/ready >/dev/null 2>&1; do
  i=$((i+1))
  if [ "$i" -gt 30 ] || ! docker ps --format '{{.Names}}' | grep -qx "$STAGE"; then
    echo 'Keycloak staging API failed to start; inspect docker logs taskflow-keycloak-stage' >&2
    exit 1
  fi
  sleep 1
done
echo 'TaskFlow staging API is healthy on http://127.0.0.1:18088 (real Keycloak OIDC enabled)'
echo 'Run: sh scripts/keycloak-live-smoke.sh'
