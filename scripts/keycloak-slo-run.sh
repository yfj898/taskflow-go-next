#!/bin/sh
# Acquire short-lived Keycloak machine credentials for bounded SLO test.
# Never writes access tokens or client secrets to files or program arguments.
set -eu
BASE=http://127.0.0.1:18090
ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

for container in taskflow-keycloak taskflow-keycloak-stage; do
  if ! docker ps --format '{{.Names}}' | grep -qx "$container"; then
    echo "Missing isolated lab container $container" >&2
    exit 2
  fi
done
if ! curl -fsS --max-time 3 http://127.0.0.1:18088/health/ready >/dev/null; then
  echo 'TaskFlow OIDC stage is not ready' >&2
  exit 2
fi
admin_password="$(docker inspect taskflow-keycloak --format '{{range .Config.Env}}{{println .}}{{end}}' | sed -n 's/^KC_BOOTSTRAP_ADMIN_PASSWORD=//p')"
admin_token="$(curl -fsS --max-time 6 -d grant_type=password -d client_id=admin-cli \
  --data-urlencode username=taskflow-lab-admin --data-urlencode "password=$admin_password" \
  "$BASE/realms/master/protocol/openid-connect/token" | jq -r '.access_token // empty')"
unset admin_password
test -n "$admin_token"
get_secret() {
  id="$(curl -fsS -H "Authorization: Bearer $admin_token" -G \
    --data-urlencode "clientId=$1" "$BASE/admin/realms/taskflow-lab/clients" | jq -r '.[0].id // empty')"
  test -n "$id"
  curl -fsS -H "Authorization: Bearer $admin_token" \
    "$BASE/admin/realms/taskflow-lab/clients/$id/client-secret" | jq -r '.value // empty'
}
export TASKFLOW_SLO_TENANT_A_SECRET="$(get_secret taskflow-tenant-a)"
export TASKFLOW_SLO_TENANT_B_SECRET="$(get_secret taskflow-tenant-b)"
export TASKFLOW_KEYCLOAK_ISSUER="$BASE/realms/taskflow-lab"
unset admin_token
test -n "$TASKFLOW_SLO_TENANT_A_SECRET" && test -n "$TASKFLOW_SLO_TENANT_B_SECRET"
exec python3 "$ROOT/scripts/keycloak-slo-load.py" "$@"
