#!/bin/sh
# Real Keycloak OIDC -> two TaskFlow Pods in a fresh Kind cluster -> actual
# Kubernetes Deployment Provision/Delete, with tenant A/B isolation.
# Reuses the existing Saga E2E to initialize a proven disposable cluster.
# Does not access or modify any existing TaskFlow business DB or cluster.
set -eu

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT"
NAME=taskflow-kind-keycloak-20261009
NS=taskflow-kind-keycloak-20261009
TOOLS=/tmp/taskflow-release-k8s-tools
KIND="$TOOLS/kind"
KUBECTL="$TOOLS/kubectl"
CONFIG=/tmp/taskflow-kind-keycloak-20261009-kubeconfig
LAB=http://127.0.0.1:18090
API=http://127.0.0.1:18280/api/v1/cloud
restore_latest=0
if docker image inspect taskflow-go:latest >/dev/null 2>&1; then
  ORIGINAL_LATEST="$(docker image inspect taskflow-go:latest --format '{{.Id}}')"
  restore_latest=1
fi
forward_pid=""
keycloak_connected=0
phase=preflight

if [ ! -x "$KIND" ] || [ ! -x "$KUBECTL" ]; then
  echo 'Kind / kubectl tooling not installed' >&2
  exit 2
fi
if ! docker ps --format '{{.Names}}' | grep -qx taskflow-keycloak; then
  echo 'Isolated Keycloak lab required' >&2
  exit 2
fi
if "$KIND" get clusters 2>/dev/null | grep -qx "$NAME"; then
  echo "Refusing to modify existing Kind cluster $NAME" >&2
  exit 2
fi

cleanup() {
  code=$?
  if [ -n "$forward_pid" ]; then
    kill "$forward_pid" >/dev/null 2>&1 || true
  fi
  if [ "$keycloak_connected" = 1 ]; then
    docker network disconnect kind taskflow-keycloak >/dev/null 2>&1 || true
  fi
  "$KIND" delete cluster --name "$NAME" >/dev/null 2>&1 || true
  if [ "$restore_latest" = 1 ]; then
    docker tag "$ORIGINAL_LATEST" taskflow-go:latest >/dev/null 2>&1 || true
  else
    # CI runners start without this local tag. Remove only the tag built by
    # the disposable E2E, never images owned by an existing local deployment.
    docker image rm taskflow-go:latest >/dev/null 2>&1 || true
  fi
  if [ "$code" -ne 0 ]; then
    echo "KIND_KEYCLOAK_E2E_FAILED phase=$phase code=$code" >&2
  fi
}
trap cleanup EXIT INT TERM
k() { "$KUBECTL" --kubeconfig "$CONFIG" "$@"; }

phase=baseline_full_kubernetes_lifecycle
echo 'Preparing isolated Kind cluster with existing full Saga E2E ...'
CLOUD_SAGA_E2E_CLUSTER="$NAME" \
CLOUD_SAGA_E2E_NAMESPACE="$NS" \
CLOUD_SAGA_E2E_KUBECONFIG="$CONFIG" \
CLOUD_SAGA_E2E_KEEP_CLUSTER=1 \
  sh scripts/cloud-control-kind-e2e.sh >/tmp/taskflow-kind-keycloak-baseline-20261009.log 2>&1
grep -q 'CLOUD_CONTROL_KIND_E2E_OK' /tmp/taskflow-kind-keycloak-baseline-20261009.log
echo 'PASS isolated Kind resource lifecycle baseline'

phase=attach_keycloak_to_kind_network
if docker network inspect kind --format '{{range .Containers}}{{.Name}}{{" "}}{{end}}' | grep -qw taskflow-keycloak; then
  echo 'Keycloak already in Kind Docker network; refusing to mutate shared network' >&2
  exit 2
fi
docker network connect kind taskflow-keycloak
keycloak_connected=1
KEYCLOAK_IP="$(docker inspect taskflow-keycloak --format '{{(index .NetworkSettings.Networks "kind").IPAddress}}')"
if [ -z "$KEYCLOAK_IP" ]; then
  echo 'Could not resolve Keycloak IP in Kind network' >&2
  exit 1
fi
echo 'Keycloak is reachable through private Kind Docker network (not public HTTP)'

phase=switch_two_pods_to_strict_oidc
# The existing E2E secret contains a legacy bearer. Explicit empty env values
# override old envFrom secrets so strict OIDC-only config validation succeeds.
k -n "$NS" set env deployment/taskflow \
  CONTROL_PLANE_AUTH_ENABLED=true \
  CONTROL_PLANE_AUTH_MODE=oidc \
  CONTROL_PLANE_BEARER_TOKEN= \
  CONTROL_PLANE_PREVIOUS_BEARER_TOKEN= \
  CONTROL_PLANE_OIDC_RBAC_ENABLED=true \
  CONTROL_PLANE_OIDC_ISSUER="$LAB/realms/taskflow-lab" \
  CONTROL_PLANE_OIDC_AUDIENCE=taskflow-control-plane \
  CONTROL_PLANE_OIDC_JWKS_URL="http://$KEYCLOAK_IP:8080/realms/taskflow-lab/protocol/openid-connect/certs" \
  CONTROL_PLANE_OIDC_GROUPS_CLAIM=groups \
  'CONTROL_PLANE_OIDC_ROLE_MAPPINGS=operators=control-plane-operator;readers=control-plane-reader' \
  CLOUD_TENANT_AUTHZ_ENABLED=true >/dev/null
k -n "$NS" rollout status deployment/taskflow --timeout=180s >/dev/null
replicas="$(k -n "$NS" get deploy taskflow -o jsonpath='{.status.readyReplicas}')"
test "$replicas" = 2

k -n "$NS" port-forward service/taskflow 18280:80 >/tmp/taskflow-kind-keycloak-portforward.log 2>&1 &
forward_pid=$!
i=0
until curl -fsS --max-time 2 http://127.0.0.1:18280/health/ready >/dev/null 2>&1; do
  i=$((i+1))
  if [ "$i" -gt 35 ]; then
    echo 'OIDC-enabled Kind TaskFlow API did not become ready' >&2
    exit 1
  fi
  sleep 1
done
code="$(curl -sS --max-time 5 -o /dev/null -w '%{http_code}' "$API/resources")"
test "$code" = 401
echo 'PASS two real Kind TaskFlow Pods started with OIDC-only auth'

phase=fetch_real_service_credentials
password="$(docker inspect taskflow-keycloak --format '{{range .Config.Env}}{{println .}}{{end}}' | sed -n 's/^KC_BOOTSTRAP_ADMIN_PASSWORD=//p')"
admin="$(curl -fsS --max-time 6 -d grant_type=password -d client_id=admin-cli \
  --data-urlencode username=taskflow-lab-admin --data-urlencode "password=$password" \
  "$LAB/realms/master/protocol/openid-connect/token" | jq -r '.access_token // empty')"
unset password
test -n "$admin"
issue_token() {
  uuid="$(curl -fsS -G -H "Authorization: Bearer $admin" \
    --data-urlencode "clientId=$1" "$LAB/admin/realms/taskflow-lab/clients" | jq -r '.[0].id // empty')"
  test -n "$uuid"
  secret="$(curl -fsS -H "Authorization: Bearer $admin" \
    "$LAB/admin/realms/taskflow-lab/clients/$uuid/client-secret" | jq -r '.value // empty')"
  test -n "$secret"
  curl -fsS -d grant_type=client_credentials --data-urlencode "client_id=$1" \
    --data-urlencode "client_secret=$secret" \
    "$LAB/realms/taskflow-lab/protocol/openid-connect/token" | jq -r '.access_token // empty'
}
A="$(issue_token taskflow-tenant-a)"
B="$(issue_token taskflow-tenant-b)"
unset admin secret
test -n "$A" && test -n "$B"

phase=tenant_quota
quota='{"cpu_milli_limit":6000,"memory_mib_limit":6144,"replica_limit":8}'
for tenant in team-a team-b; do
  if [ "$tenant" = team-a ]; then token="$A"; else token="$B"; fi
  result="$(curl -sS --max-time 8 -o /dev/null -w '%{http_code}' -X PUT \
    -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
    --data "$quota" "$API/tenants/$tenant/quota")"
  test "$result" = 200
done
result="$(curl -sS --max-time 8 -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $B" "$API/tenants/team-a/quota")"
test "$result" = 403

phase=oidc_authenticated_provision_kubernetes
payload="{\"tenant_id\":\"team-a\",\"name\":\"oidc-kind-real\",\"provider\":\"kubernetes\",\"spec\":{\"compute\":{\"namespace\":\"$NS\",\"image\":\"nginx:1.27-alpine\",\"replicas\":1,\"cpu\":\"50m\",\"memory\":\"64Mi\"},\"network\":{\"enabled\":false},\"storage\":{\"enabled\":false}}}"
response="$(curl -fsS --max-time 10 -X POST -H "Authorization: Bearer $A" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: kind-keycloak-oidc-provision-20261009' \
  --data "$payload" "$API/workflows/provision")"
workflow="$(printf '%s' "$response" | jq -r '.workflow.id // empty')"
resource="$(printf '%s' "$response" | jq -r '.resource.id // empty')"
test -n "$workflow" && test -n "$resource"
status="$(curl -sS --max-time 6 -o /dev/null -w '%{http_code}' \
  -H "Authorization: Bearer $B" "$API/resources/$resource")"
test "$status" = 404
echo 'PASS real JWT isolation against Kubernetes-backed resource ID'

wait_workflow() {
  id="$1"; expected="$2"
  i=0
  while [ "$i" -lt 160 ]; do
    state="$(curl -fsS --max-time 6 -H "Authorization: Bearer $A" "$API/workflows/$id" | jq -r '.workflow.status // empty')"
    if [ "$state" = "$expected" ]; then return 0; fi
    curl -sS --max-time 15 -o /dev/null -X POST -H "Authorization: Bearer $A" \
      "$API/workflows/$id/reconcile" || true
    i=$((i+1))
    sleep 0.5
  done
  echo "workflow $id failed to reach $expected: $state" >&2
  return 1
}
wait_workflow "$workflow" succeeded
k -n "$NS" rollout status deployment/oidc-kind-real --timeout=120s >/dev/null
k -n "$NS" get deployment oidc-kind-real >/dev/null
echo 'PASS real Keycloak JWT created a live Kubernetes Deployment through Saga'

phase=oidc_authenticated_delete_kubernetes
deleted="$(curl -fsS --max-time 10 -X POST -H "Authorization: Bearer $A" \
  -H 'Idempotency-Key: kind-keycloak-oidc-delete-20261009' \
  "$API/resources/$resource/workflows/delete")"
delete_workflow="$(printf '%s' "$deleted" | jq -r '.workflow.id // empty')"
test -n "$delete_workflow"
wait_workflow "$delete_workflow" succeeded
if k -n "$NS" get deployment oidc-kind-real >/dev/null 2>&1; then
  echo 'Live Deployment still present after delete Saga' >&2
  exit 1
fi
usage="$(curl -fsS -H "Authorization: Bearer $A" "$API/tenants/team-a/quota")"
test "$(printf '%s' "$usage" | jq -r '.cpu_milli_used')" = 0
test "$(printf '%s' "$usage" | jq -r '.replica_used')" = 0
echo 'PASS live Kubernetes object removed and tenant A quota fully released'
echo 'KIND_KEYCLOAK_FULL_STACK_E2E_PASS'
