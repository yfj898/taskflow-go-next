#!/bin/sh
# An independent operations client using real Keycloak-issued JWTs against
# the isolated, live TaskFlow HTTP/MySQL staging API. No Kubernetes operations.
set -eu

BASE=http://127.0.0.1:18090
TASKFLOW=http://127.0.0.1:18088
ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
BIN=/tmp/taskflowctl-keycloak-20261009
OUT=/tmp/taskflowctl-keycloak-smoke-20261009.json
cleanup() { rm -f "$BIN" "$OUT"; }
trap cleanup EXIT INT TERM

for name in taskflow-keycloak taskflow-keycloak-stage; do
  if ! docker ps --format '{{.Names}}' | grep -qx "$name"; then
    echo "Lab prerequisite $name is not running" >&2
    exit 2
  fi
done
if ! curl -fsS --max-time 4 "$TASKFLOW/health/ready" >/dev/null; then
  echo 'Staging TaskFlow not ready' >&2
  exit 2
fi

admin_password="$(docker inspect taskflow-keycloak --format '{{range .Config.Env}}{{println .}}{{end}}' | sed -n 's/^KC_BOOTSTRAP_ADMIN_PASSWORD=//p')"
admin="$(curl -fsS --max-time 7 -d grant_type=password -d client_id=admin-cli \
  --data-urlencode username=taskflow-lab-admin --data-urlencode "password=$admin_password" \
  "$BASE/realms/master/protocol/openid-connect/token" | jq -r '.access_token // empty')"
unset admin_password
test -n "$admin"

token() {
  id="$(curl -fsS -H "Authorization: Bearer $admin" -G \
    --data-urlencode "clientId=$1" "$BASE/admin/realms/taskflow-lab/clients" | jq -r '.[0].id // empty')"
  test -n "$id"
  secret="$(curl -fsS -H "Authorization: Bearer $admin" \
    "$BASE/admin/realms/taskflow-lab/clients/$id/client-secret" | jq -r '.value // empty')"
  test -n "$secret"
  curl -fsS --max-time 7 -d grant_type=client_credentials \
    --data-urlencode "client_id=$1" --data-urlencode "client_secret=$secret" \
    "$BASE/realms/taskflow-lab/protocol/openid-connect/token" | jq -r '.access_token // empty'
}

a="$(token taskflow-tenant-a)"
b="$(token taskflow-tenant-b)"
reader="$(token taskflow-tenant-a-reader)"
unset admin secret
test -n "$a" && test -n "$b" && test -n "$reader"

docker run --rm --network host \
  -v taskflow-go-mod-cache:/go/pkg/mod -v taskflow-go-build-cache:/root/.cache/go-build \
  -v "$ROOT:/src:ro" -v /tmp:/out -w /src \
  -e GOPROXY=https://goproxy.cn,direct -e HOST_UID="$(id -u)" -e HOST_GID="$(id -g)" \
  golang:1.26.8-alpine sh -c \
  '/usr/local/go/bin/go build -o /out/taskflowctl-keycloak-20261009 ./cmd/taskflowctl && chown "$HOST_UID:$HOST_GID" /out/taskflowctl-keycloak-20261009'

TASKFLOW_ACCESS_TOKEN="$a" "$BIN" -endpoint "$TASKFLOW" quota team-a > "$OUT"
jq -e '.tenant_id=="team-a"' "$OUT" >/dev/null
echo 'PASS taskflowctl quota team-a: real Keycloak token and database'

TASKFLOW_ACCESS_TOKEN="$a" "$BIN" -endpoint "$TASKFLOW" resources team-a 20 0 > "$OUT"
jq -e 'type=="array"' "$OUT" >/dev/null
echo 'PASS taskflowctl resources team-a: authorized resource listing'

TASKFLOW_ACCESS_TOKEN="$a" "$BIN" -endpoint "$TASKFLOW" \
  quota-set team-a 4000 4096 8 -key=staging-ops-quota-20261009 > "$OUT"
jq -e '.cpu_milli_limit==4000 and .replica_limit==8' "$OUT" >/dev/null
echo 'PASS taskflowctl quota-set: real authenticated operator write'

if TASKFLOW_ACCESS_TOKEN="$b" "$BIN" -endpoint "$TASKFLOW" quota team-a >"$OUT" 2>&1; then
  echo 'FAIL tenant B could read tenant A' >&2
  exit 1
fi
grep -q 'HTTP 403' "$OUT"
echo 'PASS taskflowctl cross-tenant read denied HTTP 403'

if TASKFLOW_ACCESS_TOKEN="$reader" "$BIN" -endpoint "$TASKFLOW" \
  quota-set team-a 4000 4096 8 -key=unauthorized-readonly-20261009 >"$OUT" 2>&1; then
  echo 'FAIL read-only Keycloak identity modified quota' >&2
  exit 1
fi
grep -q 'HTTP 403' "$OUT"
echo 'PASS taskflowctl read-only mutation rejected HTTP 403'

if TASKFLOW_ACCESS_TOKEN="$a" "$BIN" -endpoint "$TASKFLOW" \
  quota-set team-a 4000 4096 8 >"$OUT" 2>&1; then
  echo 'FAIL mutation without business idempotency key' >&2
  exit 1
fi
grep -q -- '-key=' "$OUT"
echo 'PASS taskflowctl blocks write without a stable idempotency key'
echo 'TASKFLOWCTL_KEYCLOAK_MYSQL_E2E_PASS'
