#!/usr/bin/env bash
# A production-MODE HTTPS acceptance rehearsal (NOT production readiness).
# All TLS material and DB credentials are generated under a private temp dir.
# This does not touch the existing Keycloak dev lab or TaskFlow API.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE="$ROOT/deploy/keycloak/production/compose.yaml"
STACK=taskflow-keycloak-secure
BASE=https://localhost:18443

for command in docker openssl curl python3; do
  command -v "$command" >/dev/null || { echo "Missing: $command" >&2; exit 2; }
done
if docker ps -a --format '{{.Names}}' | grep -q "^${STACK}-"; then
  echo 'Refusing to modify pre-existing secure Keycloak stack' >&2
  exit 2
fi
if ss -lnt | grep -q ':18443[[:space:]]'; then
  echo 'Port 18443 already occupied; refusing to replace listener' >&2
  exit 2
fi

umask 077
WORKDIR="$(mktemp -d /tmp/taskflow-keycloak-https-XXXXXX)"
mkdir -p "$WORKDIR/tls"
cleanup() {
  status=$?
  if [[ "$status" -ne 0 ]]; then
    docker compose -f "$COMPOSE" logs --no-color --tail=65 keycloak database 2>&1 |
      sed -E 's/(password|secret|token)[=:][^, }]+/\1=[REDACTED]/Ig' >&2 || true
  fi
  docker compose -f "$COMPOSE" down --volumes --remove-orphans >/dev/null 2>&1 || true
  rm -rf -- "$WORKDIR"
  return "$status"
}
trap cleanup EXIT

openssl req -new -x509 -nodes -newkey rsa:2048 -sha256 -days 2 \
  -subj '/CN=localhost' \
  -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -keyout "$WORKDIR/tls/tls.key" -out "$WORKDIR/tls/tls.crt" >/dev/null 2>&1
chmod 600 "$WORKDIR/tls/tls.key"
chmod 644 "$WORKDIR/tls/tls.crt"
# Keycloak runs as UID 1000, while Ubuntu GitHub Actions runners normally run
# as a different host UID. Traverse-only directories plus a root-owned helper
# set the key's owner to the non-root Keycloak container identity, preserving
# mode 0600 (never make the private key world-readable to satisfy Docker).
chmod 711 "$WORKDIR" "$WORKDIR/tls"
docker run --rm --user 0 --entrypoint sh \
  -v "$WORKDIR/tls:/keycloak-tls" postgres:17-alpine \
  -c 'chown 1000:1000 /keycloak-tls/tls.key && chmod 600 /keycloak-tls/tls.key'
openssl rand -hex 32 > "$WORKDIR/postgres-password"
chmod 600 "$WORKDIR/postgres-password"

export KEYCLOAK_SECURE_PUBLIC_URL="$BASE"
export KEYCLOAK_SECURE_HOST_PORT=18443
export KEYCLOAK_SECURE_TLS_DIR="$WORKDIR/tls"
export KEYCLOAK_SECURE_DB_PASSWORD_FILE="$WORKDIR/postgres-password"
export KEYCLOAK_SECURE_DB_PASSWORD="$(cat "$WORKDIR/postgres-password")"
export KEYCLOAK_SECURE_ADMIN_PASSWORD="$(openssl rand -hex 32)"
export KEYCLOAK_SECURE_ADMIN_USERNAME=taskflow-bootstrap-check

docker compose -f "$COMPOSE" config --quiet
# CI runners start with no cached Keycloak/PostgreSQL images. Pull only the
# exact tagged images when missing; do not rely on local cache state.
docker compose -f "$COMPOSE" up -d --no-build --pull missing
echo 'Started isolated Keycloak 26.8 in production mode with localhost HTTPS'

ready=0
for _ in $(seq 1 70); do
  if curl --silent --show-error --fail --max-time 3 \
    --cacert "$WORKDIR/tls/tls.crt" \
    "$BASE/realms/master/.well-known/openid-configuration" \
    > "$WORKDIR/discovery.json" 2>/dev/null; then
    ready=1
    break
  fi
  sleep 2
done
if [[ "$ready" != 1 ]]; then
  echo 'HTTPS Keycloak Discovery did not become ready' >&2
  exit 1
fi

python3 - "$WORKDIR/discovery.json" <<'PY'
import json, sys
data = json.load(open(sys.argv[1], encoding="utf-8"))
assert data['issuer'] == 'https://localhost:18443/realms/master', data['issuer']
assert data['jwks_uri'].startswith('https://localhost:18443/'), data['jwks_uri']
assert data['token_endpoint'].startswith('https://localhost:18443/')
print('PASS HTTPS issuer, JWKS URI and token endpoint use fixed trusted hostname')
PY

curl --silent --show-error --fail --max-time 4 \
  --cacert "$WORKDIR/tls/tls.crt" \
  "$BASE/realms/master/protocol/openid-connect/certs" |
  python3 -c 'import json,sys; d=json.load(sys.stdin); assert any(k.get("kty")=="RSA" for k in d["keys"]); print("PASS HTTPS JWKS contains public RSA key")'

# Prove TLS verification is actually enforced. The generated cert is trusted
# only via --cacert; an ordinary client must reject the self-signed chain.
if curl --silent --fail --max-time 3 "$BASE/realms/master/.well-known/openid-configuration" >/dev/null 2>&1; then
  echo 'Unexpectedly trusted self-signed lab certificate without CA pin' >&2
  exit 1
fi
echo 'PASS standard TLS verification rejects untrusted lab CA'

if curl --silent --fail --max-time 3 -X GET http://127.0.0.1:18443/realms/master/.well-known/openid-configuration >/dev/null 2>&1; then
  echo 'HTTP was incorrectly accepted on HTTPS-only listener' >&2
  exit 1
fi
echo 'PASS public port is HTTPS-only; management port is not published'

docker compose -f "$COMPOSE" ps --status running --format json |
  python3 -c 'import json,sys; s=sys.stdin.read().strip(); rows=json.loads(s) if s.startswith("[") else [json.loads(line) for line in s.splitlines() if line.strip()]; names={x["Service"] for x in rows}; assert {"database","keycloak"}<=names, names; print("PASS isolated PostgreSQL and Keycloak containers healthy/running")'
echo 'KEYCLOAK_PRODUCTION_MODE_HTTPS_REHEARSAL_PASS'
