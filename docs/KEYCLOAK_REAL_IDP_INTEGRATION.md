# Real Keycloak OIDC integration — TaskFlow-Go (2026-10-09)

## Summary and boundaries

This repository now contains a **real, self-hosted Keycloak 26.8.0 identity
provider** lab, backed by an isolated persistent PostgreSQL 17 database.
Keycloak actually authenticates four confidential OAuth clients and signs
their RS256 access tokens. TaskFlow reads the identity provider's JWKS,
validates signature/issuer/audience/expiry and uses the signed `groups` and
`tenant_id` claims to enforce RBAC and MySQL tenant isolation.

This is a **loopback-only developer integration lab**, not a production IdP.
Keycloak runs with `start-dev` over HTTP; **never** expose ports 18090 or
18088 directly to the Internet. Production requires HTTPS, trusted hostnames,
external secret management, audited admin identities, backup/restore and a
proper operational availability design.

## Source files

| Path | Role |
| --- | --- |
| `deploy/keycloak/compose.yaml` | Keycloak + PostgreSQL deployment, private network/localhost port |
| `deploy/keycloak/realm-taskflow-lab.json` | Idempotent import of realm and four confidential workload clients; **no client secrets are hardcoded** |
| `internal/handler/cloud_keycloak_integration_test.go` | Real signed JWT -> JWKS -> Gin/RBAC -> MySQL integration test |
| `scripts/keycloak-verify.sh` | Automatically obtains four lab service credentials and runs real-token integration test with disposable MySQL |
| `scripts/keycloak-stage-up.sh` | Starts a separate TaskFlow API with verified OIDC-only mode and isolated MySQL/Redis |
| `scripts/keycloak-live-smoke.sh` | Makes HTTP requests to the actual staging server, checking 13 positive/negative authorization conditions |

The lab includes **four machine-to-machine service identities**:

| Client | Signed token `tenant_id` | Signed `groups` | Purpose |
| --- | --- | --- | --- |
| `taskflow-tenant-a` | `team-a` | `["operators"]` | Create/update own resources |
| `taskflow-tenant-b` | `team-b` | `["operators"]` | Independent tenant access |
| `taskflow-tenant-a-reader` | `team-a` | `["readers"]` | GET allowed, mutation denied |
| `taskflow-tenant-a-no-grant` | not present | `["operators"]` | Fail-closed negative case |

Protocol mappers are **hardcoded per workload client by design** in this
integration lab. For real *human* user sign-in, configure Keycloak group
membership and user attribute mappers so claim values come from centrally
managed user/group assignments rather than static test-client mappers. Enable
MFA and use Authorization Code + PKCE for interactive client login. Do not
enable password grant for production users.

## First boot (local-only)

From the `taskflow-go` directory:

```sh
# Fresh deployment only. Keep credentials in a protected secret manager for
# persistent management of the Compose stack; do not commit them.
export KEYCLOAK_DB_PASSWORD="$(openssl rand -hex 24)"
export KEYCLOAK_ADMIN_PASSWORD="$(openssl rand -hex 24)"
docker compose -f deploy/keycloak/compose.yaml up -d

curl -fsS http://127.0.0.1:18090/realms/taskflow-lab/.well-known/openid-configuration | jq '{issuer,jwks_uri,token_endpoint}'
```

**Important:** when the persistent PostgreSQL volume already exists, do not
replace its database password with a newly generated one: the existing
database credential must be used. `docker restart` of the preexisting
containers preserves their startup environment. If recreating containers,
retrieve stored credentials from a properly secured source; do not keep
plaintext passwords in source control. This lab stores runtime passwords in
Docker container configuration, visible to local Docker administrators.

Admin console on the Linux host: `http://127.0.0.1:18090/admin/`.
Bootstrap username: `taskflow-lab-admin`.
The bootstrap password was generated at deployment and is **not present in
this repository**. For this isolated lab only, a Docker administrator can
recover it from the container environment via `docker inspect`; rotate or
replace this bootstrap identity before adopting a more secure deployment.

On Windows with SSH access to Linux, tunnel rather than exposing the lab:

```sh
ssh -N -L 18090:127.0.0.1:18090 <linux-user>@<linux-server>
```

The browser can then open `http://127.0.0.1:18090/admin/`.

## Issuer, audience and signature verification

Local Keycloak OIDC metadata:

```
issuer: http://127.0.0.1:18090/realms/taskflow-lab
audience: taskflow-control-plane
jwks: http://127.0.0.1:18090/realms/taskflow-lab/protocol/openid-connect/certs
```

The isolated TaskFlow API runs in a Docker network, so its JWKS fetch URL is
`http://taskflow-keycloak:8080/realms/taskflow-lab/protocol/openid-connect/certs`
while its configured `CONTROL_PLANE_OIDC_ISSUER` matches the *external*
issuer in the JWT. This distinction is required because `127.0.0.1` inside
the TaskFlow container would otherwise refer to TaskFlow, not Keycloak.

The staging deployment sets:

```
CONTROL_PLANE_AUTH_ENABLED=true
CONTROL_PLANE_AUTH_MODE=oidc
CONTROL_PLANE_OIDC_RBAC_ENABLED=true
CONTROL_PLANE_OIDC_GROUPS_CLAIM=groups
CONTROL_PLANE_OIDC_ROLE_MAPPINGS=operators=control-plane-operator;readers=control-plane-reader
CONTROL_PLANE_OIDC_ISSUER=http://127.0.0.1:18090/realms/taskflow-lab
CONTROL_PLANE_OIDC_AUDIENCE=taskflow-control-plane
CLOUD_TENANT_AUTHZ_ENABLED=true
```

The existing production TaskFlow container keeps its own configuration. Do
not globally flip any of the above flags before migrating the target DB's
cloud-order uniqueness index and reviewing its identity-provider grants.

## Verify the real IdP in the isolated lab

```sh
# Full Go Gin test against actual Keycloak-signed tokens and disposable MySQL:
sh scripts/keycloak-verify.sh

# Separate TaskFlow server at 127.0.0.1:18088 with Keycloak enabled:
sh scripts/keycloak-stage-up.sh

# Actual HTTP authorization/RBAC checks against that server:
sh scripts/keycloak-live-smoke.sh

# Non-destructive overlap-window rotation (one higher-priority RSA key in lab):
sh scripts/keycloak-rotation-smoke.sh

# Controlled short IdP outage. Stops/restarts only the isolated Keycloak lab:
sh scripts/keycloak-idp-outage-smoke.sh

# Independent Kind cluster, actual two-pod TaskFlow + Keycloak + Kubernetes
# resource Provision/Delete with scoped OIDC JWTs (several minutes):
sh scripts/cloud-control-kind-oidc-e2e.sh

# Short live SLO sample. Longer runs support JWT automatic refresh:
sh scripts/keycloak-slo-run.sh --duration 60 --rate 40 --concurrency 12 --output /tmp/taskflow-keycloak-slo.json

# Health and readiness:
curl -fsS http://127.0.0.1:18088/health/live
curl -fsS http://127.0.0.1:18088/health/ready
```

The first script can launch a disposable test MySQL instance, stops it when
the script created it, and never uses the running `taskflow-go-mysql-1`
business database. The second script launches a separate staging MySQL/Redis
pair and TaskFlow API; there is no host port collision with the normal 8080
service. The staging TaskFlow runs with `CLOUD_CONTROL_ENABLED=false`; this
auth-focused lab does **not** apply actual Kubernetes resources.

Observed on 2026-10-09 in the connected Linux workspace:

- Keycloak 26.8.0 OIDC discovery returned HTTP 200.
- Four actual Keycloak `client_credentials` access tokens had the intended
  `iss`, `aud`, `tenant_id`/missing grant and `groups` claims.
- `TestKeycloakRealTokenTenantIsolation` **PASS** with actual Keycloak JWKS,
  tenant-scoped MySQL, RBAC denial and invalid-signature rejection.
- Actual staging API at `127.0.0.1:18088` passed 13 authorization HTTP checks
  (401 for missing/forged JWT, 403 for foreign tenant, reader mutation or
  missing tenant claim, 200 for permitted tenant quota read/write).
- Restarted the isolated Keycloak container; persisted realm and service
  clients remained available from PostgreSQL, and all 13 staging HTTP checks
  passed again without changing the running TaskFlow staging container.
- Added a higher-priority RSA-generated signing key using the Keycloak Admin
  API. Both new and still-valid old tokens remained accepted by the separate
  TaskFlow API, and the deliberately corrupted signature was rejected (401).
- Keycloak-issued JWTs were also rejected by Go verifiers configured with a
  wrong audience, wrong issuer or a simulated verifier clock past the token
  expiry. This is a clock-injection expiry test, not a real multi-minute wait.
- A controlled lab-only Keycloak container stop verified that OAuth token
  issuance becomes unavailable but a previously verified, still-valid JWT
  remained accepted during its short JWKS cache interval; after Keycloak
  restarted, newly issued JWTs were accepted again. This is **not** an IdP
  high-availability or revocation guarantee.
- **Full-stack Kind/Keycloak E2E passed:** a newly created two-replica Kind
  TaskFlow deployment switched to OIDC-only. A genuinely issued team-a JWT
  created a Kubernetes Deployment through Saga, team-b could not access the
  resource ID, and a signed team-a delete JWT removed the workload and
  released CPU/replica quota. The script deletes the Kind cluster and restores
  the original `taskflow-go:latest` image tag. Result:
  `KIND_KEYCLOAK_FULL_STACK_E2E_PASS`. The GitHub Actions workflow
  `.github/workflows/kind-oidc-fullstack.yml` is manually triggered, not a
  claimed completed cloud CI run.

## Identity system readiness still requires

1. Deploy Keycloak in production mode with HTTPS, stable trusted hostname,
   external secret management and PostgreSQL backup/restore.
2. Replace per-client hardcoded claims with managed user attributes/group
   membership for human users; enable MFA and secure client flows.
3. Test signing key rotation against TaskFlow's cached JWKS (verify both
   old and new keys during rotation). Test logout/revocation propagation and
   short-lived token expiry handling.
4. Introduce a constrained, reviewed operator break-glass procedure; the
   existing OIDC-only cloud authorization intentionally forbids legacy bearer
   tokens when tenant mode is enabled.
5. Re-run Kind real-Kubernetes resource lifecycle and 72-hour SLO soak tests
   with the finalized HTTPS IdP address. Do not claim production readiness on
   the basis of the above development-lab passing tests alone.

Official references: https://www.keycloak.org/getting-started/getting-started-docker ,
https://www.keycloak.org/server/containers ,
https://www.keycloak.org/admin-api/protocol-mappers .
