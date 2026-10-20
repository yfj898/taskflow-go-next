# Keycloak production-mode deployment gate

This is an intentionally **isolated, non-public production-mode** deployment,
not a declaration that the identity system is production-ready. The existing
`deploy/keycloak/compose.yaml` continues to serve the development lab on 18090
and is not modified by these operations.

## Architecture and security properties

- Keycloak 26.8.0 runs `start`, **never** `start-dev`, with a fixed HTTPS
  hostname and PEM certificate and key. HTTP login listener is disabled.
- PostgreSQL has persistent storage, a private internal-only network and no
  host port; DB password is provided via a Docker Compose secret file.
- The Keycloak public TLS listener binds to host `127.0.0.1:18443` by default;
  `9000` management/health/metrics is not published to the host.
- Administrative credentials and Keycloak's DB password are injected as
  environment variables. **Docker administrators can inspect env values**;
  use a cloud secrets controller, Kubernetes Secrets with envelope encryption,
  or a proper secret operator for a real production cluster. Do not paste
  passwords into shell command history, commit them or copy Docker inspection
  output into public logs.
- The Compose network/deployment is not a multi-node HA architecture, even
  though it runs Keycloak in production mode. A real rollout additionally
  requires ≥2 Keycloak instances, a HA database, tested backup/restore and
  trusted TLS reachable by all callers.
- Never import `realm-taskflow-lab.json` as a production realm. It contains
  static machine-client tenant/group claim mappers intended for tests only.

## Production prerequisites

Provide a trusted DNS issuer such as `https://auth.example.com`, a valid
certificate whose SAN matches that hostname, private key with restrictive
permissions, production PostgreSQL credentials from a secret manager, and
approved operator account. For the actual deployment, the Keycloak issuer
must be exactly the `issuer` emitted by OIDC Discovery, and every TaskFlow
replica must trust the TLS certificate and be able to fetch JWKS over HTTPS.

## Configuration preview without starting services

```sh
export KEYCLOAK_SECURE_PUBLIC_URL=https://auth.example.com
export KEYCLOAK_SECURE_TLS_DIR=/secure/keycloak/tls
export KEYCLOAK_SECURE_DB_PASSWORD_FILE=/secure/keycloak/db-password
export KEYCLOAK_SECURE_DB_PASSWORD='injected-by-secret-manager'
export KEYCLOAK_SECURE_ADMIN_PASSWORD='injected-by-secret-manager'
docker compose -f deploy/keycloak/production/compose.yaml config --quiet
```

Do not use the example placeholders for an actual deployment. The lab
verification script `scripts/keycloak-production-mode-smoke.sh` generates
a **throwaway, self-signed localhost** certificate and random credentials
outside this repository and starts this Compose stack only on loopback.
The lab does **not** validate publicly trusted TLS or production HA.

## Deployment exit criteria

1. All URLs including OIDC Discovery, JWKS, redirect and admin endpoints use
   the exact stable HTTPS issuer; TLS validation succeeds without `-k`.
2. Certificates are issued and renewed through an approved certificate
   service. Admin endpoints are separately restricted to an operator network.
3. Human identities use Authorization Code + PKCE, approved group/tenant
   mappers, MFA and documented offboarding/revocation handling.
4. Backup/restore has been rehearsed; at least two nodes and database failover
   have been exercised under authenticated load.
5. Key signing-key rotation and JWKS TTL behavior are audited. Break-glass
   identities and audit exports have explicit owners.
6. Keycloak version, container digest, migration plan and trusted CA bundle
   are pinned in CI and release manifests; secrets are not present in Git.

See official Keycloak documentation:
https://www.keycloak.org/server/configuration-production
https://www.keycloak.org/server/enabletls
https://www.keycloak.org/server/hostname
https://www.keycloak.org/server/management-interface
