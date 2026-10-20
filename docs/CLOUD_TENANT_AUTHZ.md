# Cloud resource tenant authorization (opt-in)

## Scope and threat model

This mode isolates the public `/api/v1/cloud` resource, quota, order and Saga
workflow APIs by tenant. A valid token for tenant A must not read, create,
resize, delete, manually reconcile or enumerate tenant B resources by ID,
`tenant_id` query, `tenant_id` request body, or path tenant parameter.

Existing control-plane OIDC role checks are **not** tenant isolation: role
`control-plane-operator` controls permitted operations, while a signed
`tenant_id` or `tenant_ids` claim controls permitted objects. Even operator or
administrator roles do not override tenant grants in this mode.

## Enable only with verified OIDC tokens

The new `CLOUD_TENANT_AUTHZ_ENABLED` flag defaults to `false` for backward
compatibility. Enable it together with:

```
CONTROL_PLANE_AUTH_ENABLED=true
CONTROL_PLANE_AUTH_MODE=oidc
CONTROL_PLANE_OIDC_RBAC_ENABLED=true
CLOUD_TENANT_AUTHZ_ENABLED=true
```

Set the existing `CONTROL_PLANE_OIDC_ISSUER`, `CONTROL_PLANE_OIDC_AUDIENCE`,
`CONTROL_PLANE_OIDC_JWKS_URL`, `CONTROL_PLANE_OIDC_GROUPS_CLAIM`, and
`CONTROL_PLANE_OIDC_ROLE_MAPPINGS` to production identity-provider values.
The token must contain a signed `tenant_id` string or `tenant_ids` string
array issued by the configured OIDC issuer. A header such as `X-Tenant-ID`
does **not** confer access. Token signature, `iss`, `aud`, and expiry are
verified before grants are stored in the request context.

Bearer and hybrid authentication modes are deliberately disallowed when this
flag is enabled, since legacy operator tokens have no trusted tenant claim.

## API behavior

- Missing tenant grant / explicit unauthorized tenant parameter: HTTP 403.
- Foreign resource/workflow IDs resolve to HTTP 404 rather than revealing
  whether the resource exists under another tenant.
- Resource listing without `tenant_id`: automatically scoped if there is
  exactly one granted tenant; otherwise HTTP 400 and an explicit `tenant_id`
  must be provided.
- A non-existent resource still returns the existing HTTP 404 behavior.
- Ownership is checked before any caller-triggered reconcile, resize or delete.
- Internal reconciler/worker operations do not depend on user JWTs.
- When the feature is disabled, the previous API behavior remains unchanged.

## Test and rollout gates

```
go test ./internal/handler ./internal/config ./internal/service
go test -race ./internal/handler ./internal/config
go vet ./...
TASKFLOW_MYSQL_INTEGRATION_DSN='root:...@tcp(127.0.0.1:3306)/disposable_db?parseTime=true' \
  go test -count=1 ./internal/repository -run '^TestCloudTenantScopeMySQLIntegration$'
```

Before activating in a shared cluster, additionally run the real MySQL/Kind
Saga E2E and two-tenant adversarial tests in staging. Validate negative tests
for resource IDs, workflow IDs, replay keys, mutation endpoints and paginated
queries. Existing operators may need an appropriate issuer-issued tenant
claim; do **not** switch on during an active incident without preparing
recovery operator access.

## Database-level controls now implemented

HTTP middleware binds grants from the **verified** JWT to `context.Context`,
and repository read queries and transaction-time `SELECT ... FOR UPDATE`
carry `tenant_id IN (...)` predicates. The code prevents foreign resource
reads and updates even when the caller guesses a valid resource ID. A real
MySQL test checks resource IDs, orders, workflows, quota and writes.

Cloud order uniqueness is being moved from globally unique
`idempotency_key` to `(tenant_id, idempotency_key)`. This is an intentionally
**manual** two-phase migration. Read
[`runbooks/CLOUD_ORDER_SCHEMA_MIGRATION.md`](runbooks/CLOUD_ORDER_SCHEMA_MIGRATION.md)
before enabling `CLOUD_TENANT_AUTHZ_ENABLED`. The API refuses to start in
tenant-isolated mode while the old globally unique index remains.

## Remaining hardening

- Validate two simultaneously active OIDC identities through full REST APIs
  against a disposable MySQL instance and Kind, not only repository tests.
- Add actual identity-provider / audience / JWKS rotation testing and
  negative RBAC tests for each mutation.
- Enforce all *future* new cloud API entrypoints through the same middleware
  and tenant-scope tests; worker-originated reconciliation is intentionally
  privileged and uses a separate, unscoped internal context.
- Reconcile long-lived clients and old idempotency keys before a live rollout.
