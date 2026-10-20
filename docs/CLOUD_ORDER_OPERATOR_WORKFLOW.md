# Real TaskFlow business order workflow — `taskflowctl`

**Users:** cloud platform operators, tenant service accounts and internal
application teams. `taskflowctl` is an actual Go client of the existing
OIDC-protected HTTP API, not a mocked control-plane backend. It makes no
direct database or Kubernetes calls and does not bypass RBAC.

## Build and authenticate

```bash
go build -o ./bin/taskflowctl ./cmd/taskflowctl
export TASKFLOW_ENDPOINT=https://taskflow.internal.example.com
# Obtain a short-lived OIDC access token from the trusted IdP; do not commit,
# print, or put it in shell command arguments. For non-lab use HTTPS.
export TASKFLOW_ACCESS_TOKEN="$(your-trusted-oidc-login-command)"
```

Token sources: `TASKFLOW_ACCESS_TOKEN` or `-token-file /private/path` (mode
`0600`). The CLI refuses plaintext HTTP except loopback, refuses negative
quotas, validates all resource IDs and spec JSON, has bounded timeouts,
returns non-zero on HTTP 401/403/404/409/5xx, and never retries writes
automatically. `pkg/cloudclient` handles endpoint requests and typed errors.

On Windows, use `TASKFLOW_ACCESS_TOKEN`. The CLI rejects `-token-file`
because its POSIX mode check cannot validate Windows ACLs. An invalid file
option does not silently fall back to an environment token.

## A real tenant order lifecycle

Provision is an asynchronous **Saga**. The first HTTP 202 means *accepted*,
not “Kubernetes is ready”; clients must query the durable workflow state.

```bash
# 1. Tenant quota and existing resources
./bin/taskflowctl quota team-a
./bin/taskflowctl resources team-a 20 0

# 2. A stable key is derived from a real ticket/order and reused on retry.
./bin/taskflowctl provision team-a preview-app \
  examples/cloud-resource-order/bundle.json -key=change-20261009-001
# Response contains workflow.id, resource.id and order.id. Do not assume 202
# means completed. Save the identifiers in the calling system.

# 3. Inspect execution steps, order history and wait for terminal state.
./bin/taskflowctl workflow <WORKFLOW_ID>
./bin/taskflowctl wait <WORKFLOW_ID> -wait-timeout=10m
./bin/taskflowctl orders <RESOURCE_ID>

# 4. Change capacity (edit the bundle.json file with the desired replica count)
./bin/taskflowctl resize <RESOURCE_ID> \
  examples/cloud-resource-order/bundle.json -key=change-20261009-002

# 5. Delete resource via a new durable Saga; resource removal must be observed
# before reserved CPU/memory/replica quota is released.
./bin/taskflowctl delete <RESOURCE_ID> -key=change-20261009-003
./bin/taskflowctl workflows <RESOURCE_ID>
./bin/taskflowctl quota team-a
```

**Important:** The example namespace must already exist and the TaskFlow
Kubernetes ServiceAccount must have the needed permissions. This bundled spec
is a real request template, not a secret or permission grant. An operator must
provide correctly scoped Kubernetes RBAC/ResourceQuota before use. Configure
`CLOUD_CONTROL_ENABLED=true` only in an authorized test or production
deployment with a configured Provider; the Keycloak-only 18088 staging API
has Kubernetes operations intentionally disabled.

## Why the architecture is enterprise relevant

| Customer problem | Mechanism | Evidence |
| --- | --- | --- |
| API timed out after creating Kubernetes object | Durable workflow steps, outcome-unknown handling and observe-before-compensate | Kind + Chaos E2E verifies owner Pod kill and replay without duplicate Service UID |
| Duplicate HTTP submission by calling service | Tenant-scoped idempotency key, unique DB index, conditional mutation | Real Keycloak/HTTP/MySQL replay test across team-a and team-b |
| Quota oversold under concurrent requests | MySQL transaction, row locks, optional bucket ledger and compensating release | Real MySQL 160 contenders / capacity 80; no over-allocation |
| Tenant A sees tenant B resource | OIDC signed tenant claim, RBAC and scoped persistence queries | Keycloak signed JWT → two Kind TaskFlow Pods → real Deployment (404 foreign resource) |
| Caller cannot inspect partial failure | `workflow`, `steps`, `order`, `resource`, `quota` queries and structured errors | Go SDK tests and `taskflowctl` command tests |

## Verification / limitations

```bash
go test -count=1 ./pkg/cloudclient ./cmd/taskflowctl
go test -race -count=1 ./pkg/cloudclient ./cmd/taskflowctl
go build ./cmd/taskflowctl
```

The CLI is an operational interface for existing services, not evidence that
external companies are using this code. All throughput and correctness
figures refer to recorded controlled experiments. GitHub cloud CI is not
claimed until there is an actual remote Actions run. A 72-hour mixed Saga
stress test is not claimed by the short OIDC GET SLO tests.
