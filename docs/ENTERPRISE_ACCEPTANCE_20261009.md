# TaskFlow-Go enterprise acceptance ledger (2026-10-09)

## Product scope

Enterprise internal multi-tenant Kubernetes resource application and reliable
asynchronous-workflow control plane. The product is not a claim to support all
public-cloud VMs, VPCs or every workflow feature in Temporal. Scope includes
cloud resources, persistent Saga, quota, identity isolation, operations,
release safety and a callable API.

`PASS` means an observed reproducible test passed under its stated boundary.
`PARTIAL` and `NOT VERIFIED` are intentionally not counted as complete.

| Area | Current status | Evidence / remaining gate |
| --- | --- | --- |
| Verified OIDC tenant scopes | PASS in tests; deployment OFF | Signed JWT -> Gin -> Service -> MySQL tests; enable explicitly after identity provider integration |
| DB ownership for resource/order/workflow | PASS on isolated MySQL | Scoped reads, tenant-fenced transactional locks, foreign-ID access tests |
| Tenant-scoped idempotency | PASS on isolated MySQL | Composite `(tenant_id, key)` uniqueness; explicit two-phase index migration; duplicate-tenant-key test |
| Safe old/new index coexistence | PASS in isolated mixed-version process drill | Historical `taskflow-go:local` and new `enterprise-acceptance-20261009` both started during expand, old writer stopped, contract completed, new binary started OIDC-only and accepted a real Keycloak JWT. `scripts/cloud-mixed-version-rollout-smoke.sh` |
| All tables versioned migrations | PARTIAL | Cloud-order uniqueness has explicit Expand/Contract; CloudWorkflow + CloudWorkflowStep now support **opt-in** operator-managed `workflow-expand` with a version ledger and fail-closed startup verification. Remaining tables still use legacy GORM bootstrap |
| Cloud REST API contract | PASS structural | OpenAPI 3.1: 12 paths / 15 operations; Gin route parity test; richer generated client/API model conformance remains optional enhancement |
| Go consumer SDK | PASS in tests | Credential source, explicit idempotency, structured API errors, context cancellation, state reads/polling; external production caller NOT VERIFIED |
| Cloud resource order consumer interface | PASS in isolated live staging | New `cmd/taskflowctl` + `pkg/cloudclient/operations.go` covers quota/resource/order/workflow/Provision/Resize/Delete/wait; real Keycloak JWT → taskflowctl → API/MySQL positive and 403 negative checks passed. `TASKFLOWCTL_KEYCLOAK_MYSQL_E2E_PASS`; no customer production adoption claimed |
| MySQL Saga batch regressions | PASS on isolated DB | Added omitted quota-bucket table in fixture; use a dedicated schema for destructive test package |
| Whole Go unit suite / vet / format | PASS at this snapshot | `go test ./...`, `go vet ./...`, `gofmt`; run again for each change |
| Cloud API consumer integration | PASS on isolated MySQL | Independent `pkg/cloudclient` + real HTTP signed OIDC + Gin + MySQL; identical idempotency keys across different tenants accepted, intra-tenant retry reused original order |
| Docker release image | PASS locally | `taskflow-go:enterprise-acceptance-20261009` plus updated `enterprise-schema-20261009` built. `/schema-migrate --help` executed as UID 10001; `workflow-status` confirmed in separate CLI. Images are local, not published registry digests |
| Race detector | PASS locally | `go test -race -count=1 ./...` passed across all packages using `golang:1.26.8-bookworm` (CGO/GCC available). This fixes the earlier Alpine toolchain blocker |
| GitHub Actions execution | NOT VERIFIED | `actionlint` passed and local Go/race/security/Keycloak gates passed, but cloud Actions cannot start until a private `taskflow-go` repository is created and the commit is pushed |
| Real identity provider integration (development lab) | PASS in isolated lab | Keycloak 26.8.0 + dedicated PostgreSQL, four real client_credentials tokens, actual JWKS verification, two-tenant/reader/no-grant/forged-signature scenarios |
| Real TaskFlow API with Keycloak | PASS in staging | Independent port 18088, isolated MySQL + Redis, `CLOUD_TENANT_AUTHZ_ENABLED=true`; 13 HTTP authorization checks passed without changing existing production API |
| Keycloak identity persistence after restart | PASS in staging | Docker Keycloak restart + PostgreSQL retained realm, clients and OAuth credentials; repeated 13 HTTP smoke checks passed |
| Real Keycloak signing-key rotation | PASS in development lab | Keycloak Admin API created higher-priority RS256 key; old/new JWKS published, both outstanding tokens accepted, corrupted token 401. Script `keycloak-rotation-smoke.sh` |
| Expired/audience/issuer JWT validation | PASS with real JWT + injected verifier clock/policies | Go tested actual signed Keycloak tokens under simulated expired clock, wrong audience and wrong issuer; does not imply live revocation propagation |
| Real Keycloak outage/recovery | PASS in isolated lab | Controlled `docker stop/start` of only dev Keycloak: cached valid JWT remained accepted, new token issuance failed during outage, discovery and fresh token validation recovered. `scripts/keycloak-idp-outage-smoke.sh` |
| Keycloak production-mode HTTPS reference | PASS only in isolated TLS rehearsal | Separate `keycloak/keycloak:26.8.0 start` service (Quarkus prod), PostgreSQL 17, fixed HTTPS issuer, cert SAN/trust validation, HTTPS JWKS, HTTP disabled; self-signed 2-day localhost cert, all lab containers removed after test. `scripts/keycloak-production-mode-smoke.sh` |
| Production identity-provider rollout | NOT VERIFIED | Existing auth lab is HTTP start-dev with per-client static claims; production reference is loopback-only and uses a short-lived self-signed cert. Trusted public DNS/TLS, external secrets, IdP HA, managed human users/MFA and emergency access are still required |
| Kubernetes Kind live resource lifecycle post-change | PASS in isolated Kind | `CLOUD_CONTROL_KIND_E2E_OK` and `CLOUD_CONTROL_KIND_CHAOS_OK`; real Deployment/Service/PVC, two replicas, killed lease owner, surviving replica takeover, compensation, zero remaining quota. Cluster removed; original latest Docker tag restored |
| Keycloak + Kind end-to-end resource operations | PASS in isolated full-stack cluster | Two OIDC-only TaskFlow replicas using real Keycloak JWT/JWKS; team-a provisioned live Kubernetes Deployment via Saga, team-b received 404 for resource ID, signed delete removed object and released quota. `KIND_KEYCLOAK_FULL_STACK_E2E_PASS` |
| Keycloak HTTP read-path SLO | PASS for short 60s sample | 2,400 requests, 40.009 RPS, HTTP 200 100%, p50 18.673 ms, p95 20.287 ms, p99 22.440 ms; 12 concurrent, no mutation traffic. JSON at `/tmp/taskflow-keycloak-slo-60s-20261009.json` |
| Keycloak access-token refreshing under load | PASS in short 135s sample | 5,400 requests, 40.001 RPS, HTTP 200 100%, p95 20.269 ms, p99 21.396 ms, two credentials each refreshed once; `/tmp/taskflow-keycloak-slo-refresh-20261009.json` |
| Checkpointed OIDC 72h soak tooling | PASS for short functional pilot only | `keycloak-72h-soak.py` ran 0.005h (~18s), two windows / 175 requests / 13 permission checks, no gaps over 15s. It explicitly reported `final_72h_pass=false`. Runbook: `docs/runbooks/KEYCLOAK_72H_SOAK.md` |
| Multi-hour workload and SLO | NOT VERIFIED | An uninterrupted real 72h test was **not executed**; the available short read/authorization checks cannot substitute for mixed Saga read/write, IdP/DB failover, alert testing and full elapsed window |
| Zero-downtime production version rollout | NOT VERIFIED | Mixed-version *process* migration passed in disposable DB; production traffic, mixed-version Kind Pods, incident rollback and artifact digests have not been proven |
| Managed workflow schema opt-in | PASS in disposable MySQL and runtime staging | `TestManagedWorkflowSchemaMySQL` passed; new image with `CLOUD_WORKFLOW_SCHEMA_MANAGED=true` started ready on separate port 18085, then was stopped; no automatic DDL in managed mode |
| Real user adoption | NOT VERIFIED | Separate service, organizational user, stable release and an actual retained operational runbook |
| Local Git source history | PASS locally | Created private-ready local `main` repository, committed a reviewed initial snapshot, excluded keys and binaries, ran a preliminary secret pattern scan; see `docs/GITHUB_CI_ONBOARDING.md` |
| Remote GitHub source provenance | BLOCKED by missing GitHub CLI authorization/repository | Connected account `yfj898` has no accessible `taskflow-go` repo; local `gh auth status` has no login and connector lacks create-repository. `scripts/github-private-bootstrap.sh` safely creates a private repo after account-owner login. No remote push, PR or cloud Actions run is claimed |

## Safety invariants

1. Do not automatically enable `CLOUD_TENANT_AUTHZ_ENABLED` in an existing
   deployment. The mode requires verified OIDC-only credentials, RBAC and a
   completed composite unique-index contract migration.
2. Do not run manual schema contract while any legacy writer is running.
3. Never use a live DB for tests that call `AutoMigrate`, delete table rows,
   or drop indexes. Integration tests now reject DSNs lacking an explicit
   `_ci`/`_test`/`test_` schema name; also use network/user isolation.
4. Do not point Kind/E2E scripts at an existing production Kubernetes cluster.
5. Keep metric claims scoped: historical local load results are not production
   SLA, and test success is not customer deployment evidence.

## Reproducible local validation

Run without any integration DSN for a safe, isolated Go unit run:

```bash
go mod tidy -diff -go=1.25.0
go test -count=1 ./...
go vet ./...
test -z "$(gofmt -l ./cmd ./internal ./pkg)"
docker compose config --quiet
```

With a **disposable MySQL 8.4 instance**, give each package its own DB schema:

```bash
TASKFLOW_MYSQL_INTEGRATION_DSN='root:...@tcp(127.0.0.1:3306)/taskflow_repo_ci?parseTime=true' \
  go test -count=1 ./internal/repository
TASKFLOW_MYSQL_INTEGRATION_DSN='root:...@tcp(127.0.0.1:3306)/taskflow_handler_ci?parseTime=true' \
  go test -count=1 ./internal/handler
TASKFLOW_MYSQL_INTEGRATION_DSN='root:...@tcp(127.0.0.1:3306)/taskflow_service_ci?parseTime=true' \
  go test -count=1 ./internal/service
TASKFLOW_SCHEMA_MIGRATION_TEST_DSN='root:...@tcp(127.0.0.1:3306)/empty_schema_ci?parseTime=true' \
  go test -count=1 ./internal/schemamigrate
```

Use a **fresh, empty** schema for the migration test; it intentionally builds
and drops its own historical indexes. Tests that delete all cloud resource
rows must never share a schema with another package running concurrently.

## Next explicit gates to claim enterprise deployment readiness

1. P0: External OIDC provider + two real tenant accounts + negative permission
   matrix across API methods and full migration rollback simulation.
2. P0: Kind 2+ controller replicas with real resource creation, resize,
   deletion, crash/uncertain-outcome compensation and tenant authorization.
3. P0: Rehearse `expand` -> new binary overlap -> drain old -> `contract` ->
   enable tenant mode in a staged release, recording zero accepted unsafe writes.
4. P1: Soak 72 hours with continuous customer-like resource requests, mixed
   success/failure and SLO dashboards; record zero cross-tenant access,
   no lost resource orders and bounded workflow convergence.
5. P1: Publish versioned release images and record SBOM, scan and immutable
   digests; evidence should be tied to a Git commit when source control exists.
6. P2: Split remaining AutoMigrate tables into explicit versioned migrations,
   and validate backward-compatible field and index evolution.

No gate above may be marked PASS solely because a script, README or Kubernetes
manifest exists. A failing runtime check should block the rollout.
