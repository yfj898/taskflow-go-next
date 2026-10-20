# Cloud orders idempotency: safe two-phase migration

**Safety:** Never perform this against a production database merely to make a
test pass. Stop and obtain approval if old application writers cannot be
identified and drained. MySQL DDL commits independently of transactions;
backup and recovery are operational requirements.

## The reason for the split

Legacy `cloud_orders.idempotency_key` is globally unique. Multiple tenants
must instead have independently unique `(tenant_id, idempotency_key)` pairs.
Simply changing the GORM field tags is unsafe: AutoMigrate may remove the
legacy index at startup while older Pods are still writing. The main server
therefore deliberately skips AutoMigrate for an **existing** `cloud_orders`
table and only creates the table when missing.

## Preflight

1. Take a verified backup and record the current release and database schema.
2. Confirm that the DSN points to the **intended** writer/schema, not a
   replica or a similarly named production environment.
3. List all TaskFlow API/worker deployments that can create cloud orders,
   including other regions and old deployment replicas.
4. Confirm sufficient free disk and an acceptable DDL lock window.

```bash
export TASKFLOW_SCHEMA_DSN='user:password@tcp(mysql-writer:3306)/taskflow?parseTime=true'
go run ./cmd/schema-migrate --phase=status
```

The status command only reads `information_schema` and the migration journal.
The release container also bundles `/schema-migrate`; run it as a separate,
approved one-shot Kubernetes Job with a pinned image digest and tightly scoped
DDL credentials. Never run `contract` as part of normal API startup.

## Expand (old and new writers may coexist)

```bash
go run ./cmd/schema-migrate --phase=expand
go run ./cmd/schema-migrate --phase=status
```

Expect `CompositeReady: true` and at least one `LegacyUnique` index. Both
constraints remain enforced. Redeploy the new application version with
`CLOUD_TENANT_AUTHZ_ENABLED=false`. New code performs all idempotency lookups
using tenant plus key even before the contract phase.

## Contract (irreversible compatibility boundary)

1. Ensure **all legacy writers in every region** have stopped; check actual
   process/deployment versions and drain sessions rather than relying only on
   replica counts or deployment desired state.
2. Verify tenant-specific idempotency behavior with a test tenant.
3. Back up the schema and confirm the emergency recovery plan. Rolling back
   to an old binary after contract is not supported unless global uniqueness
   can be restored and all duplicate keys are reconciled first.
4. Explicitly authorize the contract operation:

```bash
go run ./cmd/schema-migrate --phase=contract --ack-old-writers-stopped
go run ./cmd/schema-migrate --phase=status
```

Expect `CompositeReady: true`, no `LegacyUnique`, and both migration versions
recorded in `taskflow_schema_migrations`. The command acquires the same
advisory lock as application schema bootstrap, detects historical index names
through `information_schema`, and can be safely retried after a partial DDL
completion. It deliberately cannot detect whether old writers have stopped;
that external fact is the operator's responsibility.

Now enable the previously opt-in OIDC-only tenant isolation mode. Startup
refuses this mode while a global idempotency index is still present.

## Rollback and incidents

- Before contract, the composite index can stay present; restore the previous
  application and leave `CLOUD_TENANT_AUTHZ_ENABLED=false`.
- After contract, do **not** roll back to a legacy writer. Use the new version
  or a forward fix. Restoring the old unique index requires checking for and
  resolving cross-tenant duplicate values first.
- When migration records and actual indexes disagree after interrupted DDL,
  rerun the same explicit phase and inspect the schema. Do not delete history
  or manually change unique indexes without a reviewed plan.

## Repeatable disposable MySQL evidence

```bash
TASKFLOW_SCHEMA_MIGRATION_TEST_DSN='root:...@tcp(127.0.0.1:3306)/empty_disposable_schema?parseTime=true' \
  go test -count=1 -v ./internal/schemamigrate -run '^TestCloudOrdersExpandContractMySQL$'
```

The test proves: initial old index present; explicit approval required;
expand retains old uniqueness; contract allows equal keys across tenants but
rejects duplicates inside a tenant; application startup does not recreate
the legacy index; and repeated migration invocations are idempotent.

**Current limitation:** Other TaskFlow tables still rely on the historical
GORM AutoMigrate bootstrap. This runbook establishes explicit versioning for
the cloud order uniqueness change only, not a blanket claim that every table
has been converted to managed versioned migrations.

## Second migration cohort: CloudWorkflow / CloudWorkflowStep

An additional opt-in migration moves the two Saga workflow tables away from
implicit startup AutoMigrate. This operation runs GORM expansion **only in the
explicit administrator-invoked schema-migrate command**, records a version in
`taskflow_schema_migrations`, and verifies required columns and indexes on
future application startups. Do not run this without the usual schema backup,
reviewed DDL window and database privileges.

```bash
# Do this before switching the new deployment to managed workflow mode.
go run ./cmd/schema-migrate --phase=workflow-expand
go run ./cmd/schema-migrate --phase=workflow-status

# Then, on the new TaskFlow API (and only after status passes):
export CLOUD_WORKFLOW_SCHEMA_MANAGED=true
```

The flag defaults to false for historical compatibility. In managed mode,
startup refuses missing migration evidence, absent workflow indexes or the
legacy one-workflow-per-resource unique constraint. It does not silently
repair the tables. The explicit migration command may be invoked repeatedly
and includes the historical index normalization step. This cohort is limited
to `cloud_workflows` and `cloud_workflow_steps`; remaining tables stay on
the legacy GORM bootstrap until migrated in subsequent cohorts.
