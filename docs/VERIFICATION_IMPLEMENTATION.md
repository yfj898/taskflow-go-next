# Verification implementation and evidence

The vNext verification API, Go SDK/CLI and trusted Linux execution path are implemented and consumed by the isolated development service on port 18089. Start with [LOCAL_DEVELOPMENT.md](../LOCAL_DEVELOPMENT.md) for actual usage; the independent environment is managed through the [standalone Linux quickstart](VERIFICATION_QUICKSTART.md).

## Source baseline / M0

2026-10-19: restored exactly the six entry-point Go sources listed in `_source_supplement_20261019.json` from the read-only `@1` workspace filesystem. Workspace HEAD was `ef1d68176f07dc9285d633f1f93549e6dbfb8b4a`; `git ls-files` confirms these six files were ignored/untracked, so the HEAD identifies workspace context, not their versioned contents. Original 388-file snapshot is retained; `.gitignore` binary names are now root-anchored. Remote source and Windows CLI/SDK portability fixes were retained.

Windows Go1.26.4:

```powershell
$env:GOTOOLCHAIN='local'
$env:GOWORK='off'
$env:GOCACHE=Join-Path $PWD 'tmp\go-build-cache'
$env:GOMODCACHE=Join-Path $PWD 'tmp\gomod-cache'
go test -count=1 ./internal/domain ./internal/verification ./pkg/cloudclient ./cmd/taskflowctl
```

Observed PASS for these four packages. Default module cache initially denied writes; a coordinated writable cache plus approved network access resolved dependency download. Windows `cmd/server`, `cmd/executor-worker`, `cmd/taskflowctl` builds passed. Affected `executor`, `worker`, `service`, `repository`, `handler`, `cmd/server`, `cmd/schema-migrate` tests/vet passed. Linux-only and disposable MySQL tests were skipped in the Windows run; their subsequent actual Linux results are recorded below, separately from cross-platform build evidence.

## M2 implemented surface

Consumer contract: [VERIFICATION_API.md](VERIFICATION_API.md).

- `internal/domain/verification.go`: run, report, per-check result and generation-bound attempt records.
- `internal/verification/config.go`: operator-only project/profile/argv registration, project subjects, required-check admission and private registered-profile snapshot.
- `internal/repository/verification_repository.go`: explicit migration journal; one transaction for run/task/outbox; UUID uniqueness; fenced report progress/settlement; stale-attempt history.
- `internal/service/verification_service.go` and `internal/handler/verification.go`: authenticated submission/query/artifact access. Authorization precedes UUID lookup. An accepted UUID remains recoverable after profile registry revisions advance.
- `internal/executor/verification.go`: exact-commit archive into a fresh per-attempt directory; fixed argv with configured environment; real stdout/stderr files; assertion failure is permanent; Linux cancellation terminates the child process group.
- `internal/worker/pool.go`: new verification branch skips AI budget/audit persistence, renews with generation, and atomically settles report/task. Existing task behavior is retained.
- Generic task service rejects the reserved type; public lists exclude it before pagination; public detail returns not found. Unregistered verification executors reject instead of falling back to Demo.
- `pkg/cloudclient/verification.go`, `cmd/taskflowctl`: submit/get APIs and CLI; token handling and cloud commands retained. SDK does not automatically retry mutations.
- `cmd/server` wires actual registration/service/executor/routes; `cmd/schema-migrate --phase=verification-expand|verification-status` operates the new schema explicitly.

Profile argv is captured at admission in the private task payload, not obtained from the caller. The API never accepts repository paths, commands or credentials. A run continues to execute its accepted profile after a server config update. Current operator project mapping supplies repository/cache locations and access grants.

## Runtime verification

Pure/HTTP substitute tests passed on Windows. MySQL tests explicitly require `TASKFLOW_MYSQL_INTEGRATION_DSN` pointing at a disposable `*_test` or `*_ci` schema. Do not use the original deployment database. Cross-built Linux programs are under `bin/`:

```sh
./bin/verification-executor-linux.test -test.v -test.run '^TestVerification'
TASKFLOW_MYSQL_INTEGRATION_DSN='<disposable DSN>' ./bin/verification-repository-linux.test -test.v -test.run '^TestVerificationMySQL'
TASKFLOW_MYSQL_INTEGRATION_DSN='<disposable DSN>' ./bin/verification-worker-linux.test -test.v -test.run '^TestVerificationWorkerMySQL'
```

The repository test submits the same UUID concurrently 12 times and checks one run/task/outbox; it exercises conflict, recovery, old-generation renewal/late settlement rejection and atomic report/task reads. The worker test executes real passing/failing commands, checks no business retry, no AI report transport, and no inherited secret environment. The executor test checks a committed file rather than dirty working-tree contents, preserves the source directory, and executes the captured profile after runtime registry edits.

### Observed Linux/MySQL results on 2026-10-19

Integration owner supplied Ubuntu-24.04 and isolated `taskflow_next_test`. Initial real execution found that Git emits a legal PAX global metadata header (tar type 103); the original extractor rejected it. The correction ignores PAX metadata as metadata, without accepting links or escaping paths. Regression tests cover regular traversal, absolute paths, symbolic/hard links and extended PAX paths; the metadata itself is never written as a file.

- PASS: real Git archive of a fixed commit, exact source content rather than dirty local content, unchanged developer repository, accepted-profile command snapshot.
- PASS: actual passing and failing subprocesses through Worker/MySQL, no business retry, structured reports outside `Task.Result`/AI audit, no inherited test-secret environment.
- PASS: deterministic shutdown/settlement race for completed failed and passed reports. Neither becomes pending or receives a new generation; only incomplete shutdown work follows recovery. Evidence: `docs/VERIFICATION_SHUTDOWN_RACE_20261019.log`.
- PASS from integration owner's unchanged candidate tests: 12 concurrent same-UUID requests create one run/task/outbox; UUID conflict; stale recovery and old-generation renew/settle rejection; handler authentication/project denial. Those unaffected tests were not rerun for the PAX-only correction.

Executor/Worker correction evidence: `docs/VERIFICATION_RUNTIME_CHECKS_20261019.log`. Earlier machine-local runtime logs were retained by the integration owner; reproducible commands and public run records are included here.

### Actual API/CLI business replay

The integration owner ran the real CLI against the ready service and saved [the complete run responses and checks](evidence/taskflow-replay-results.json), plus the six actual check logs in the same evidence directory. The supplied sample is self-owned and uses controlled fault injection.

| Run | Code commit | Observed result |
| --- | --- | --- |
| `b35d63d6-be40-4d94-94de-3393632cf722` | `7aca2e8ec80fd4796f7ff8780960bab615340bb5` | failed; frontend test command exits 1 with 2/11 actual assertions failing; backend and build pass |
| `28b49b55-8da2-4665-861a-c011875538a3` | `1750bd795b9a002fb4b0c2aa6a64946070796aa1` | success; 3/3 fixed checks pass |

The same replay verified CLI submit/get, identical UUID returning the same task, changed commit with the same UUID returning 409, missing required checks returning 400, unauthenticated run access returning 401, public generic task detail returning 404, and registered artifact downloads. Previous failed runs remain separate from the repaired run.

### Actual process interruption and recovery

The shutdown-race candidate was built separately as `bin/taskflow-server-linux-next`, then consumed by the integration owner during an idle normal stop/start into stable `bin/taskflow-server-linux`.

The integration owner then performed a real SIGKILL/restart during run `316a368c-4d29-4abe-b044-0b9eff318f6d`, task 3, at fixed commit `1750bd795b9a002fb4b0c2aa6a64946070796aa1`. After natural lease expiry, the same run/task recovered as generation 2. Generation 1 remains `incomplete` with its partial checks; generation 2 passed 3/3 checks and the task reached success. The [full before/after recovery record](evidence/taskflow-abrupt-recovery.json) preserves both attempts. This observed process recovery is separate from deterministic repository/fencing and shutdown-settlement race tests.

Source/contract changes do not require rerunning unchanged checks solely for reassurance.

The v1 registered-command exit convention is explicit in the API guide: nonzero process exit is a failed check, while failed process startup/interruption is incomplete. Compilation and dependency-preparation logs retain their cause. The integration owner prepares locked Linux dependencies separately; only the controlled `presale-null` failing assertions are used as business-failure evidence.
