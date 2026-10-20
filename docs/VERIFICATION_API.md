# Verification API v1

TaskFlow owns execution facts; callers own change/snapshot facts. This contract is the implementation target for the new `verify_change` task type. It does not change existing cloud resource APIs.

## Submit and recover a run

`POST /api/v1/verification-runs`, authenticated by existing Bearer/OIDC. The verified operator must be in the server-side project's `subjects` list. First durable creation returns **202**, an identical UUID request returns **200** and the same task. No automatic client mutation retry.

```json
{"run_id":"d05fb8d9-655c-4c20-8b2d-b2594c5a4dc3","project_id":"demo-workspace","target_commit":"0123456789012345678901234567890123456789","check_profile_id":"verify-change","check_profile_revision":"1","check_ids":["backend-test","frontend-test","frontend-build"],"external_snapshot_id":"snapshot-42"}
```

`check_ids` is an unordered set, normalized in the registered profile order. It must include all required checks. All other execution fields, including the external snapshot reference, must match on UUID reuse; mismatch is **409**. Permission is evaluated before any idempotent return. The full commit must exist in the registered local Git repository. Paths, commands, repository URLs and credentials never come from this request.

`GET /api/v1/verification-runs/:run_id` returns this same shape:

```json
{
  "run_id":"d05fb8d9-655c-4c20-8b2d-b2594c5a4dc3",
  "project_id":"demo-workspace",
  "target_commit":"0123456789012345678901234567890123456789",
  "check_profile_id":"verify-change",
  "check_profile_revision":"1",
  "check_ids":["backend-test","frontend-test","frontend-build"],
  "external_snapshot_id":"snapshot-42",
  "task_id":123,
  "status":"failed",
  "generation":1,
  "last_error":"check backend-test exited with status 1",
  "attempts":[{
    "generation":1,
    "started_at":"2026-10-19T12:00:00Z",
    "finished_at":"2026-10-19T12:00:01Z",
    "verdict":"failed",
    "error":"check backend-test exited with status 1",
    "checks":[
      {"id":"backend-test","required":true,"status":"failed","exit_code":1,"artifact_id":"check-0","error":"exit status 1"},
      {"id":"frontend-test","required":true,"status":"passed","exit_code":0,"artifact_id":"check-1"},
      {"id":"frontend-build","required":true,"status":"passed","exit_code":0,"artifact_id":"check-2"}
    ]
  }],
  "created_at":"2026-10-19T12:00:00Z"
}
```

`attempts` is empty before a worker starts. Each attempt's checks start as `not_run`. `verdict` is empty while running; terminal values are `passed`, `failed`, `incomplete`. Required failure dominates required not-run; all required passed is the only passing verdict. Optional failures are visible but do not fail a passing required plan. Process exit due to shutdown is `incomplete` and recovered through existing pending/generation semantics; assertion failures are permanent. Only the current generation can renew or settle, and report plus task transition commit atomically.

For v1, an exited fixed command with a nonzero status is a failed check; this includes compilation or dependency-preparation failures reported by that command. The full log preserves the real phase/cause. Failure to start a process, interruption or loss of executable results produces `incomplete`. The runner does not infer an error class from log text. A dependency/network failure is not evidence that business assertions were exercised. Shutdown only re-enqueues incomplete work; a completed failed/passed result remains terminal if shutdown races its final settlement.

Artifact URL: `GET /api/v1/verification-runs/:run_id/artifacts/:artifact_id?generation=1`. `generation` is mandatory, disambiguates history, and cannot be a filesystem path. It returns `text/plain; charset=utf-8`, only after the log is closed and its artifact ID persisted. Authorization is checked again. Structured facts are in the JSON response, never parsed from `Task.Result`.

Errors use existing `{"error":"reason"}`: **400** invalid input/unsupported profile/check; **401** invalid identity; **403** project not granted; **404** missing run/artifact; **409** conflicting UUID; **503** disabled/unavailable execution configuration; **500** persistence/read failure. Unknown submit outcome must be recovered using the same UUID.

## Go SDK and CLI

Reuse `pkg/cloudclient.Client` authentication, endpoint, transport and `APIError`:

- `SubmitVerification(ctx, VerificationRequest) (*VerificationRun, error)`
- `GetVerificationRun(ctx, runID) (*VerificationRun, error)`
- `GetVerificationArtifact(ctx, runID, generation, artifactID) ([]byte, error)`
- CLI `taskflowctl verification-submit REQUEST_JSON_FILE`
- CLI `taskflowctl verification-get RUN_UUID`

The submit JSON carries the stable run UUID; no extra business-key argument. Existing Windows `TASKFLOW_ACCESS_TOKEN` and POSIX token-file behavior remains.

## Server registration

`TASKFLOW_VERIFICATION_CONFIG` points to an operator-maintained JSON file. Empty means no new verification submissions. It requires existing authentication enabled, Linux, `EXECUTION_BACKEND=local`. Startup verifies the explicit verification schema migration; it never AutoMigrates these new tables.

```json
{
  "work_root":"/srv/taskflow-next/work",
  "artifact_root":"/srv/taskflow-next/reports",
  "projects":[{
    "id":"demo-workspace",
    "repository":"/srv/source/change-workspace-demo",
    "subjects":["weknora-demo-service"],
    "profile":{
      "id":"verify-change","revision":"1",
      "checks":[
        {"id":"backend-test","required":true,"argv":["go","test","./..."],"directory":"backend"},
        {"id":"frontend-test","required":true,"argv":["npm","test"],"directory":"frontend"},
        {"id":"frontend-build","required":true,"argv":["npm","run","build"],"directory":"frontend"}
      ]
    },
    "environment":{"PATH":"/usr/local/go/bin:/usr/bin:/bin","GOTOOLCHAIN":"local","GOMODCACHE":"/srv/taskflow-next/go-mod","GOCACHE":"/srv/taskflow-next/go-build"}
  }]
}
```

Only trusted source is registered. Every attempt extracts a specific commit into its own work directory without resetting the developer repository. Commands are argv, with no shell, use only configured environment and an attempt-specific HOME/TMP. Never configure secrets, SSH agent, or Docker socket. Admission snapshots the registered profile into the private task payload, so later configuration edits do not change an accepted run's commands. New templates must get a new revision; do not overwrite the meaning of an active revision. Profile IDs/revisions are confirmed in snapshots; the first supported template ID is `verify-change`.

For the supplied `change-workspace-demo` repository, revision `1` registers these three required checks in order: `backend-test` = `go test -count=1 ./...` at repository root; `frontend-test` = `npm run verify:frontend-test` in `frontend`; `frontend-build` = `npm run build` in `frontend`. The trusted versioned frontend script runs `npm ci` then `npm test` without a shell. Build reuses the same attempt's installed modules. `DEMO_NPM_CACHE` may point to a dedicated dependency cache; dependency/network failures remain actual nonzero check results.

Migration: `TASKFLOW_SCHEMA_DSN=... schema-migrate --phase=verification-expand`, status `--phase=verification-status`. Existing startup migrates old tables as before; new verification tables are operator-managed additions. Stop new submissions and drain new workers before disabling; retain reports and schema.
