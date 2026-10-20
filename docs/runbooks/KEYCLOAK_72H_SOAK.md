# 72-hour OIDC reliability acceptance — runnable procedure

**Scope:** a dedicated, **loopback-only** TaskFlow staging deployment at
`http://127.0.0.1:18088` with a real Keycloak 26.8.0 issuer at 18090.
This is a measured workload, not an assertion that running the script alone
proves production-grade reliability.

## Exact execution

Use a supervised foreground terminal or a reviewed dedicated systemd Job, not
an ad hoc detached shell. Ensure both Keycloak/PostgreSQL and TaskFlow stage
have resource limits, free disk space and observability before launching.

```bash
cd taskflow-go

# A short functional rehearsal. It has a unique output path and MUST NOT be
# labelled a 72-hour acceptance result.
python3 scripts/keycloak-72h-soak.py --hours 0.005 --window-seconds 9 \
  --rate 10 --concurrency 4 --output /tmp/taskflow-oidc-soak-pilot

# Actual 72-hour OIDC GET + periodic authorization/tenant quota-write profile:
python3 scripts/keycloak-72h-soak.py --hours 72 --window-seconds 300 \
  --rate 20 --concurrency 12 --authz-interval-seconds 3600 \
  --output /tmp/taskflow-oidc-soak-72h-<unique-run-id>
```

The script **does not run asynchronously on behalf of the assistant**. The
actual three-day run must remain alive in an approved operator environment.
For each 5-minute measurement window the runner saves an individual JSON
report, an append-and-fsync `events.ndjson`, and atomic `summary.json` with
observed elapsed hours, cumulative HTTP 200 ratio, worst per-window p99,
gaps, and how many 13-case RBAC/tenant-write checks were completed.
Interruption sets `failed_or_interrupted`; rerunning the same output directory
is intentionally prohibited to prevent papering over a gap.

## Conditions to accept this workload

1. **72 real elapsed hours** with no interruption or missing measurement
   windows. The summary must report `final_72h_pass: true`.
2. HTTP 200 success ratio ≥99.9%, no uncounted transport errors and all
   scheduled requests attempted. Authorization probes must all pass.
3. p99 for **every window** below 1,000 ms. This is a conservative per-window
   gate, not a statistically calculated global p99.
4. Inter-window load gaps ≤15 s. Every Keycloak refresh must succeed; if
   token issuance fails, the run fails or records its outcome.
5. Before declaring production reliability, **separately** repeat with
   mixed Provision/Resize/Delete Saga orders in an isolated Kubernetes cluster,
   forced Pod death, MySQL locks and failover, IdP failover, documented SLO
   alert firing/recovery and database restore testing.
6. Review Prometheus/Grafana SLO trends and incident logs in the same test
   window. A perfect GET-path test does **not** validate write-path SLO,
   external providers, identity cluster HA or disaster recovery.

## Scope disclaimer

`keycloak-72h-soak.py` intentionally uses the previously validated bounded
`keycloak-slo-run.sh` in consecutive windows and runs the full 13-assertion
`keycloak-live-smoke.sh` periodically. It is a **read-heavy OIDC/API SLO
component**, not the whole organization's production acceptance plan.
The target environment's `CLOUD_CONTROL_ENABLED=false` means no actual
Kubernetes objects are created in this long-run profile. Never report this
as 72 hours of real K8s Saga provision/delete throughput.
