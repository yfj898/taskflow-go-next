#!/usr/bin/env python3
"""Build a compact JSON/Markdown report for cloud Saga scale experiments."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def lock_status(path: Path) -> dict[str, int]:
    result: dict[str, int] = {}
    if not path.exists():
        return result
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.split("\t")
        if len(parts) == 2:
            try:
                result[parts[0]] = int(parts[1])
            except ValueError:
                pass
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    root = args.root

    admission = []
    for path in sorted(root.glob("admission-*.json")):
        data = read_json(path)
        before = lock_status(path.with_suffix(".locks.before"))
        after = lock_status(path.with_suffix(".locks.after"))
        data["row_lock_delta"] = {
            key: after.get(key, 0) - before.get(key, 0)
            for key in sorted(set(before) | set(after))
        }
        data["source"] = path.name
        admission.append(data)
    admission.sort(key=lambda item: (item.get("quota_mode", "locked"), item["tenant_mode"], int(item["concurrency"])))

    controllers = []
    for path in sorted(root.glob("controller-*.json")):
        if path.stem.endswith("-admission"):
            continue
        data = read_json(path)
        data["source"] = path.name
        controllers.append(data)
    controllers.sort(key=lambda item: (
        item.get("db_mode", "baseline"),
        int(item.get("worker_concurrency", 1)),
        int(item.get("observe_backoff_ms", 0)),
        int(item["controller_replicas"]),
    ))

    if not admission or not controllers:
        raise SystemExit("missing admission or controller benchmark results")

    one_replica_by_case = {
        (
            item.get("db_mode", "baseline"),
            int(item.get("worker_concurrency", 1)),
            int(item.get("observe_backoff_ms", 0)),
        ): float(item["throughput_workflows_s"])
        for item in controllers
        if int(item["controller_replicas"]) == 1
    }
    legacy_by_replicas = {
        int(item["controller_replicas"]): float(item["throughput_workflows_s"])
        for item in controllers
        if item.get("db_mode", "baseline") == "legacy" and int(item.get("observe_backoff_ms", 0)) == 0
    }
    baseline_by_replicas = {
        int(item["controller_replicas"]): float(item["throughput_workflows_s"])
        for item in controllers
        if item.get("db_mode", "baseline") == "baseline"
        and int(item.get("worker_concurrency", 1)) == 1
        and int(item.get("observe_backoff_ms", 0)) == 0
    }
    for item in controllers:
        mode = item.get("db_mode", "baseline")
        workers = int(item.get("worker_concurrency", 1))
        backoff = int(item.get("observe_backoff_ms", 0))
        replicas = int(item["controller_replicas"])
        throughput = float(item["throughput_workflows_s"])
        baseline_rps = one_replica_by_case.get((mode, workers, backoff), 0.0)
        legacy_rps = legacy_by_replicas.get(replicas, 0.0)
        same_replica_baseline = baseline_by_replicas.get(replicas, 0.0)
        item["speedup_vs_1"] = round(throughput / baseline_rps, 3) if baseline_rps else None
        item["scale_efficiency_vs_1"] = round(throughput / (baseline_rps * replicas), 3) if baseline_rps else None
        item["throughput_vs_legacy_same_replicas"] = round(throughput / legacy_rps, 3) if legacy_rps else None
        item["throughput_vs_baseline_same_replicas"] = round(throughput / same_replica_baseline, 3) if same_replica_baseline else None

    summary = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "admission": admission,
        "controllers": controllers,
    }
    (root / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    with (root / "admission-curve.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "quota_mode", "tenant_mode", "tenant_count", "concurrency", "requests", "throughput_rps",
            "p50_ms", "p95_ms", "p99_ms", "row_lock_waits_delta", "row_lock_time_ms_delta",
        ])
        for item in admission:
            lock = item.get("row_lock_delta", {})
            lat = item["latency_ms"]
            writer.writerow([
                item.get("quota_mode", "locked"), item["tenant_mode"], item["tenant_count"], item["concurrency"], item["requests_target"],
                item["throughput_rps"], lat["p50"], lat["p95"], lat["p99"],
                lock.get("Innodb_row_lock_waits", 0), lock.get("Innodb_row_lock_time", 0),
            ])

    with (root / "controller-scaling-curve.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "db_mode", "worker_concurrency", "observe_backoff_ms", "controller_replicas", "workflows", "wall_s", "throughput_workflows_s",
            "p50_completion_s", "p95_completion_s", "p99_completion_s", "speedup_vs_1", "scale_efficiency_vs_1",
            "throughput_vs_baseline_same_replicas", "throughput_vs_legacy_same_replicas", "compute_attempts_per_workflow",
        ])
        for item in controllers:
            lat = item["completion_latency_s"]
            writer.writerow([
                item.get("db_mode", "baseline"), item.get("worker_concurrency", 1), item.get("observe_backoff_ms", 0), item["controller_replicas"], item["workflows"], item["wall_s"], item["throughput_workflows_s"],
                lat["p50"], lat["p95"], lat["p99"], item["speedup_vs_1"], item["scale_efficiency_vs_1"],
                item["throughput_vs_baseline_same_replicas"], item["throughput_vs_legacy_same_replicas"], item.get("compute_attempts_per_workflow", 0),
            ])

    lines = [
        "# Cloud Saga scale benchmark",
        "",
        f"Generated: {summary['generated_at']}",
        "",
        "## Order admission / quota contention",
        "",
        "| Quota mode | Tenant mode | Tenants | Concurrency | Requests | RPS | P50 ms | P95 ms | P99 ms | Row-lock waits Δ | Row-lock time Δ ms |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in admission:
        lock = item.get("row_lock_delta", {})
        lat = item["latency_ms"]
        lines.append(
            f"| {item.get('quota_mode', 'locked')} | {item['tenant_mode']} | {item['tenant_count']} | {item['concurrency']} | {item['requests_target']} | "
            f"{item['throughput_rps']:.2f} | {lat['p50']:.3f} | {lat['p95']:.3f} | {lat['p99']:.3f} | "
            f"{lock.get('Innodb_row_lock_waits', 0)} | {lock.get('Innodb_row_lock_time', 0)} |"
        )

    lines.extend([
        "",
        "## Controller replica scaling",
        "",
        "| DB mode | Workers | Backoff ms | Controllers | Workflows | Interval ms | Wall s | Workflow/s | P50 completion s | P95 s | P99 s | Compute attempts/WF | Speedup within case | Efficiency | vs baseline |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ])
    for item in controllers:
        lat = item["completion_latency_s"]
        speedup = item["speedup_vs_1"]
        efficiency = item["scale_efficiency_vs_1"]
        vs_baseline = item["throughput_vs_baseline_same_replicas"]
        lines.append(
            f"| {item.get('db_mode', 'baseline')} | {item.get('worker_concurrency', 1)} | {item.get('observe_backoff_ms', 0)} | {item['controller_replicas']} | {item['workflows']} | {item.get('controller_interval_ms', 20)} | {item['wall_s']:.3f} | "
            f"{item['throughput_workflows_s']:.3f} | {lat['p50']:.3f} | {lat['p95']:.3f} | {lat['p99']:.3f} | "
            f"{item.get('compute_attempts_per_workflow', 0):.2f} | {speedup:.3f}× | {efficiency * 100:.1f}% | "
            f"{vs_baseline:.3f}× |"
        )
    lines.extend([
        "",
        "Admission measures accepted Provision-order persistence while Saga controllers are disabled. Controller scaling uses real Kubernetes Deployment objects with replicas=0 to exercise apiserver/SSA/Observe without scheduling workload Pods.",
        "",
    ])
    markdown = "\n".join(lines)
    (root / "summary.md").write_text(markdown, encoding="utf-8")
    print(markdown)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
