#!/usr/bin/env python3
"""Compare two cloud Saga scale benchmark runs without changing the load model."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def load_run(root: Path) -> tuple[dict, dict[str, str]]:
    summary = json.loads((root / "summary.json").read_text(encoding="utf-8"))
    env: dict[str, str] = {}
    for line in (root / "environment.txt").read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.startswith("---"):
            key, value = line.split("=", 1)
            env[key] = value
    return summary, env


def pct_change(new: float, old: float) -> float | None:
    if old == 0:
        return None
    return (new / old - 1.0) * 100.0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("optimized", type=Path)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()

    baseline, env_a = load_run(args.baseline)
    optimized, env_b = load_run(args.optimized)
    args.output_root.mkdir(parents=True, exist_ok=True)

    admission_a = {(x["tenant_mode"], int(x["concurrency"])): x for x in baseline["admission"]}
    admission_b = {(x["tenant_mode"], int(x["concurrency"])): x for x in optimized["admission"]}
    controller_a = {int(x["controller_replicas"]): x for x in baseline["controllers"]}
    controller_b = {int(x["controller_replicas"]): x for x in optimized["controllers"]}

    admission_rows = []
    for key in sorted(set(admission_a) & set(admission_b), key=lambda x: (x[0], x[1])):
        a, b = admission_a[key], admission_b[key]
        lock_a = a.get("row_lock_delta", {})
        lock_b = b.get("row_lock_delta", {})
        admission_rows.append({
            "tenant_mode": key[0],
            "concurrency": key[1],
            "baseline_rps": a["throughput_rps"],
            "optimized_rps": b["throughput_rps"],
            "rps_change_pct": round(pct_change(float(b["throughput_rps"]), float(a["throughput_rps"])), 3),
            "baseline_p95_ms": a["latency_ms"]["p95"],
            "optimized_p95_ms": b["latency_ms"]["p95"],
            "p95_change_pct": round(pct_change(float(b["latency_ms"]["p95"]), float(a["latency_ms"]["p95"])), 3),
            "baseline_lock_waits": lock_a.get("Innodb_row_lock_waits", 0),
            "optimized_lock_waits": lock_b.get("Innodb_row_lock_waits", 0),
            "lock_waits_change_pct": round(pct_change(float(lock_b.get("Innodb_row_lock_waits", 0)), float(lock_a.get("Innodb_row_lock_waits", 0))), 3),
            "baseline_lock_time_ms": lock_a.get("Innodb_row_lock_time", 0),
            "optimized_lock_time_ms": lock_b.get("Innodb_row_lock_time", 0),
        })

    controller_rows = []
    for replicas in sorted(set(controller_a) & set(controller_b)):
        a, b = controller_a[replicas], controller_b[replicas]
        controller_rows.append({
            "replicas": replicas,
            "baseline_workflows_s": a["throughput_workflows_s"],
            "optimized_workflows_s": b["throughput_workflows_s"],
            "throughput_change_pct": round(pct_change(float(b["throughput_workflows_s"]), float(a["throughput_workflows_s"])), 3),
            "baseline_p95_s": a["completion_latency_s"]["p95"],
            "optimized_p95_s": b["completion_latency_s"]["p95"],
            "p95_change_pct": round(pct_change(float(b["completion_latency_s"]["p95"]), float(a["completion_latency_s"]["p95"])), 3),
        })

    result = {
        "schema_version": 1,
        "baseline": {
            "quota_admission_mode": env_a.get("quota_admission_mode", "unknown"),
            "saga_db_mode": env_a.get("saga_db_mode", "unknown"),
            "image": env_a.get("taskflow_image_id", "unknown"),
        },
        "optimized": {
            "quota_admission_mode": env_b.get("quota_admission_mode", "unknown"),
            "saga_db_mode": env_b.get("saga_db_mode", "unknown"),
            "image": env_b.get("taskflow_image_id", "unknown"),
        },
        "admission": admission_rows,
        "controllers": controller_rows,
    }
    (args.output_root / "comparison.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")

    with (args.output_root / "admission-ab.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(admission_rows[0].keys()))
        writer.writeheader()
        writer.writerows(admission_rows)
    with (args.output_root / "controller-ab.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(controller_rows[0].keys()))
        writer.writeheader()
        writer.writerows(controller_rows)

    lines = [
        "# Cloud Saga persistence A/B",
        "",
        f"Baseline: quota={result['baseline']['quota_admission_mode']}, saga_db={result['baseline']['saga_db_mode']}",
        f"Optimized: quota={result['optimized']['quota_admission_mode']}, saga_db={result['optimized']['saga_db_mode']}",
        "",
        "## Quota admission A/B",
        "",
        "| Tenant mode | C | Baseline RPS | Optimized RPS | Δ RPS | Baseline P95 ms | Optimized P95 ms | Δ P95 | Lock waits A→B |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in admission_rows:
        lines.append(
            f"| {row['tenant_mode']} | {row['concurrency']} | {row['baseline_rps']:.2f} | {row['optimized_rps']:.2f} | "
            f"{row['rps_change_pct']:+.2f}% | {row['baseline_p95_ms']:.2f} | {row['optimized_p95_ms']:.2f} | "
            f"{row['p95_change_pct']:+.2f}% | {row['baseline_lock_waits']}→{row['optimized_lock_waits']} |"
        )
    lines += [
        "",
        "## Saga DB A/B",
        "",
        "| Controllers | Baseline wf/s | Optimized wf/s | Δ throughput | Baseline P95 s | Optimized P95 s | Δ P95 |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in controller_rows:
        lines.append(
            f"| {row['replicas']} | {row['baseline_workflows_s']:.3f} | {row['optimized_workflows_s']:.3f} | "
            f"{row['throughput_change_pct']:+.2f}% | {row['baseline_p95_s']:.3f} | {row['optimized_p95_s']:.3f} | "
            f"{row['p95_change_pct']:+.2f}% |"
        )
    markdown = "\n".join(lines) + "\n"
    (args.output_root / "comparison.md").write_text(markdown, encoding="utf-8")
    print(markdown)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
