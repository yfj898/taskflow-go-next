#!/usr/bin/env python3
"""Aggregate repeated TaskFlow benchmark runs into JSON and Markdown reports."""

from __future__ import annotations

import argparse
import json
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


METRICS = {
    "requests": ("requests",),
    "rps": ("rps",),
    "latency_avg_ms": ("latency_ms", "avg"),
    "latency_p50_ms": ("latency_ms", "p50"),
    "latency_p95_ms": ("latency_ms", "p95"),
    "latency_p99_ms": ("latency_ms", "p99"),
    "latency_max_ms": ("latency_ms", "max"),
}


def _nested_number(payload: dict[str, Any], path: Iterable[str]) -> float:
    value: Any = payload
    for key in path:
        if not isinstance(value, dict) or key not in value:
            raise ValueError(f"missing benchmark field: {'.'.join(path)}")
        value = value[key]
    if value is None or isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"benchmark field is not numeric: {'.'.join(path)}={value!r}")
    return float(value)


def _stats(values: list[float]) -> dict[str, float | None]:
    mean = statistics.fmean(values)
    sample_variance = statistics.variance(values) if len(values) > 1 else None
    stddev = statistics.stdev(values) if len(values) > 1 else None
    cv_pct = (stddev / mean * 100) if stddev is not None and mean != 0 else None
    return {
        "median": statistics.median(values),
        "mean": mean,
        "min": min(values),
        "max": max(values),
        "sample_variance": sample_variance,
        "stddev": stddev,
        "cv_pct": cv_pct,
    }


def _sort_key(path: Path) -> tuple[int, str]:
    suffix = path.parent.name.removeprefix("run-")
    try:
        return int(suffix), str(path)
    except ValueError:
        return 10**9, str(path)


def discover_runs(root: Path) -> list[Path]:
    runs = sorted(root.glob("run-*/load.json"), key=_sort_key)
    if not runs:
        raise ValueError(f"no run-*/load.json files found under {root}")
    return runs


def build_summary(run_files: list[Path]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    metric_values: dict[str, list[float]] = {name: [] for name in METRICS}
    status_totals: dict[str, int] = {}
    error_totals: dict[str, int] = {}
    concurrencies: set[int] = set()
    durations: list[float] = []

    for index, path in enumerate(run_files, start=1):
        payload = json.loads(path.read_text(encoding="utf-8"))
        concurrency = int(payload.get("concurrency", 0))
        if concurrency <= 0:
            raise ValueError(f"invalid concurrency in {path}: {concurrency}")
        concurrencies.add(concurrency)

        duration = _nested_number(payload, ("duration_s",))
        durations.append(duration)

        metrics: dict[str, float] = {}
        for name, field_path in METRICS.items():
            value = _nested_number(payload, field_path)
            metric_values[name].append(value)
            metrics[name] = value

        status = payload.get("status", {})
        errors = payload.get("errors", {})
        if not isinstance(status, dict) or not isinstance(errors, dict):
            raise ValueError(f"status/errors must be objects in {path}")
        for code, count in status.items():
            status_totals[str(code)] = status_totals.get(str(code), 0) + int(count)
        for error, count in errors.items():
            error_totals[str(error)] = error_totals.get(str(error), 0) + int(count)

        rows.append(
            {
                "run": index,
                "source": str(path),
                "concurrency": concurrency,
                "duration_s": duration,
                "metrics": metrics,
                "status": status,
                "errors": errors,
            }
        )

    if len(concurrencies) != 1:
        raise ValueError(f"runs used different concurrency values: {sorted(concurrencies)}")

    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_count": len(rows),
        "conditions": {
            "concurrency": next(iter(concurrencies)),
            "duration_s_median": statistics.median(durations),
        },
        "aggregates": {name: _stats(values) for name, values in metric_values.items()},
        "status_totals": status_totals,
        "error_totals": error_totals,
        "runs": rows,
    }


def _fmt(value: float | None, digits: int = 3) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}"


def render_markdown(summary: dict[str, Any]) -> str:
    aggregates = summary["aggregates"]
    lines = [
        "# TaskFlow multi-run benchmark",
        "",
        f"Generated: {summary['generated_at']}",
        "",
        f"Runs: {summary['run_count']}",
        f"Concurrency: {summary['conditions']['concurrency']}",
        f"Median measured duration: {_fmt(summary['conditions']['duration_s_median'])} s",
        "",
        "## Aggregate stability",
        "",
        "| Metric | Median | Mean | Sample variance | Std dev | CV |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]

    labels = {
        "requests": "Requests",
        "rps": "Throughput (RPS)",
        "latency_avg_ms": "Average latency (ms)",
        "latency_p50_ms": "P50 latency (ms)",
        "latency_p95_ms": "P95 latency (ms)",
        "latency_p99_ms": "P99 latency (ms)",
        "latency_max_ms": "Max latency (ms)",
    }
    for name in METRICS:
        stats = aggregates[name]
        cv = stats["cv_pct"]
        lines.append(
            f"| {labels[name]} | {_fmt(stats['median'])} | {_fmt(stats['mean'])} | "
            f"{_fmt(stats['sample_variance'])} | {_fmt(stats['stddev'])} | "
            f"{_fmt(cv, 2) if cv is not None else 'n/a'}% |"
        )

    lines.extend(
        [
            "",
            "## Per-run results",
            "",
            "| Run | Requests | RPS | Avg ms | P50 ms | P95 ms | P99 ms | Max ms | Errors |",
            "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in summary["runs"]:
        metrics = row["metrics"]
        errors = sum(int(value) for value in row["errors"].values())
        lines.append(
            f"| {row['run']} | {int(metrics['requests'])} | {_fmt(metrics['rps'], 2)} | "
            f"{_fmt(metrics['latency_avg_ms'])} | {_fmt(metrics['latency_p50_ms'])} | "
            f"{_fmt(metrics['latency_p95_ms'])} | {_fmt(metrics['latency_p99_ms'])} | "
            f"{_fmt(metrics['latency_max_ms'])} | {errors} |"
        )

    lines.extend(
        [
            "",
            "## Delivery outcome",
            "",
            f"HTTP status totals: `{json.dumps(summary['status_totals'], sort_keys=True)}`",
            "",
            f"Transport error totals: `{json.dumps(summary['error_totals'], sort_keys=True)}`",
            "",
            "`environment.txt` in the same directory records the host/container environment used for the run.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path, help="directory containing run-*/load.json")
    parser.add_argument("--json", dest="json_path", type=Path)
    parser.add_argument("--markdown", dest="markdown_path", type=Path)
    args = parser.parse_args()

    run_files = discover_runs(args.root)
    summary = build_summary(run_files)
    json_path = args.json_path or args.root / "summary.json"
    markdown_path = args.markdown_path or args.root / "summary.md"

    json_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    markdown_path.write_text(render_markdown(summary), encoding="utf-8")
    print(render_markdown(summary))
    print(f"json: {json_path}")
    print(f"markdown: {markdown_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
