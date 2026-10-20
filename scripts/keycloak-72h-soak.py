#!/usr/bin/env python3
"""Checkpointed 72-hour OIDC stage SLO runner (only private loopback stage).

Run in the foreground under a supervised job. The script does not start a
background process, and will never claim 72h PASS before time has elapsed.
It executes bounded read-load windows and periodic real authorization/write
probes against the dedicated Keycloak/TaskFlow lab. Results are durable JSON.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import pathlib
import subprocess
import sys
import time


ROOT = pathlib.Path(__file__).resolve().parent
DEFAULT_DURATION_HOURS = 72.0


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def save_json_atomic(path: pathlib.Path, data: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fd:
        json.dump(data, fd, ensure_ascii=False, indent=2, sort_keys=True)
        fd.write("\n")
        fd.flush()
        os.fsync(fd.fileno())
    os.replace(tmp, path)


def summarize(events: list[dict], elapsed: float, required: float, state: str) -> dict:
    responses = sum(e["result"].get("attempted_requests", 0) for e in events)
    ok = sum(e["result"].get("status_codes", {}).get("200", 0) for e in events)
    requests_planned = sum(e["result"].get("planned_requests", 0) for e in events)
    p99_values = [
        e["result"].get("latency_ms", {}).get("p99") for e in events
        if e["result"].get("latency_ms", {}).get("p99") is not None
    ]
    failed = [e["seq"] for e in events if not e["result"].get("pass", False) or not e.get("authz_pass", True)]
    gaps = [events[i]["start_epoch"] - events[i-1]["end_epoch"] for i in range(1, len(events))]
    availability = ok / responses if responses else 0.0
    completed = elapsed >= required and state == "complete"
    return {
        "name": "taskflow-real-keycloak-72h-checkpointed-soak",
        "scope": "isolated 127.0.0.1:18088 OIDC quota GET + periodic 13 authorization/quota-write checks; NO Kind lifecycle traffic",
        "state": state,
        "generated_at": utc_now(),
        "hours_required": round(required / 3600, 6),
        "hours_elapsed": round(elapsed / 3600, 6),
        "complete": completed,
        "windows_completed": len(events),
        "planned_requests": requests_planned,
        "attempted_requests": responses,
        "http_200": ok,
        "availability": round(availability, 7),
        "worst_window_p99_ms": max(p99_values) if p99_values else None,
        "max_inter_window_gap_s": round(max(gaps), 3) if gaps else 0.0,
        "authorization_checks_completed": sum(e.get("authz_tests", 0) for e in events),
        "failed_window_numbers": failed,
        "slo_gates_passed_so_far": bool(events) and not failed and availability >= .999
            and responses == requests_planned and (max(p99_values) if p99_values else float("inf")) < 1000
            and (max(gaps) if gaps else 0) <= 15,
        "final_72h_pass": False,  # filled only after the required window ends
        "note": "Worst per-window p99 is a conservative gate, not the global p99. Workload excludes write-heavy Saga and IdP HA."
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--hours", type=float, default=DEFAULT_DURATION_HOURS)
    p.add_argument("--window-seconds", type=int, default=300)
    p.add_argument("--rate", type=float, default=20)
    p.add_argument("--concurrency", type=int, default=12)
    p.add_argument("--authz-interval-seconds", type=int, default=3600)
    p.add_argument("--output", type=pathlib.Path, default=pathlib.Path("/tmp/taskflow-keycloak-72h-soak"))
    args = p.parse_args()
    if not (0.001 <= args.hours <= 72):
        p.error("hours must be 0.001 .. 72")
    if not (5 <= args.window_seconds <= 3600 and 1 <= args.concurrency <= 64 and .1 <= args.rate <= 100):
        p.error("unsafe load parameters")
    if args.authz_interval_seconds < 60:
        p.error("authorization check cadence must be >= 60 seconds")
    out = args.output.resolve()
    if not str(out).startswith("/tmp/taskflow-"):
        p.error("output path must be a unique /tmp/taskflow-* directory")
    if out.exists():
        p.error("output directory already exists; refuse overwrite or accidental false restart")
    out.mkdir(mode=0o700, parents=True)
    os.chmod(out, 0o700)
    (out / "windows").mkdir(mode=0o700)

    duration = args.hours * 3600
    start = time.monotonic()
    deadline = start + duration
    next_authz = start
    events: list[dict] = []
    state = "running"

    try:
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            window_duration = min(args.window_seconds, remaining)
            if window_duration < 0.5:
                time.sleep(remaining)
                break
            seq = len(events) + 1
            output = out / "windows" / f"window-{seq:05d}.json"
            started_at = time.time()
            cmd = ["sh", str(ROOT / "keycloak-slo-run.sh"),
                   "--duration", str(window_duration), "--rate", str(args.rate),
                   "--concurrency", str(args.concurrency), "--output", str(output)]
            run = subprocess.run(cmd, cwd=str(ROOT.parent), capture_output=True, text=True)
            if not output.exists():
                raise RuntimeError(f"window {seq} failed without report: exit {run.returncode}, stderr={run.stderr[-900:]}")
            result = json.loads(output.read_text(encoding="utf-8"))
            if run.returncode != 0 or not result.get("pass", False):
                raise RuntimeError(f"window {seq} failed gate: exit={run.returncode}, details={result['gates']}")

            authz_tests = 0
            if time.monotonic() >= next_authz:
                probe = subprocess.run(["sh", str(ROOT / "keycloak-live-smoke.sh")],
                                       cwd=str(ROOT.parent), capture_output=True, text=True)
                if probe.returncode != 0 or "KEYCLOAK_LIVE_STAGE_SMOKE_PASS" not in probe.stdout:
                    raise RuntimeError(f"authorization probe failed during window {seq}: {probe.stdout[-700:]} {probe.stderr[-700:]}")
                authz_tests = 13
                next_authz = time.monotonic() + args.authz_interval_seconds

            events.append({"seq": seq, "timestamp": utc_now(),
                           "start_epoch": round(started_at, 3), "end_epoch": round(time.time(), 3),
                           "result_file": str(output), "result": result,
                           "authz_pass": True, "authz_tests": authz_tests})
            with (out / "events.ndjson").open("a", encoding="utf-8") as fd:
                fd.write(json.dumps(events[-1], ensure_ascii=False, sort_keys=True) + "\n")
                fd.flush()
                os.fsync(fd.fileno())
            summary = summarize(events, time.monotonic() - start, duration, state)
            save_json_atomic(out / "summary.json", summary)
            print(f"window={seq} completed; count={result['attempted_requests']} p99={result['latency_ms']['p99']}ms cumulative_availability={summary['availability']}; checkpoint={out}/summary.json", flush=True)
        state = "complete"
    except (Exception, KeyboardInterrupt) as err:
        state = "failed_or_interrupted"
        print(f"SOAK_STOPPED: {err}", file=sys.stderr, flush=True)

    summary = summarize(events, time.monotonic() - start, duration, state)
    summary["final_72h_pass"] = summary["complete"] and summary["slo_gates_passed_so_far"] and args.hours >= 72
    save_json_atomic(out / "summary.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0 if summary["complete"] and summary["slo_gates_passed_so_far"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
