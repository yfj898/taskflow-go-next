#!/usr/bin/env python3
"""Dependency-free HTTP keep-alive load generator used by perf-benchmark.sh."""

import http.client
import json
import math
import sys
import threading
import time
from collections import Counter


host = "127.0.0.1"
port = int(sys.argv[1]) if len(sys.argv) > 1 else 18080
concurrency = int(sys.argv[2]) if len(sys.argv) > 2 else 100
duration = float(sys.argv[3]) if len(sys.argv) > 3 else 30.0

payload = json.dumps(
    {"name": "perf", "type": "compute", "payload": "performance-test"},
    separators=(",", ":"),
)
headers = {
    "Content-Type": "application/json",
    "Content-Length": str(len(payload)),
    "Connection": "keep-alive",
}

latencies = []
statuses = Counter()
errors = Counter()
lock = threading.Lock()
barrier = threading.Barrier(concurrency + 1)
stop_at = [0.0]


def worker():
    local_latencies = []
    local_statuses = Counter()
    local_errors = Counter()
    conn = None
    barrier.wait()

    while time.perf_counter() < stop_at[0]:
        try:
            if conn is None:
                conn = http.client.HTTPConnection(host, port, timeout=10)
            started = time.perf_counter()
            conn.request("POST", "/api/v1/tasks", body=payload, headers=headers)
            response = conn.getresponse()
            response.read()
            local_latencies.append((time.perf_counter() - started) * 1000)
            local_statuses[response.status] += 1
        except Exception as exc:  # benchmark must count transport failures
            local_errors[type(exc).__name__] += 1
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            conn = None

    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass

    with lock:
        latencies.extend(local_latencies)
        statuses.update(local_statuses)
        errors.update(local_errors)


threads = [threading.Thread(target=worker, daemon=True) for _ in range(concurrency)]
for thread in threads:
    thread.start()

barrier.wait()
started = time.perf_counter()
stop_at[0] = started + duration
for thread in threads:
    thread.join()
elapsed = time.perf_counter() - started

latencies.sort()


def percentile(p):
    if not latencies:
        return None
    index = min(len(latencies) - 1, max(0, math.ceil(p * len(latencies)) - 1))
    return round(latencies[index], 3)


summary = {
    "concurrency": concurrency,
    "duration_s": round(elapsed, 3),
    "requests": len(latencies),
    "rps": round(len(latencies) / elapsed, 2) if elapsed else 0,
    "latency_ms": {
        "avg": round(sum(latencies) / len(latencies), 3) if latencies else None,
        "p50": percentile(0.50),
        "p95": percentile(0.95),
        "p99": percentile(0.99),
        "max": round(latencies[-1], 3) if latencies else None,
    },
    "status": dict(statuses),
    "errors": dict(errors),
}
print(json.dumps(summary, ensure_ascii=False))

