#!/usr/bin/env python3
"""HTTP keep-alive load generator for cloud Provision order admission."""

from __future__ import annotations

import argparse
import http.client
import json
import math
import threading
import time
from collections import Counter
from pathlib import Path


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    index = min(len(values) - 1, max(0, math.ceil(p * len(values)) - 1))
    return round(values[index], 3)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoints", required=True, help="comma-separated host:port endpoints")
    parser.add_argument("--token", required=True)
    parser.add_argument("--requests", type=int, default=600)
    parser.add_argument("--concurrency", type=int, default=64)
    parser.add_argument("--tenant-mode", choices=("hot", "sharded"), default="hot")
    parser.add_argument("--tenant-prefix", default="scale-tenant")
    parser.add_argument("--tenant-count", type=int, default=32)
    parser.add_argument("--name-prefix", default="scale-order")
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--image", default="nginx:1.27-alpine")
    parser.add_argument("--replicas", type=int, default=1)
    parser.add_argument("--cpu", default="1m")
    parser.add_argument("--memory", default="1Mi")
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    endpoints = [item.strip() for item in args.endpoints.split(",") if item.strip()]
    if not endpoints:
        parser.error("at least one endpoint is required")
    if args.requests <= 0 or args.concurrency <= 0 or args.tenant_count <= 0:
        parser.error("requests, concurrency and tenant-count must be positive")

    next_index = 0
    index_lock = threading.Lock()
    result_lock = threading.Lock()
    barrier = threading.Barrier(args.concurrency + 1)
    latencies: list[float] = []
    statuses: Counter[int] = Counter()
    errors: Counter[str] = Counter()
    endpoint_counts: Counter[str] = Counter()
    response_errors: Counter[str] = Counter()

    def take_index() -> int | None:
        nonlocal next_index
        with index_lock:
            if next_index >= args.requests:
                return None
            value = next_index
            next_index += 1
            return value

    def worker(worker_id: int) -> None:
        local_latencies: list[float] = []
        local_statuses: Counter[int] = Counter()
        local_errors: Counter[str] = Counter()
        local_endpoint_counts: Counter[str] = Counter()
        local_response_errors: Counter[str] = Counter()
        endpoint = endpoints[worker_id % len(endpoints)]
        host, port_raw = endpoint.rsplit(":", 1)
        port = int(port_raw)
        conn: http.client.HTTPConnection | None = None
        barrier.wait()

        while True:
            index = take_index()
            if index is None:
                break
            tenant = (
                args.tenant_prefix
                if args.tenant_mode == "hot"
                else f"{args.tenant_prefix}-{index % args.tenant_count:02d}"
            )
            name = f"{args.name_prefix}-{index:06d}"
            compute: dict[str, object] = {
                "namespace": args.namespace,
                "image": args.image,
                "replicas": args.replicas,
            }
            if args.cpu:
                compute["cpu"] = args.cpu
            if args.memory:
                compute["memory"] = args.memory
            payload = json.dumps(
                {
                    "tenant_id": tenant,
                    "name": name,
                    "provider": "kubernetes",
                    "spec": {
                        "compute": compute,
                        "network": {"enabled": False},
                        "storage": {"enabled": False},
                    },
                },
                separators=(",", ":"),
            )
            headers = {
                "Authorization": f"Bearer {args.token}",
                "Content-Type": "application/json",
                "Content-Length": str(len(payload)),
                "Idempotency-Key": f"{args.name_prefix}-{index:06d}",
                "Connection": "keep-alive",
            }
            started = time.perf_counter()
            try:
                if conn is None:
                    conn = http.client.HTTPConnection(host, port, timeout=args.timeout)
                conn.request("POST", "/api/v1/cloud/workflows/provision", body=payload, headers=headers)
                response = conn.getresponse()
                body = response.read()
                elapsed_ms = (time.perf_counter() - started) * 1000
                local_latencies.append(elapsed_ms)
                local_statuses[response.status] += 1
                local_endpoint_counts[endpoint] += 1
                if response.status != 202:
                    try:
                        message = json.loads(body.decode("utf-8", "replace")).get("error", "unknown")
                    except Exception:
                        message = body[:160].decode("utf-8", "replace") or "empty"
                    local_response_errors[f"{response.status}:{message}"] += 1
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
        with result_lock:
            latencies.extend(local_latencies)
            statuses.update(local_statuses)
            errors.update(local_errors)
            endpoint_counts.update(local_endpoint_counts)
            response_errors.update(local_response_errors)

    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(args.concurrency)]
    for thread in threads:
        thread.start()
    barrier.wait()
    started = time.perf_counter()
    for thread in threads:
        thread.join()
    elapsed = time.perf_counter() - started
    latencies.sort()

    result = {
        "schema_version": 1,
        "requests_target": args.requests,
        "responses": len(latencies),
        "accepted": statuses.get(202, 0),
        "concurrency": args.concurrency,
        "tenant_mode": args.tenant_mode,
        "tenant_count": 1 if args.tenant_mode == "hot" else args.tenant_count,
        "duration_s": round(elapsed, 6),
        "throughput_rps": round(args.requests / elapsed, 3) if elapsed else 0.0,
        "response_rps": round(len(latencies) / elapsed, 3) if elapsed else 0.0,
        "latency_ms": {
            "avg": round(sum(latencies) / len(latencies), 3) if latencies else None,
            "p50": percentile(latencies, 0.50),
            "p95": percentile(latencies, 0.95),
            "p99": percentile(latencies, 0.99),
            "max": round(latencies[-1], 3) if latencies else None,
        },
        "status": {str(key): value for key, value in sorted(statuses.items())},
        "transport_errors": dict(errors),
        "response_errors": dict(response_errors),
        "endpoint_requests": dict(endpoint_counts),
        "spec": {
            "replicas": args.replicas,
            "cpu": args.cpu,
            "memory": args.memory,
            "network": False,
            "storage": False,
        },
    }
    rendered = json.dumps(result, ensure_ascii=False, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(rendered)
    return 0 if result["accepted"] == args.requests and not errors else 2


if __name__ == "__main__":
    raise SystemExit(main())
