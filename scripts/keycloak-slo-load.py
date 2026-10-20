#!/usr/bin/env python3
"""Bounded authentic-OIDC GET load for the separate TaskFlow staging API.

No resource creation, writes, or Kubernetes operations. Two real Keycloak
client_credentials identities alternate, and credentials are refreshed
during sustained multi-hour runs. The report records exact workload bounds.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path


def pct(sorted_values: list[float], percentile: float) -> float | None:
    if not sorted_values:
        return None
    at = max(0, min(len(sorted_values) - 1, math.ceil(len(sorted_values) * percentile) - 1))
    return round(sorted_values[at], 3)


class OAuthCredentials:
    def __init__(self, issuer: str, client_id: str, secret: str):
        self._url = issuer.rstrip("/") + "/protocol/openid-connect/token"
        self._client_id = client_id
        self._secret = secret
        self._token = ""
        self._expires_at = 0.0
        self._mutex = threading.Lock()
        self.refreshes = 0

    def bearer(self) -> str:
        with self._mutex:
            if time.monotonic() < self._expires_at and self._token:
                return self._token
            payload = urllib.parse.urlencode(
                {
                    "grant_type": "client_credentials",
                    "client_id": self._client_id,
                    "client_secret": self._secret,
                }
            ).encode()
            request = urllib.request.Request(
                self._url,
                data=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            with urllib.request.urlopen(request, timeout=5) as response:
                reply = json.load(response)
            token = reply.get("access_token", "")
            expires_in = reply.get("expires_in", 0)
            if not token or not isinstance(expires_in, (int, float)) or expires_in <= 0:
                raise ValueError("Keycloak did not supply a usable access token")
            self._token = token
            self._expires_at = time.monotonic() + min(float(expires_in) * 0.55, 120)
            self.refreshes += 1
            return token


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="http://127.0.0.1:18088")
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--rate", type=float, default=40)
    parser.add_argument("--concurrency", type=int, default=12)
    parser.add_argument("--timeout", type=float, default=3)
    parser.add_argument("--p99-ms", type=float, default=1000)
    parser.add_argument("--availability", type=float, default=0.999)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    base = args.base.rstrip("/")
    if base != "http://127.0.0.1:18088":
        parser.error("only the private isolated TaskFlow stage endpoint is allowed")
    if not (1 <= args.duration <= 259200 and 0.1 <= args.rate <= 100 and 1 <= args.concurrency <= 64):
        parser.error("duration=1..259200s, rate=0.1..100rps, concurrency=1..64")
    if not (0 < args.availability <= 1 and args.p99_ms > 0 and args.timeout > 0):
        parser.error("invalid SLO constraints")
    issuer = os.environ.get("TASKFLOW_KEYCLOAK_ISSUER", "")
    secret_a = os.environ.get("TASKFLOW_SLO_TENANT_A_SECRET", "")
    secret_b = os.environ.get("TASKFLOW_SLO_TENANT_B_SECRET", "")
    if not issuer or not secret_a or not secret_b:
        parser.error("two Keycloak service credentials and issuer are required")

    credentials = [
        OAuthCredentials(issuer, "taskflow-tenant-a", secret_a),
        OAuthCredentials(issuer, "taskflow-tenant-b", secret_b),
    ]
    for cred in credentials:
        cred.bearer()  # fail immediately rather than midway through the run

    count = math.ceil(args.duration * args.rate)
    statuses: Counter[int] = Counter()
    transport_errors: Counter[str] = Counter()
    latency: list[float] = []
    mutex = threading.Lock()
    start = time.monotonic()

    def worker(worker_id: int) -> None:
        my_statuses: Counter[int] = Counter()
        my_errors: Counter[str] = Counter()
        my_latency: list[float] = []
        for i in range(worker_id, count, args.concurrency):
            due = start + i / args.rate
            if due > start + args.duration:
                break
            remaining = due - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)
            owner = i % 2
            tenant = "team-a" if owner == 0 else "team-b"
            path = "/api/v1/cloud/tenants/" + tenant + "/quota"
            try:
                token = credentials[owner].bearer()
                req = urllib.request.Request(base + path, headers={"Authorization": "Bearer " + token})
                started = time.perf_counter()
                try:
                    with urllib.request.urlopen(req, timeout=args.timeout) as response:
                        response.read()
                        code = response.status
                except urllib.error.HTTPError as exc:
                    code = exc.code
                    exc.close()
                elapsed_ms = (time.perf_counter() - started) * 1000
                my_latency.append(elapsed_ms)
                my_statuses[code] += 1
            except Exception as exc:
                my_errors[type(exc).__name__] += 1
        with mutex:
            statuses.update(my_statuses)
            transport_errors.update(my_errors)
            latency.extend(my_latency)

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        list(pool.map(worker, range(args.concurrency)))
    elapsed = time.monotonic() - start
    latency.sort()
    successes = statuses.get(200, 0)
    attempted = sum(statuses.values()) + sum(transport_errors.values())
    p99 = pct(latency, 0.99)
    observed_availability = successes / attempted if attempted else 0.0
    results = {
        "test": "real-keycloak-signed-jwt-http-slo",
        "scope": "standalone loopback TaskFlow; quota GET; no Kubernetes writes",
        "duration_configured_s": args.duration,
        "duration_elapsed_s": round(elapsed, 3),
        "rate_target_rps": args.rate,
        "concurrency": args.concurrency,
        "planned_requests": count,
        "attempted_requests": attempted,
        "status_codes": {str(k): v for k, v in sorted(statuses.items())},
        "transport_errors": dict(transport_errors),
        "token_refreshes": sum(creds.refreshes for creds in credentials),
        "throughput_rps": round(attempted / elapsed, 3) if elapsed else 0.0,
        "availability_ratio": round(observed_availability, 6),
        "latency_ms": {
            "p50": pct(latency, 0.50),
            "p95": pct(latency, 0.95),
            "p99": p99,
            "max": round(latency[-1], 3) if latency else None,
        },
        "gates": {
            "availability_threshold": args.availability,
            "availability_pass": observed_availability >= args.availability,
            "p99_threshold_ms": args.p99_ms,
            "p99_pass": p99 is not None and p99 < args.p99_ms,
            "scheduled_requests_complete": attempted == count,
        },
    }
    results["pass"] = all(
        (results["gates"]["availability_pass"],
         results["gates"]["p99_pass"],
         results["gates"]["scheduled_requests_complete"])
    )
    output = json.dumps(results, ensure_ascii=False, sort_keys=True, indent=2)
    print(output)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output + "\n", encoding="utf-8")
    return 0 if results["pass"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
