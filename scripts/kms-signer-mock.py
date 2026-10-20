#!/usr/bin/env python3
import hashlib
import hmac
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


BEARER_TOKEN = os.getenv("KMS_BEARER_TOKEN", "phase26-kms-token")


def parse_keys():
    raw = os.getenv("KMS_SIGNING_KEYS", "phase26-kms-v2=phase26-kms-secret")
    result = {}
    for item in raw.split(","):
        key_id, sep, secret = item.partition("=")
        if not sep or not key_id.strip() or not secret:
            raise RuntimeError(f"invalid KMS_SIGNING_KEYS entry: {item!r}")
        result[key_id.strip()] = secret.encode()
    return result


KEYS = parse_keys()
lock = threading.Lock()
state = {
    "requests": 0,
    "auth_failures": 0,
    "invalid_requests": 0,
    "last_key_id": "",
    "last_algorithm": "",
    "last_digest_sha256": "",
}


class Handler(BaseHTTPRequestHandler):
    server_version = "TaskFlowKMSSignerMock/1.0"

    def log_message(self, fmt, *args):
        return

    def _json(self, status, payload):
        body = json.dumps(payload, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self._json(200, {"status": "ok"})
            return
        if self.path == "/stats":
            with lock:
                snapshot = dict(state)
            self._json(200, snapshot)
            return
        self._json(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/sign":
            self._json(404, {"error": "not found"})
            return
        with lock:
            state["requests"] += 1
        if BEARER_TOKEN and self.headers.get("Authorization") != f"Bearer {BEARER_TOKEN}":
            with lock:
                state["auth_failures"] += 1
            self._json(401, {"error": "invalid bearer token"})
            return
        length = int(self.headers.get("Content-Length", "0"))
        try:
            request = json.loads(self.rfile.read(length) or b"{}")
            if set(request) != {"version", "key_id", "algorithm", "digest_sha256"}:
                raise ValueError("unexpected signing request fields")
            key_id = str(request["key_id"])
            algorithm = str(request["algorithm"])
            digest_hex = str(request["digest_sha256"])
            digest = bytes.fromhex(digest_hex)
            if len(digest) != hashlib.sha256().digest_size:
                raise ValueError("invalid digest length")
            key = KEYS[key_id]
            if algorithm != "hmac-sha256-digest":
                raise ValueError("unsupported algorithm")
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            with lock:
                state["invalid_requests"] += 1
            self._json(400, {"error": "invalid signing request"})
            return
        signature = "sha256=" + hmac.new(key, digest, hashlib.sha256).hexdigest()
        with lock:
            state["last_key_id"] = key_id
            state["last_algorithm"] = algorithm
            state["last_digest_sha256"] = digest_hex
        self._json(
            200,
            {"signature": signature, "key_id": key_id, "algorithm": algorithm},
        )


if __name__ == "__main__":
    port = int(os.getenv("PORT", "9701"))
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
