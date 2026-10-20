#!/usr/bin/env python3
import base64
import hashlib
import hmac
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "phase27-access")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "phase27-secret")
SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN", "")
REGION = os.getenv("AWS_REGION", "ap-southeast-1")
KMS_KEY_ID = os.getenv("AWS_KMS_KEY_ID", "phase27-kms-key")
KMS_KEY = os.getenv("AWS_KMS_HMAC_KEY", "phase27-kms-hmac-secret").encode()
lock = threading.Lock()
state = {"requests": 0, "auth_failures": 0, "generate_mac": 0, "last_key_id": ""}


def hmac_sha256(key, value):
    return hmac.new(key, value.encode(), hashlib.sha256).digest()


class Handler(BaseHTTPRequestHandler):
    server_version = "TaskFlowAWSKMSMock/1.0"

    def log_message(self, fmt, *args):
        return

    def _json(self, status, payload):
        body = json.dumps(payload, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/x-amz-json-1.1")
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
        with lock:
            state["requests"] += 1
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        if self.headers.get("X-Amz-Target") != "TrentService.GenerateMac":
            self._json(400, {"__type": "UnsupportedOperationException"})
            return
        if not self._verify_sigv4(body):
            with lock:
                state["auth_failures"] += 1
            self._json(403, {"__type": "UnrecognizedClientException"})
            return
        try:
            request = json.loads(body)
            if request["KeyId"] != KMS_KEY_ID:
                raise ValueError("key id")
            if request["MacAlgorithm"] != "HMAC_SHA_256":
                raise ValueError("algorithm")
            message = base64.b64decode(request["Message"], validate=True)
            if len(message) != hashlib.sha256().digest_size:
                raise ValueError("digest")
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            self._json(400, {"__type": "ValidationException"})
            return
        mac = hmac.new(KMS_KEY, message, hashlib.sha256).digest()
        with lock:
            state["generate_mac"] += 1
            state["last_key_id"] = KMS_KEY_ID
        self._json(
            200,
            {
                "KeyId": KMS_KEY_ID,
                "MacAlgorithm": "HMAC_SHA_256",
                "Mac": base64.b64encode(mac).decode(),
            },
        )

    def _verify_sigv4(self, body):
        if SESSION_TOKEN and self.headers.get("X-Amz-Security-Token", "") != SESSION_TOKEN:
            return False
        auth = self.headers.get("Authorization", "")
        if not auth.startswith("AWS4-HMAC-SHA256 "):
            return False
        parts = {}
        for item in auth[len("AWS4-HMAC-SHA256 "):].split(", "):
            key, sep, value = item.partition("=")
            if not sep:
                return False
            parts[key] = value
        credential = parts.get("Credential", "")
        signed_headers = parts.get("SignedHeaders", "")
        signature = parts.get("Signature", "")
        cred_parts = credential.split("/")
        if len(cred_parts) != 5 or cred_parts[0] != ACCESS_KEY:
            return False
        date, region, service, terminal = cred_parts[1:]
        if region != REGION or service != "kms" or terminal != "aws4_request":
            return False
        amz_date = self.headers.get("X-Amz-Date", "")
        if len(amz_date) < 8 or amz_date[:8] != date:
            return False
        names = signed_headers.split(";") if signed_headers else []
        canonical = []
        for name in names:
            if name == "host":
                value = self.headers.get("Host", "")
            else:
                value = self.headers.get(name, "")
            canonical.append(f"{name}:{' '.join(value.split())}\n")
        payload_hash = hashlib.sha256(body).hexdigest()
        canonical_request = "\n".join(
            ["POST", "/", "", "".join(canonical), signed_headers, payload_hash]
        )
        scope = f"{date}/{REGION}/kms/aws4_request"
        string_to_sign = (
            "AWS4-HMAC-SHA256\n"
            + amz_date
            + "\n"
            + scope
            + "\n"
            + hashlib.sha256(canonical_request.encode()).hexdigest()
        )
        k_date = hmac_sha256(("AWS4" + SECRET_KEY).encode(), date)
        k_region = hmac_sha256(k_date, REGION)
        k_service = hmac_sha256(k_region, "kms")
        k_signing = hmac_sha256(k_service, "aws4_request")
        expected = hmac.new(k_signing, string_to_sign.encode(), hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, signature)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "9702"))
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
