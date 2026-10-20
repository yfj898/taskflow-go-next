#!/usr/bin/env python3
import hashlib
import hmac
import json
import os
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


PORT = int(os.getenv("PORT", "8080"))
TOKEN = os.getenv("GOVERNANCE_BEARER_TOKEN", "phase32-governance-token")
KEY = os.getenv("GOVERNANCE_SIGNING_KEY", "phase32-governance-signing-key").encode()
KEY_ID = os.getenv("GOVERNANCE_SIGNING_KEY_ID", "phase32-governance-v1")
REPLICA_ID = os.getenv("GOVERNANCE_REPLICA_ID", "governance-primary")
RECEIPT_KEY = os.getenv("GOVERNANCE_RECEIPT_HMAC_KEY", "phase32-governance-receipt-key").encode()
RECEIPT_KEY_ID = os.getenv("GOVERNANCE_RECEIPT_HMAC_KEY_ID", "phase32-governance-receipt-v1")
RECEIPT_KEY_GENERATION = int(os.getenv("GOVERNANCE_RECEIPT_KEY_GENERATION", "1"))
MEMBERSHIP_EPOCH = int(os.getenv("GOVERNANCE_MEMBERSHIP_EPOCH", "1"))
FAULT_TERM = int(os.getenv("GOVERNANCE_FAULT_TERM", "0"))
FAULT_RECORD_HASH = os.getenv("GOVERNANCE_FAULT_RECORD_HASH", "")

lock = threading.Lock()
anchors = {}
alerts = {}
stats = {
    "anchor_requests": 0,
    "anchor_conflicts": 0,
    "alert_requests": 0,
    "signature_failures": 0,
    "auth_failures": 0,
}


def now_text():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_json(value):
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


def sign_receipt(payload):
    return "sha256=" + hmac.new(RECEIPT_KEY, canonical_json(payload), hashlib.sha256).hexdigest()


def with_receipt_signature(payload):
    signed = dict(payload)
    signed["replica_id"] = REPLICA_ID
    signed["membership_epoch"] = MEMBERSHIP_EPOCH
    signed["receipt_key_generation"] = RECEIPT_KEY_GENERATION
    signature_payload = dict(signed)
    signed["receipt_key_id"] = RECEIPT_KEY_ID
    signed["receipt_algorithm"] = "hmac-sha256"
    signed["receipt_signature"] = sign_receipt(signature_payload)
    return signed


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def send_json(self, code, payload):
        body = json.dumps(payload, separators=(",", ":")).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def authorized(self):
        if TOKEN and self.headers.get("Authorization", "") != "Bearer " + TOKEN:
            with lock:
                stats["auth_failures"] += 1
            self.send_json(401, {"error": "unauthorized"})
            return False
        return True

    def read_signed_envelope(self):
        length = int(self.headers.get("Content-Length", "0"))
        try:
            envelope = json.loads(self.rfile.read(length) or b"{}")
            payload = envelope["payload"]
            if isinstance(payload, str):
                payload_bytes = payload.encode()
                payload_obj = json.loads(payload)
            else:
                payload_obj = payload
                payload_bytes = canonical_json(payload_obj)
            if envelope.get("key_id") != KEY_ID or envelope.get("algorithm") != "hmac-sha256":
                raise ValueError("key metadata mismatch")
            expected = "sha256=" + hmac.new(KEY, payload_bytes, hashlib.sha256).hexdigest()
            if not hmac.compare_digest(expected, envelope.get("signature", "")):
                raise ValueError("signature mismatch")
            return payload_obj
        except Exception as exc:
            with lock:
                stats["signature_failures"] += 1
            self.send_json(401, {"error": str(exc)})
            return None

    def do_GET(self):
        if self.path == "/health":
            self.send_json(200, {"status": "ok"})
            return
        if not self.authorized():
            return
        if self.path == "/stats":
            with lock:
                payload = dict(stats)
                payload["anchors"] = len(anchors)
                payload["alerts"] = len(alerts)
                payload["replica_id"] = REPLICA_ID
            self.send_json(200, payload)
            return
        if self.path == "/anchors":
            with lock:
                payload = [anchors[key] for key in sorted(anchors)]
            self.send_json(200, {"items": payload})
            return
        if self.path == "/alerts":
            with lock:
                payload = list(alerts.values())
            self.send_json(200, {"items": payload})
            return
        self.send_json(404, {"error": "not found"})

    def do_POST(self):
        if not self.authorized():
            return
        payload = self.read_signed_envelope()
        if payload is None:
            return
        if self.path == "/anchors":
            self.handle_anchor(payload)
            return
        if self.path == "/alerts":
            self.handle_alert(payload)
            return
        self.send_json(404, {"error": "not found"})

    def handle_anchor(self, payload):
        if payload.get("version") != "taskflow-siem-term-anchor-v1":
            self.send_json(400, {"error": "bad anchor version"})
            return
        term = int(payload.get("term", 0))
        record_hash = str(payload.get("record_hash", ""))
        if term <= 0 or len(record_hash) != 64:
            self.send_json(400, {"error": "bad anchor identity"})
            return
        with lock:
            stats["anchor_requests"] += 1
            existing = anchors.get(term)
            if existing and existing["record_hash"] != record_hash:
                stats["anchor_conflicts"] += 1
                self.send_json(409, {"error": "immutable term anchor conflict"})
                return
            if existing:
                receipt = existing
            else:
                receipt_record_hash = record_hash
                if term == FAULT_TERM and len(FAULT_RECORD_HASH) == 64:
                    receipt_record_hash = FAULT_RECORD_HASH
                anchor_id = f"anchor-{term}-{receipt_record_hash[:16]}"
                anchored_at = now_text()
                receipt_hash = hashlib.sha256(
                    canonical_json({"anchor_id": anchor_id, "term": term, "record_hash": receipt_record_hash})
                ).hexdigest()
                receipt = with_receipt_signature({
                    "anchor_id": anchor_id,
                    "term": term,
                    "record_hash": receipt_record_hash,
                    "receipt_hash": receipt_hash,
                    "anchored_at": anchored_at,
                })
                anchors[term] = receipt
        self.send_json(200, receipt)

    def handle_alert(self, payload):
        if payload.get("version") != "taskflow-siem-incident-v1":
            self.send_json(400, {"error": "bad alert version"})
            return
        evidence_hash = str(payload.get("evidence_hash", ""))
        if len(evidence_hash) != 64:
            self.send_json(400, {"error": "bad evidence hash"})
            return
        with lock:
            stats["alert_requests"] += 1
            alerts.setdefault(evidence_hash, payload)
        receipt_hash = hashlib.sha256(
            canonical_json({"status": "accepted", "evidence_hash": evidence_hash})
        ).hexdigest()
        self.send_json(200, with_receipt_signature({
            "status": "accepted",
            "evidence_hash": evidence_hash,
            "receipt_hash": receipt_hash,
        }))


ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
