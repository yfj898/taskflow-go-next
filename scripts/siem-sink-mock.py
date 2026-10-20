#!/usr/bin/env python3
import hashlib
import hmac
import base64
import json
import os
import socket
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import parse as urllib_parse
from urllib import request as urllib_request


SIGNING_KEY = os.getenv("SIEM_SIGNING_KEY", "phase25-signing-key").encode()
KEY_ID = os.getenv("SIEM_SIGNING_KEY_ID", "phase25-key-v1")
BEARER_TOKEN = os.getenv("SIEM_BEARER_TOKEN", "phase25-sink-token")
VAULT_VERIFY_ENDPOINT = os.getenv("SIEM_VAULT_VERIFY_ENDPOINT", "").strip()
VAULT_TOKEN = os.getenv("SIEM_VAULT_TOKEN", "").strip()
VAULT_KEY_ID = os.getenv("SIEM_VAULT_KEY_ID", "").strip()
RECEIVER_ID = os.getenv("SIEM_RECEIVER_ID", "receiver-primary").strip()
RECEIVER_GENERATION = max(1, int(os.getenv("SIEM_RECEIVER_GENERATION", "1")))
RECEIVER_TERM = max(1, int(os.getenv("SIEM_RECEIVER_TERM", "1")))
ACTIVE_RECEIVER = os.getenv("SIEM_ACTIVE_RECEIVER", "receiver-primary").strip()
CHECKPOINT_HMAC_KEY = os.getenv("SIEM_CHECKPOINT_HMAC_KEY", "").encode()
CHECKPOINT_HMAC_KEY_ID = os.getenv("SIEM_CHECKPOINT_HMAC_KEY_ID", "").strip()


def signing_keys():
    raw = os.getenv("SIEM_SIGNING_KEYS", "").strip()
    if not raw:
        return {KEY_ID: SIGNING_KEY}
    result = {}
    for item in raw.split(","):
        key_id, sep, secret = item.partition("=")
        if not sep or not key_id.strip() or not secret:
            raise RuntimeError(f"invalid SIEM_SIGNING_KEYS entry: {item!r}")
        result[key_id.strip()] = secret.encode()
    return result


SIGNING_KEYS = signing_keys()

lock = threading.Lock()
state = {
    "requests": 0,
    "accepted_batches": 0,
    "accepted_events": 0,
    "signature_failures": 0,
    "auth_failures": 0,
    "forced_failures": 0,
    "duplicate_batches": 0,
    "idempotency_conflicts": 0,
    "fail_remaining": int(os.getenv("SIEM_FAIL_FIRST", "0")),
    "fail_always": os.getenv("SIEM_FAIL_ALWAYS", "false").lower() == "true",
    "drop_after_accept_remaining": int(os.getenv("SIEM_DROP_AFTER_ACCEPT", "0")),
    "last_first_sequence": 0,
    "last_last_sequence": 0,
    "last_key_id": "",
    "last_algorithm": "",
    "last_previous_key_id": "",
    "previous_signature_valid": False,
    "last_idempotency_key": "",
    "accepted_sequences": [],
    "seen_idempotency_keys": [],
    "idempotency_payloads": {},
    "receipt_ledger": {},
    "checkpoint_hash_override_sequence": 0,
    "checkpoint_hash_override": "",
    "current_term": RECEIVER_TERM,
    "active_receiver": ACTIVE_RECEIVER,
    "stale_term_rejections": 0,
    "term_conflict_rejections": 0,
    "term_updates": 0,
}


class Handler(BaseHTTPRequestHandler):
    server_version = "TaskFlowSIEMMock/1.0"

    def log_message(self, fmt, *args):
        return

    def _json(self, status, payload, headers=None):
        body = json.dumps(payload, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib_parse.urlparse(self.path)
        if parsed.path == "/health":
            self._json(200, {"status": "ok"})
            return
        if parsed.path == "/stats":
            with lock:
                snapshot = dict(state)
                snapshot.pop("receipt_ledger", None)
            self._json(200, snapshot)
            return
        if parsed.path == "/checkpoint":
            if BEARER_TOKEN and self.headers.get("Authorization") != f"Bearer {BEARER_TOKEN}":
                with lock:
                    state["auth_failures"] += 1
                self._json(401, {"error": "invalid bearer token"})
                return
            query = urllib_parse.parse_qs(parsed.query)
            try:
                after = max(0, int(query.get("after_sequence", ["0"])[0]))
                limit = max(1, min(500, int(query.get("limit", ["500"])[0])))
            except ValueError:
                self._json(400, {"error": "invalid checkpoint cursor"})
                return
            with lock:
                ledger = [dict(row) for row in state["receipt_ledger"].values()]
                override_sequence = state["checkpoint_hash_override_sequence"]
                override_hash = state["checkpoint_hash_override"]
                current_term = state["current_term"]
                active_receiver = state["active_receiver"]
            if override_sequence > 0 and override_hash:
                for row in ledger:
                    if int(row["chain_sequence"]) == override_sequence:
                        row["record_hash"] = override_hash
            ledger.sort(key=lambda row: row["chain_sequence"])
            high_watermark = 0
            head_record_hash = ""
            for row in ledger:
                sequence = int(row["chain_sequence"])
                if sequence == high_watermark + 1:
                    high_watermark = sequence
                    head_record_hash = row["record_hash"]
                elif sequence > high_watermark + 1:
                    break
            items = [
                dict(row)
                for row in ledger
                if after < int(row["chain_sequence"]) <= high_watermark
            ][:limit]
            payload = {
                "version": "taskflow-control-plane-siem-checkpoint-v1",
                "receiver_id": RECEIVER_ID,
                "receiver_generation": RECEIVER_GENERATION,
                "receiver_term": current_term,
                "active_receiver": active_receiver,
                "high_watermark_sequence": high_watermark,
                "head_record_hash": head_record_hash,
                "items": items,
            }
            headers = {
                "X-TaskFlow-Receiver-ID": RECEIVER_ID,
                "X-TaskFlow-Receiver-Generation": str(RECEIVER_GENERATION),
            }
            if CHECKPOINT_HMAC_KEY and CHECKPOINT_HMAC_KEY_ID:
                body = json.dumps(payload, separators=(",", ":")).encode()
                signature = hmac.new(CHECKPOINT_HMAC_KEY, body, hashlib.sha256).hexdigest()
                headers["X-TaskFlow-Checkpoint-Key-ID"] = CHECKPOINT_HMAC_KEY_ID
                headers["X-TaskFlow-Checkpoint-Signature"] = "sha256=" + signature
            self._json(200, payload, headers)
            return
        self._json(404, {"error": "not found"})

    def _verify_vault_signature(self, body, signature, key_id):
        if not VAULT_VERIFY_ENDPOINT or not VAULT_TOKEN:
            return False
        if VAULT_KEY_ID and key_id != VAULT_KEY_ID:
            return False
        payload = json.dumps(
            {
                "input": base64.b64encode(hashlib.sha256(body).digest()).decode(),
                "signature": signature,
                "prehashed": True,
                "signature_algorithm": "pkcs1v15",
            },
            separators=(",", ":"),
        ).encode()
        req = urllib_request.Request(
            VAULT_VERIFY_ENDPOINT,
            data=payload,
            method="POST",
            headers={"Content-Type": "application/json", "X-Vault-Token": VAULT_TOKEN},
        )
        try:
            with urllib_request.urlopen(req, timeout=3) as response:
                result = json.load(response)
        except Exception:
            return False
        return bool(result.get("data", {}).get("valid"))

    def do_POST(self):
        if self.path == "/admin/mode":
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length) or b"{}")
            with lock:
                if "fail_remaining" in payload:
                    state["fail_remaining"] = max(0, int(payload["fail_remaining"]))
                if "fail_always" in payload:
                    state["fail_always"] = bool(payload["fail_always"])
                if "drop_after_accept_remaining" in payload:
                    state["drop_after_accept_remaining"] = max(
                        0, int(payload["drop_after_accept_remaining"])
                    )
                if "checkpoint_hash_override_sequence" in payload:
                    state["checkpoint_hash_override_sequence"] = max(
                        0, int(payload["checkpoint_hash_override_sequence"])
                    )
                if "checkpoint_hash_override" in payload:
                    state["checkpoint_hash_override"] = str(payload["checkpoint_hash_override"]).strip()
            self._json(200, {"status": "updated"})
            return
        if self.path == "/control/term":
            if BEARER_TOKEN and self.headers.get("Authorization") != f"Bearer {BEARER_TOKEN}":
                with lock:
                    state["auth_failures"] += 1
                self._json(401, {"error": "invalid bearer token"})
                return
            length = int(self.headers.get("Content-Length", "0"))
            try:
                payload = json.loads(self.rfile.read(length) or b"{}")
                term = int(payload["term"])
                active_receiver = str(payload["active_receiver"]).strip()
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                self._json(400, {"error": "invalid receiver term fence"})
                return
            with lock:
                if term < state["current_term"]:
                    state["stale_term_rejections"] += 1
                    current_term = state["current_term"]
                    current_active = state["active_receiver"]
                elif term == state["current_term"] and active_receiver != state["active_receiver"]:
                    state["term_conflict_rejections"] += 1
                    current_term = state["current_term"]
                    current_active = state["active_receiver"]
                else:
                    if term > state["current_term"]:
                        state["term_updates"] += 1
                    state["current_term"] = term
                    state["active_receiver"] = active_receiver
                    current_term = term
                    current_active = active_receiver
            if term < current_term:
                self._json(409, {"error": "stale receiver term", "term": current_term, "active_receiver": current_active})
                return
            if term == current_term and active_receiver != current_active:
                self._json(409, {"error": "conflicting receiver active assignment for term", "term": current_term, "active_receiver": current_active})
                return
            self._json(200, {"receiver_id": RECEIVER_ID, "term": current_term, "active_receiver": current_active})
            return
        if self.path != "/events":
            self._json(404, {"error": "not found"})
            return

        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        with lock:
            state["requests"] += 1

        if BEARER_TOKEN and self.headers.get("Authorization") != f"Bearer {BEARER_TOKEN}":
            with lock:
                state["auth_failures"] += 1
            self._json(401, {"error": "invalid bearer token"})
            return

        try:
            receiver_term = int(self.headers.get("X-TaskFlow-Receiver-Term", "1"))
        except ValueError:
            self._json(400, {"error": "invalid receiver term"})
            return
        with lock:
            if receiver_term < state["current_term"]:
                state["stale_term_rejections"] += 1
                current_term = state["current_term"]
            else:
                if receiver_term > state["current_term"]:
                    state["current_term"] = receiver_term
                current_term = state["current_term"]
        if receiver_term < current_term:
            self._json(409, {"error": "stale receiver term", "term": current_term})
            return

        key_id = self.headers.get("X-TaskFlow-Audit-Key-ID", "")
        algorithm = self.headers.get("X-TaskFlow-Audit-Signature-Algorithm", "hmac-sha256")
        actual = self.headers.get("X-TaskFlow-Audit-Signature", "")
        if algorithm == "vault-transit-rsa-pkcs1v15-sha2-256-prehashed":
            if not self._verify_vault_signature(body, actual, key_id):
                with lock:
                    state["signature_failures"] += 1
                self._json(401, {"error": "invalid Vault Transit signature"})
                return
        else:
            signing_key = SIGNING_KEYS.get(key_id)
            if signing_key is None:
                with lock:
                    state["signature_failures"] += 1
                self._json(401, {"error": "invalid key id"})
                return
            if algorithm == "hmac-sha256":
                signed_payload = body
            elif algorithm in ("hmac-sha256-digest", "aws-kms-HMAC_SHA_256-digest"):
                signed_payload = hashlib.sha256(body).digest()
            else:
                with lock:
                    state["signature_failures"] += 1
                self._json(401, {"error": "unsupported signature algorithm"})
                return
            if algorithm == "aws-kms-HMAC_SHA_256-digest":
                expected = "base64=" + base64.b64encode(
                    hmac.new(signing_key, signed_payload, hashlib.sha256).digest()
                ).decode()
            else:
                expected = "sha256=" + hmac.new(
                    signing_key, signed_payload, hashlib.sha256
                ).hexdigest()
            if not hmac.compare_digest(expected, actual):
                with lock:
                    state["signature_failures"] += 1
                self._json(401, {"error": "invalid signature"})
                return

        previous_key_id = self.headers.get("X-TaskFlow-Audit-Previous-Key-ID", "")
        previous_signature = self.headers.get("X-TaskFlow-Audit-Previous-Signature", "")
        previous_valid = False
        if previous_key_id or previous_signature:
            previous_key = SIGNING_KEYS.get(previous_key_id)
            previous_algorithm = self.headers.get(
                "X-TaskFlow-Audit-Previous-Signature-Algorithm", "hmac-sha256"
            )
            if previous_key is None or not previous_signature:
                with lock:
                    state["signature_failures"] += 1
                self._json(401, {"error": "invalid previous key"})
                return
            previous_payload = (
                body if previous_algorithm == "hmac-sha256" else hashlib.sha256(body).digest()
            )
            previous_expected = "sha256=" + hmac.new(
                previous_key, previous_payload, hashlib.sha256
            ).hexdigest()
            if not hmac.compare_digest(previous_expected, previous_signature):
                with lock:
                    state["signature_failures"] += 1
                self._json(401, {"error": "invalid previous signature"})
                return
            previous_valid = True

        try:
            envelope = json.loads(body)
            events = envelope["events"]
            checkpoint = envelope["checkpoint"]
            sequences = [int(event["chain_sequence"]) for event in events]
            record_hashes = [str(event["record_hash"]) for event in events]
            first_sequence = int(checkpoint["batch_first_sequence"])
            last_sequence = int(checkpoint["batch_last_sequence"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            self._json(400, {"error": "invalid envelope"})
            return

        idempotency_key = self.headers.get("Idempotency-Key", "")
        payload_sha256 = hashlib.sha256(body).hexdigest()
        receipt_id = "receipt-" + hashlib.sha256(
            f"{idempotency_key}:{payload_sha256}".encode()
        ).hexdigest()[:24]
        accepted_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with lock:
            should_fail = state["fail_always"] or state["fail_remaining"] > 0
            if state["fail_remaining"] > 0:
                state["fail_remaining"] -= 1
            if should_fail:
                state["forced_failures"] += 1
        if should_fail:
            self._json(503, {"error": "forced failure"})
            return

        with lock:
            duplicate = idempotency_key in state["seen_idempotency_keys"]
            if duplicate:
                stored_payload = state["idempotency_payloads"].get(idempotency_key, "")
                if stored_payload and stored_payload != payload_sha256:
                    state["idempotency_conflicts"] += 1
                    self._json(409, {"error": "idempotency key reused with different payload"})
                    return
            if duplicate:
                state["duplicate_batches"] += 1
            else:
                state["seen_idempotency_keys"].append(idempotency_key)
                state["idempotency_payloads"][idempotency_key] = payload_sha256
                state["accepted_batches"] += 1
                state["accepted_events"] += len(events)
                state["accepted_sequences"].extend(sequences)
                for sequence, record_hash in zip(sequences, record_hashes):
                    state["receipt_ledger"][str(sequence)] = {
                        "chain_sequence": sequence,
                        "record_hash": record_hash,
                        "idempotency_key": idempotency_key,
                        "payload_sha256": payload_sha256,
                        "receipt_id": receipt_id,
                        "receiver_term": receiver_term,
                        "accepted_at": accepted_at,
                    }
            if duplicate and sequences:
                existing_receipt = state["receipt_ledger"].get(str(sequences[0]))
                if existing_receipt:
                    receipt_id = existing_receipt["receipt_id"]
            state["last_first_sequence"] = first_sequence
            state["last_last_sequence"] = last_sequence
            state["last_key_id"] = key_id
            state["last_algorithm"] = algorithm
            state["last_previous_key_id"] = previous_key_id
            state["previous_signature_valid"] = previous_valid
            state["last_idempotency_key"] = idempotency_key
            drop_after_accept = state["drop_after_accept_remaining"] > 0
            if drop_after_accept:
                state["drop_after_accept_remaining"] -= 1
        if drop_after_accept:
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.connection.close()
            return
        self._json(
            200,
            {
                "status": "duplicate" if duplicate else "accepted",
                "idempotency_key": idempotency_key,
                "first_sequence": first_sequence,
                "last_sequence": last_sequence,
                "payload_sha256": payload_sha256,
                "receipt_id": receipt_id,
                "receiver_id": RECEIVER_ID,
                "receiver_generation": RECEIVER_GENERATION,
                "receiver_term": receiver_term,
            },
        )


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
