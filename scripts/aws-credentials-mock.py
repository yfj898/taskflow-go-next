#!/usr/bin/env python3
import json
import os
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import parse as urllib_parse


ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "phase28-temp-access")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "phase28-temp-secret")
SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN", "phase28-session-token")
ROLE_ARN = os.getenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/taskflow-phase28")
ROLE_NAME = os.getenv("AWS_ROLE_NAME", "taskflow-phase28")
WEB_IDENTITY_TOKEN = os.getenv("AWS_WEB_IDENTITY_TOKEN", "phase28-web-identity-token")
IMDS_TOKEN = os.getenv("AWS_IMDS_TOKEN", "phase28-imdsv2-token")

lock = threading.Lock()
state = {
    "sts_requests": 0,
    "sts_failures": 0,
    "imds_token_requests": 0,
    "imds_role_requests": 0,
    "imds_credential_requests": 0,
    "imds_failures": 0,
}


def expiration():
    return (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat().replace("+00:00", "Z")


class Handler(BaseHTTPRequestHandler):
    server_version = "TaskFlowAWSCredentialsMock/1.0"

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
        if self.path == "/latest/meta-data/iam/security-credentials/":
            with lock:
                state["imds_role_requests"] += 1
            if self.headers.get("X-aws-ec2-metadata-token") != IMDS_TOKEN:
                with lock:
                    state["imds_failures"] += 1
                self._json(401, {"error": "invalid IMDSv2 token"})
                return
            body = (ROLE_NAME + "\n").encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path == f"/latest/meta-data/iam/security-credentials/{ROLE_NAME}":
            with lock:
                state["imds_credential_requests"] += 1
            if self.headers.get("X-aws-ec2-metadata-token") != IMDS_TOKEN:
                with lock:
                    state["imds_failures"] += 1
                self._json(401, {"error": "invalid IMDSv2 token"})
                return
            self._json(
                200,
                {
                    "Code": "Success",
                    "AccessKeyId": ACCESS_KEY,
                    "SecretAccessKey": SECRET_KEY,
                    "Token": SESSION_TOKEN,
                    "Expiration": expiration(),
                },
            )
            return
        self._json(404, {"error": "not found"})

    def do_PUT(self):
        if self.path != "/latest/api/token":
            self._json(404, {"error": "not found"})
            return
        with lock:
            state["imds_token_requests"] += 1
        ttl = self.headers.get("X-aws-ec2-metadata-token-ttl-seconds", "")
        if not ttl.isdigit() or int(ttl) <= 0:
            with lock:
                state["imds_failures"] += 1
            self._json(400, {"error": "invalid token ttl"})
            return
        body = IMDS_TOKEN.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.path not in ("/", "/sts"):
            self._json(404, {"error": "not found"})
            return
        with lock:
            state["sts_requests"] += 1
        length = int(self.headers.get("Content-Length", "0"))
        form = urllib_parse.parse_qs(self.rfile.read(length).decode())
        valid = (
            form.get("Action", [""])[0] == "AssumeRoleWithWebIdentity"
            and form.get("Version", [""])[0] == "2011-06-15"
            and form.get("RoleArn", [""])[0] == ROLE_ARN
            and form.get("WebIdentityToken", [""])[0] == WEB_IDENTITY_TOKEN
            and bool(form.get("RoleSessionName", [""])[0])
        )
        if not valid:
            with lock:
                state["sts_failures"] += 1
            self._json(400, {"error": "invalid AssumeRoleWithWebIdentity request"})
            return
        expires = expiration()
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>{ACCESS_KEY}</AccessKeyId>
      <SecretAccessKey>{SECRET_KEY}</SecretAccessKey>
      <SessionToken>{SESSION_TOKEN}</SessionToken>
      <Expiration>{expires}</Expiration>
    </Credentials>
    <SubjectFromWebIdentityToken>phase28-subject</SubjectFromWebIdentityToken>
    <Audience>sts.amazonaws.com</Audience>
    <AssumedRoleUser>
      <Arn>{ROLE_ARN.replace(':role/', ':assumed-role/')}/taskflow-phase28</Arn>
      <AssumedRoleId>AROAPHASE28:taskflow-phase28</AssumedRoleId>
    </AssumedRoleUser>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>""".encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/xml")
        self.send_header("Content-Length", str(len(xml)))
        self.end_headers()
        self.wfile.write(xml)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "9703"))
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
