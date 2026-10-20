#!/usr/bin/env python3
"""Minimal AI gateway used by the integration smoke test."""

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
import time


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path not in ("/generate", "/v1/chat/completions"):
            self.send_response(404)
            self.end_headers()
            return

        fail_status = int(os.getenv("FAIL_STATUS", "0"))
        if fail_status:
            data = json.dumps({"error": "configured mock failure"}).encode()
            self.send_response(fail_status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return

        if os.getenv("REQUIRE_TRACEPARENT", "false").lower() == "true" and not self.headers.get("traceparent"):
            data = json.dumps({"error": "missing traceparent"}).encode()
            self.send_response(503)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return

        delay_ms = int(os.getenv("DELAY_MS", "0"))
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)

        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length))
        output_tokens = 3
        provider = os.getenv("PROVIDER_NAME", "mock-primary")
        model = os.getenv("MODEL_NAME", "mock-v1")
        result_prefix = os.getenv("RESULT_PREFIX", "mock-ai:")
        cost_usd = float(os.getenv("COST_USD", "0.0001"))

        if self.path == "/v1/chat/completions":
            prompt = body["messages"][-1]["content"]
            input_tokens = max(1, len(prompt.split()))
            data = json.dumps({
                "model": body.get("model", model),
                "choices": [{
                    "message": {
                        "role": "assistant",
                        "content": result_prefix + prompt,
                    }
                }],
                "usage": {
                    "prompt_tokens": input_tokens,
                    "completion_tokens": output_tokens,
                    "total_tokens": input_tokens + output_tokens,
                },
            }).encode()
            self._send_json(data)
            return

        prompt = body["prompt"]
        input_tokens = max(1, len(prompt.split()))
        data = json.dumps({
            "result": result_prefix + prompt,
            "provider": provider,
            "model": model,
            "usage": {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "total_tokens": input_tokens + output_tokens,
                "cost_usd": cost_usd,
            },
        }).encode()
        self._send_json(data)

    def _send_json(self, data):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", int(os.getenv("PORT", "9000"))), Handler).serve_forever()
