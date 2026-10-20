#!/usr/bin/env python3
"""Static guardrails for Phase 22 control-plane authentication."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"{path}: missing {needle!r}")


require("internal/config/config.go", 'getenvBool("CONTROL_PLANE_AUTH_ENABLED", false)')
require("internal/config/config.go", 'getenv("CONTROL_PLANE_BEARER_TOKEN", "")')
require("internal/config/config.go", "func (c Config) Validate() error")
require("cmd/server/main.go", "if err := cfg.Validate(); err != nil")
require("cmd/server/main.go", "handler.ControlPlaneAuthConfig{")
require("internal/handler/control_plane_auth.go", "subtle.ConstantTimeCompare")
require("internal/handler/control_plane_auth.go", 'c.Header("WWW-Authenticate", "Bearer")')
require("internal/handler/router.go", "controlPlaneBearerAuth(controlPlaneAuth),")
require("internal/handler/router.go", "controlPlaneAudit(controlPlaneAudits, controlPlaneAuditConfig),")
require("internal/handler/router.go", 'controlV1.DELETE("/queue/dead-letters/:id"')
require("internal/handler/router.go", 'controlV1.PUT("/ai/routing-policy"')
require("internal/handler/router.go", 'v1.POST("/tasks"')
require("docker-compose.yml", "CONTROL_PLANE_AUTH_ENABLED: ${CONTROL_PLANE_AUTH_ENABLED:-false}")
require("deploy/k8s/configmap.yaml", 'CONTROL_PLANE_AUTH_ENABLED: "true"')
require("deploy/k8s/secret.example.yaml", "CONTROL_PLANE_BEARER_TOKEN:")
require("scripts/release-k8s-keda-smoke.sh", "CONTROL_PLANE_BEARER_TOKEN='release-control-plane-token'")
require("Makefile", "phase22-control-plane-auth-smoke")
require("scripts/gate.sh", "phase22-control-plane-auth-check.py")
require("README.md", "Phase 22: control-plane authentication")

print("Phase 22 control-plane authentication guardrails: ok")
