from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"{path}: missing {needle!r}")


require("internal/handler/control_plane_oidc.go", "rsa.VerifyPKCS1v15")
require("internal/handler/control_plane_oidc.go", "JWT issuer mismatch")
require("internal/handler/control_plane_oidc.go", "JWT audience mismatch")
require("internal/handler/control_plane_oidc.go", "OIDC JWKS does not contain kid")
require("internal/handler/control_plane_oidc.go", "controlPlaneOIDCRBAC")
require("internal/handler/control_plane_oidc.go", "incident-commander")
require("internal/handler/router.go", "controlPlaneOIDCRBAC(controlPlaneAuth)")
require("internal/config/config.go", "CONTROL_PLANE_AUTH_MODE")
require("internal/config/config.go", "CONTROL_PLANE_OIDC_JWKS_URL")
require("internal/config/config.go", "CONTROL_PLANE_OIDC_ROLE_MAPPINGS")
require("docker-compose.yml", "oidc-mock:")
require("deploy/k8s/configmap.yaml", "CONTROL_PLANE_OIDC_RBAC_ENABLED")
require("scripts/phase41-oidc-rbac-smoke.sh", "JWKS kid rotation")
require("Makefile", "phase41-oidc-rbac-smoke")
require("scripts/gate.sh", "phase41-oidc-rbac-check.py")
require(".github/workflows/ci.yml", "phase41-oidc-rbac-smoke.sh")
require("README.md", "### Phase 41: OIDC/JWT identity and group RBAC")

print("Phase 41 OIDC/JWT identity, JWKS rotation and group RBAC guardrails: ok")
