from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    if needle not in (ROOT / path).read_text():
        raise SystemExit(f"{path}: missing {needle!r}")


require("Dockerfile", "org.opencontainers.image.revision")
require("Dockerfile", "USER 10001")
require("security/SUPPLY_CHAIN_POLICY.md", "immutable digest")
require("deploy/k8s/kustomization.release.example.yaml", "digest: sha256:REPLACE_WITH_VERIFIED_RELEASE_DIGEST")
require(".github/workflows/supply-chain.yml", "id-token: write")
require(".github/workflows/supply-chain.yml", "cosign sign --yes")
require(".github/workflows/supply-chain.yml", "actions/attest-build-provenance@v2")
require(".github/workflows/supply-chain.yml", "aquasecurity/trivy-action")
require("scripts/phase44-supply-chain-smoke.sh", "spdx-json")
require("scripts/phase44-supply-chain-smoke.sh", "--severity CRITICAL")
require("Makefile", "phase44-supply-chain-smoke")
require("scripts/gate.sh", "phase44-supply-chain-check.py")
require("README.md", "### Phase 44: signed and attestable software supply chain")

print("Phase 44 SBOM, vulnerability gate, provenance, digest pinning and keyless signing guardrails: ok")
