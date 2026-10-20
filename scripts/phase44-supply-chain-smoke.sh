#!/usr/bin/env sh
set -eu

IMAGE="${PHASE44_IMAGE:-taskflow-go:phase44}"
REVISION="phase44-local"
phase="initialization"

phase="build provenance-labelled non-root release image"
docker build -t "$IMAGE" \
  --build-arg GOPROXY=https://goproxy.cn,direct \
  --build-arg VERSION=phase44 \
  --build-arg REVISION="$REVISION" \
  --build-arg SOURCE=https://github.com/yfj898/taskflow-go . >/dev/null
[ "$(docker image inspect "$IMAGE" --format '{{.Config.User}}')" = "10001" ]
[ "$(docker image inspect "$IMAGE" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')" = "$REVISION" ]
digest="$(docker image inspect "$IMAGE" --format '{{.Id}}')"
case "$digest" in sha256:*) ;; *) echo "unexpected image digest $digest" >&2; exit 1;; esac

phase="generate SPDX SBOM for exact image"
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock anchore/syft:v1.33.0 "$IMAGE" -o spdx-json >/tmp/phase44-sbom.spdx.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase44-sbom.spdx.json'))
assert d.get('spdxVersion','').startswith('SPDX-'), d.get('spdxVersion')
assert len(d.get('packages',[])) > 0
PY

phase="gate fixed critical image vulnerabilities"
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v "$HOME/.cache/trivy:/root/.cache/" aquasec/trivy:0.67.2 image \
  --scanners vuln --severity CRITICAL --ignore-unfixed --exit-code 1 "$IMAGE" >/tmp/phase44-trivy.txt

phase="validate immutable digest deployment and keyless signing workflow"
grep -q 'digest: sha256:REPLACE_WITH_VERIFIED_RELEASE_DIGEST' deploy/k8s/kustomization.release.example.yaml
grep -q 'cosign sign --yes' .github/workflows/supply-chain.yml
grep -q 'id-token: write' .github/workflows/supply-chain.yml
grep -q 'actions/attest-build-provenance@v2' .github/workflows/supply-chain.yml

echo "Phase 44 SPDX SBOM and fixed-CRITICAL vulnerability gate: ok"
echo "Phase 44 non-root OCI provenance, digest pinning and GitHub OIDC keyless signing policy: ok"
