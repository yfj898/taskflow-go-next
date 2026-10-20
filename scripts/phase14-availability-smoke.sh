#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

python3 scripts/phase14-k8s-check.py

# Phase 14 is Kubernetes-policy-only, so Docker Compose cannot honestly emulate
# the eviction API. Re-run the Phase 13 termination-budget checker because the
# PDB/rollout policy is useful only if an evicted Pod still has time to drain.
python3 scripts/phase13-k8s-check.py

echo "phase14 disruption/rollout availability policy: ok"
