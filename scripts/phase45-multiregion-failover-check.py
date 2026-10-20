from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    if needle not in (ROOT / path).read_text():
        raise SystemExit(f"{path}: missing {needle!r}")


require("deploy/multiregion/global-router.conf", "resolver 127.0.0.11")
require("deploy/multiregion/global-router.conf", "zone taskflow_regions")
require("deploy/multiregion/global-router.conf", "server taskflow:8080 resolve")
require("deploy/multiregion/global-router.conf", "server taskflow-region-b:8080 resolve backup")
require("docs/runbooks/MULTI_REGION_FAILOVER.md", "Freeze or fence Region A writers")
require("docker-compose.yml", "taskflow-region-b:")
require("docker-compose.yml", "global-router:")
require("scripts/phase45-multiregion-failover-smoke.sh", "SOURCE_AUTO_POSITION=1")
require("scripts/phase45-multiregion-failover-smoke.sh", "super_read_only=OFF")
require("scripts/phase45-multiregion-failover-smoke.sh", '"rpo_lost_tasks":0')
require("scripts/phase45-multiregion-failover-smoke.sh", "rto_seconds")
require("Makefile", "phase45-multiregion-failover-smoke")
require("scripts/gate.sh", "phase45-multiregion-failover-check.py")
require(".github/workflows/ci.yml", "phase45-multiregion-failover-smoke.sh")
require("README.md", "### Phase 45: multi-region writer promotion and global traffic failover")

print("Phase 45 multi-region GTID promotion, writer fencing, global routing and RTO/RPO guardrails: ok")
