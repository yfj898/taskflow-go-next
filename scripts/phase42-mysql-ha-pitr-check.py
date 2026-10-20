from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    if needle not in (ROOT / path).read_text():
        raise SystemExit(f"{path}: missing {needle!r}")


require("internal/repository/mysql.go", "NewMySQLWithFailover")
require("internal/repository/mysql.go", "PingContext")
require("internal/config/config.go", "MYSQL_FAILOVER_DSNS")
require("cmd/server/main.go", "mysql writer startup candidate selected=%d")
require("docker-compose.yml", "mysql-replica:")
require("docker-compose.yml", "--gtid-mode=ON")
require("deploy/k8s/secret.example.yaml", "MYSQL_FAILOVER_DSNS")
require("scripts/phase42-mysql-ha-pitr-smoke.sh", "SOURCE_AUTO_POSITION=1")
require("docker-compose.yml", "mysql-recovery:")
require("scripts/phase42-mysql-ha-pitr-smoke.sh", "SOURCE_LOG_FILE")
require("scripts/phase42-mysql-ha-pitr-smoke.sh", "SOURCE_LOG_POS")
require("scripts/phase42-mysql-ha-pitr-smoke.sh", "sha256sum -c")
require("Makefile", "phase42-mysql-ha-pitr-smoke")
require("scripts/gate.sh", "phase42-mysql-ha-pitr-check.py")
require(".github/workflows/ci.yml", "phase42-mysql-ha-pitr-smoke.sh")
require("README.md", "### Phase 42: MySQL HA startup failover and PITR drill")

print("Phase 42 MySQL GTID replica, startup writer failover and PITR guardrails: ok")
