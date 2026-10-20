from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    if needle not in (ROOT / path).read_text():
        raise SystemExit(f"{path}: missing {needle!r}")


require("prometheus.yml", "rule_files:")
require("prometheus.yml", "alertmanager:9093")
require("monitoring/alerts.yml", "taskflow:slo:http_availability:ratio5m")
require("monitoring/alerts.yml", "TaskFlowHTTPAvailabilityBurnRateFast")
require("monitoring/alerts.yml", "TaskFlowQueueStatsStale")
require("monitoring/alerts.yml", "TaskFlowGovernanceDebtFenced")
require("monitoring/alerts.yml", "taskflow:slo:cloud_saga_provider_error_ratio5m")
require("monitoring/alerts.yml", "TaskFlowCloudSagaBacklogStuck")
require("monitoring/alerts.yml", "TaskFlowCloudSagaProviderErrors")
require("monitoring/alertmanager.yml", "repeat_interval")
require("monitoring/grafana/taskflow-overview.json", "taskflow-production-overview")
require("monitoring/grafana/taskflow-overview.json", "taskflow_cloud_saga_runnable_workflows")
require("monitoring/grafana/taskflow-overview.json", "taskflow_cloud_saga_workflow_claims_total")
require("monitoring/grafana/taskflow-overview.json", "taskflow_cloud_saga_step_operations_total")
require("docs/runbooks/CONTROL_PLANE_SLO.md", "## Recovery exit criteria")
require("docs/runbooks/CONTROL_PLANE_SLO.md", "## Cloud Saga")
require("docker-compose.yml", "alertmanager:")
require("docker-compose.yml", "grafana:")
require("scripts/phase43-slo-observability-smoke.sh", "promtool")
require("Makefile", "phase43-slo-observability-smoke")
require("scripts/gate.sh", "phase43-slo-observability-check.py")
require(".github/workflows/ci.yml", "phase43-slo-observability-smoke.sh")
require("README.md", "### Phase 43: SLO dashboards, alerts and runbooks")

print("Phase 43 SLO recording rules, Alertmanager, Grafana and runbook guardrails: ok")
