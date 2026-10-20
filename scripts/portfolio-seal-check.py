from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"{path}: missing {needle!r}")


for doc in (
    "docs/ARCHITECTURE.md",
    "docs/FULL_PROJECT_GUIDE_ZH.md",
    "docs/PORTFOLIO.md",
    "docs/INTERVIEW_GUIDE.md",
    "docs/PORTFOLIO_ZH.md",
    "docs/INTERVIEW_GUIDE_ZH.md",
):
    if not (ROOT / doc).is_file():
        raise SystemExit(f"missing portfolio document: {doc}")

require("README.md", "## Portfolio summary")
require("README.md", "docs/ARCHITECTURE.md")
require("README.md", "docs/FULL_PROJECT_GUIDE_ZH.md")
require("README.md", "docs/PORTFOLIO.md")
require("README.md", "docs/INTERVIEW_GUIDE.md")
require("README.md", "docs/PORTFOLIO_ZH.md")
require("README.md", "docs/INTERVIEW_GUIDE_ZH.md")
require("README.md", "5,781.98 RPS")
require("README.md", "RPO lost tasks = 0")

require("docs/ARCHITECTURE.md", "Transactional Outbox")
require("docs/ARCHITECTURE.md", "GTID")
require("docs/ARCHITECTURE.md", "rpo_lost_tasks = 0")
require("docs/ARCHITECTURE.md", "not a production SLA")

require("docs/FULL_PROJECT_GUIDE_ZH.md", "# TaskFlow-Go 完整项目链路学习手册")
require("docs/FULL_PROJECT_GUIDE_ZH.md", "CreateWithOutbox")
require("docs/FULL_PROJECT_GUIDE_ZH.md", "Dispatcher.publishClaimed")
require("docs/FULL_PROJECT_GUIDE_ZH.md", "Pool.execute")
require("docs/FULL_PROJECT_GUIDE_ZH.md", "AIProviderRouter.ExecuteDetailed")
require("docs/FULL_PROJECT_GUIDE_ZH.md", "rpo_lost_tasks = 0")

require("docs/PORTFOLIO.md", "Resume-ready version — Go backend / cloud-native")
require("docs/PORTFOLIO.md", "Resume-ready version — AI application engineer")
require("docs/PORTFOLIO.md", "5,781.98 RPS")
require("docs/PORTFOLIO.md", "51 s RTO")
require("docs/PORTFOLIO.md", "Claims to avoid")

require("docs/INTERVIEW_GUIDE.md", "60-second answer")
require("docs/INTERVIEW_GUIDE.md", "Why Transactional Outbox")
require("docs/INTERVIEW_GUIDE.md", "Five-minute demo flow")
require("docs/INTERVIEW_GUIDE.md", "5,781.98 RPS")

require("docs/PORTFOLIO_ZH.md", "简历版：Go 后端 / 云原生")
require("docs/PORTFOLIO_ZH.md", "简历版：AI 应用工程师")
require("docs/PORTFOLIO_ZH.md", "5,781.98 RPS")
require("docs/PORTFOLIO_ZH.md", "RPO=0")

require("docs/INTERVIEW_GUIDE_ZH.md", "1 分钟项目介绍")
require("docs/INTERVIEW_GUIDE_ZH.md", "为什么不用 MQ 的 exactly-once")
require("docs/INTERVIEW_GUIDE_ZH.md", "5 分钟 Demo 顺序")
require("docs/INTERVIEW_GUIDE_ZH.md", "RTO = 51 s")

print("Portfolio README, architecture, resume summary and interview guide guardrails: ok")
