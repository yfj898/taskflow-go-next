# TaskFlow-Go vNext：提交一次可信研发验证

本开发版已经提供独立的验证 API、Go SDK、CLI 和 Linux local Executor。它接收注册项目的完整 Git commit，按服务端固定检查模板执行，保存逐项结果、日志和历史 generation。原资源 API/CLI 保持可用；ChangePilot 是可选调用方。

首次独立克隆从 [Linux quickstart](docs/VERIFICATION_QUICKSTART.md) 启动。下文 `18089` 和数据位置属于原隔离验收环境，不要求新用户复用。已有服务时不要再启动一个抢占同端口/队列的进程。

## 通过 CLI 提交与查询

Windows CLI 使用现有 `TASKFLOW_ACCESS_TOKEN` 环境变量。使用运行环境已经提供的凭据，不把实际 Token 写进请求文件、仓库或日志。Unix 也支持模式 0600 的 `-token-file`；Windows 未实现 ACL 校验，使用该参数会明确报错，不能绕过文件权限判断。

```powershell
# 在本仓库根目录执行
$env:TASKFLOW_ENDPOINT = 'http://127.0.0.1:18089'
New-Item -ItemType Directory -Force -Path tmp | Out-Null
$request = [ordered]@{
  run_id = [guid]::NewGuid().ToString()
  project_id = 'change-workspace-demo'
  target_commit = '1750bd795b9a002fb4b0c2aa6a64946070796aa1'
  check_profile_id = 'verify-change'
  check_profile_revision = '1'
  check_ids = @('backend-test', 'frontend-test', 'frontend-build')
  external_snapshot_id = 'standalone-local-verification'
}
$request | ConvertTo-Json | Set-Content -Encoding utf8NoBOM tmp/verification-request.json
.\bin\taskflowctl.exe verification-submit tmp/verification-request.json
.\bin\taskflowctl.exe verification-get $request.run_id
```

首次受理返回持久化的 run，初始状态可能仍为 pending。响应不明时保留同一个请求文件/UUID，通过 `verification-get` 或再次提交原文件恢复关联，不能重新生成 UUID 猜测失败。重跑一轮已经结束的检查才创建新 UUID。

首个模板按顺序执行 `backend-test`、`frontend-test`、`frontend-build`，三项都必需。调用方不能提供代码目录、命令、仓库 URL 或凭据。报告和日志读取仍须项目授权；旧 `/api/v1/tasks` 入口不能读出验证任务。

Go 调用复用 `pkg/cloudclient.Client`，新增方法是 `SubmitVerification`、`GetVerificationRun`、`GetVerificationArtifact`。请求/响应、权限、幂等、日志下载及服务端注册配置见 [API 使用说明](docs/VERIFICATION_API.md)。

## 已观察到的真实结果

集成环境是 Ubuntu-24.04、local backend、独立 MySQL/Redis；代码使用自有游戏商城/SaaS 样例，不接入企业生产系统。

- 故障注入 commit `7aca2e8ec80fd4796f7ff8780960bab615340bb5`：run `b35d63d6-be40-4d94-94de-3393632cf722`，前端 11 项测试中 2 项断言失败，Task failed，失败记录和原始日志保留。
- 修复 commit `1750bd795b9a002fb4b0c2aa6a64946070796aa1`：run `28b49b55-8da2-4665-861a-c011875538a3`，3/3 检查通过，Task success。
- 实际 CLI 提交/读取、相同 UUID 返回相同 task、输入冲突 409、缺必需检查 400、无身份 401、公开通用详情 404、授权日志下载均通过。
- 实际 SIGKILL 后重启：同一 run `316a368c-4d29-4abe-b044-0b9eff318f6d` / task 3 自然失租后进入 generation 2；generation 1 保留 incomplete，generation 2 的 3/3 检查通过，最终 success。
- 独立 MySQL 测试验证 12 并发同 UUID 只创建一个 run/Task/Outbox、generation fencing 和恢复；Linux 真命令测试验证固定 commit、不改脏工作区、日志/环境边界。已修复真实 Git PAX 元数据兼容故障及停机与终态结算竞态。

集成结果与六项日志在 [真实回放证据](docs/evidence/taskflow-replay-results.json)，进程中断前后完整记录在 [真实恢复证据](docs/evidence/taskflow-abrupt-recovery.json)。测试与个人改动范围见 [实现说明](docs/VERIFICATION_IMPLEMENTATION.md)。

## 本地构建与相关检查

```powershell
$env:GOTOOLCHAIN = 'local'
$env:GOWORK = 'off'
$env:GOCACHE = Join-Path $PWD 'tmp\go-build-cache'
$env:GOMODCACHE = Join-Path $PWD 'tmp\gomod-cache'
go test -count=1 ./internal/domain ./internal/verification ./internal/executor ./internal/worker ./internal/service ./internal/repository ./internal/handler ./pkg/cloudclient ./cmd/taskflowctl
go vet ./internal/executor ./internal/worker ./internal/service ./internal/repository ./internal/handler ./pkg/cloudclient ./cmd/taskflowctl
go build -o .\bin\taskflowctl.exe ./cmd/taskflowctl
go build -o .\bin\taskflow-server.exe ./cmd/server
go build -o .\bin\taskflow-worker.exe ./cmd/executor-worker
```

已在 Windows Go 1.26.4 完成受影响包 test/vet、server/worker/CLI 构建和帮助检查；`go.mod` 的原始版本与推荐 toolchain 保持。缓存只在命令进程内指定，不改全局 Go 配置。缺失依赖下载最初受沙箱限制，获准网络后完成。

Windows 支持 SDK/CLI 和服务源码构建；**新验证执行只在 Linux local backend 启用**。MySQL 集成测试必须显式使用独立 `*_test`/`*_ci` 数据库；Windows 跳过的 Linux 用例已有单独的 Linux 实测记录。Keycloak、Kind、GPU/跨地域等原系统集成没有作为本次验收执行。

## 开发版、来源与保留边界

原版仍在 `@1`：`/media/bili-guo/1235578e-e896-4ce5-9fdc-6318e4960f4c3/any/taskflow-go`，本轮没有修改该旧版。

- `_source_snapshot.json` 保留首次复制的 388 个非忽略文件、2,771,312 字节清单和源工作区 `main` / `ef1d68176f07dc9285d633f1f93549e6dbfb8b4a` 背景。
- `_source_supplement_20261019.json` 保留补齐的 6 个 Go 入口源码。它们原本被同名二进制忽略规则误匹配且未纳入 Git 跟踪，来自源文件系统，不宣称已包含在上述 commit。对应 `.gitignore` 规则已经根锚定。
- Windows 原有修复保留：超时测试使用已过期 Deadline；Token 文件处理保留 Unix 权限要求并明确 Windows 限制。
- 运行数据、真实凭据、原 `.git` 历史未复制。当前 vNext 的本地实施快照可以独立版本化，但不能伪造成原版完整历史；保留两份来源清单。
- `bin/`、`tmp/`、`artifacts/`、`local-results/`、环境文件、日志和私密文件均不进入源码快照。

## 项目职责

TaskFlow 核心不包含预售、商品或工单优先级字段。新业务包引用注册项目/检查模板；业务规则和人工交接由 ChangePilot 负责。固定命令非零退出记录 failed，启动不了或中断记录 incomplete；日志保留具体阶段原因，依赖准备失败不能作为业务断言失败的演示证据。

跨项目职责见 [项目首页](README.md)，当前可执行入口以 [独立启动说明](docs/VERIFICATION_QUICKSTART.md) 为准。
