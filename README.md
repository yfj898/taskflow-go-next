# TaskFlow-Go vNext

**面向研发变更的可信验证执行服务。** 接收注册项目的确定 Git commit，执行服务端固定检查，保存真实退出码、逐项报告和原始日志。Go SDK、CLI 与 HTTP 共用同一个运行契约。

与 [ChangePilot](https://github.com/yfj898/change-pilot) 配合时，工作台管理需求、版本化资料、AI 建议、人工确认和交接；TaskFlow 只记录执行事实。也可脱离工作台独立使用 API/CLI。

## 关键设计

- **确定输入**：完整 commit 与服务端固定的 `verify-change` 模板 revision；请求不能提交 shell、目录、仓库地址或凭据。
- **可恢复提交**：稳定 run UUID；首次受理 202，同输入重交 200/同一 Task，冲突 409。响应未知时查询或原样重交，不换 UUID 猜测失败。
- **可靠调度**：run、Task、Outbox 在同一事务创建；复用队列、租约和 generation fencing，旧 generation 不能覆盖新结果。
- **真实检查**：Linux local executor 将确定 commit 归档到独立目录；区分 passed、failed、incomplete，保留历史 attempt 与日志。
- **访问隔离**：项目授权先于幂等返回；验证任务不能经原公开通用 Task API 读取。
- **保留旧入口**：原任务、云资源与 taskflowctl 云操作契约保留。历史能力和运行手册见 [原版 README](README.legacy.md)。

## 从一次真实运行开始

1. [独立 Linux 启动与提交](docs/VERIFICATION_QUICKSTART.md)：新数据库、注册业务仓库、迁移、CLI 提交与查询。
2. [API / SDK / CLI 契约](docs/VERIFICATION_API.md)：权限、幂等、状态、generation 与日志。
3. [实现与已观察证据](docs/VERIFICATION_IMPLEMENTATION.md)：关键代码和实际故障恢复。
4. [Go/Vue 受控业务样例](https://github.com/yfj898/change-workspace-demo)：商品预售、SaaS 工单，以及明确标注的故障注入与修复。

```sh
# 按 quickstart 配置服务、身份和请求文件后
go build -o bin/taskflowctl ./cmd/taskflowctl
./bin/taskflowctl verification-submit request.json
./bin/taskflowctl verification-get <同一个-run-UUID>
```

## 已验证的结果

2026-10-19/20 隔离 Ubuntu/MySQL/Redis 环境：

| 场景 | 观察结果 |
| --- | --- |
| 商品 null 交付时间故障注入 | 前端 2/11 断言失败，run failed，原始失败报告保留 |
| revert 修复的确定 commit | 后端测试、前端测试、前端构建 3/3 必需检查通过 |
| 同 UUID 12 并发提交 | 一条 run、一条 Task、一条 Outbox |
| 真实 SIGKILL 后重启 | 同 run/task 失租后恢复到 generation 2；generation 1 保留 incomplete，generation 2 通过 |
| 原样重交、冲突和授权日志读取 | 同任务恢复、409 输入冲突与访问边界按契约生效 |

详见 [公开脱敏记录](docs/evidence/README.md)。这些是自建业务、隔离单机与受控故障结果，和原系统性能、Kind、OIDC 等历史证据分开记录。

## 检查与平台边界

```sh
go test -count=1 ./internal/domain ./internal/verification ./internal/executor ./internal/worker ./internal/service ./internal/repository ./internal/handler ./pkg/cloudclient ./cmd/taskflowctl
go vet ./internal/executor ./internal/worker ./internal/service ./internal/repository ./internal/handler ./pkg/cloudclient ./cmd/taskflowctl
```

MySQL 用例需要指向独立 `*_test` / `*_ci` 数据库的 `TASKFLOW_MYSQL_INTEGRATION_DSN`，未设置会跳过。新验证执行限 Linux local backend；Windows 支持源码构建、SDK 与 CLI。只注册可信代码，当前 runner **不是不可信多租户代码沙箱**。

自动 CI 聚焦 vNext 相关 Go 包与独立 MySQL。原全栈 CI 保留为手动工作流；镜像签名/推送须手动选择，不因源码上传自动发布镜像。

## 来源与职责

vNext 来自使用者已有 TaskFlow-Go 工作区的独立源码快照，不冒充原仓库完整 Git 历史。两份 `_source_*.json` 记录首批文件及补齐入口的来源差异；旧部署未参与本轮迁移。保留 Go module `github.com/yfj898/taskflow-go`，不因展示仓库名破坏现有 import。

业务规则在业务仓库，需求、知识引用与交接在 ChangePilot；TaskFlow 核心不添加商品、预售或工单字段。运行数据、凭据、模型、构建缓存与个人简历不属于本仓库。
