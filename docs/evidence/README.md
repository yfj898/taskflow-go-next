# 受控 Linux 验证记录

2026-10-19，Ubuntu-24.04，TaskFlow local executor，独立 MySQL/Redis，自建 Go/Vue 商品与工单业务。以下是实际 HTTP/CLI 服务运行结果，不是模拟生成的报告，也不代表生产事故或企业采用。

- [失败与修复响应](taskflow-replay-results.json)：故障注入 commit `7aca2e8ec80fd4796f7ff8780960bab615340bb5` 对应 run `b35d63d6-be40-4d94-94de-3393632cf722`；修复 commit `1750bd795b9a002fb4b0c2aa6a64946070796aa1` 对应 run `28b49b55-8da2-4665-861a-c011875538a3`。
- [SIGKILL 前后恢复](taskflow-abrupt-recovery.json)：同 run/task 从 generation 1 incomplete 恢复为 generation 2 passed。
- 六份 `taskflow-*-generation1-*.log` 是两轮的实际检查输出。仅将本机工作区前缀替换为 `<WORKSPACE>`，保留断言、退出行为与时间记录。

故障轮前端 2/11 断言失败，修复轮 3/3 必需检查通过。12 并发同 UUID 的数据库用例、授权和 generation fencing 的源码/运行说明见 [实现说明](../VERIFICATION_IMPLEMENTATION.md)。

业务代码及三个 fixture commit 位于 [change-workspace-demo](https://github.com/yfj898/change-workspace-demo)。公开记录不包含服务令牌、运行数据库、登录会话或个人简历。
