# 验证记录

验证日期：2026-09-06。环境：Linux amd64、Go 1.25.0、Docker；wal2json 2.6。

## 已执行

- `go test -race ./...`：通过。
- `go vet ./...`：通过。
- 行解码模糊测试 5 秒：通过，约 57,000 次输入。
- Linux amd64 构建与命令帮助检查：通过。
- Windows amd64 交叉编译：通过；未在 Windows 上进行运行/断电验证。
- `JOBS=3 bash scripts/test-matrix.sh`：最终完整运行退出码 0，以下全部版本通过。

| PostgreSQL | 结果 |
|---|---|
| 9.4.26 | PASS |
| 9.5.25 | PASS |
| 9.6.24 | PASS |
| 10.23 | PASS |
| 11.22 | PASS |
| 12.22 | PASS |
| 13.23 | PASS |
| 14.24 | PASS |
| 15.19 | PASS |
| 16.15 | PASS |
| 17.11 | PASS |
| 18.6 | PASS |

## 故障与数据场景

版本矩阵执行真实源库上的全量和增量测试：联合主键、主键修改、删除、高精度 numeric、NaN、bytea、NULL、boolean、jsonb、未变化的外置 TOAST、事务回滚、HTTP 接收成功但响应丢失、采集停止期间写入、复制连接被终止、PostgreSQL 容器被 KILL 后重新启动，以及结构漂移时停止。

另一项真实数据库测试验证：容量不足的未完成全量不能被读取/投递，扩大容量后会重建初始化代次，快照边界之后并发发生的修改不进入旧快照且会出现在后续增量。

本地队列测试使用子进程不执行 Close 直接退出，分别验证暂存未发布、已发布未 ACK、已 ACK 的恢复状态。HTTP 测试验证持久 ACK 匹配、错误 ACK、重定向、失败状态及请求重试身份不变。app 和 delivery 测试检查 goroutine 泄漏。

## Prometheus 增量验证

加入官方 `client_golang v1.24.1` 后重新执行全部单元/竞态测试、vet、Linux/Windows 构建及上述 12 个 PostgreSQL 版本矩阵，全部通过。`go mod verify` 通过。

使用 Prometheus 3.5.0 的 `promtool check config` 验证抓取配置和告警示例，通过，包含 6 条规则。

新增测试覆盖独立 registry、Counter 重建归零、队列 Gauge 持久恢复、未提交行不计入已提交计数、已持久化重放不重复计数、HTTP 重试/错误 ACK 不增加确认投递量、指标标签与隐私、官方指标 lint、并发采样、OpenMetrics 协商、抓取失败、接口路径限制、端口占用先于采集报错、退出关闭监听，以及 telemetry goroutine 泄漏检查。真实 PG 测试启用 `/metrics` 并验证快照/schema 计数、stream 状态和采集器重启后的指标。

安全扫描 `go run golang.org/x/vuln/cmd/govulncheck@latest ./...` **未通过**：当前 Go 1.25.0 标准库与既有 pgx 5.9.1 共报告 29 个可达漏洞。扫描提示部分标准库修复需要 Go 1.25.13，pgx 的 [GO-2026-5004](https://pkg.go.dev/vuln/GO-2026-5004) 修复版本为 5.9.2。这里只记录扫描结果，未自动升级全局工具链或顺带更新既有数据库驱动；部署前应升级到适用的安全补丁版本并重新跑扫描和兼容矩阵。功能测试通过不代表安全扫描通过。

## 保证边界

这些测试不等于模拟存储设备永久丢失、所有文件系统的断电行为或数据库 HA 切换；上述场景不在首版保证范围内。尚未对用户提供的编码 SQL 示例进行结构适配，也未运行针对真实业务规模的吞吐基准。schema 功能限于初始化元数据发送与运行时漂移保护，未实现实时 DDL 自动复制。
