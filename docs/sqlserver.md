# SQL Server 全量与 CDC

使用 `source_type: "sqlserver"` 选择新采集端；省略该字段仍为 PostgreSQL，旧 PG 配置和持久化指纹保持兼容。驱动为 Microsoft 官方 `github.com/microsoft/go-mssqldb`，不需要 wal2json。

代码以 SQL Server 2008/2008 R2 可用的 SQL 和 CDC 元数据为基线，但 **2008/2008 R2 尚未通过真实引擎验证，不应视为已完成生产兼容认证**。现代 SQL Server 的 Testcontainers 测试不能替代旧引擎验证，详见下文。

## 配置与启动

参考根目录 `config.sqlserver.example.json`，设置 `SQLSERVER_DSN` 和 `CDC_TOKEN`。DSN 示例（不要把实际密码提交到仓库）：

```text
sqlserver://collector:URL_ENCODED_PASSWORD@localhost:1433?database=your_db&encrypt=true
```

旧版 SQL Server 的 TLS 能力取决于补丁与证书配置。不要自动降级加密或关闭证书验证；测试夹具中的 `encrypt=disable` 只用于临时、隔离容器。驱动参数见 [Microsoft 驱动说明](https://github.com/microsoft/go-mssqldb#connection-parameters-and-dsn)。

```powershell
.\go-sync.exe check --config config.sqlserver.json
.\go-sync.exe run --config config.sqlserver.json
.\go-sync.exe status --config config.sqlserver.json
```

`check` 只读；不会启用 CDC、获取全量表锁或写标记。`run` 也不会创建表、安装触发器、调整数据库参数或启动 SQL Server Agent。

| sqlserver 配置 | 默认值 | 含义 |
|---|---|---|
| `allow_snapshot_locks` | `false` | 首次/重新全量前必须明确设为 true，表示已安排维护窗口 |
| `fence_table` | 必填 | 独立 CDC 标记表，不能出现在业务 `tables` 中 |
| `poll_interval` | `1s` | 没有新事务时的轮询间隔 |
| `query_timeout` | `5m` | 单次增量事务读取和本地暂存的超时 |
| `snapshot_timeout` | `1h` | 首次全量总超时，包含等锁与写本地队列 |
| `fence_timeout` | `2m` | 标记提交后等待 CDC 捕获的超时 |

普通重启从已有队列续传，不重新全量；已经封存快照后可把 `allow_snapshot_locks` 改回 false。SQL Server 与 PG 必须使用不同的 `source_id`、`data_dir`。SQL Server 不使用 `slot`。

## DBA 准备

以下是由 DBA 审核和执行的准备步骤，不是采集器自动执行的迁移：

1. 确认实际 SQL Server 版本/版本类别支持 CDC，并启用目标数据库 CDC。2008 Express 不能作为本功能的测试替代品。
2. 启用 `ALLOW_SNAPSHOT_ISOLATION ON`；仅启用 RCSI 不够。
3. 启用并运行 SQL Server Agent/CDC capture job；设置足够的 CDC 保留期，覆盖完整全量时间、故障时间和增量追赶时间。
4. 每张所选表恰好保留一个 CDC capture instance，捕获全部列。存在多个实例时本版拒绝猜选；本版不支持分区表。DBA 必须禁止对所有所选表执行 SWITCH 等不进入 CDC 的数据移动，包括非分区表之间的 SWITCH。
5. 准备专用标记表，例如 `dbo.go_sync_fence`，且只有一列 `token varchar(64) NOT NULL PRIMARY KEY`；同样启用 CDC，不分区。
6. 授予业务表 SELECT、CDC 元数据/所选 CT 表 SELECT、数据库身份查询所需可见性，以及标记表 INSERT。采集器直接读取 CT 以保留新版 `command_id`，仅有 CDC TVF 执行权限不够。不要为方便直接给生产采集账号 sysadmin。

CDC 启用参数的人工执行示例（业务表及标记表分别执行，名称替换成实际值）：

```sql
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'items',
    @role_name = NULL,
    @supports_net_changes = 0;
```

采集器会拒绝分区表，但不会替 DBA 管理 DDL 权限。尤其不能以 `@allow_partition_switch=0` 作为非分区表的安全保证：SQL Server 对非分区表忽略该设置，标志始终为 1。必须在权限和变更流程中禁止 SWITCH，否则 CDC 可能漏记数据移动。参考 [Microsoft CDC 启用文档](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-enable-table-transact-sql)。

标记表中每次全量尝试新增一个随机 token，正常增量不写标记。失败重试留下的标记用于排查，可由 DBA 定期清理过期记录；采集器不删除这张表或其中的历史行。

## 全量与增量衔接

```text
锁住所选业务表 → 独立提交标记 → 全量写入本地不可见队列
  → 提交读事务并释放表锁 → 等待 CDC 返回标记提交 LSN
  → 原子回填暂存消息 LSN 并封存快照
  → 从标记 LSN 之后采集 CDC → 完整事务落盘 → HTTP 投递
```

首次全量会在整个业务表读取期间持有所选表的 `TABLOCKX`，阻塞它们的写入，也可能阻塞普通读；不适合未经维护安排的大型在线库。已取得全部表锁后再提交标记，避免把滞后的 CDC 高水位误当作当前快照位点。业务行全部暂存后先提交读事务释放表锁，再等待 CDC 捕获标记，避免表锁阻塞 CDC 扫描。等待失败或进程崩溃只会留下不可见暂存数据，下次恢复会丢弃并重新全量；未封存快照不会推送。

增量每轮在一个 SQL Server SNAPSHOT 事务中验证全部实例的低水位、固定高水位，并读取同一提交 LSN 的全部表变更。这样清理任务在读取期间删除 CT 行时，不会出现逐表查询读到不一致窗口的问题。一次只暂存一个完整源事务，内存按批次使用；队列必须能容纳整个事务和结束标记。接收端处理逻辑无需另起一套。

## 协议与类型

沿用 [HTTP 协议](protocol.md)：`snapshot_begin`、`schema`、`snapshot_rows`、`snapshot_end`、`transaction_rows`、`transaction_end`；消息 ID、连续 seq、严格 ACK、幂等重试均不变。

- `lsn` 和 `transaction` 使用 `0x` 加 20 位十六进制字符串；接收端必须把它们视为源专属、不透明的位点，不能用 PG LSN 解析器处理。
- schema 保留 `schema/name/columns` 及列的 `name/type/not_null/default/primary_key`，增加 `source_type: "sqlserver"`、capture instance 身份、排序规则等；不会伪造 PG OID。
- 精确整数、decimal/numeric、money 在 SQL 内转换为文本，避免通过 float64。float/real 使用二进制值转换成可往返的十进制字符串。
- 字符保持 Unicode、空串和尾空格；SQL NULL 保持 JSON null；bit 使用 `true`/`false` 字符串。
- binary/varbinary/image/rowversion 使用 `0x` 十六进制字符串；SQL Server `timestamp` 是 rowversion，不是日期时间。
- 日期时间采用 SQL Server ISO 文本；无时区类型不附加时区，datetimeoffset 保留偏移。
- CDC 主键更新可能是一个 update，也可能是 delete + insert，两种形式均在一个事务内发送。
- 无主键表沿用 v2 的 `full_row` 身份及多重集合语义，不能把重复行合并成一条。未变化 MAX 列的旧值按 update mask 与新值重建；不把缺失旧值误解为 NULL。

本版拒绝计算列、未捕获列、source/CT 类型或排序规则不一致，以及未实现的类型（如 sql_variant、空间/CLR 类型）。无主键表另外拒绝 text/ntext/image/xml，因为不能保证完整旧身份。结构变化或 capture instance 替换时停止，尚不自动重建实例和适配新结构。

## 事务、恢复与限制

源 COMMIT 的变化按提交 LSN 归组，ROLLBACK 不会产生可应用变更。完整行数据和 `transaction_end` 都落盘、源读取成功结束后，才原子发布并推进本地位点。HTTP 失败只重试已发布的同一消息；服务器仍须持久化暂存、去重，在事务结束标记后原子应用。

本地队列丢失、数据库身份/恢复分支变化、CDC 清理导致位点过期时停止。不会把位点强行推进到新的最小 LSN。恢复需要人工确认新的全量和 generation，不能保留旧代次假装连续。

SQL Server 2008 没有新版 `__$command_id`。本版仅在字段存在时使用它，否则使用 seqval，并拒绝不配对/歧义的更新记录。但这不能修复旧引擎所有 deferred update 排序缺陷；必须用实际业务更新模式在 2008 上验收，不能据此宣称任意复杂事务都已兼容。参考 [Microsoft KB3030352](https://support.microsoft.com/help/3030352)。禁止人工修改 CT、CDC 元数据或通过禁用/重建 CDC 绕过检查。

## 监控

复用 `/metrics` 和公共快照、事务、HTTP、队列、重试指标。新增：

- `go_sync_sqlserver_cdc_retention_gaps_total`：已检测的保留期缺口，发生后采集停止。
- `go_sync_sqlserver_cdc_polls_total{result}` 与 `go_sync_sqlserver_cdc_poll_duration_seconds`：区分有进展、空闲、失败和取消，并统计轮询耗时。
- `go_sync_sqlserver_snapshot_*`：快照扫描进度、锁/fence 等待阶段及失败次数。扫描进度不代表快照已经发布。
- `go_sync_sqlserver_scan_age_seconds`、`capture_latency_seconds`、`checkpoint_lag_seconds`、`retention_margin_seconds`：只读健康采样；未知值为 NaN，并以 `go_sync_sqlserver_health_sample_timestamp_seconds` 的 0 表示当前不可用。

健康采样会读取 `sys.dm_cdc_log_scan_sessions`。SQL Server 2008 通常需要 `VIEW DATABASE STATE`；缺少权限时只增加健康采样错误并输出节流告警，不中断同步。`retention_margin_seconds` 是与当前低水位的时间距离，不是 CDC 清理任务还剩多久执行的预测。

SQL Server 不采样 PG WAL 字节指标。`capture_streaming=1` 或成功轮询不代表 CDC job 一定正在推进；还应监控源端 SQL Server Agent/capture job 与保留时间。

## 日志

`run` 的运行日志统一输出到 stderr，格式为 JSON，并固定携带 `source_type`、`source_id`。SQL Server 链路记录启动与停止、本地恢复及丢弃的未发布消息数、重连次数和退避时间、快照锁/fence/逐表暂存进度、每分钟 durable LSN 与队列摘要，以及 HTTP 重试和恢复。日志不会输出 DSN、密码、请求头、响应正文或业务行值；数据库驱动错误只对外保留安全错误分类。

快照逐表日志用 `table_index` 和总表数定位进度，不输出业务表名。CDC 健康采样权限不足的告警最多每五分钟一次，恢复后会输出恢复事件；磁盘容量告警最多每分钟一次并在恢复时提示。

## 测试 SQL Server 2008

默认集成测试仍统一由 Testcontainers 创建和清理数据库容器，没有手动 Docker 后门。默认现代引擎回归命令：

```sh
go test -tags=integration -run '^TestSQLServerSnapshotCDCRecovery$' -v ./internal/sqlserver -timeout 15m
```

2008 需要真实旧引擎，**设置现代 SQL Server 的 compatibility_level=100 并不是 2008 测试**。微软官方 Linux SQL Server 容器从 2017 系列起，不能直接用于 2008。[官方容器说明](https://learn.microsoft.com/en-us/sql/linux/quickstart-install-connect-docker)

真实 2008 R2 由专用 Hyper-V 验收环境覆盖，因为微软没有可由 Testcontainers 启动的 2008 容器镜像。该测试必须显式提供地址和由密钥管理器注入的密码；夹具创建随机命名的独立数据库，启用 snapshot isolation/CDC，并在测试结束时强制删除该数据库。未配置地址时仅该验收用例跳过，不影响默认 Testcontainers 回归：

```sh
GO_SYNC_TEST_MSSQL_EXTERNAL_ADDR=10.10.0.101:21433 \
GO_SYNC_TEST_MSSQL_USER=sa \
GO_SYNC_TEST_MSSQL_PASSWORD='injected-secret' \
go test -tags=integration -count=1 \
  -run '^TestSQLServer2008R2SnapshotCDCRecovery$' -v ./internal/sqlserver -timeout 15m
```

验收用例会查询真实 `ProductVersion` 和 `Edition`，只接受主版本 10 的 Enterprise，并等待 SQL Server Agent 可用。密码不得写入仓库、命令历史或测试日志。

若继续坚持旧引擎也由 Testcontainers 管理，需要准备合法授权、可运行的自定义 Windows 容器镜像，而不是把 2008 Windows 二进制塞入 Linux SQL Server 镜像。镜像需启动真实引擎、启用 Agent、按测试夹具约定接受测试密码并暴露 1433；宿主机/虚拟化设备要求还需按镜像单独配置。

镜像就绪后，夹具支持指定镜像并验证实际引擎主版本，版本不符直接失败：

```sh
GO_SYNC_TEST_MSSQL_IMAGE=your-registry/sqlserver-2008-test:your-tag \
GO_SYNC_TEST_MSSQL_MAJOR=10 \
go test -tags=integration -run '^TestSQLServerSnapshotCDCRecovery$' -v ./internal/sqlserver -timeout 15m
```

日志输出真实 ProductVersion，2008 的 `10.0.*` 与 2008 R2 的 `10.50.*` 应分别建立验收记录。容器由 CleanupContainer/Ryuk 清理；测试队列使用 t.TempDir，不连接生产库。

2008 额外验收重点：SQL/TDS/TLS 连接、空表/大表全量、边界处并发写入、同事务多表多次更新、反复修改主键、deferred updates、删除再插入、无主键重复行、MAX/legacy LOB、回滚/保存点、断线、Agent 停止、CDC 清理跨过位点，以及 32/64 位采集器。没有对应环境时明确记录未验证，不静默跳过后报成功。
