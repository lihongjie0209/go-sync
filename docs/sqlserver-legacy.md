# SQL Server 2000 / 2008 R2 Express 全量与增量

使用 `source_type: "sqlserver_legacy"`。SQL Server 2000 和不提供 CDC 的 SQL Server 2008 R2 Express 使用本模式：它自动创建自有 Outbox 表及每张目标表的 AFTER DML 触发器。`check` 始终只读；`run` 仅在 `sqlserver_legacy.auto_install=true` 时安装缺少的对象。

## 一致性与恢复

- 业务 DML 与 Outbox 写入处于同一源事务，ROLLBACK 不留下事件。
- 采集器不使用 `IDENTITY` 最大值过滤事件，因为并发事务可以乱序提交。它总是读取当前仍存在且已经提交的最小事件。
- 每个事件完整落入本地队列并发布后，才按精确 `change_id` 删除源 Outbox 行。这个顺序的故障窗口只会重复，不会漏数。
- 同一个 `source_id/prefix` 使用 `sp_getapplock` 排他租约，拒绝两个采集器同时消费。
- SQL Server 2000 无法自动判断跨多条语句的原始事务提交边界。本模式把一次已提交 DML 语句的全部行镜像作为一个协议事务；UPDATE 在同一协议事务内发送 DELETE 与 INSERT 行镜像。它保持语句级原子发布、至少一次和回滚过滤，但不承诺把源端多语句事务合并成一个 HTTP 事务。并发事务的 IDENTITY 分配顺序不等于提交顺序，协议 `lsn` 只保持单调用于恢复观测，不代表精确提交次序。

## 自动安装对象

默认前缀 `go_sync_legacy`，默认 owner 为 `dbo`：

- `go_sync_legacy_control`：安装版本和随机源身份。
- `go_sync_legacy_events`：提交后可见的事件头。
- `go_sync_legacy_values`：完整普通列值；内部使用 SQL Server 2000 可可靠读取的定长上限类型，不依赖旧 LOB 元数据。
- `go_sync_legacy_tr_<object_id>`：各目标表的 INSERT/UPDATE/DELETE 触发器。

同名触发器只有包含本项目管理标记时才被接受；不会覆盖未知对象。捕获范围或表结构改变时采集器安全停止，必须清空本地代次、确认遗留 Outbox 的处理方式后重新初始化，避免新旧行结构混用。

采集账号至少需要目标表 SELECT、Outbox SELECT/INSERT/DELETE、`sp_getapplock`，以及首次安装所需的 CREATE TABLE、CREATE TRIGGER 和目标表 ALTER 权限。自动安装会改变源数据库，建议先在备份副本验证触发器开销和现有触发器的交互。

## 类型和驱动限制

SQL Server 2000 严格模式拒绝包含 `text`、`ntext` 或 `image` 的目标表。SQL Server 2008 R2 模式允许有主键表使用这些旧 LOB 类型：INSERT/UPDATE 通过主键从业务表读取完整新镜像，DELETE 只记录主键并将无关 LOB 占位为 NULL。这样不会从 AFTER 触发器禁止访问的 `inserted/deleted` LOB 列读取数据。含旧 LOB 的无主键表仍会被拒绝，因为无法构造可靠的行身份。

2008 R2 模式的 Outbox 使用 `varchar(max)`、`nvarchar(max)` 和 `varbinary(max)`，不会把照片等值截断到 8 KiB。目标 PostgreSQL 对应类型分别为 `text` 和 `bytea`。

连接层使用 Microsoft `go-mssqldb` 的公开兼容分支。它先协商现代 TDS，使 SQL Server 2008 R2 能正确返回 `max` 类型元数据；连接失败时才回退专用于 SQL Server 2000 的 TDS 7.1 兼容模式。该实现已在 SQL Server 2000 Enterprise 8.00.194（32 位）验证基础兼容，并在 SQL Server 2008 R2 10.50 SP2 Express Advanced Services（64 位）验证全库目录检查及 LOB 触发器生成。

SQL Server 2000 无法使用当前驱动的 TLS 握手，因此 legacy DSN 必须显式设置 `encrypt=disable`。只应在可信内网或受保护隧道中使用，并通过独立、最小权限账号限制风险。

## 首次运行

复制 `config.sqlserver-legacy.example.json`，设置环境变量并先执行只读检查：

```powershell
$env:SQLSERVER_DSN = 'sqlserver://collector:password@server:1433?database=legacy_db&encrypt=disable'
./go-sync.exe check --config config.sqlserver-legacy.json
```

安排维护窗口后，将 `allow_snapshot_locks` 改为 `true` 再执行 `run`。首次全量通过 `TABLOCKX,HOLDLOCK` 固定 Outbox 边界，会阻塞所选表写入。全量成功后可以保持该配置；它只控制是否允许新的初始快照，不会让日常增量持续持锁。
