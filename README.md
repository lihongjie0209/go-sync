# go-sync

Go 编写的变化采集端：PostgreSQL 逻辑 slot、SQL Server CDC、全量、本地持久队列和 HTTP 至少一次投递。只提供采集端，不包含业务接收服务器。

SQL Server 接入及配置见 [SQL Server 全量与 CDC](docs/sqlserver.md) 和 `config.sqlserver.example.json`。SQL Server 2008/2008 R2 仍需真实旧引擎验收；首次全量表锁必须显式开启。以下原有配置说明默认针对 PostgreSQL。

## 发布包

推送 `v*` 标签会由 GoReleaser 创建 GitHub Release，发布 Windows x64 与 x86 ZIP 和统一的 SHA-256 `checksums.txt`。每个 ZIP 包含可执行文件、PostgreSQL/SQL Server 配置示例及 `docs` 文档；版本号通过构建参数写入 `go-sync version`。

普通 push 和 pull request 会运行竞态测试、`go vet`、模块一致性检查、Testcontainers 集成测试，并生成保留 7 天的 Windows 快照包。正式发布示例：

```bash
git tag v0.1.0
git push origin v0.1.0
```

## 快速开始

需要 Go 1.25+、PostgreSQL 9.4+ 和 wal2json 2.6。生产环境使用仍在维护的 PostgreSQL 版本；9.x 兼容性通过独立测试维护。

Windows 版现内嵌 PG 9.4/9.5/9.6 全部 78 个小版本的 x86/x64 wal2json 2.6 发布包。默认同机部署，`run` 在插件缺失时验证本机实例并按完整版本、PG 进程位数自动安装，不联网、不覆盖已有 DLL。配置、权限和 PostgreSQL 修改建议见 [Windows 插件安装](docs/windows-plugin.md)。`check` 仍保持只读。

```sh
make build
# 参照 config.example.json 创建 config.json，并设置 GO_SYNC_DSN、GO_SYNC_HTTP_TOKEN。
./bin/go-sync check --config config.json
./bin/go-sync run --config config.json
./bin/go-sync status --config config.json
```

`check` 只读检查数据库版本、逻辑复制配置、连接权限、表结构；不会创建或消费 slot。插件加载与具体选项在首次创建、启动本项目 slot 时验证。`run` 会创建配置指定的 slot，但不会修改业务表、复制身份、数据库参数或安装触发器。

配置为严格 JSON，未知字段和无效值会被拒绝。`dsn` 和 `http_headers` 支持 `${ENV_NAME}`；其他字段不展开环境变量。相对路径相对于启动目录。`source_id` 在同一接收端必须唯一。

## PostgreSQL 准备

由数据库管理员安装 wal2json，配置并按需重启数据库：

```ini
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
```

在支持 `output_plugin_libraries` 参数的新补丁版本中，把 `wal2json` 加入允许列表。不要把新参数直接写入不支持它的旧版 PostgreSQL。

采集账号需要复制权限、数据库 CONNECT、schema USAGE、白名单表 SELECT。PG 9.4–9.6 的逻辑复制连接匹配 `pg_hba.conf` 中的 `replication` 规则；PG 10+ 匹配具体数据库规则。普通全量查询仍需要普通数据库连接规则。请使用适合部署环境的认证和 TLS 设置。

首版支持带单列或联合主键的普通持久表，要求 `REPLICA IDENTITY DEFAULT` 或 `FULL`。启动时拒绝无主键表、分区/继承表、临时/非日志表、启用 RLS 的表和生成列。非默认复制身份不会自动修改。

连接统一使用 UTF-8。拒绝 SQL_ASCII 数据库，避免把任意字节文本编码为 JSON 时产生不可逆替换。

## schema 同步

每次初始化发送 `schema` 消息，包含 schema/table 名称、源表 OID、列顺序、列名、PostgreSQL 类型及类型 OID、NULL 约束、默认表达式、主键列和 replica identity。这些消息与快照共用持久队列，先于对应数据发送。

这是**初始化结构元数据同步**，不是实时 DDL 复制，也不是自动执行建表 SQL。接收端解释元数据、创建目标结构。普通索引、外键、CHECK、权限、触发器、序列当前值、扩展及用户自定义类型定义不在元数据范围内；默认表达式可能引用源库序列或函数，接收端不能不加处理地直接执行。

采集过程中每 5 秒检查结构指纹，同时核对变化记录的列类型。发现变化停止并报告重新初始化需求。轮询无法完整捕获两次检查之间发生又撤销的 DDL，也无法可靠捕获旧版 PG 上的 TRUNCATE，因此持续采集期间仍要求禁止 DDL/TRUNCATE。没有安装 DDL 事件触发器。

## 可靠性与恢复

```text
源库事务 → 本地暂存块 → 原子发布完整事务及 durable_lsn → 确认 slot
                                       ↓
                                HTTP 有序发送
                                       ↓
                         持久化 ACK → delivered_seq → 回收
```

- 队列采用 bbolt 同步落盘，事件发布与 `durable_lsn` 更新在同一事务提交，禁止 `NoSync`。
- 每个源只允许一个采集进程；队列文件锁防止同一持久目录并发打开。slot 必须由本采集端独占，禁止其他程序消费或手动推进。
- 崩溃后清理不可见的未完成事务块，从持久位置重放；已发布但未被 HTTP 确认的消息保留原始内容和 ID 重发。
- 收到响应不明确、请求超时或连接中断均可能产生重复。接收端必须持久化后 ACK，并按消息 ID 去重；否则无法保证端到端至少一次。
- PostgreSQL 重启回退 slot 进度时，已经持久化的事务按事务结束 LSN 跳过。
- 采集磁盘永久损坏、源 WAL/slot 丢失、主备切换不提供自动无损恢复。检测到源身份/时间线变化、slot 不匹配或可检测的 slot 超前时停止。
- 网络、服务重启及临时资源不足重试；结构、协议、存储等永久错误保留队列并退出。

### 全量阶段

创建 slot 并导出快照，在一个可重复读事务中读取所有白名单表，完整落本地后才发布快照。扫描使用流式读取与有限批量，不把全库装入内存；快照事务期间持有表的 ACCESS SHARE 锁。

全量未完成而中断时，新一轮启动仅重建本项目拥有且未激活的初始化 slot，重新全量。未完成快照从未发送给接收端。全量已完成则直接续传，并从对应 LSN 继续采集。

初始化重做用于建立最新数据基线，不承诺保留失败初始化期间的每条历史变化。初始化期间源库保留 WAL，本地必须容纳完整快照。大事务也必须能完整容纳；若未发布数据已占满容量、且没有可投递积压可回收，则明确报错，扩容后重启。

### 容量与运维

- `queue_bytes` 默认 10 GiB，限制未确认及暂存消息的逻辑大小；bbolt 文件和元数据会额外占用空间。文件删除记录后复用空闲页，不自动缩小文件。
- `reserve_bytes` 默认 256 MiB，为 ACK、元数据及文件扩展保留磁盘余量；不是文件系统配额，不能防止其他进程抢占磁盘。
- 达到容量上限时暂停采集和推进 slot，继续投递已发布消息。此时积压重新转移到源库 WAL。
- `status` 读取每 5 秒原子更新的 `status.json`，包含队列进度、大小、最早待发送时间和磁盘余量。硬崩溃后 `running` 可能陈旧，应同时检查 `updated_at` 和进程状态。
- 日志每 30 秒输出 `durable_lsn` 和 `wal_retained_bytes`。没有可确认的事务时不会把 WAL 心跳位置误报为已持久化位置；低活跃数据库也需监控 WAL 保留量。
- 状态目录应位于预先配置好的持久本地文件系统；容器必须挂载持久卷。不要使用临时目录、网络共享或从旧备份单独覆盖队列文件。

变更表白名单、结构或丢失 slot 后，先停止旧采集并保留旧目录/slot，检查积压及副本状态。确需重新初始化时使用新的持久目录和新 slot，让接收端按新的 `generation` 发布替代快照。旧数据确认不再需要后由管理员显式清理，程序不会自动删除流式阶段的 slot。

## HTTP 协议

参见 [docs/protocol.md](docs/protocol.md)。默认 500 行或约 1 MiB 一批，单行硬上限 16 MiB；HTTP 默认超时 30 秒。400/401/403/413、错误 ACK 或非 200 成功码不会被当成持久化确认，也不会跳过消息。

## Prometheus 监控

使用 Prometheus 官方 Go 客户端。配置 `"metrics_addr": "127.0.0.1:9108"` 后，`run` 提供独立的 `GET /metrics` 接口：

```sh
curl http://127.0.0.1:9108/metrics
```

省略或设为 `""` 则禁用；示例配置绑定本机。监听失败会在启动采集前报错，退出时关闭接口。修改监控地址无需重新全量，不影响持久队列或采集范围指纹。接口不提供鉴权/TLS，不注册 pprof；跨机器抓取时只监听可信内网，并通过防火墙或反向代理限制访问。

覆盖采集事务/行数、连接重试、WAL 保留与字节积压、队列已发布/暂存消息、磁盘余量、HTTP 耗时/重试/无效 ACK、确认投递量、schema 漂移及 Go/进程指标。Counter 在进程重启时归零，队列 Gauge 从磁盘恢复；成功 HTTP 请求和本地确认投递分别计数。

完整指标口径、PromQL、抓取配置和告警示例见 [docs/metrics.md](docs/metrics.md)。Schema 自动适配仍是待实现项；监控功能不改变当前“发现结构漂移则停止”的行为。

## 测试

```sh
make test
make vet
# 仅指向隔离测试库：测试会创建/删除自己的表和 slot，并终止自己的复制连接。
GO_SYNC_TEST_DSN='postgres://postgres@127.0.0.1:55434/postgres?sslmode=disable' make integration
# 构建源代码固定版本的临时 PostgreSQL，自动清理测试容器；需 Docker 和网络。
JOBS=3 bash scripts/test-matrix.sh
# 可只测某个版本
bash scripts/test-matrix.sh 9.4.26
```

测试覆盖同步落盘后的进程突然退出、未发布数据恢复、严格 ACK、响应丢失、精度/NULL/TOAST、主键修改、事务回滚、复制断连、离线写入、快照容量不足重做、快照期间并发写入及结构漂移。版本矩阵还会强制终止并重新启动它自己创建的 PostgreSQL 测试容器，验证源库崩溃后的续传。测试接收器只用于测试，不是可部署服务。

版本矩阵为 PG 9.4.26、9.5.25、9.6.24、10.23、11.22、12.22、13.23、14.24、15.19、16.15、17.11、18.6，统一使用 wal2json 2.6。依赖锁定在 `go.mod` / `go.sum`；升级后重新运行矩阵。不要把源码镜像的 trust 认证配置用于生产。
