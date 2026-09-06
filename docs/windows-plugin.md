# Windows wal2json 自动安装

采集器内嵌 [wal2json-windows 的 wal2json_2_6 发布包](https://github.com/lihongjie0209/wal2json-windows/releases/tag/wal2json_2_6)，启动时不联网、不编译。包含 PG **9.4.0–9.4.26、9.5.0–9.5.25、9.6.0–9.6.24** 各自的 x86/x64 DLL，共 156 个精确匹配的目标。每个包都通过其对应完整版本与位数的 Windows PG 加载、解码及 LSN 验证；内嵌 coverage.json 记录覆盖范围，不安装标记为跳过的目标。

必须匹配完整 PG 版本与 **PG 进程位数**，不是采集器或 Windows 系统位数。实际测试发现：9.5.25 编译的 DLL 虽然能在 9.5.2 LOAD，但 `nextlsn` 会变成 `0/0`。因此不按大版本盲目复用 DLL，其他补丁版本需要单独构建验证。

## 采集器配置

在现有配置中增加：

```json
{
  "wal2json_auto_install": true,
  "postgres_bin_dir": "D:\\pms\\PostgreSQL\\bin"
}
```

这是合并到现有配置的片段，不是完整配置。`wal2json_auto_install` 默认 true；非 Windows 平台不执行安装。`postgres_bin_dir` 默认空，从连接对应的本机 backend 进程自动识别；建议显式指定，增加路径约束。这两项不改变采集范围指纹，不要求清除已有队列。

安装发生在 `run` 的源配置、表兼容性和持久化源身份检查之后、快照或流启动之前。`check` 保持只读，不安装插件。所以应先完成下面的 PG 参数配置。

## PostgreSQL 9.5.2 配置建议

先用 `SHOW config_file` 确认实际配置文件位置，备份该文件，在 `postgresql.conf` 中调整：

```conf
wal_level = logical
max_replication_slots = 10
max_wal_senders = 32
```

这里根据你报告的 `hot_standby / 0 / 32` 给出建议：将 WAL 改为 logical、预留 10 个槽，保留现有 32 个发送进程上限。具体槽数量按采集器数量和其他复制用途规划。参数修改后需要管理员安排 **重启 PostgreSQL**，不是仅 reload。采集器不会自动修改配置或重启服务。

使用普通数据库连接验证生效：

```sql
SELECT version(), current_setting('wal_level'),
       current_setting('max_replication_slots'),
       current_setting('max_wal_senders');
```

数据库角色需要逻辑复制所需权限；自动 `LOAD` 和本机证明需要超级用户权限。若生产规范不允许，管理员手动部署 DLL，然后设置 `wal2json_auto_install=false`。不要为了安装静默提权。

同机连接建议 `host=127.0.0.1 port=5432`。PG 9.5 还需要允许复制协议连接：在 `pg_hba.conf` 中确认普通数据库规则及 `replication` 规则都允许该角色。按实际角色配置，例如：

```conf
host pms_db_client pms_db_role 127.0.0.1/32 md5
host replication   pms_db_role 127.0.0.1/32 md5
```

不要覆盖已有规则；注意顺序，确保未被前面的拒绝规则匹配。仅 HBA 修改可 reload，但上面的 WAL/槽参数仍需重启。PG 9.5 不支持 SCRAM；不要为省事开放全网 trust。

无主键表的 UPDATE/DELETE 另需管理员设置 `REPLICA IDENTITY FULL`；安装插件不会替你改表。同步槽会保留 WAL，应监控磁盘；PG 9.5 没有现代版本的 WAL 槽保留上限参数。

## 安全边界

- 已能 LOAD 的插件保持原样，不自动升级。
- 缺失时要求实际连接是 loopback，验证 backend 的 Windows 进程、启动时间、可执行文件路径和 PE 位数。
- 在数据库报告的本机 data_directory 写入随机临时挑战文件，由同一 SQL 连接读回后删除，避免把端口转发误判为同机实例。
- 需要读取 PG 进程、临时写入 data_directory、写入 lib 目录的操作系统权限。路径不匹配、权限不足、未知版本时明确失败。
- 发布包和 DLL 双重 SHA256 校验；只解压固定 DLL 到内存，不按 ZIP 路径写磁盘。
- 先写临时文件并落盘，再用不覆盖的原子硬链接发布；要求目标文件系统支持硬链接（通常 NTFS）。已有文件、目录或链接不被替换。
- 安装失败不更改 PG 配置或复制槽；复制成功但 LOAD 失败会保留 DLL 并报错，供管理员诊断，不反复覆盖。
- DLL 安装测试只证明插件兼容，不等于采集器所有恢复场景已验证。PG 9.x 已停止维护，这些包不提供数据库安全补丁。

## 发布来源

完整 ZIP（含上游源码、许可证、构建 manifest 和测试输出）保存在 `internal/walplugin/assets` 并由 Go embed 打包。上游 commit 固定为 `75629c2e1e81a12350cc9d63782fc53252185d8d`。新增包必须先在 GitHub CI 使用对应 Windows PG 验证，核对发布校验和，再更新内嵌资产与明确的版本白名单。
