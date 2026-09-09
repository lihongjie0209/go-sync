# Prometheus 指标

## 启用与抓取

配置片段：

```json
{"metrics_addr": "127.0.0.1:9108"}
```

`run` 提供 `GET /metrics`（也支持 HEAD），官方 `client_golang` 负责文本格式与 OpenMetrics 协商。使用独立 registry，不向全局 registry 注册。接口最多同时处理 4 个抓取请求，不查询远程 PostgreSQL、不消费队列，也不向业务接收端发送任何内容。

默认关闭；[配置示例](../config.example.json) 显式绑定本机。监听地址格式为 `host:port`，端口 1–65535。`check` 和 `status` 不启动 HTTP 服务。进程退出后接口也关闭，须对 Prometheus 自身生成的 `up` 设置告警。没有单独的 health/readiness 路径。

同机部署的抓取配置见 [prometheus.example.yml](prometheus.example.yml)。跨机器或容器抓取需要自行配置可达地址；容器中的 `127.0.0.1` 不是宿主机。接口无内置鉴权和 TLS，不要直接暴露到公网。

## 指标口径

下表省略统一前缀 `go_sync_`。时间用秒，大小用字节，Counter 使用 `_total`。标签仅使用有限的 `phase`、`result` 或 HTTP `code`；没有表名、消息 ID、LSN、DSN、HTTP URL、令牌、错误正文或行内容标签。可在 Prometheus 抓取配置里添加稳定的 `source` 标签。

| 名称 | 类型 | 含义 |
| --- | --- | --- |
| `capture_phase{phase}` | Gauge | `uninitialized` / `snapshot` / `stream` 持久阶段，one-hot；不是当前网络状态 |
| `source_info{engine}` | Gauge | 当前数据源类型，值恒为 1；用于约束 PostgreSQL/SQL Server 专用告警 |
| `capture_streaming` | Gauge | 逻辑复制连接已经建立；退出 stream 时归零，不等同于最近仍有进度 |
| `capture_attempts_total` | Counter | 采集尝试，包含首次启动 |
| `capture_errors_total` / `capture_reconnects_total` | Counter | 失败尝试 / 计划重试次数；关闭取消不算失败，首次连接失败也可产生重试 |
| `capture_snapshots_total` / `capture_snapshot_rows_total` | Counter | 成功完整落盘并发布的快照 / 其中行数；失败的快照不计入 |
| `capture_transactions_total` / `capture_transaction_rows_total` | Counter | 原子发布到本地队列的增量事务 / 行数；尚未提交及已持久化重放不重复计数 |
| `capture_wal_payload_bytes_total` | Counter | 收到的逻辑复制 payload 字节，包含重放；不是物理 WAL 量 |
| `capture_last_receive_timestamp_seconds` | Gauge | 最近有效复制帧（包括心跳）的接收时间；未收到为 0 |
| `capture_last_commit_timestamp_seconds` | Gauge | 最近快照或增量事务本地发布的时间；未发布为 0 |
| `wal_retained_bytes` | Gauge | 源库当前 WAL 与本 slot `restart_lsn` 的差值 |
| `capture_lag_bytes` | Gauge | 源库当前 WAL 与本地 durable LSN 的差值；包含其他表/数据库的 WAL，不等于待同步业务字节 |
| `wal_sample_timestamp_seconds` | Gauge | 最近 WAL 查询采样时间；0 表示尚未知；约每 30 秒更新，受断连、背压和快照影响 |
| `queue_bytes` / `queue_limit_bytes` | Gauge | 队列中未确认及暂存消息的序列化大小 / 配置上限；不是 bbolt 文件占用 |
| `queue_pending_messages` | Gauge | 已发布、未本地确认的消息数，包括正在 HTTP 请求中的消息 |
| `queue_staged_messages` | Gauge | 未完成快照/事务的不可见暂存消息数 |
| `queue_oldest_pending_age_seconds` | Gauge | 最早已发布未确认消息距本地入队的时间；无已发布消息为 0；不是源事务提交延迟 |
| `queue_full_total` / `capture_backpressure` | Counter / Gauge | 因队列/磁盘容量失败的 append 次数 / 是否正等待释放容量；不能容纳完整事务时仍会报错退出 |
| `disk_free_bytes` / `disk_reserve_bytes` | Gauge | 队列文件系统可用空间 / 配置保留量 |
| `http_requests_total{result}` | Counter | 请求结果：`success`、`retryable_error`、`permanent_error`、`canceled`；只有 200 + 有效匹配 ACK 才成功 |
| `http_retries_total` | Counter | 已计划的 HTTP 重试次数 |
| `http_invalid_acks_total` | Counter | 200 响应中 ACK 超长、JSON 错误、ID 或序号不匹配的次数 |
| `http_request_duration_seconds` | Histogram | 单次 HTTP 尝试耗时，包括读响应和验证 ACK；不包括退避等待和本地队列 ACK |
| `delivery_messages_total` / `delivery_bytes_total` | Counter | 远端 ACK 验证成功且本地 ACK 落盘后的消息数 / 原始序列化字节数；重传不会单独增加 |
| `delivery_last_ack_timestamp_seconds` | Gauge | 最近成功本地 ACK 的时间；未 ACK 为 0 |
| `schema_changes_total` | Counter | 检测到结构指纹或行结构漂移的次数，不代表 schema 已同步成功 |
| `schema_delivered_total` | Counter | 远端和本地均确认的 `schema` 消息数 |
| `snapshot_duration_seconds` | Histogram | 成功快照尝试的耗时，包括扫描、入队和发布 |
| `sqlserver_snapshot_active` / `sqlserver_snapshot_start_timestamp_seconds` | Gauge | SQL Server 快照是否正在执行 / 本轮开始时间 |
| `sqlserver_snapshot_scanned_rows` / `sqlserver_snapshot_completed_tables` | Gauge | 本轮已扫描暂存行数 / 表数；失败时可能非零，不代表已发布 |
| `sqlserver_snapshot_failures_total` | Counter | SQL Server 快照失败次数；正常关闭取消不计入 |
| `sqlserver_snapshot_wait_start_timestamp_seconds{stage}` | Gauge | 等待锁或 CDC fence 的开始时间；非等待状态为 0 |
| `sqlserver_snapshot_wait_duration_seconds{stage,result}` | Histogram | 锁和 fence 等待耗时，成功与失败分开 |
| `sqlserver_cdc_polls_total{result}` | Counter | CDC 轮询结果：`progress`、`idle`、`error`、`canceled` |
| `sqlserver_cdc_poll_duration_seconds` | Histogram | CDC 轮询、校验和本地暂存耗时 |
| `sqlserver_cdc_retention_gaps_total` | Counter | 已确认的 CDC 保留期缺口；发生后安全停止，不跳过数据 |
| `sqlserver_scan_age_seconds` | Gauge | 源端最近一次已完成 CDC 扫描距源端当前时间；包含空扫描，未知为 NaN |
| `sqlserver_capture_latency_seconds` | Gauge | 最近一次非空且无错的捕获扫描延迟；空扫描或未知为 NaN |
| `sqlserver_checkpoint_lag_seconds` | Gauge | 已捕获最高水位时间减本地 durable 位点时间；不包含尚未捕获日志和 HTTP 积压 |
| `sqlserver_retention_margin_seconds` | Gauge | durable 位点距各表当前低水位中最严格边界的时间距离；不是预计清理倒计时 |
| `sqlserver_health_sample_timestamp_seconds{signal}` | Gauge | 各健康值最近有效采样的本地时间；0 表示当前未知，必须与值一起判断 |
| `sqlserver_health_errors_total` | Counter | 健康采样失败次数；采集继续运行 |
| `sqlserver_legacy_outbox_events_deleted_total` | Counter | SQL Server 2000 Outbox 事件在本地发布成功后从源端删除的数量 |
| `sqlserver_legacy_outbox_delete_errors_total` | Counter | 本地发布后源端精确删除失败的次数；恢复时可能产生至少一次重复 |
| `sqlserver_legacy_polls_total{result}` / `sqlserver_legacy_poll_duration_seconds` | Counter / Histogram | SQL Server 2000 Outbox 轮询结果与耗时 |
| `capture_recovery_discarded_messages_total` | Counter | 故障恢复成功清理的、从未发布的暂存消息数 |
| `metrics_requests_total{code}` / `metrics_request_duration_seconds` | Counter / Histogram | 监控接口自身的返回码/耗时；当前请求结束后才计入 |

此外注册官方 `go_*` 和 `process_*` collector；进程指标可用性受操作系统影响。

所有 Counter、连接状态、最近时间和 WAL 采样在进程重启后重置；队列相关 Gauge 每次抓取在单个本地只读事务中恢复实时值。监控不是审计账本：落盘完成到内存 Counter 增加之间若崩溃，累计数可能少计，持久状态和至少一次语义不受影响。不要将 Counter 当成跨重启的精确对账依据。

正常空闲时 `last_commit` 和 `last_ack` 不更新；仅凭距上次提交/ACK 的时长不能判断故障。WAL Gauge 应结合采样时间、连接状态和积压判断，不能把初始的 0 当成已经追平。使用 SQL 中的 LSN 差值避免先将绝对 LSN 转为 float64 后相减；巨大差值作为 Prometheus 样本仍受浮点数精度限制。

当前 schema 漂移仍会导致进程停止。致命错误计数可能来不及被下一次抓取观察到，因此必须同时监控 `up` 和退出日志。自动 schema 全量重推、按历史结构适配增量的功能尚未实现。

## PromQL 示例

```promql
# 采集/投递吞吐
rate(go_sync_capture_transaction_rows_total[5m])
rate(go_sync_delivery_messages_total[5m])

# 队列占用比例
go_sync_queue_bytes / go_sync_queue_limit_bytes

# 单实例 HTTP P99（多实例聚合需先按 le 等维度 sum）
histogram_quantile(0.99, rate(go_sync_http_request_duration_seconds_bucket[5m]))

# HTTP 错误比例（取消不视为投递失败）
sum by (job, instance) (rate(go_sync_http_requests_total{result=~"retryable_error|permanent_error"}[5m]))
/
clamp_min(sum by (job, instance) (rate(go_sync_http_requests_total{result!="canceled"}[5m])), 0.001)

# 有积压且五分钟没有确认投递
(go_sync_queue_pending_messages > 0)
and (increase(go_sync_delivery_messages_total[5m]) == 0)

# PostgreSQL 已处于增量阶段但 WAL 采样超过两分钟未更新或从未更新
(time() - go_sync_wal_sample_timestamp_seconds > 120)
and on (job, instance) (go_sync_capture_phase{phase="stream"} == 1)
and on (job, instance) (go_sync_source_info{engine="postgres"} == 1)
```

[告警示例](alerts.example.yml) 的阈值需按实际队列容量、业务规模和快照耗时调整，不会自动安装到 Prometheus。

参考：[官方 Go 客户端](https://github.com/prometheus/client_golang)、[指标命名约定](https://prometheus.io/docs/practices/naming/)。
