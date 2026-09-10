# HTTP CDC 协议 v1

本项目只实现发送方。以下是接收方必须满足的契约；HTTP 200 本身不代表业务已经可靠处理。

## 请求与确认

每次 `POST <http_url>` 发送一条消息对象：

```json
{
  "version": "v1",
  "source_id": "example",
  "generation": "2d8a06aeb56e46108180888b1be7e8e6",
  "schema_version": "9f16...c18a",
  "seq": 8,
  "message_id": "example/2d8a06aeb56e46108180888b1be7e8e6/8",
  "kind": "transaction_rows",
  "transaction": "0/16A4130",
  "lsn": "0/16A4130",
  "rows": [{
    "schema": "public",
    "table": "items",
    "operation": "update",
    "ordinal": 1,
    "key": [{"name":"id","type":"bigint","value":"2"}],
    "old_key": [{"name":"id","type":"bigint","value":"1"}],
    "columns": [{"name":"id","type":"bigint","value":"2"}]
  }],
  "created_at": "2026-09-06T10:00:00Z"
}
```

请求头包含 `Content-Type: application/json`、`Idempotency-Key: <message_id>` 和配置的认证头。不跟随重定向，防止把认证信息和行数据发到其他地址。

接收端在**消息及去重记录一起持久化成功之后**返回：

```http
HTTP/1.1 200 OK
Content-Type: application/json

{"message_id":"example/2d8a06aeb56e46108180888b1be7e8e6/8","ack_seq":8}
```

ACK 必须匹配当前消息的 ID 和序号。不得返回“已入内存队列”“稍后处理”的确认。已落盘的分块可以先 ACK，再等事务结束后应用，但暂存分块也必须可从服务器故障中恢复。ACK 响应最大 64 KiB。

发送端同一时间只发送一条消息，收到 ACK 并提交本地发送进度后才发送下一条。重试保持请求体、消息 ID、序号不变。接收端应永久保留每个 generation 的连续接收进度，或实现等价的持久去重机制；不能只依赖短 TTL 内存缓存。

## 消息顺序

```text
snapshot_begin
schema（每张表一条）
schema_end（标准 gRPC 在线结构刷新时）
snapshot_rows（多条）
snapshot_end
transaction_rows（零条或多条）
transaction_end
...后续事务...
```

`seq` 在同一个 `source_id + generation` 内从 1 连续递增，存储为 uint64；接收端应使用精确整数解析，不能经由 JavaScript Number/float64 丢失精度。`generation` 只在重新初始化时改变，普通重启保留。

| kind | 语义 |
|---|---|
| `snapshot_begin` | 新快照开始；`tables` 为 `[{"schema":"public","name":"items"}]` 白名单，`lsn` 为快照对应的增量起点 |
| `schema` | `schema` 对象描述一张表；和本次快照一起暂存 |
| `schema_end` | 在线结构刷新完整结束；接收端原子验证并激活新的 `schema_version` |
| `snapshot_rows` | `operation=read`；完整行，按主键进入本次快照暂存区 |
| `snapshot_end` | 完整快照结束；原子发布该代次副本，替换白名单范围的旧基线 |
| `transaction_rows` | 已提交源事务的一部分；暂存到 `transaction` 对应的事务中 |
| `transaction_end` | 源事务完整；把已接收的所有分块按 ordinal 顺序原子应用 |

事务按源库提交顺序发送，事务内行序号从 1 开始；`chunk` 从 0 开始，值为 0 时 JSON 可省略。事务结束消息的 `chunk` 等于事务数据分块总数。`transaction` 和 `lsn` 为事务结束位置；事务 ID 的唯一性还需结合 source_id 和 generation。可能发送零数据行的事务结束标记，用于有序推进。

采集器强制执行“完整事务先落盘，再推送”：收到 wal2json 的匹配提交标记后，先确保所有 `transaction_rows` 和 `transaction_end` 写入本地同步持久化队列，再原子更新可发送边界和持久化 LSN。发送器只能读取已发布消息，不能读取尚在暂存的事务分块；复制反馈也只使用成功持久化的 LSN。缓存不是仅保存在内存中，也不会为了释放容量而提前发送半个事务。

事务中途断线或重启时，未发布分块会被丢弃并从上一个持久化位点重读；已完整持久化、尚未获得 HTTP ACK 的消息继续重试。队列必须容纳一个完整事务（包括结束标记）；容量不足且没有已发布消息可释放时，采集器报错停止，需扩容后重启，不跳过事务。

完整本地缓存不等于单个 HTTP 请求：大事务仍可能分块传输，网络也可能在任意分块之间中断。接收端仍须持久化暂存、去重，并在 `transaction_end` 到达后原子应用事务，不能把每个分块直接视为完整事务。

新的 generation 到达时，接收端应保持旧副本可用，单独构建新基线；只在 `snapshot_end` 完整到达后替换，删除旧基线中已不存在的行。基线发布、连续 ACK 进度及后续增量处理必须具备一致的恢复语义。

## 列与 schema

SQL Server 采集端沿用同一消息、ACK 和事务协议，但位点是 `0x` 加 20 位十六进制的源专属字符串。接收端不能把所有 `lsn` 解析成 PG LSN。SQL Server schema 的 `source_type` 为 `sqlserver`，列类型名和无损文本编码遵循 [SQL Server 类型约定](sqlserver.md#协议与类型)；下面的具体类型说明针对 PostgreSQL。

每个列值都是 `{name,type,value}`。非空 `value` 是字符串，按其 PostgreSQL 类型解释：

- 整数、numeric、浮点使用无损文本，包括 NaN/Infinity；不转换成 float64。
- boolean 使用 `"true"` / `"false"`；bytea 使用 `"\\x..."` 十六进制 PostgreSQL 文本。
- 日期时间统一在 ISO DateStyle、UTC 会话下输出；无时区类型不附加时区含义。
- json/jsonb 的 value 是包含 JSON 内容的字符串，需再次按 JSON 解析；数组及其他类型使用 PostgreSQL 文本。
- `value:null` 表示 SQL NULL；列没有出现在 `columns` 中表示本次消息未提供值，特别是未变化的 TOAST 列，必须保留旧值。

INSERT/read 包含完整行；UPDATE 是补丁，使用 old_key 定位旧行，再写入新 key/columns；DELETE 使用 key 删除。没有变化前完整行的保证。

`schema` 示例：

```json
{
  "schema":"public",
  "name":"items",
  "oid":16384,
  "replica_identity":"d",
  "columns":[
    {"name":"id","type":"bigint","type_oid":20,"not_null":true,"default":null,"primary_key":true},
    {"name":"body","type":"text","type_oid":25,"not_null":false,"default":null,"primary_key":false}
  ]
}
```

列数组保留源列顺序。OID 只标识源库对象，不可直接作为目标库 OID 使用。

`schema_version` 是采集端持久化的完整结构 hash。快照中的所有消息使用同一版本。标准 gRPC 在线刷新先发送每张配置表的完整 `schema`，再发送同版本的 `schema_end`；后续增量才允许使用新版本。PostgreSQL 在 WAL 解码首次发现行结构与持久结构不匹配时触发该屏障，并从原 durable LSN 重放；连续跨越多个不兼容 DDL 边界时会安全停止并要求重新初始化。HTTP 模式维持原有消息类型序列，旧接收端可以忽略新增 JSON 字段；标准服务端会持久化 pending/active 版本并拒绝跨版本或未完成屏障的行消息。

## 错误与上限

网络错误、读取 ACK 中断、408、429、5xx 会重试；指数退避加抖动，默认上限 60 秒。不使用失败响应中的正文，避免暴露行数据或认证信息。

其他状态码、非 JSON ACK、ACK 不匹配或过大都会停止投递并保留队列。包括 202、204、409 和 413；重复请求必须返回匹配的 200 ACK。程序不会把失败消息移动到死信队列后继续推进。

批量大小是软上限，不包含所有 JSON 信封开销；单个较大行允许独立成批。行事件超过 `max_row_bytes` 时采集失败，扩容后从持久位置重放。接收端和反向代理必须允许至少单行硬上限加 JSON 信封的请求体。
