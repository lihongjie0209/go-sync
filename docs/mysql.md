# MySQL 5.6 and 5.7

Use `source_type: mysql` and a `mysql://user:password@host:3306/database` DSN. The configured replication `server_id` must be nonzero and unique.

The collector requires InnoDB tables and these server settings:

```ini
[mysqld]
server-id=1
log-bin=mysql-bin
binlog-format=ROW
binlog-row-image=FULL
```

The account needs `SELECT`, `REPLICATION SLAVE`, `REPLICATION CLIENT`, and `RELOAD`. `RELOAD` is used only for the short initial `FLUSH TABLES WITH READ LOCK` window that establishes an exact snapshot/binlog boundary. Enable this explicitly with `mysql.allow_snapshot_lock`.

Transactions are buffered in the durable local queue and become deliverable only after their XID commit event. A crash discards an unpublished tail and restarts from the last durable file/position, providing at-least-once delivery. The collector stops if that binlog file has expired. Configure `expire_logs_days` for the longest expected outage plus delivery backlog.

Tables without primary keys use protocol v2 `full_row` identity. Duplicate rows remain inherently ambiguous for downstream UPDATE or DELETE; the receiver must apply one matching occurrence per event.

DDL causes the selected tables to be inspected again. If their structure changed, a complete schema set is published atomically before subsequent row events are decoded with the new structure.

## Tested compatibility

| Release | Testcontainers coverage | Result |
| --- | --- | --- |
| 5.0 | No current official `mysql` image | CI reports an explicit skip |
| 5.1 | No current official `mysql` image | CI reports an explicit skip |
| 5.5.62 | Pinned official image, prerequisite probe | Rejected: no `binlog_row_image`, so FULL before/after rows cannot be required |
| 5.6.51 | Pinned official image, snapshot/binlog/rollback/keyless/DDL test | Supported |
| 5.7.44 | Pinned official image, snapshot/binlog/rollback/keyless/DDL test | Supported |

MySQL 5.2–5.4 are not supported GA release lines and are not represented as compatibility targets.
