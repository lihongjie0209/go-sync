# Standard gRPC server

`go-sync-server` receives the existing ordered event protocol over a
collector-initiated, bidirectional gRPC stream and applies it to pre-created
PostgreSQL 15 or newer tables. `source_id` is the server-side syncer ID.

```powershell
$env:GO_SYNC_ADMIN_TOKEN = 'replace-me'
$env:PMS_SYNC_TOKEN = 'replace-me'
$env:PMS_TARGET_DSN = 'postgres://role:password@host:5432/database'
go-sync-server.exe check --config server.config.json
go-sync-server.exe run --config server.config.json
```

Windows 服务：

```powershell
go-sync-server.exe service install --config C:\go-sync\server.config.json
go-sync-server.exe service start
go-sync-server.exe service status
go-sync-server.exe service stop
go-sync-server.exe service uninstall
```

TLS is mandatory. Each syncer has its own bearer token; administration uses a
separate token. The target database is selected by its DSN. All configured
source schemas map into `target_schema` and retain their table names, so source
table names must be unique within a syncer. Business tables are never created;
columns are validated and are altered only by the explicitly enabled safe
nullable-column policy described below. The service account must be allowed to
create and migrate `metadata_schema` and, when that policy is enabled, alter the
configured target tables.

When configured tables have foreign keys, list parent tables before child
tables. Snapshot publication deletes in reverse order and inserts in configured
order. Non-deferrable constraints, triggers and target-only business rules stay
enabled; violations stop synchronization instead of being bypassed.

The receiver keeps `received_seq` and `applied_seq` in the same PostgreSQL
database as the destination. Transaction chunks are durable before ACK and are
applied only when `transaction_end` arrives. Snapshot rows are staged durably
and published at `snapshot_end`. Tables without primary keys use one-occurrence
full-row matching.

Snapshot and transaction staging is read back in bounded keyset pages, so the
server does not hold an entire snapshot or transaction in Go memory. Publication
is still one PostgreSQL transaction. A persisted transaction identifier rejects
interleaved chunks and mismatched transaction endings. Every newly received
sequence stores a SHA-256 payload digest; a replay with the same sequence but
different content is rejected instead of silently acknowledged.

While a collector stream is active, the server holds a PostgreSQL session-level
advisory lock derived from `metadata_schema` and `syncer_id`. This guarantees one
active writer across multiple `go-sync-server` instances using the same target
database. Losing that dedicated connection stops delivery before another event
is stored.

For embedded file columns, the original path is written to `source_column` and
the decoded bytes are written to the configured, pre-created PostgreSQL `bytea`
`content_column`.

## Schema version barrier

Every newly queued event carries the durable source schema hash. Online schema
refresh sends the complete configured table set followed by `schema_end`; the
server stages the set and atomically activates it only at that boundary. Row
events with an old/new/missing version, or rows arriving before `schema_end`,
are rejected without advancing the receive checkpoint.

`auto_add_nullable_columns` is disabled by default. When enabled for a syncer,
the server may add a missing nullable, non-key column whose source type is in a
strict built-in allowlist. Identifiers remain quoted and type text is never used
unless it matches the allowlist. Missing NOT NULL/key columns, unsupported
types, column deletion, type changes and narrowing fail closed. PostgreSQL and
MySQL 5.6/5.7 collectors using the standard gRPC transport can emit the online
full-schema barrier and continue incremental synchronization. PostgreSQL does
so when decoding first observes a row whose shape no longer matches the durable
schema; offline DDL without a following row, or multiple incompatible DDL
generations before replay reaches their WAL boundaries, fails closed and needs
reinitialization. SQL Server collectors currently still require reinitialization
when they detect source DDL.

## Metrics and VictoriaMetrics

Collectors using gRPC periodically send their official Prometheus client
snapshot through the authenticated stream. The server adds `syncer_id` and
`component="collector"`; its own metrics use `component="server"`. Both are
periodically written to VictoriaMetrics through its
`/api/v1/import/prometheus` endpoint. Only the newest collector snapshot is
held while VictoriaMetrics is unavailable, so observability failures never
consume the durable change queue. Local `/metrics` endpoints remain available.

## Hot reload

The server watches the configuration file and also exposes the authenticated
`Admin.ReloadConfig` RPC. A candidate is fully parsed, its TLS key pair loaded,
and every PostgreSQL target connected and validated before one atomic switch.
Failure leaves the previous configuration active. Syncers, tokens, target
connections, TLS certificates, VictoriaMetrics settings and log level are hot
reloadable. Listener addresses, metrics address, log file and rotation settings
require restart.

The current gRPC release requests and replays retained sequence numbers. If the
requested sequence predates the collector archive it fails closed and reports
that a new snapshot is required; automated repair commands and scheduled hash
reconciliation are reserved by the protocol but not yet enabled.
