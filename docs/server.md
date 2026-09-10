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
table names must be unique within a syncer. Business tables and columns are
validated but never created or altered. The service account must be allowed to
create and migrate `metadata_schema`.

When configured tables have foreign keys, list parent tables before child
tables. Snapshot publication deletes in reverse order and inserts in configured
order. Non-deferrable constraints, triggers and target-only business rules stay
enabled; violations stop synchronization instead of being bypassed.

The receiver keeps `received_seq` and `applied_seq` in the same PostgreSQL
database as the destination. Transaction chunks are durable before ACK and are
applied only when `transaction_end` arrives. Snapshot rows are staged durably
and published at `snapshot_end`. Tables without primary keys use one-occurrence
full-row matching.

For embedded file columns, the original path is written to `source_column` and
the decoded bytes are written to the configured, pre-created PostgreSQL `bytea`
`content_column`.

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
