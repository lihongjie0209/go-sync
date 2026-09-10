# Testing

The default suite is deterministic and includes race detection, crash-boundary
subprocess tests, malformed input checks, transaction rollback checks and
goroutine leak detection:

```bash
go test -race -shuffle=on ./...
```

Database recovery tests use Testcontainers and always register container
cleanup with the test process. They cover PostgreSQL target restart recovery,
duplicate delivery after a lost ACK, atomic transaction rollback, interrupted
snapshot publication, PostgreSQL capture, SQL Server CDC, MySQL 5.6/5.7,
filesystem commit/filtering and S3 object storage:

```bash
go test -race -count=1 -timeout=55m -tags=integration ./...
```

Benchmarks measure protocol JSON encoding, durable bbolt queue cycles and SQL
expression construction. Run several samples; a single result is only a smoke
check and is not a regression conclusion:

```bash
go test -run '^$' -bench . -benchmem -benchtime=3s -count=10 \
  ./internal/event ./internal/queue ./internal/server
```

The opt-in stability suite repeatedly persists, publishes, reads and
acknowledges messages, periodically closing and reopening the queue to verify
its checkpoints. It defaults to two minutes through `make stability`; override
the duration as needed:

```bash
GO_SYNC_STABILITY_DURATION=30m make stability
```

GitHub Actions runs unit/race and short benchmark smoke tests on each change,
the Testcontainers integration suite on each change, and a five-minute
stability/race test every night or on manual dispatch.

Tag releases are published to GitHub first and then mirrored to `cf-file` under
`release/<repository>/<tag>/`. The release workflow requires the repository
secret `CF_FILE_PASSWORD`; a failed mirror job can be retried independently
without rebuilding or republishing the GitHub Release.
