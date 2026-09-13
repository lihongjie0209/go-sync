package sqlserverlegacy

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	mssql "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/msdsn"
	"go-sync/internal/config"
)

type databaseError struct{ cause error }

func (e *databaseError) Error() string {
	var serverError mssql.Error
	if errors.As(e.cause, &serverError) {
		return fmt.Sprintf("sqlserver legacy error %d; inspect database logs", serverError.Number)
	}
	return "sqlserver legacy operation failed; verify tds connectivity, permissions and server logs"
}
func (e *databaseError) Unwrap() error { return e.cause }
func dbError(err error) error {
	if err == nil {
		return nil
	}
	return &databaseError{cause: err}
}

func open(ctx context.Context, c config.Config) (*sql.DB, error) {
	parsed, err := msdsn.Parse(c.DSN)
	if err != nil {
		return nil, errors.New("invalid sqlserver legacy dsn")
	}
	if parsed.Database == "" {
		return nil, errors.New("sqlserver legacy dsn must specify database")
	}
	if parsed.Encryption != msdsn.EncryptionDisabled {
		return nil, errors.New("sqlserver legacy dsn must set encrypt=disable because SQL Server 2000 does not support the driver's TLS handshake")
	}
	parsed.AppName, parsed.LogFlags, parsed.DialTimeout = "go-sync-legacy", 0, 10*time.Second
	// SQL Server 2008 R2 needs its modern TDS metadata for max-sized values.
	// SQL Server 2000 cannot negotiate that connection, so fall back to the
	// explicitly supported TDS 7.1 compatibility mode only when necessary.
	db, err := openConfig(ctx, parsed)
	if err == nil {
		return db, nil
	}
	parsed.LegacyTDS71 = true
	db, legacyErr := openConfig(ctx, parsed)
	if legacyErr != nil {
		return nil, errors.Join(err, legacyErr)
	}
	return db, nil
}

func openConfig(ctx context.Context, parsed msdsn.Config) (*sql.DB, error) {
	db := sql.OpenDB(mssql.NewConnectorConfig(parsed))
	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(3)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		return nil, errors.Join(dbError(err), db.Close())
	}
	return db, nil
}

func retryable(err error) bool {
	var databaseErr *databaseError
	if !errors.As(err, &databaseErr) {
		return false
	}
	var serverError mssql.Error
	if errors.As(databaseErr.cause, &serverError) {
		return serverError.Number == 1205 || serverError.Number == 1222
	}
	var networkErr *net.OpError
	return errors.As(databaseErr.cause, &networkErr) || errors.Is(databaseErr.cause, driver.ErrBadConn) ||
		errors.Is(databaseErr.cause, io.EOF) || errors.Is(databaseErr.cause, io.ErrUnexpectedEOF) ||
		errors.Is(databaseErr.cause, context.DeadlineExceeded)
}

func rollback(tx *sql.Tx, result *error) {
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		*result = errors.Join(*result, dbError(err))
	}
}
