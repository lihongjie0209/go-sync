package sqlserver

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

// databaseError retains the cause for retry classification, never its message:
// driver/server errors may include credentials, SQL text or business row values.
type databaseError struct{ cause error }

func (e *databaseError) Error() string {
	var se mssql.Error
	if errors.As(e.cause, &se) {
		return fmt.Sprintf("sqlserver error %d; inspect database logs", se.Number)
	}
	return "sqlserver operation failed; verify connectivity, permissions and server logs"
}
func (e *databaseError) Unwrap() error { return e.cause }
func dbError(err error) error {
	if err == nil {
		return nil
	}
	return &databaseError{cause: err}
}

func open(ctx context.Context, c config.Config) (*sql.DB, error) {
	pc, err := msdsn.Parse(c.DSN)
	if err != nil {
		return nil, errors.New("invalid sqlserver dsn")
	}
	if pc.Database == "" {
		return nil, errors.New("sqlserver dsn must specify database")
	}
	pc.AppName = "go-sync"
	pc.LogFlags = 0
	pc.DialTimeout = 10 * time.Second
	connector := mssql.NewConnectorConfig(pc)
	db := sql.OpenDB(connector)
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
	// A local filesystem timeout must never turn into a database retry loop.
	var de *databaseError
	if !errors.As(err, &de) {
		return false
	}
	var se mssql.Error
	if errors.As(de.cause, &se) {
		switch se.Number {
		case 1205, 1222, 40197, 40501, 40613:
			return true
		default:
			return false
		}
	}
	var ne *net.OpError
	return errors.As(de.cause, &ne) || errors.Is(de.cause, driver.ErrBadConn) ||
		errors.Is(de.cause, io.EOF) || errors.Is(de.cause, io.ErrUnexpectedEOF) ||
		errors.Is(de.cause, context.DeadlineExceeded)
}

func rollback(tx *sql.Tx, result *error) {
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		*result = errors.Join(*result, dbError(err))
	}
}
