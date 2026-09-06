package app

import (
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
)

func safeError(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return fmt.Sprintf("postgres error (sqlstate %s); inspect database logs", pe.Code)
	}
	var ce *pgconn.ConnectError
	if errors.As(err, &ce) {
		return "postgres connection failed; verify connectivity and credentials"
	}
	return err.Error()
}
