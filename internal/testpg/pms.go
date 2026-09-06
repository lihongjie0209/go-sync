//go:build integration

package testpg

import (
	"context"
	_ "embed"
	"regexp"

	"github.com/jackc/pgx/v5"
	"go-sync/internal/config"
)

// PMS contains only allowlisted DDL extracted from the supplied PostgreSQL 9.5.2
// schema. Source rows, role passwords, owners, grants and sequence values are absent.
//
//go:embed testdata/pms_schema.sql
var PMS string

func LoadPMS(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, PMS)
	return err
}

func PMSTables() []config.Table {
	var tables []config.Table
	for _, match := range regexp.MustCompile(`(?m)^CREATE TABLE ([a-z_0-9]+) \(`).FindAllStringSubmatch(PMS, -1) {
		tables = append(tables, config.Table{Schema: "public", Name: match[1]})
	}
	return tables
}
