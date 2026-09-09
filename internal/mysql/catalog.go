package mysql

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/client"
	"go-sync/internal/config"
)

type Column struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	ColumnType string `json:"column_type"`
	Nullable   bool   `json:"nullable"`
	PrimaryKey bool   `json:"primary_key"`
}

type Table struct {
	Schema      string   `json:"schema"`
	Name        string   `json:"name"`
	Engine      string   `json:"engine"`
	RowIdentity string   `json:"row_identity"`
	Columns     []Column `json:"columns"`
}

type Inspection struct {
	Version        string  `json:"version"`
	ServerUUID     string  `json:"server_uuid"`
	Database       string  `json:"database"`
	LogBin         string  `json:"log_bin"`
	BinlogFormat   string  `json:"binlog_format"`
	BinlogRowImage string  `json:"binlog_row_image"`
	Tables         []Table `json:"tables"`
}

func Inspect(ctx context.Context, cfg config.Config) (Inspection, error) {
	conn, _, err := connect(ctx, cfg)
	if err != nil {
		return Inspection{}, err
	}
	defer conn.Close()
	return inspect(conn, cfg)
}

func inspect(conn *client.Conn, cfg config.Config) (Inspection, error) {
	r, err := conn.Execute("SELECT VERSION(), @@server_uuid, DATABASE(), @@log_bin, @@binlog_format, @@binlog_row_image")
	if err != nil {
		return Inspection{}, fmt.Errorf("mysql inspect settings: %w", err)
	}
	get := func(i int) string { value, _ := r.GetString(0, i); return value }
	info := Inspection{Version: get(0), ServerUUID: get(1), Database: get(2), LogBin: get(3), BinlogFormat: get(4), BinlogRowImage: get(5)}
	if !strings.HasPrefix(info.Version, "5.6.") && !strings.HasPrefix(info.Version, "5.7.") {
		return info, fmt.Errorf("mysql version %s is unsupported; expected 5.6.x or 5.7.x", info.Version)
	}
	if info.LogBin != "1" || !strings.EqualFold(info.BinlogFormat, "ROW") || !strings.EqualFold(info.BinlogRowImage, "FULL") {
		return info, errors.New("mysql requires log_bin=ON, binlog_format=ROW and binlog_row_image=FULL")
	}
	for _, ref := range cfg.Tables {
		t, err := inspectTable(conn, ref)
		if err != nil {
			return info, err
		}
		info.Tables = append(info.Tables, t)
	}
	return info, nil
}

func inspectTable(conn *client.Conn, ref config.Table) (Table, error) {
	r, err := conn.Execute("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", ref.Schema, ref.Name)
	if err != nil || r.RowNumber() != 1 {
		return Table{}, fmt.Errorf("mysql inspect table %s: table not found or inaccessible", ref.String())
	}
	engine, _ := r.GetString(0, 0)
	if !strings.EqualFold(engine, "InnoDB") {
		return Table{}, fmt.Errorf("mysql table %s must use InnoDB", ref.String())
	}
	r, err = conn.Execute("SELECT COLUMN_NAME,DATA_TYPE,COLUMN_TYPE,IS_NULLABLE,COLUMN_KEY FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION", ref.Schema, ref.Name)
	if err != nil {
		return Table{}, fmt.Errorf("mysql inspect columns %s: %w", ref.String(), err)
	}
	t := Table{Schema: ref.Schema, Name: ref.Name, Engine: engine}
	for i := 0; i < r.RowNumber(); i++ {
		name, _ := r.GetString(i, 0)
		typ, _ := r.GetString(i, 1)
		columnType, _ := r.GetString(i, 2)
		nullable, _ := r.GetString(i, 3)
		key, _ := r.GetString(i, 4)
		t.Columns = append(t.Columns, Column{Name: name, Type: typ, ColumnType: columnType, Nullable: nullable == "YES", PrimaryKey: key == "PRI"})
	}
	if len(t.Columns) == 0 {
		return Table{}, fmt.Errorf("mysql table %s has no visible columns", ref.String())
	}
	for _, c := range t.Columns {
		if c.PrimaryKey {
			t.RowIdentity = "primary_key"
			break
		}
	}
	if t.RowIdentity == "" {
		t.RowIdentity = "full_row"
	}
	return t, nil
}

func schemaHash(tables []Table) string {
	b, _ := json.Marshal(tables)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
