package sqlserverlegacy

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"go-sync/internal/config"
)

func triggerName(c config.Config, objectID int) string {
	return c.SQLServerLegacy.Prefix + "_tr_" + strconv.Itoa(objectID)
}

func install(ctx context.Context, db *sql.DB, c config.Config, tables []Table) (result error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return dbError(err)
	}
	defer rollback(tx, &result)
	control, events, values := names(c)
	manifest := manifestName(c)
	var controlID, eventsID, valuesID, manifestID sql.NullInt64
	if err := tx.QueryRowContext(ctx, "SELECT OBJECT_ID(@p1,'U'),OBJECT_ID(@p2,'U'),OBJECT_ID(@p3,'U'),OBJECT_ID(@p4,'U')", control, events, values, manifest).
		Scan(&controlID, &eventsID, &valuesID, &manifestID); err != nil {
		return dbError(err)
	}
	allMissing := !controlID.Valid && !eventsID.Valid && !valuesID.Valid && !manifestID.Valid
	allPresent := controlID.Valid && eventsID.Valid && valuesID.Valid && manifestID.Valid
	if !allMissing && !allPresent {
		return errors.New("partial sqlserver legacy installation found; manual repair required")
	}
	if allMissing {
		statements := []string{
			"CREATE TABLE " + control + ` (singleton int NOT NULL PRIMARY KEY CHECK(singleton=1),product_id varchar(32) NOT NULL,source_guid uniqueidentifier NOT NULL,schema_version int NOT NULL,scope_hash char(64) NOT NULL)`,
			"INSERT INTO " + control + fmt.Sprintf(" (singleton,product_id,source_guid,schema_version,scope_hash) VALUES (1,'go-sync/sqlserver-legacy',NEWID(),%d,'%s')", schemaVersion, schemaHash(tables)),
			"CREATE TABLE " + events + ` (change_id bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,statement_id uniqueidentifier NOT NULL,object_id int NOT NULL,operation char(1) NOT NULL,created_at datetime NOT NULL DEFAULT GETDATE())`,
			"CREATE INDEX " + quote(c.SQLServerLegacy.Prefix+"_events_statement") + " ON " + events + " (statement_id,change_id)",
			"CREATE TABLE " + values + ` (change_id bigint NOT NULL,ordinal smallint NOT NULL,is_null bit NOT NULL,value_text varchar(8000) NULL,value_unicode nvarchar(4000) NULL,value_binary varbinary(8000) NULL,PRIMARY KEY(change_id,ordinal))`,
			"CREATE TABLE " + manifest + ` (object_id int NOT NULL PRIMARY KEY,trigger_owner varchar(64) NOT NULL,trigger_name varchar(128) NOT NULL,schema_hash char(64) NOT NULL)`,
		}
		for _, statement := range statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return dbError(err)
			}
		}
	} else {
		var version int
		var product string
		var installedHash string
		if err := tx.QueryRowContext(ctx, "SELECT product_id,schema_version,scope_hash FROM "+control+" WHERE singleton=1").Scan(&product, &version, &installedHash); err != nil {
			return dbError(err)
		}
		if product != "go-sync/sqlserver-legacy" {
			return errors.New("sqlserver legacy object names collide with an unmanaged installation")
		}
		if version != schemaVersion {
			return errors.New("sqlserver legacy installation has an unsupported schema version")
		}
		if installedHash != schemaHash(tables) {
			var pending int
			if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+events).Scan(&pending); err != nil {
				return dbError(err)
			}
			if pending != 0 {
				return errors.New("sqlserver legacy schema changed while outbox events are pending")
			}
			if err := removeManagedTriggers(ctx, tx, manifest); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, "UPDATE "+control+" SET scope_hash=@p1 WHERE singleton=1", schemaHash(tables)); err != nil {
				return dbError(err)
			}
		}
	}
	for _, table := range tables {
		if err := installTrigger(ctx, tx, c, table); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "IF NOT EXISTS(SELECT 1 FROM "+manifest+" WHERE object_id=@p1) INSERT INTO "+manifest+
			" (object_id,trigger_owner,trigger_name,schema_hash) VALUES (@p1,@p2,@p3,@p4)", table.ObjectID, table.Schema, table.TriggerName, schemaHash([]Table{table})); err != nil {
			return dbError(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return dbError(err)
	}
	return nil
}

func removeManagedTriggers(ctx context.Context, tx *sql.Tx, manifest string) (result error) {
	rows, err := tx.QueryContext(ctx, "SELECT trigger_owner,trigger_name FROM "+manifest)
	if err != nil {
		return dbError(err)
	}
	var triggers [][2]string
	for rows.Next() {
		var trigger [2]string
		if err := rows.Scan(&trigger[0], &trigger[1]); err != nil {
			rows.Close()
			return dbError(err)
		}
		triggers = append(triggers, trigger)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return dbError(err)
	}
	if err := rows.Close(); err != nil {
		return dbError(err)
	}
	for _, trigger := range triggers {
		name := quote(trigger[0]) + "." + quote(trigger[1])
		if _, err := tx.ExecContext(ctx, "IF OBJECT_ID(@p1,'TR') IS NOT NULL DROP TRIGGER "+name, name); err != nil {
			return dbError(err)
		}
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM "+manifest); err != nil {
		return dbError(err)
	}
	return nil
}

func installTrigger(ctx context.Context, tx *sql.Tx, c config.Config, table Table) error {
	name := quote(table.Schema) + "." + quote(table.TriggerName)
	var id sql.NullInt64
	if err := tx.QueryRowContext(ctx, "SELECT OBJECT_ID(@p1,'TR')", name).Scan(&id); err != nil {
		return dbError(err)
	}
	if id.Valid {
		var owned int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM syscomments
 WHERE id=@p1 AND text LIKE '%go-sync legacy managed v1%'`, id.Int64).Scan(&owned); err != nil {
			return dbError(err)
		}
		if owned == 0 {
			return errors.New("sqlserver legacy trigger name collides with an unmanaged object")
		}
		return nil
	}
	if _, err := tx.ExecContext(ctx, triggerSQL(c, table)); err != nil {
		return dbError(err)
	}
	return nil
}

func triggerSQL(c config.Config, table Table) string {
	_, events, values := names(c)
	var declarations, fields, variables []string
	for i, col := range table.Columns {
		variable := fmt.Sprintf("@v%d", i+1)
		declarations = append(declarations, variable+" "+variableType(col))
		fields = append(fields, quote(col.Name))
		variables = append(variables, variable)
	}
	body := func(cursor, source, operation string) string {
		var inserts []string
		for i, col := range table.Columns {
			v := variables[i]
			text, unicode, binary := valueExpressions(col, v)
			inserts = append(inserts, "INSERT INTO "+values+" (change_id,ordinal,is_null,value_text,value_unicode,value_binary) VALUES (@id,"+
				strconv.Itoa(i+1)+",CASE WHEN "+v+" IS NULL THEN 1 ELSE 0 END,"+text+","+unicode+","+binary+")")
		}
		return "DECLARE " + cursor + " CURSOR LOCAL STATIC READ_ONLY FOR SELECT " + strings.Join(fields, ",") + " FROM " + source + "\n" +
			"OPEN " + cursor + "\nFETCH NEXT FROM " + cursor + " INTO " + strings.Join(variables, ",") + "\nWHILE @@FETCH_STATUS=0 BEGIN\n" +
			"INSERT INTO " + events + " (statement_id,object_id,operation) VALUES (@statement," + strconv.Itoa(table.ObjectID) + ",'" + operation + "')\n" +
			"SET @id=CONVERT(bigint,SCOPE_IDENTITY())\n" + strings.Join(inserts, "\n") + "\nFETCH NEXT FROM " + cursor + " INTO " + strings.Join(variables, ",") +
			"\nEND\nCLOSE " + cursor + "\nDEALLOCATE " + cursor + "\n"
	}
	trigger := quote(table.Schema) + "." + quote(table.TriggerName)
	return "CREATE TRIGGER " + trigger + " ON " + table.sqlName() + " FOR INSERT,UPDATE,DELETE AS\n" +
		"/* go-sync legacy managed v1 */\nSET NOCOUNT ON\nDECLARE @statement uniqueidentifier,@id bigint\nDECLARE " + strings.Join(declarations, ",") +
		"\nSET @statement=NEWID()\nIF EXISTS(SELECT 1 FROM deleted) BEGIN\n" + body("go_sync_deleted", "deleted", "D") + "END\n" +
		"IF EXISTS(SELECT 1 FROM inserted) BEGIN\n" + body("go_sync_inserted", "inserted", "I") + "END"
}

func variableType(col Column) string {
	switch col.BaseType {
	case "timestamp":
		return "binary(8)"
	default:
		return col.Type
	}
}

func valueExpressions(col Column, variable string) (text, unicode, binary string) {
	text, unicode, binary = "NULL", "NULL", "NULL"
	switch col.BaseType {
	case "binary", "varbinary", "timestamp", "float", "real":
		binary = "CONVERT(varbinary(8000)," + variable + ")"
	case "nvarchar", "nchar":
		unicode = "CONVERT(nvarchar(4000)," + variable + ")"
	case "datetime", "smalldatetime":
		text = "CONVERT(varchar(30)," + variable + ",126)"
	case "money", "smallmoney":
		text = "CONVERT(varchar(100)," + variable + ",2)"
	default:
		text = "CONVERT(varchar(8000)," + variable + ")"
	}
	return text, unicode, binary
}
