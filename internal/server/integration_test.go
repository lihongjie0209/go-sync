//go:build integration

package server

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go-sync/internal/event"
	"go-sync/internal/serverconfig"
)

func TestTargetSnapshotTransactionKeylessAndFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	image := os.Getenv("GO_SYNC_TEST_TARGET_PG_IMAGE")
	if image == "" {
		image = "postgres:15-alpine"
	}
	container, err := testcontainers.Run(ctx, image,
		testcontainers.WithEnv(map[string]string{"POSTGRES_PASSWORD": "secret", "POSTGRES_DB": "sync_test"}),
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(90*time.Second)),
	)
	testcontainers.CleanupContainer(t, container)
	if err != nil {
		t.Fatal(err)
	}
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	dsn := (&url.URL{Scheme: "postgres", User: url.UserPassword("postgres", "secret"), Host: net.JoinHostPort(host, port.Port()), Path: "sync_test", RawQuery: "sslmode=disable"}).String()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `CREATE TABLE public.documents(id bigint PRIMARY KEY,path text,content bytea,body text);
        CREATE TABLE public.bag(n integer,body text)`); err != nil {
		t.Fatal(err)
	}
	cfg := serverconfig.Syncer{ID: "client", Token: "token", AutoAddColumns: true, Postgres: serverconfig.Postgres{DSN: dsn, TargetSchema: "public", MetadataSchema: "go_sync_meta"},
		Tables:      []serverconfig.Table{{SourceSchema: "src", Name: "documents"}, {SourceSchema: "src", Name: "bag"}},
		FileColumns: []serverconfig.FileColumn{{SourceSchema: "src", Table: "documents", SourceColumn: "path", ContentColumn: "content"}},
		FileStorage: serverconfig.FileStorage{Backend: "directory", Directory: t.TempDir(), Events: []string{"create", "update"}, MaxFileBytes: 1 << 20}}
	target, err := openTarget(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	path, encoded, body := "a.txt", base64.StdEncoding.EncodeToString([]byte("file body")), "first"
	messages := []event.Message{
		{Kind: "snapshot_begin", Tables: []event.TableRef{{Schema: "src", Name: "documents"}, {Schema: "src", Name: "bag"}}},
		{Kind: "schema", Schema: json.RawMessage(`{"schema":"src","name":"documents"}`)},
		{Kind: "snapshot_rows", Rows: []event.Row{
			{Schema: "src", Table: "documents", Operation: "read", Columns: []event.Column{{Name: "id", Value: ptr("1")}, {Name: "path", Value: &encoded, Encoding: "base64", SourceValue: &path}, {Name: "body", Value: &body}}},
			{Schema: "src", Table: "bag", Operation: "read", Identity: "full_row", Columns: []event.Column{{Name: "n", Value: ptr("1")}, {Name: "body", Value: ptr("same")}}},
			{Schema: "src", Table: "bag", Operation: "read", Identity: "full_row", Columns: []event.Column{{Name: "n", Value: ptr("1")}, {Name: "body", Value: ptr("same")}}},
		}},
		{Kind: "snapshot_end"},
	}
	for i := range messages {
		storeMessage(t, ctx, target, &messages[i], uint64(i+1), "g")
	}
	var gotPath, gotBody string
	var content []byte
	if err := conn.QueryRow(ctx, "SELECT path,content,body FROM documents WHERE id=1").Scan(&gotPath, &content, &gotBody); err != nil {
		t.Fatal(err)
	}
	if gotPath != path || string(content) != "file body" || gotBody != body {
		t.Fatalf("bad file mapping: %q %q %q", gotPath, content, gotBody)
	}
	var count int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM bag").Scan(&count); err != nil || count != 2 {
		t.Fatalf("keyless snapshot: %d %v", count, err)
	}
	transaction := "tx-1"
	row := event.Row{Schema: "src", Table: "bag", Identity: "full_row", Operation: "delete", Key: []event.Column{{Name: "n", Value: ptr("1")}, {Name: "body", Value: ptr("same")}}}
	chunk := event.Message{Kind: "transaction_rows", Transaction: transaction, Rows: []event.Row{row}}
	storeMessage(t, ctx, target, &chunk, 5, "g")
	p, err := target.Progress(ctx)
	if err != nil || p.Received != 5 || p.Applied != 4 {
		t.Fatalf("chunk progress: %+v %v", p, err)
	}
	end := event.Message{Kind: "transaction_end", Transaction: transaction}
	storeMessage(t, ctx, target, &end, 6, "g")
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM bag").Scan(&count); err != nil || count != 1 {
		t.Fatalf("keyless delete: %d %v", count, err)
	}
	p, err = target.Progress(ctx)
	if err != nil || p.Received != 6 || p.Applied != 6 {
		t.Fatalf("commit progress: %+v %v", p, err)
	}

	// A lost ACK causes the collector to redeliver. The durable sequence makes
	// the replay a no-op rather than applying the transaction twice.
	storeMessage(t, ctx, target, &end, 6, "g")
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM bag").Scan(&count); err != nil || count != 1 {
		t.Fatalf("duplicate delivery changed target: %d %v", count, err)
	}
	conflictingEnd := end
	conflictingEnd.Transaction = "different-payload"
	if err := storeMessageError(ctx, target, &conflictingEnd, 6, "g"); err == nil {
		t.Fatal("same sequence with a different payload was accepted")
	}

	// Transaction rows survive a server-process restart and are applied only
	// after the matching transaction_end arrives.
	recoveryChunk := event.Message{Kind: "transaction_rows", Transaction: "tx-restart", Rows: []event.Row{{Schema: "src", Table: "bag", Identity: "full_row", Operation: "insert", Columns: []event.Column{{Name: "n", Value: ptr("2")}, {Name: "body", Value: ptr("restart")}}}}}
	storeMessage(t, ctx, target, &recoveryChunk, 7, "g")
	target.Close()
	target, err = openTarget(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	recoveryEnd := event.Message{Kind: "transaction_end", Transaction: "tx-restart"}
	storeMessage(t, ctx, target, &recoveryEnd, 8, "g")
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM bag").Scan(&count); err != nil || count != 2 {
		t.Fatalf("restart recovery did not apply transaction: %d %v", count, err)
	}

	// A row failure at transaction_end rolls back every row and leaves the
	// applied checkpoint unchanged so that the sequence can be retried.
	failingChunk := event.Message{Kind: "transaction_rows", Transaction: "tx-fail", Rows: []event.Row{
		{Schema: "src", Table: "documents", Operation: "update", Key: []event.Column{{Name: "id", Value: ptr("1")}}, Columns: []event.Column{{Name: "body", Value: ptr("must-roll-back")}}},
		{Schema: "src", Table: "bag", Identity: "full_row", Operation: "delete", Key: []event.Column{{Name: "n", Value: ptr("999")}, {Name: "body", Value: ptr("missing")}}},
	}}
	storeMessage(t, ctx, target, &failingChunk, 9, "g")
	wrongEnd := event.Message{Kind: "transaction_end", Transaction: "wrong-transaction"}
	if err := storeMessageError(ctx, target, &wrongEnd, 10, "g"); err == nil {
		t.Fatal("mismatched transaction_end was accepted")
	}
	failingEnd := event.Message{Kind: "transaction_end", Transaction: "tx-fail"}
	if err := storeMessageError(ctx, target, &failingEnd, 10, "g"); err == nil {
		t.Fatal("transaction with unmatched identity unexpectedly committed")
	}
	if err := conn.QueryRow(ctx, "SELECT body FROM documents WHERE id=1").Scan(&gotBody); err != nil || gotBody != "first" {
		t.Fatalf("failed transaction was not rolled back: %q %v", gotBody, err)
	}
	p, err = target.Progress(ctx)
	if err != nil || p.Received != 9 || p.Applied != 8 {
		t.Fatalf("failed transaction advanced progress: %+v %v", p, err)
	}

	// Starting a new generation safely abandons the failed old transaction.
	rows := make([]event.Row, applyPageSize+25)
	for i := range rows {
		id := fmt.Sprint(1000 + i)
		rows[i] = event.Row{Schema: "src", Table: "documents", Operation: "read", Columns: []event.Column{{Name: "id", Value: &id}, {Name: "body", Value: ptr("recovered-snapshot")}}}
	}
	newSnapshot := []event.Message{
		{Kind: "snapshot_begin", Tables: []event.TableRef{{Schema: "src", Name: "documents"}}},
		{Kind: "snapshot_rows", Rows: rows},
	}
	for i := range newSnapshot {
		storeMessage(t, ctx, target, &newSnapshot[i], uint64(i+1), "g2")
	}
	target.Close()
	target, err = openTarget(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	newSnapshotEnd := event.Message{Kind: "snapshot_end"}
	storeMessage(t, ctx, target, &newSnapshotEnd, 3, "g2")
	if err := conn.QueryRow(ctx, "SELECT body FROM documents WHERE id=1000").Scan(&gotBody); err != nil || gotBody != "recovered-snapshot" {
		t.Fatalf("snapshot restart recovery failed: %q %v", gotBody, err)
	}
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM documents").Scan(&count); err != nil || count != len(rows) {
		t.Fatalf("paginated snapshot row count: %d, want %d: %v", count, len(rows), err)
	}
	if err := storeMessageError(ctx, target, &event.Message{Kind: "transaction_end"}, 5, "g2"); err == nil {
		t.Fatal("non-contiguous sequence was accepted")
	}

	documentsSchema := event.Message{Kind: "schema", SchemaVersion: "schema-v2", Schema: json.RawMessage(`{
        "schema":"src","name":"documents","columns":[
        {"name":"id","type":"bigint","not_null":true,"primary_key":true},
        {"name":"path","type":"text"},{"name":"body","type":"text"},
        {"name":"extra","type":"text"}]}`)}
	storeMessage(t, ctx, target, &documentsSchema, 4, "g2")
	earlyRows := event.Message{Kind: "transaction_rows", SchemaVersion: "schema-v2", Transaction: "too-early"}
	if err := storeMessageError(ctx, target, &earlyRows, 5, "g2"); err == nil {
		t.Fatal("row delivery was accepted before schema_end")
	}
	bagSchema := event.Message{Kind: "schema", SchemaVersion: "schema-v2", Schema: json.RawMessage(`{
        "schema":"src","name":"bag","columns":[
        {"name":"n","type":"integer"},{"name":"body","type":"text"}]}`)}
	storeMessage(t, ctx, target, &bagSchema, 5, "g2")
	schemaEnd := event.Message{Kind: "schema_end", SchemaVersion: "schema-v2"}
	storeMessage(t, ctx, target, &schemaEnd, 6, "g2")
	var dataType string
	if err := conn.QueryRow(ctx, `SELECT data_type FROM information_schema.columns WHERE table_schema='public' AND table_name='documents' AND column_name='extra'`).Scan(&dataType); err != nil || dataType != "text" {
		t.Fatalf("nullable column was not added: %q %v", dataType, err)
	}
	rowWithNewColumn := event.Message{Kind: "transaction_rows", SchemaVersion: "schema-v2", Transaction: "after-schema", Rows: []event.Row{{
		Schema: "src", Table: "documents", Operation: "insert", Columns: []event.Column{{Name: "id", Value: ptr("2000")}, {Name: "extra", Value: ptr("new-value")}},
	}}}
	storeMessage(t, ctx, target, &rowWithNewColumn, 7, "g2")
	afterSchemaEnd := event.Message{Kind: "transaction_end", SchemaVersion: "schema-v2", Transaction: "after-schema"}
	storeMessage(t, ctx, target, &afterSchemaEnd, 8, "g2")
	var extra string
	if err := conn.QueryRow(ctx, "SELECT extra FROM documents WHERE id=2000").Scan(&extra); err != nil || extra != "new-value" {
		t.Fatalf("incremental row did not use refreshed schema: %q %v", extra, err)
	}
	filePayload := []byte("standalone file body")
	fileHash := sha256.Sum256(filePayload)
	fileMeta := &event.FileChange{Path: "nested/report.txt", Operation: "create", Size: int64(len(filePayload)), Mode: 0o640, SHA256: hex.EncodeToString(fileHash[:]), Chunks: 1}
	fileBegin := event.Message{Kind: "file_begin", Transaction: "file-1", SchemaVersion: "schema-v2", File: fileMeta}
	storeMessage(t, ctx, target, &fileBegin, 9, "g2")
	chunkMeta := *fileMeta
	chunkMeta.Data = base64.StdEncoding.EncodeToString(filePayload)
	fileChunk := event.Message{Kind: "file_chunk", Transaction: "file-1", SchemaVersion: "schema-v2", File: &chunkMeta}
	storeMessage(t, ctx, target, &fileChunk, 10, "g2")
	badMeta := *fileMeta
	badHash := sha256.Sum256([]byte("different"))
	badMeta.SHA256 = hex.EncodeToString(badHash[:])
	badEnd := event.Message{Kind: "file_end", Transaction: "file-1", SchemaVersion: "schema-v2", File: &badMeta}
	if err := storeMessageError(ctx, target, &badEnd, 11, "g2"); err == nil {
		t.Fatal("inconsistent file batch was accepted")
	}
	if _, err := os.Stat(filepath.Join(cfg.FileStorage.Directory, "nested", "report.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed file batch became visible: %v", err)
	}
	fileEnd := event.Message{Kind: "file_end", Transaction: "file-1", SchemaVersion: "schema-v2", File: fileMeta}
	storeMessage(t, ctx, target, &fileEnd, 11, "g2")
	storedFile, err := os.ReadFile(filepath.Join(cfg.FileStorage.Directory, "nested", "report.txt"))
	if err != nil || string(storedFile) != string(filePayload) {
		t.Fatalf("standalone file was not committed: %q %v", storedFile, err)
	}
	fileDelete := event.Message{Kind: "file_delete", SchemaVersion: "schema-v2", File: &event.FileChange{Path: "nested/report.txt", Operation: "delete"}}
	storeMessage(t, ctx, target, &fileDelete, 12, "g2")
	if _, err := os.Stat(filepath.Join(cfg.FileStorage.Directory, "nested", "report.txt")); err != nil {
		t.Fatalf("server delete filter did not retain file: %v", err)
	}

	unsafeSchema := event.Message{Kind: "schema", SchemaVersion: "schema-v3", Schema: json.RawMessage(`{
        "schema":"src","name":"documents","columns":[
        {"name":"id","type":"bigint","not_null":true,"primary_key":true},
        {"name":"required_new","type":"text","not_null":true}]}`)}
	storeMessage(t, ctx, target, &unsafeSchema, 13, "g2")
	unsafeBag := bagSchema
	unsafeBag.SchemaVersion = "schema-v3"
	storeMessage(t, ctx, target, &unsafeBag, 14, "g2")
	unsafeEnd := event.Message{Kind: "schema_end", SchemaVersion: "schema-v3"}
	if err := storeMessageError(ctx, target, &unsafeEnd, 15, "g2"); err == nil {
		t.Fatal("non-null target column was added automatically")
	}
	var requiredExists bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='documents' AND column_name='required_new')`).Scan(&requiredExists); err != nil || requiredExists {
		t.Fatalf("unsafe schema change was not rolled back: %v %v", requiredExists, err)
	}

	other, err := openTarget(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	firstLease, err := target.acquireLease(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.acquireLease(ctx); err == nil {
		firstLease.Close()
		t.Fatal("two server instances acquired the same syncer lease")
	}
	firstLease.Close()
	secondLease, err := other.acquireLease(ctx)
	if err != nil {
		t.Fatalf("lease was not released: %v", err)
	}
	secondLease.Close()
}

func ptr(value string) *string { return &value }

func storeMessage(t *testing.T, ctx context.Context, target *target, message *event.Message, seq uint64, generation string) {
	t.Helper()
	message.Version, message.SourceID, message.Generation, message.Seq = "v2", "client", generation, seq
	message.ID = fmt.Sprintf("client/%s/%d", generation, seq)
	raw, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Store(ctx, *message, raw); err != nil {
		t.Fatalf("store sequence %d: %v", seq, err)
	}
}

func storeMessageError(ctx context.Context, target *target, message *event.Message, seq uint64, generation string) error {
	message.Version, message.SourceID, message.Generation, message.Seq = "v2", "client", generation, seq
	message.ID = fmt.Sprintf("client/%s/%d", generation, seq)
	raw, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return target.Store(ctx, *message, raw)
}
