//go:build integration

package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
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
	cfg := serverconfig.Syncer{ID: "client", Token: "token", Postgres: serverconfig.Postgres{DSN: dsn, TargetSchema: "public", MetadataSchema: "go_sync_meta"},
		Tables:      []serverconfig.Table{{SourceSchema: "src", Name: "documents"}, {SourceSchema: "src", Name: "bag"}},
		FileColumns: []serverconfig.FileColumn{{SourceSchema: "src", Table: "documents", SourceColumn: "path", ContentColumn: "content"}}}
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
	newSnapshot := []event.Message{
		{Kind: "snapshot_begin", Tables: []event.TableRef{{Schema: "src", Name: "documents"}}},
		{Kind: "snapshot_rows", Rows: []event.Row{{Schema: "src", Table: "documents", Operation: "read", Columns: []event.Column{{Name: "id", Value: ptr("2")}, {Name: "body", Value: ptr("recovered-snapshot")}}}}},
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
	if err := conn.QueryRow(ctx, "SELECT body FROM documents WHERE id=2").Scan(&gotBody); err != nil || gotBody != "recovered-snapshot" {
		t.Fatalf("snapshot restart recovery failed: %q %v", gotBody, err)
	}
	if err := storeMessageError(ctx, target, &event.Message{Kind: "transaction_end"}, 5, "g2"); err == nil {
		t.Fatal("non-contiguous sequence was accepted")
	}
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
