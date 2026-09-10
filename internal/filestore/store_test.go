package filestore

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go-sync/internal/serverconfig"
)

func TestDirectoryStorePutReplaceDeleteAndRejectTraversal(t *testing.T) {
	root := t.TempDir()
	store, err := Open(serverconfig.FileStorage{Backend: "directory", Directory: root, Prefix: "replica"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	source := filepath.Join(t.TempDir(), "source")
	write := func(value string) {
		t.Helper()
		if err := os.WriteFile(source, []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := store.Put(context.Background(), "nested/file.txt", source, 0o640, time.Unix(100, 0)); err != nil {
			t.Fatal(err)
		}
	}
	write("first")
	write("replacement")
	payload, err := os.ReadFile(filepath.Join(root, "replica", "nested", "file.txt"))
	if err != nil || string(payload) != "replacement" {
		t.Fatalf("stored payload = %q, error = %v", payload, err)
	}
	if err := store.Put(context.Background(), "../escape", source, 0o600, time.Now()); err == nil {
		t.Fatal("path traversal was accepted")
	}
	if err := store.Delete(context.Background(), "nested/file.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "replica", "nested", "file.txt")); !os.IsNotExist(err) {
		t.Fatalf("deleted file still exists: %v", err)
	}
}
